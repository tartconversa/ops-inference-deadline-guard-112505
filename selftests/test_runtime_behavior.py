"""运行时需求的端到端自测（不放入 tests/，避免与评分用例重名）。

运行：
    python -m unittest discover -s selftests -v
"""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from rehearsal import (  # noqa: E402
    RehearsalEngine,
    RehearsalConfig,
    run_rehearsal,
    ceil_half,
    variant_cost,
)
from rehearsal.model import RequestStatus, AttemptStatus, attempt_key  # noqa: E402

ROOT = _ROOT
MATERIALS = ROOT / "materials"


class CostModelTest(unittest.TestCase):
    def test_ceil_half(self):
        self.assertEqual(ceil_half(43), 22)
        self.assertEqual(ceil_half(57), 29)
        self.assertEqual(ceil_half(36), 18)
        self.assertEqual(ceil_half(22), 11)
        self.assertEqual(variant_cost(45, "full"), 45)
        self.assertEqual(variant_cost(45, "small"), 23)

    def test_ceiling_sums_match_crafted_boundaries(self):
        # a1: r11 上界57 + c21 上界50 = 107 <= work_capacity 110
        self.assertLessEqual(57 + 50, 110)
        # b1: c23#2 small18 + r11#2 small29 = 47 <= 56
        self.assertLessEqual(18 + 29, 56)
        # r13 small 上界17 <= b2 cap24
        self.assertLessEqual(17, 24)


class MaterialStoryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.result = run_rehearsal(MATERIALS, write=False)
        cls.e = cls.result.engine

    def test_no_capacity_invariant_violations(self):
        self.assertEqual(self.e.check_invariants(), [])

    def test_initial_placements_match_callbacks(self):
        e = self.e
        self.assertEqual(e.get_attempt("r11", 1)["worker"], "a1")
        self.assertEqual(e.get_attempt("r12", 1)["worker"], "a2")
        self.assertEqual(e.get_attempt("c21", 1)["worker"], "a1")
        self.assertEqual(e.get_attempt("c23", 1)["worker"], "a2")
        self.assertEqual(e.get_attempt("c23", 2)["worker"], "b1")

    def test_c23_queued_until_real_release_then_admitted_at_35(self):
        admits = [t for t in self.e.trace
                  if t.get("request_id") == "c23" and t["kind"] == "admitted"
                  and t.get("attempt_no") == 1]
        self.assertEqual(len(admits), 1)
        self.assertEqual(admits[0]["at_ms"], 35)
        waits = [t for t in self.e.trace
                 if t.get("request_id") == "c23" and t["kind"] == "waiting_for_primary_pool"]
        self.assertTrue(waits)
        self.assertEqual(waits[0]["at_ms"], 33)

    def test_c22_queued_then_rejected_deadline_infeasible(self):
        self.assertEqual(self.e.status("c22"), RequestStatus.REJECTED.value)
        kinds = [(t["at_ms"], t["kind"]) for t in self.e.trace
                 if t.get("request_id") == "c22"]
        self.assertIn((15, "queued"), kinds)
        rejected = [t for t in self.e.trace
                    if t.get("request_id") == "c22" and t["kind"] == "rejected"]
        self.assertTrue(rejected)
        self.assertEqual(rejected[0]["latest_start_ms"], 20)

    def test_cancel_does_not_free_capacity_until_released(self):
        cfg = RehearsalConfig.from_dict({"fleet": [
            {"worker": "w1", "pool": "p", "slots": 1, "work_capacity": 100,
             "variant": "full", "p95_ms": 10, "return_ms": 5}]})
        e = RehearsalEngine(cfg)
        row = {"at_ms": 0, "op": "submit", "request_id": "r", "tenant": "t",
               "model_family": "text", "deadline_ms": 1000, "units": 10,
               "allow_small": False, "units_ceiling": 20}
        e.submit_request(row)
        self.assertEqual(e.capacity.worker_slots_used("w1"), 1)
        e.cancel_request("r", at_ms=4)
        # 取消后、released 实证前，容量仍被占用
        self.assertEqual(e.capacity.worker_slots_used("w1"), 1)
        self.assertIn("r#1", e.capacity.charges)
        e.handle_callback({"at_ms": 9, "occurred_ms": 8, "worker": "w1",
                           "request_id": "r", "attempt_no": 1, "event": "released"})
        self.assertEqual(e.capacity.worker_slots_used("w1"), 0)
        self.assertNotIn("r#1", e.capacity.charges)

    def test_usage_corrects_actual_but_bound_kept(self):
        a = self.e.get_attempt("r11", 1)
        self.assertEqual(a["observed_units"], 54)
        self.assertEqual(a["bound_units"], 57)
        self.assertEqual(a["status"], AttemptStatus.RELEASED.value)

    def test_late_first_chunk_locks_c21_and_never_dispatches_backup(self):
        rep = self.e.request_report("c21")
        self.assertEqual(rep["delivered_attempt"], 1)
        self.assertEqual(rep["status"], RequestStatus.DELIVERED.value)
        # prepared #2 exists as a plan but was never dispatched to a replica
        a2 = self.e.get_attempt("c21", 2)
        self.assertIsNotNone(a2)
        self.assertNotEqual(a2["status"], AttemptStatus.DISPATCHED.value)
        dispatched = [t for t in self.e.actions
                      if t.get("request_id") == "c21" and t["action"] == "dispatch_attempt"
                      and t.get("attempt_no") == 2]
        self.assertEqual(dispatched, [])
        # egress query API
        self.assertIsNotNone(self.e.query_first_chunk("c21", 1))
        self.assertFalse(self.e.stopped_receiving("c21", 1))

    def test_c23_backup_launched_only_after_empty_close(self):
        launches = [t for t in self.e.actions
                    if t.get("request_id") == "c23"
                    and t["action"] == "dispatch_prepared_backup"]
        self.assertEqual(len(launches), 1)
        self.assertEqual(launches[0]["at_ms"], 42)
        self.assertTrue(self.e.stopped_receiving("c23", 1))
        self.assertTrue(self.e.egress.is_empty_and_closed("c23", 1))
        self.assertEqual(self.e.status("c23"), RequestStatus.COMPLETED.value)
        self.assertEqual(self.e.request_report("c23")["delivered_attempt"], 2)

    def test_exactly_one_delivery_receipt_per_request(self):
        keys = [k for k in self.e.egress.outbox if k == "result:c21"]
        self.assertEqual(len(keys), 1)
        keys23 = [k for k in self.e.egress.outbox if k == "result:c23"]
        self.assertEqual(len(keys23), 1)

    def test_blast_radius_scoped_to_b1(self):
        down = [t for t in self.e.trace if t["kind"] == "node_down_blast_radius"
                and t["worker"] == "b1"][-1]
        self.assertEqual(down["affected_attempts"], ["r11#2"])
        self.assertIn("b2", down["same_pool_unaffected_workers"])
        # r13 runs on b2 and is not in b1 radius
        self.assertEqual(self.e.get_attempt("r13", 1)["worker"], "b2")

    def test_occupancy_distinguishes_live_and_ended(self):
        occ = self.e.occupancy("pool")
        self.assertIn("r13", occ["requests_with_live_charges"])
        for ended in ("c22", "c23", "r12", "c21"):
            self.assertIn(ended, occ["ended_user_requests"])

    def test_final_live_charges_three(self):
        self.assertEqual(len(self.e.capacity.charges), 3)


class AdmissionSafetyTest(unittest.TestCase):
    def _cfg(self, workers):
        return RehearsalConfig.from_dict({"fleet": workers, "reservations": []})

    def test_ceiling_growth_cannot_overcommit_worker(self):
        cfg = self._cfg([
            {"worker": "w1", "pool": "p", "slots": 4, "work_capacity": 100,
             "variant": "full", "p95_ms": 10, "return_ms": 5},
        ])
        e = RehearsalEngine(cfg)
        # 每个上界 40：最多 2 个（80），第 3 个 120 超限
        for i in range(2):
            e.submit_request({"at_ms": i, "op": "submit", "request_id": f"r{i}",
                              "tenant": "t", "model_family": "text", "deadline_ms": 1000,
                              "units": 10, "allow_small": False, "units_ceiling": 40})
        self.assertEqual(e.status("r0"), "admitted")
        self.assertEqual(e.status("r1"), "admitted")
        e.submit_request({"at_ms": 2, "op": "submit", "request_id": "r2",
                          "tenant": "t", "model_family": "text", "deadline_ms": 1000,
                          "units": 10, "allow_small": False, "units_ceiling": 40})
        self.assertEqual(e.status("r2"), "queued")
        self.assertEqual(e.check_invariants(), [])

    def test_slots_are_enforced_independently_of_units(self):
        cfg = self._cfg([
            {"worker": "w1", "pool": "p", "slots": 1, "work_capacity": 10_000,
             "variant": "full", "p95_ms": 10, "return_ms": 5},
        ])
        e = RehearsalEngine(cfg)
        for rid in ("a", "b"):
            e.submit_request({"at_ms": 0, "op": "submit", "request_id": rid,
                              "tenant": "t", "model_family": "text", "deadline_ms": 1000,
                              "units": 1, "allow_small": False, "units_ceiling": 1})
        self.assertEqual(e.status("a"), "admitted")
        self.assertEqual(e.status("b"), "queued")

    def test_low_cost_only_for_explicit_degradation_and_feasible(self):
        cfg = RehearsalConfig.from_dict({
            "fleet": [
                {"worker": "big", "pool": "main", "slots": 1, "work_capacity": 100,
                 "variant": "full", "p95_ms": 100, "return_ms": 5},
                {"worker": "cheap", "pool": "lowcost", "slots": 10, "work_capacity": 1000,
                 "variant": "small", "p95_ms": 10, "return_ms": 5},
            ],
            "policy": {"downgrade_mode": "immediate"},
        })
        e = RehearsalEngine(cfg)
        # 不接受降级：即使 main 满了也不能去 cheap
        e.submit_request({"at_ms": 0, "op": "submit", "request_id": "x", "tenant": "t",
                          "model_family": "text", "deadline_ms": 10_000, "units": 1,
                          "allow_small": False, "units_ceiling": 1})
        e.submit_request({"at_ms": 0, "op": "submit", "request_id": "y", "tenant": "t",
                          "model_family": "text", "deadline_ms": 10_000, "units": 1,
                          "allow_small": False, "units_ceiling": 1})
        self.assertEqual(e.get_attempt("x", 1)["worker"], "big")
        self.assertIsNone(e.get_attempt("y", 1))
        self.assertEqual(e.status("y"), "queued")

    def test_deadline_infeasible_degradation_rejected(self):
        cfg = RehearsalConfig.from_dict({"fleet": [
            {"worker": "cheap", "pool": "fb", "slots": 10, "work_capacity": 1000,
             "variant": "small", "p95_ms": 50, "return_ms": 5}]})
        e = RehearsalEngine(cfg)
        e.submit_request({"at_ms": 0, "op": "submit", "request_id": "late", "tenant": "t",
                          "model_family": "text", "deadline_ms": 10, "units": 10,
                          "allow_small": True, "units_ceiling": 20})
        self.assertEqual(e.status("late"), "rejected")


class ExactlyOnceAndRestartTest(unittest.TestCase):
    def test_restart_reconcile_and_no_duplicate_result(self):
        with tempfile.TemporaryDirectory() as d:
            run_rehearsal(MATERIALS, out_dir=d)
            restored = RehearsalEngine.restore(d)
            self.assertEqual(len(restored.capacity.charges), 3)
            # 控制程序重启后重复收到/重放 c23 首段不能产生第二个结果
            before = len(restored.egress.deliveries)
            restored.handle_callback({"at_ms": 90, "occurred_ms": 56, "worker": "b1",
                                      "request_id": "c23", "attempt_no": 2,
                                      "event": "first_chunk", "units": None})
            report = restored.reconcile()
            self.assertTrue(report["ok"], report["mismatches"])
            self.assertEqual(restored.request_report("c23")["delivered_attempt"], 2)
            # outbox 幂等：同一 result 键重复 publish 返回 duplicate
            r1 = restored.egress.publish("result:c23", {"request_id": "c23"}, 91)
            self.assertTrue(r1["duplicate"])
            self.assertEqual(len(restored.egress.deliveries), before)

    def test_egress_disconnect_reconnect_flushes_idempotently(self):
        e = RehearsalEngine()
        e.egress.disconnect()
        first = e.egress.publish("result:z", {"request_id": "z"}, 1)
        self.assertFalse(first["delivered"])
        flushed = e.egress.reconnect()
        self.assertEqual(len(flushed), 1)
        again = e.egress.publish("result:z", {"request_id": "z"}, 2)
        self.assertTrue(again["duplicate"])

    def test_cross_attempt_chunk_is_suppressed(self):
        e = RehearsalEngine(RehearsalConfig.from_dict({"fleet": [
            {"worker": "w", "pool": "p", "slots": 4, "work_capacity": 1000,
             "variant": "full", "p95_ms": 10, "return_ms": 5}]}))
        e.submit_request({"at_ms": 0, "op": "submit", "request_id": "r", "tenant": "t",
                          "model_family": "text", "deadline_ms": 1000, "units": 1,
                          "allow_small": True, "units_ceiling": 2})
        # 第一次首段归属 #1
        e.handle_callback({"at_ms": 5, "worker": "w", "request_id": "r",
                           "attempt_no": 1, "event": "first_chunk"})
        # 伪造 #2 晚到首段：必须被压制
        e.handle_callback({"at_ms": 6, "worker": "w", "request_id": "r",
                           "attempt_no": 2, "event": "first_chunk"})
        self.assertEqual(e.request_report("r")["delivered_attempt"], 1)
        suppressed = [t for t in e.trace if t["kind"] == "cross_attempt_delivery_suppressed"]
        self.assertTrue(suppressed)


class ProbeAndReclaimTest(unittest.TestCase):
    def test_duplicate_closed_empty_does_not_spawn_third_attempt(self):
        e = run_rehearsal(MATERIALS, write=False).engine
        before = sorted(k for k in e.attempts if k.startswith("c23#"))
        e.handle_callback({"at_ms": 43, "occurred_ms": 41, "worker": "a2",
                           "request_id": "c23", "attempt_no": 1,
                           "event": "sink_closed_empty"})
        e.handle_callback({"at_ms": 44, "occurred_ms": 41, "worker": "a2",
                           "request_id": "c23", "attempt_no": 1,
                           "event": "sink_closed_empty"})
        after = sorted(k for k in e.attempts if k.startswith("c23#"))
        self.assertEqual(before, after)
        self.assertEqual(after, ["c23#1", "c23#2"])

    def test_reclaim_deadline_marked_met_on_timely_release(self):
        cfg = RehearsalConfig.from_dict({
            "fleet": [{"worker": "w", "pool": "main", "slots": 2, "work_capacity": 200,
                       "variant": "full", "p95_ms": 10, "return_ms": 5}],
            "reservations": [
                {"tenant": "owner", "pool": "main", "model_family": "text",
                 "start_ms": 0, "end_ms": 100, "units": 80},
            ],
        })
        e = RehearsalEngine(cfg)
        # borrower uses owner's idle reservation
        e.submit_request({"at_ms": 0, "op": "submit", "request_id": "b", "tenant": "borrower",
                          "model_family": "text", "deadline_ms": 1000, "units": 50,
                          "allow_small": False, "units_ceiling": 70})
        rec = e.reclaim_reservation("owner", pool="main", units=70, at_ms=2)
        self.assertEqual(rec["reclaim_deadline_ms"], 7)
        loan = e.ledger.loans[0]
        e.handle_callback({"at_ms": 6, "worker": "w", "request_id": "b",
                           "attempt_no": 1, "event": "released"})
        self.assertIsNotNone(loan.returned_at_ms)
        self.assertTrue(loan.met_deadline)

    def test_probe_occupies_and_release_frees_capacity(self):
        result = run_rehearsal(MATERIALS, write=False)
        e = result.engine
        before = e.capacity.worker_observed_used("a2")
        probe = e.submit_probe("a2", at_ms=84, units=4)
        self.assertTrue(probe["admitted"])
        self.assertEqual(e.capacity.worker_bound_used("a2"), before + 4)
        key = probe["attempt_key"]
        self.assertTrue(e.release_probe(key, at_ms=90))
        self.assertEqual(e.capacity.worker_bound_used("a2"), before)

    def test_reclaim_records_deadline_and_waits_for_release(self):
        e = RehearsalEngine(RehearsalConfig.from_materials(MATERIALS))
        e.submit_request({"at_ms": 0, "op": "submit", "request_id": "r11",
                          "tenant": "review", "model_family": "text", "deadline_ms": 90,
                          "units": 43, "allow_small": True, "units_ceiling": 57})
        self.assertTrue(any(l.lender == "caption" for l in e.ledger.loans))
        rec = e.reclaim_reservation("caption", pool="main", units=49, at_ms=10)
        self.assertIsNotNone(rec["reclaim_deadline_ms"])
        self.assertEqual(rec["reclaim_deadline_ms"], 10 + 26)
        self.assertTrue(rec["loans"])
        # 催还不立即移除容量：仍等 released 实证
        self.assertIn("r11#1", e.capacity.charges)


class ReplayConfigTest(unittest.TestCase):
    def test_new_config_replay_changes_outcome_deterministically(self):
        base = run_rehearsal(MATERIALS, write=False)
        immediate_cfg = RehearsalConfig.from_materials(
            MATERIALS, policy={"downgrade_mode": "immediate"})
        fast = run_rehearsal(MATERIALS, config=immediate_cfg, write=False)
        # immediate 模式下 c23 在 33 就直接去备用池，与 deadline 模式不同
        self.assertEqual(fast.engine.get_attempt("c23", 1)["worker"], "b1")
        self.assertEqual(base.engine.get_attempt("c23", 1)["worker"], "a2")

    def test_overlay_low_cost_worker(self):
        cfg = RehearsalConfig.from_materials(MATERIALS)
        cfg2 = cfg.overlay({"fleet": [
            {"worker": "b3", "pool": "fallback", "slots": 4, "work_capacity": 80,
             "variant": "small", "p95_ms": 12, "return_ms": 18}]})
        self.assertIn("b3", cfg2.workers)
        self.assertIn("a1", cfg2.workers)  # merge 保留原副本


class StreamIngestTest(unittest.TestCase):
    def test_ingest_stream_of_events(self):
        e = RehearsalEngine(RehearsalConfig.from_materials(MATERIALS))
        out = e.ingest({"at_ms": 0, "op": "submit", "request_id": "z1", "tenant": "review",
                        "model_family": "text", "deadline_ms": 90, "units": 4,
                        "allow_small": True, "units_ceiling": 8})
        self.assertTrue(out)
        self.assertIn(e.status("z1"), ("admitted", "queued"))


if __name__ == "__main__":
    unittest.main()
