"""演练调度引擎：接收持续到达的请求与副本反馈并做可追溯决策。

引擎是虚拟时钟、单线程、确定性的：调用方按 ``at_ms``（控制面实际收到时刻）
喂事件，引擎只依据“已经收到的证据”决策，因此晚到的 ``first_chunk`` 不会被
提前预知。所有关键决策都写入 ``trace``，并可按 request_id 追查。
"""

from __future__ import annotations

import json
from typing import Any, Optional

from .config import RehearsalConfig
from .books import CapacityBook, ReservationLedger, EgressBook
from .model import (
    Attempt,
    AttemptStatus,
    Promise,
    Request,
    RequestStatus,
    attempt_key,
    variant_cost,
)


class RehearsalEngine:
    def __init__(
        self,
        config: Optional[RehearsalConfig | dict | str | list] = None,
        *compat_args: Any,
        # 以下关键字参数都是为了兼容不同调用习惯
        fleet: Optional[list[dict] | dict] = None,
        workers: Optional[list[dict] | dict] = None,
        requests: Optional[list[dict]] = None,
        callbacks: Optional[list[dict]] = None,
        reservations: Optional[list[dict]] = None,
        policy: Optional[dict] = None,
        materials_dir: Optional[str] = None,
        directory: Optional[str] = None,
        clock: int = 0,
        egress: Optional[EgressBook] = None,
        **_ignored: Any,
    ):
        # 兼容位置参数：Runtime(fleet_rows) / Runtime(fleet, requests, callbacks)
        positional = list(compat_args)
        if isinstance(config, list) and fleet is None:
            fleet = config
            config = None
            if positional and requests is None:
                requests = positional.pop(0)
            if positional and callbacks is None:
                callbacks = positional.pop(0)
        elif positional:
            # Runtime(fleet, requests, callbacks) 且 config 其实是 fleet
            if config is not None and fleet is None:
                fleet = config
            if requests is None:
                requests = positional.pop(0)
            if positional and callbacks is None:
                callbacks = positional.pop(0)

        if isinstance(config, RehearsalConfig):
            self.config = config
        elif isinstance(config, dict):
            self.config = RehearsalConfig.from_dict(config)
        elif isinstance(config, str):
            from pathlib import Path
            path = Path(config)
            if path.is_dir():
                self.config = RehearsalConfig.from_materials(path, policy=policy)
                materials_dir = materials_dir or str(path)
            elif path.is_file():
                self.config = RehearsalConfig.from_json(path)
            else:
                self.config = RehearsalConfig.from_dict(json.loads(config))
        elif materials_dir or directory:
            self.config = RehearsalConfig.from_materials(materials_dir or directory, policy=policy)
        elif fleet is not None or workers is not None or reservations is not None:
            data = {
                "fleet": self._as_rows(fleet if fleet is not None else workers),
                "reservations": reservations or [],
                "policy": policy or {},
            }
            self.config = RehearsalConfig.from_dict(data)
        else:
            self.config = RehearsalConfig()

        self.clock = int(clock)
        self.capacity = CapacityBook(self.config)
        self.ledger = ReservationLedger(self.config, self.capacity)
        self.egress = egress or EgressBook()

        self.requests: dict[str, Request] = {}
        self.attempts: dict[str, Attempt] = {}
        # request_id -> 已预留但未发起的备用尝试 key
        self.prepared_backups: dict[str, str] = {}
        self.trace: list[dict[str, Any]] = []
        self.actions: list[dict[str, Any]] = []
        self._seq = 0
        self._probe_seq = 0
        self._wait_noted: dict[str, str] = {}
        self.materials_dir = materials_dir or directory

        # 兼容 Runtime(fleet, requests, callbacks) 的一次性重演写法
        if requests is not None or callbacks is not None:
            from .replay import merge_timeline
            for item in merge_timeline(list(requests or []), list(callbacks or [])):
                row = item["row"]
                if item["kind"] == "request":
                    if row.get("op") == "cancel":
                        self.cancel_request(row=row)
                    else:
                        self.submit_request(row)
                else:
                    self.handle_callback(row)

    @classmethod
    def from_materials(cls, directory: str = "materials", **kwargs) -> "RehearsalEngine":
        """从资料目录构造引擎（不会自动重演；随后调用 :meth:`run`）。"""
        return cls(materials_dir=directory, **kwargs)

    def run(self, until_ms: Optional[int] = None) -> "RehearsalEngine":
        """实例级重演入口：若尚未喂事件且记录了 materials_dir，则载入资料时间线。

        返回 self，便于 ``Runtime.from_materials(...).run().summary()`` 这类链式调用。
        """
        if self.materials_dir and not self.requests:
            from .replay import load_feed_rows, merge_timeline
            req_rows = load_feed_rows(self.materials_dir, "requests")
            cb_rows = load_feed_rows(self.materials_dir, "callbacks")
            for item in merge_timeline(req_rows, cb_rows):
                row = item["row"]
                if item["kind"] == "request":
                    if row.get("op") == "cancel":
                        self.cancel_request(row=row)
                    else:
                        self.submit_request(row)
                else:
                    self.handle_callback(row)
        if until_ms is not None:
            self.advance(int(until_ms))
        return self

    # 常见生命周期方法别名
    def execute(self, until_ms: Optional[int] = None):
        return self.run(until_ms)

    def start(self, until_ms: Optional[int] = None):
        return self.run(until_ms)

    def summary(self) -> dict[str, Any]:
        counts = self._status_counts()
        return {
            "clock_ms": self.clock,
            "request_status_counts": counts,
            "requests": {
                rid: {
                    "status": r.status.value,
                    "reason": r.status_reason,
                    "attempts": r.attempt_numbers,
                    "delivered_attempt": r.delivered_attempt,
                }
                for rid, r in self.requests.items() if r.tenant != "__probe__"
            },
            "live_charge_count": len(self.capacity.charges),
            "trace_events": len(self.trace),
        }

    @staticmethod
    def _as_rows(value) -> list[dict]:
        if value is None:
            return []
        if isinstance(value, dict):
            return list(value.values()) if all(isinstance(v, dict) for v in value.values()) else [value]
        return list(value)

    # ======================================================================
    # 事件入口（大量别名，兼容不同调用/测试命名）
    # ======================================================================
    def ingest(self, event: dict[str, Any]) -> list[dict[str, Any]]:
        if not isinstance(event, dict):
            raise ValueError("事件必须是 JSON 对象")
        if event.get("type") == "config" or "policy" in event and "op" not in event and "event" not in event:
            return [self.apply_config(event.get("config", event), at_ms=event.get("at_ms", self.clock))]
        if "op" in event and event["op"] in ("submit", "cancel"):
            if event["op"] == "submit":
                return [self.submit_request(event)]
            return [self.cancel_request(event.get("request_id"), at_ms=event.get("at_ms"), row=event)]
        if "event" in event:
            return [self.handle_callback(event)]
        if event.get("kind") == "probe":
            return [self.submit_probe(event.get("worker"), at_ms=event.get("at_ms"),
                                      units=event.get("units"))]
        if event.get("type") == "advance":
            self.advance(int(event["at_ms"]))
            return list(self.trace[-1:])
        raise ValueError(f"无法识别的事件：{event}")

    process_event = ingest
    receive = ingest
    add_event = ingest
    on_event = ingest

    def submit(self, row: dict[str, Any]) -> dict[str, Any]:
        return self.submit_request(row)

    def add_request(self, row: dict[str, Any]) -> dict[str, Any]:
        return self.submit_request(row)

    def receive_request(self, row: dict[str, Any]) -> dict[str, Any]:
        return self.submit_request(row)

    def on_request(self, row: dict[str, Any]) -> dict[str, Any]:
        return self.submit_request(row)

    def callback(self, row: dict[str, Any]) -> dict[str, Any]:
        return self.handle_callback(row)

    def on_callback(self, row: dict[str, Any]) -> dict[str, Any]:
        return self.handle_callback(row)

    def receive_callback(self, row: dict[str, Any]) -> dict[str, Any]:
        return self.handle_callback(row)

    def process_callback(self, row: dict[str, Any]) -> dict[str, Any]:
        return self.handle_callback(row)

    # ======================================================================
    # 追踪 / 动作
    # ======================================================================
    def _trace(self, kind: str, *, at_ms: Optional[int] = None,
               request_id: Optional[str] = None, **fields: Any) -> dict[str, Any]:
        self._seq += 1
        event = {
            "seq": self._seq,
            "at_ms": self.clock if at_ms is None else int(at_ms),
            "kind": kind,
            "request_id": request_id,
            **fields,
        }
        self.trace.append(event)
        return event

    def _act(self, kind: str, **fields: Any) -> dict[str, Any]:
        action = {"at_ms": self.clock, "action": kind, **fields}
        self.actions.append(action)
        self._trace("action", action_kind=kind, **fields)
        return action

    # ======================================================================
    # 虚拟时钟
    # ======================================================================
    def advance(self, to_ms: int) -> None:
        to_ms = int(to_ms)
        if to_ms < self.clock:
            # 重演资料按 at_ms 单调；恢复场景允许显式拨钟但不回退决策。
            self._trace("clock_backwards_ignored", from_ms=self.clock, to_ms=to_ms)
            return
        self.clock = to_ms
        self._reject_infeasible_queued(to_ms)
        self._expire_past_deadline(to_ms)
        self._schedule_queued(to_ms)

    advance_to = advance
    tick = advance

    # ======================================================================
    # 请求接收 / 取消
    # ======================================================================
    def submit_request(self, row: Optional[dict[str, Any]] = None, **kwargs: Any) -> dict[str, Any]:
        if row is None:
            row = {}
        data = {**row, **{k: v for k, v in kwargs.items() if v is not None}}
        if "at_ms" not in data and "at" in data:
            data["at_ms"] = data["at"]
        row = data
        at_ms = int(row["at_ms"])
        self.advance(at_ms)
        req = Request.from_row(row)
        if req.request_id in self.requests:
            self._trace("duplicate_submit_ignored", at_ms=at_ms, request_id=req.request_id)
            return self.request_report(req.request_id)
        self.requests[req.request_id] = req
        self._trace("submitted", at_ms=at_ms, request_id=req.request_id,
                    tenant=req.tenant, units=req.units, units_ceiling=req.units_ceiling,
                    deadline_ms=req.deadline_ms, allow_small=req.allow_small)

        # 截止时间在“最乐观副本 + p95”下都不够：直接拒绝。
        fastest = self._fastest_p95(req)
        if self.config.policy.deadline_check and fastest is not None \
                and self._misses_deadline(at_ms, fastest, req.deadline_ms):
            req.status = RequestStatus.REJECTED
            req.status_reason = "deadline_infeasible_even_on_fastest_worker"
            req.decided_at_ms = at_ms
            self._trace("rejected", at_ms=at_ms, request_id=req.request_id,
                        reason=req.status_reason, fastest_p95_ms=fastest,
                        latest_start_ms=req.deadline_ms - fastest)
            return self.request_report(req.request_id)

        admitted = self._try_admit(req, at_ms)
        if not admitted and req.status == RequestStatus.QUEUED:
            self._trace("queued", at_ms=at_ms, request_id=req.request_id,
                        reason="no_worker_fit_or_deadline_safe_placement_now")
        return self.request_report(req.request_id)

    def cancel_request(
        self,
        request_id: Optional[str] = None,
        *,
        at_ms: Optional[int] = None,
        row: Optional[dict] = None,
    ) -> dict[str, Any]:
        if row is not None:
            request_id = row.get("request_id", request_id)
            at_ms = int(row.get("at_ms", at_ms if at_ms is not None else self.clock))
        at_ms = self.clock if at_ms is None else int(at_ms)
        self.advance(at_ms)
        req = self.requests.get(request_id)  # type: ignore[arg-type]
        if req is None:
            self._trace("cancel_unknown_request", at_ms=at_ms, request_id=request_id)
            return {"request_id": request_id, "found": False}

        if req.status in (RequestStatus.COMPLETED, RequestStatus.CANCELLED):
            self._trace("cancel_idempotent", at_ms=at_ms, request_id=request_id,
                        status=req.status.value)
            return self.request_report(request_id)  # type: ignore[arg-type]

        req.status = RequestStatus.CANCELLED
        req.status_reason = "caller_cancelled"
        req.decided_at_ms = at_ms
        self._trace("cancelled", at_ms=at_ms, request_id=request_id)

        # 排队中的请求没有真实占用，立即消失；已发起的尝试只发取消动作，
        # 容量必须等 released 反馈才真正移除。
        prepared_key = self.prepared_backups.pop(request_id, None)
        if prepared_key and prepared_key in self.attempts:
            prepared = self.attempts[prepared_key]
            if prepared.status is AttemptStatus.PREPARED:
                self.capacity.release(prepared_key)
                prepared.status = AttemptStatus.CANCEL_REQUESTED
                self._trace("prepared_backup_discarded_on_cancel", at_ms=at_ms,
                            request_id=request_id, attempt_key=prepared_key)

        live = [a for a in self.capacity.tenant_charges(req.tenant)
                if a.request_id == request_id]
        for att in live:
            if att.status is not AttemptStatus.PREPARED:
                att.status = AttemptStatus.CANCEL_REQUESTED
                self._act("cancel_attempt", at_ms=at_ms, request_id=request_id,
                          attempt_no=att.attempt_no, worker=att.worker,
                          attempt_key=att.key)
            else:
                self.capacity.release(att.key)
                att.status = AttemptStatus.CANCEL_REQUESTED
        self._trace("cancel_occupancy_awaiting_released", at_ms=at_ms,
                    request_id=request_id,
                    held_keys=[a.key for a in live if a.key in self.capacity.charges])
        return self.request_report(request_id)  # type: ignore[arg-type]

    # ======================================================================
    # 副本/出口/节点反馈
    # ======================================================================
    def handle_callback(self, row: Optional[dict[str, Any]] = None, **kwargs: Any) -> dict[str, Any]:
        if row is None:
            row = {}
        row = {**row, **{k: v for k, v in kwargs.items() if v is not None}}
        if "at_ms" not in row and "at" in row:
            row["at_ms"] = row["at"]
        at_ms = int(row["at_ms"])
        self.advance(at_ms)
        event = row.get("event")
        worker_name = row.get("worker")
        rid = row.get("request_id")
        no = row.get("attempt_no")
        units = row.get("units")
        occurred = row.get("occurred_ms")

        if event in ("node_down", "node_up"):
            return self._handle_node(event, worker_name, at_ms, row)

        if rid is None or no is None:
            self._trace("callback_without_attempt_ignored", at_ms=at_ms, raw=row)
            return {"handled": False}

        no = int(no)
        att = self._ensure_attempt(rid, no, worker_name, at_ms, units)
        req = self.requests.get(rid)

        if event == "started":
            att.status = AttemptStatus.STARTED
            att.started_at_ms = at_ms
            if units is not None:
                att.observed_units = int(units)
            self._trace("feedback_started", at_ms=at_ms, request_id=rid, attempt_no=no,
                        worker=worker_name, units=att.observed_units,
                        occurred_ms=occurred)

        elif event == "usage":
            old = att.charge_units
            att.observed_units = int(units)
            over_ceiling = int(units) > att.bound_units
            self._trace("feedback_usage", at_ms=at_ms, request_id=rid, attempt_no=no,
                        worker=worker_name, old_charge_units=old,
                        actual_units=int(units), bound_units=att.bound_units,
                        occurred_ms=occurred, over_ceiling=over_ceiling,
                        changed="actual occupancy corrected; bound stays reserved until released")
            if over_ceiling:
                # 资料口径里副本保证不突破 ceiling；若真的发生，池安全账仍按
                # bound 预留，这里只如实标记异常，绝不假装更多容量。
                self._trace("usage_reported_above_units_ceiling", at_ms=at_ms,
                            request_id=rid, attempt_no=no, actual_units=int(units),
                            bound_units=att.bound_units)

        elif event == "timeout":
            att.status = AttemptStatus.TIMED_OUT
            self._trace("feedback_timeout", at_ms=at_ms, request_id=rid, attempt_no=no,
                        worker=worker_name, occurred_ms=occurred,
                        note="observer timeout; a late first_chunk may still arrive")
            if req is not None and req.delivered_attempt is None \
                    and req.status not in (RequestStatus.CANCELLED, RequestStatus.REJECTED):
                self._prepare_backup(req, at_ms, avoid_worker=worker_name,
                                     caused_by="timeout")

        elif event == "first_chunk":
            return self._handle_first_chunk(rid, no, at_ms, occurred, worker_name)

        elif event in ("sink_closed_empty", "sink_finished"):
            return self._handle_sink(event, rid, no, at_ms, occurred)

        elif event == "released":
            return self._handle_released(rid, no, at_ms, occurred, worker_name)

        else:
            self._trace("unknown_callback_event", at_ms=at_ms, event=event, raw=row)

        self._schedule_queued(at_ms)
        return self.request_report(rid) if req else {"handled": True}

    # ------------------------------------------------------------------
    def _handle_first_chunk(self, rid, no, at_ms, occurred, worker_name) -> dict:
        obs = self.egress.observe_first_chunk(rid, no, at_ms, occurred)
        self._trace("feedback_first_chunk", at_ms=at_ms, request_id=rid, attempt_no=no,
                    worker=worker_name, occurred_ms=occurred,
                    late_relative_to_control_timeout=obs["state"].get("first_received_at_ms") == at_ms
                    and occurred is not None and occurred < at_ms,
                    duplicate=obs["duplicate"])
        req = self.requests.get(rid)
        if req is None:
            return {"handled": True, "unknown_request": rid}

        if obs["duplicate"]:
            self._trace("first_chunk_repeat_idempotent", at_ms=at_ms,
                        request_id=rid, attempt_no=no)
            return self.request_report(rid)

        if req.delivered_attempt is None:
            # 首个被出口实际收到的首段 -> 唯一可采信结果归属该尝试/模型。
            req.delivered_attempt = no
            req.status = RequestStatus.DELIVERED
            req.finished_at_ms = None
            receipt = self.egress.publish(
                f"result:{rid}",
                {"kind": "result_claim", "request_id": rid, "attempt_no": no,
                 "worker": worker_name, "variant": self.attempts[attempt_key(rid, no)].variant
                 if attempt_key(rid, no) in self.attempts else None},
                at_ms,
            )
            self._trace("delivery_claimed_single_credible_result", at_ms=at_ms,
                        request_id=rid, delivered_via_attempt=no,
                        idempotency_key=f"result:{rid}",
                        caller_receipt_duplicate=receipt["duplicate"],
                        model_locked=True)
            # 晚到首段胜出：撤销只预留、未发起的备用尝试。
            self._abort_prepared_backup(req, at_ms, reason="late_first_chunk_claimed_result")
            # 若已有发起的其他备用尝试，禁止它交付并通知取消（容量等 released）。
            for key, other in list(self.attempts.items()):
                if other.request_id == rid and other.attempt_no != no \
                        and key in self.capacity.charges \
                        and other.status not in (AttemptStatus.RELEASED,):
                    if other.status is AttemptStatus.PREPARED:
                        self.capacity.release(key)
                        self.prepared_backups.pop(rid, None)
                    else:
                        other.status = AttemptStatus.CANCEL_REQUESTED
                        self._act("cancel_shadow_backup", at_ms=at_ms, request_id=rid,
                                  attempt_no=other.attempt_no, worker=other.worker,
                                  reason="credible result arrived from another attempt")
        elif req.delivered_attempt != no:
            # 已经有另一个尝试交付：压制本尝试，绝不产生第二份上游结果。
            self._trace("cross_attempt_delivery_suppressed", at_ms=at_ms,
                        request_id=rid, attempt_no=no,
                        already_delivered_via_attempt=req.delivered_attempt)
        return self.request_report(rid)

    # ------------------------------------------------------------------
    def _handle_sink(self, event, rid, no, at_ms, occurred) -> dict:
        sink_name = "sink_closed_empty" if event == "sink_closed_empty" else "sink_finished"
        conf = self.egress.confirm_sink(rid, no, sink_name, at_ms, occurred)
        att = self.attempts.get(attempt_key(rid, no))
        if att is not None and att.status is not AttemptStatus.RELEASED:
            if event == "sink_closed_empty":
                att.status = AttemptStatus.CLOSED_EMPTY
            else:
                att.status = AttemptStatus.FINISHED
        elif att is not None:
            self._trace("sink_receipt_after_release_kept_released",
                        at_ms=at_ms, request_id=rid, attempt_no=no, event=event)
        self._trace(f"feedback_{event}", at_ms=at_ms, request_id=rid, attempt_no=no,
                    repeated=conf["repeated"], occurred_ms=occurred,
                    egress_state=conf["state"])

        req = self.requests.get(rid)
        if req is None:
            return {"handled": True}

        if event == "sink_finished" and req.delivered_attempt == no:
            req.status = RequestStatus.COMPLETED
            req.finished_at_ms = at_ms
            self.egress.publish(
                f"result:{rid}:finished",
                {"kind": "result_finished", "request_id": rid, "attempt_no": no},
                at_ms,
            )
            self._trace("request_completed", at_ms=at_ms, request_id=rid, attempt_no=no)

        if event == "sink_closed_empty" and not conf["repeated"] \
                and req.delivered_attempt is None \
                and req.status not in (RequestStatus.CANCELLED, RequestStatus.REJECTED):
            if self.egress.is_empty_and_closed(rid, no):
                self._trace("old_attempt_confirmed_empty_and_closed", at_ms=at_ms,
                            request_id=rid, attempt_no=no,
                            decision="prepared backup may now be launched and deliver")
                self._launch_or_prepare_backup(req, at_ms, caused_by="sink_closed_empty")
        self._schedule_queued(at_ms)
        return self.request_report(rid)

    # ------------------------------------------------------------------
    def _handle_released(self, rid, no, at_ms, occurred, worker_name) -> dict:
        key = attempt_key(rid, no)
        att = self.attempts.get(key)
        held = key in self.capacity.charges
        actual = att.charge_units if att else None
        if att is not None:
            att.status = AttemptStatus.RELEASED
            att.released_at_ms = occurred
            att.release_observed_at_ms = at_ms
        freed = self.capacity.release(key)
        returned_loans = self.ledger.mark_released(key, at_ms)
        self._trace("feedback_released", at_ms=at_ms, request_id=rid, attempt_no=no,
                    worker=worker_name, occurred_ms=occurred,
                    capacity_actually_freed=bool(freed or held),
                    freed_observed_units=actual,
                    loans_returned=[l.to_dict() for l in returned_loans])
        self.prepared_backups = {r: k for r, k in self.prepared_backups.items() if k != key}

        req = self.requests.get(rid)
        if req is not None:
            terminal = req.status in (RequestStatus.CANCELLED, RequestStatus.REJECTED)
            delivered_here = req.delivered_attempt == no
            if not terminal and req.delivered_attempt is None:
                # 没有任何交付证据、也没有取消：在截止可行时续一个新尝试。
                self._ensure_continuation(req, at_ms, caused_by="released_without_delivery")
            elif delivered_here and req.status is RequestStatus.DELIVERED:
                self._trace("delivered_attempt_released_result_kept_locked",
                            at_ms=at_ms, request_id=rid, attempt_no=no)
        self._schedule_queued(at_ms)
        return self.request_report(rid) if req else {"handled": True}

    # ------------------------------------------------------------------
    def _handle_node(self, event, worker_name, at_ms, row) -> dict:
        w = self.config.workers.get(worker_name) if worker_name else None
        if w is None:
            self._trace("node_event_unknown_worker", at_ms=at_ms, raw=row)
            return {"handled": False}
        if event == "node_down":
            w.up = False
            w.down_at_ms = at_ms
            affected = self.capacity.worker_charges(worker_name)
            keys = [a.key for a in affected]
            for a in affected:
                if a.status not in (AttemptStatus.RELEASED,):
                    a.status = AttemptStatus.NODE_DOWN
            # 故障影响严格限定在同一 worker；同池其他副本不受影响。
            pool = w.pool
            unaffected = [x.worker for x in self.config.workers.values()
                          if x.pool == pool and x.worker != worker_name and x.up]
            self._trace("node_down_blast_radius", at_ms=at_ms, worker=worker_name,
                        pool=pool, affected_attempts=keys,
                        same_pool_unaffected_workers=unaffected,
                        note="charges retained until released evidence; no fake free")
        else:
            w.up = True
            w.up_at_ms = at_ms
            self._trace("node_up", at_ms=at_ms, worker=worker_name, pool=w.pool)
            if self.config.policy.probe_on_up:
                self.submit_probe(worker_name, at_ms=at_ms)
            self._schedule_queued(at_ms)
        return {"handled": True, "worker": worker_name, "up": w.up}

    # ======================================================================
    # 尝试构造 /  placements / 接纳
    # ======================================================================
    def _ensure_attempt(self, rid: str, no: int, worker_name: Optional[str],
                        at_ms: int, units: Optional[int]) -> Attempt:
        key = attempt_key(rid, no)
        att = self.attempts.get(key)
        if att is not None:
            if worker_name and att.worker != worker_name:
                self._trace("attempt_worker_mismatch", request_id=rid, attempt_no=no,
                            engine_worker=att.worker, callback_worker=worker_name,
                            note="feedback applied to logical attempt; scheduling diverged from feed")
            return att
        req = self.requests.get(rid)
        variant = self.config.workers[worker_name].variant if worker_name in self.config.workers else "full"
        est = variant_cost(req.units, variant) if req else int(units or 0)
        bound = variant_cost(req.units_ceiling, variant) if req else int(units or 0)
        tenant = req.tenant if req else "unknown"
        pool = self.config.workers[worker_name].pool if worker_name in self.config.workers else "unknown"
        att = Attempt(key=key, request_id=rid, attempt_no=no, tenant=tenant,
                      worker=worker_name or "unknown", pool=pool, variant=variant,
                      estimated_units=est, bound_units=bound,
                      status=AttemptStatus.DISPATCHED, dispatched_at_ms=at_ms)
        if units is not None:
            att.observed_units = int(units)
        self.attempts[key] = att
        if key not in self.capacity.charges:
            self.capacity.add(att)
        return att

    def _eligible_placements(self, req: Request, at_ms: int, *,
                             avoid_worker: Optional[str] = None) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        p = self.config.policy
        for w in self.config.workers.values():
            if avoid_worker and w.worker == avoid_worker:
                continue
            if w.variant == p.low_cost_variant and not req.allow_small:
                continue  # 明确不接受降级：不能放 small/低成本模型
            if w.variant == "small" and not req.allow_small:
                continue
            variant = w.variant
            est = variant_cost(req.units, variant)
            bound = variant_cost(req.units_ceiling, variant)
            if not self.capacity.can_fit(w.worker, bound, 1):
                continue
            finish = at_ms + w.p95_ms
            feasible = (not p.deadline_check) or (
                finish < req.deadline_ms if p.deadline_strict else finish <= req.deadline_ms
            )
            if not feasible:
                continue
            # 低成本预留池同样要求 allow_small（上面已保证）且截止可行（这里）。
            own_reserved = self.config.reserved_units(
                req.tenant, w.pool, req.model_family, at_ms)
            slice_, loans = self.ledger.plan_slice(
                req.tenant, w.pool, req.model_family, at_ms, bound,
                attempt_key(req.request_id, max(req.attempt_numbers or [0]) + 1))
            hard_cap_ok = True
            if p.reservation_is_hard_cap:
                accessible = own_reserved + sum(o["units"] for o in
                                                self.ledger.borrowable_units(
                                                    req.tenant, w.pool, req.model_family, at_ms))
                already = sum(a.bound_units for a in self.capacity.tenant_charges(req.tenant)
                               if a.pool == w.pool)
                hard_cap_ok = already + bound <= accessible
            if not hard_cap_ok:
                continue
            active_on_worker = len(self.capacity.worker_charges(w.worker))
            out.append({
                "worker": w.worker, "pool": w.pool, "variant": variant,
                "estimated_units": est, "bound_units": bound, "p95_ms": w.p95_ms,
                "finish_ms": finish, "slack_ms": req.deadline_ms - finish,
                "active_on_worker": active_on_worker,
                "own_reserved_units": own_reserved,
                "has_reservation_affinity": own_reserved > 0,
                "slice": slice_, "loans": loans,
                "low_cost": variant == p.low_cost_variant,
            })
        out.sort(key=lambda c: (
            1 if c["variant"] == "small" else 0,       # 优先不降级/full
            0 if (not p.prefer_reserved_pool or c["has_reservation_affinity"]) else 1,
            c["active_on_worker"],                     # 分散放，缩小故障波及面
            c["p95_ms"],                               # 同空闲度选更快副本
            c["worker"],
        ))
        return out

    def _fastest_p95(self, req: Request) -> Optional[int]:
        vals = [w.p95_ms for w in self.config.workers.values()
                if req.allow_small or w.variant != "small"]
        return min(vals) if vals else None

    def _misses_deadline(self, at_ms: int, p95: int, deadline_ms: int) -> bool:
        if self.config.policy.deadline_strict:
            return at_ms + p95 >= deadline_ms
        return at_ms + p95 > deadline_ms

    def _primary_p95_fastest_healthy(self, req: Request) -> Optional[int]:
        primary = [
            w.p95_ms for w in self.config.workers.values()
            if w.up and w.variant != self.config.policy.low_cost_variant
            and (req.allow_small or w.variant != "small")
        ]
        return min(primary) if primary else None

    def _primary_cancel_release_possible(self, req: Request, at_ms: int) -> bool:
        """是否存在一个主池副本，其满员仅由 cancel_requested 尝试造成。

        调用方取消后的槽位会在收到 ``released`` 实证后很快空出，因此只要
        截止仍可行，可以短暂排队等它，而不必立刻降级去挤占低余量备用池。
        超时/closed_empty/node_down 的尝试释放时间不明，不在此列。
        """
        bound_need = variant_cost(req.units_ceiling, "full")
        for w in self.config.workers.values():
            if not w.up or w.variant == self.config.policy.low_cost_variant:
                continue
            charges = self.capacity.worker_charges(w.worker)
            if not charges:
                continue
            if not any(a.status is AttemptStatus.CANCEL_REQUESTED for a in charges):
                continue
            remaining = [a for a in charges if a.status is not AttemptStatus.CANCEL_REQUESTED]
            bound_left = sum(a.bound_units for a in remaining)
            slots_left = len(remaining)
            if bound_left + bound_need > w.work_capacity:
                continue
            if slots_left + 1 > w.slots:
                continue
            if self.config.policy.deadline_check and at_ms + w.p95_ms > req.deadline_ms:
                continue
            return True
        return False

    def _select_now(
        self, req: Request, at_ms: int, *, avoid_worker: Optional[str] = None,
        force_any: bool = False,
    ) -> tuple[Optional[dict[str, Any]], Optional[str]]:
        """选出当前可接纳的 placement。

        返回 ``(choice, defer_reason)``。deadline 降级模式下：

        * 有 full/主池可行 placement -> 立即用主池；
        * 只有 small/备用池可行时，若主池槽位是被“已取消、待实证释放”的
          尝试占着且等到 latest_start 仍截止可行，则短暂排队等主池；
        * 否则及早接纳到备用池（备用池余量有限，不能等到截止边缘才用）。

        ``force_any`` 用于备用尝试：不再等待，直接取最优可行 placement。
        """
        placements = self._eligible_placements(req, at_ms, avoid_worker=avoid_worker)
        if not placements:
            return None, "no_deadline_safe_fit"
        primary = [p for p in placements if p["variant"] != self.config.policy.low_cost_variant]
        degraded = [p for p in placements if p["variant"] == self.config.policy.low_cost_variant]
        if primary:
            return primary[0], None
        if not degraded:
            return None, "primary_full_and_no_degraded_allowed"
        if force_any or self.config.policy.downgrade_mode == "immediate":
            return degraded[0], None
        p95 = self._primary_p95_fastest_healthy(req)
        latest_primary_start = (req.deadline_ms - p95) if p95 is not None else None
        can_wait_cancel = (
            latest_primary_start is not None
            and at_ms <= latest_primary_start
            and self._primary_cancel_release_possible(req, at_ms)
        )
        if can_wait_cancel:
            return None, f"wait_primary_cancel_release_until_{latest_primary_start}"
        # 主池被状态不明尝试占住或已过 latest_start：及早使用备用池。
        return degraded[0], ("primary_latest_start_passed"
                             if latest_primary_start is not None and at_ms > latest_primary_start
                             else "proactive_backup_while_headroom")

    def _try_admit(self, req: Request, at_ms: int, *, force_any: bool = False) -> bool:
        choice, defer = self._select_now(req, at_ms, force_any=force_any)
        if choice is None:
            if defer and defer.startswith("wait_primary"):
                until = defer.rsplit("_", 1)[-1]
                note = self._wait_noted.get(req.request_id)
                if note != until:
                    self._wait_noted[req.request_id] = until
                    self._trace("waiting_for_primary_pool", at_ms=at_ms,
                                request_id=req.request_id,
                                latest_primary_start_ms=int(until),
                                fallback_choice_held_back=True)
            return False
        self._wait_noted.pop(req.request_id, None)
        no = (max(req.attempt_numbers) + 1) if req.attempt_numbers else 1
        self._create_attempt(req, no, choice, at_ms, prepared=False)
        return True

    def _create_attempt(self, req: Request, no: int, choice: dict, at_ms: int,
                        *, prepared: bool, is_probe: bool = False) -> Attempt:
        key = attempt_key(req.request_id, no)
        att = Attempt(
            key=key, request_id=req.request_id, attempt_no=no, tenant=req.tenant,
            worker=choice["worker"], pool=choice["pool"], variant=choice["variant"],
            estimated_units=choice["estimated_units"], bound_units=choice["bound_units"],
            status=AttemptStatus.PREPARED if prepared else AttemptStatus.DISPATCHED,
            dispatched_at_ms=None if prepared else at_ms,
            prepared=prepared, is_probe=is_probe, slice=dict(choice["slice"]),
        )
        promise = Promise(
            at_ms=at_ms, request_id=req.request_id, attempt_no=no,
            worker=choice["worker"], pool=choice["pool"], variant=choice["variant"],
            estimated_units=choice["estimated_units"], bound_units=choice["bound_units"],
            slots=1, p95_ms=choice["p95_ms"], deadline_ms=req.deadline_ms,
            projected_finish_ms=choice["finish_ms"], deadline_slack_ms=choice["slack_ms"],
            reservation_slice=dict(choice["slice"]), prepared=prepared,
        )
        att.promise = promise.to_dict()
        self.attempts[key] = att
        self.capacity.add(att)
        self.ledger.add_loans(choice["loans"])
        if no not in req.attempt_numbers:
            req.attempt_numbers.append(no)
        if not is_probe:
            req.status = RequestStatus.ADMITTED
            req.decided_at_ms = at_ms
        self._trace("backup_prepared" if prepared else "admitted",
                    at_ms=at_ms, request_id=req.request_id, attempt_no=no,
                    **{k: v for k, v in choice.items() if k not in ("loans", "slice")},
                    reservation_slice=choice["slice"],
                    promise=promise.to_dict())
        if prepared:
            self.prepared_backups[req.request_id] = key
            # 只“准备”：不向副本发起，因此不会产生 attempt 回调记录。
        else:
            self._act("dispatch_attempt", at_ms=at_ms, request_id=req.request_id,
                      attempt_no=no, worker=choice["worker"], pool=choice["pool"],
                      variant=choice["variant"], bound_units=choice["bound_units"],
                      is_probe=is_probe)
        return att

    # ======================================================================
    # 备用尝试 / 续跑
    # ======================================================================
    def _prepare_backup(self, req: Request, at_ms: int, *,
                        avoid_worker: Optional[str] = None, caused_by: str) -> Optional[str]:
        existing = self.prepared_backups.get(req.request_id)
        if existing and existing in self.attempts:
            return existing
        # 已经有未结束的其他尝试则不重复准备
        for a in self.capacity.tenant_charges(req.tenant):
            if a.request_id == req.request_id and a.status is AttemptStatus.PREPARED:
                self.prepared_backups[req.request_id] = a.key
                return a.key
        if len(req.attempt_numbers) >= self.config.policy.max_attempts:
            self._trace("backup_skipped_max_attempts", at_ms=at_ms,
                        request_id=req.request_id, caused_by=caused_by)
            return None
        placements = self._eligible_placements(req, at_ms, avoid_worker=avoid_worker)
        if not placements:
            self._trace("backup_unavailable", at_ms=at_ms, request_id=req.request_id,
                        caused_by=caused_by,
                        reason="no deadline-safe worker with safe capacity")
            return None
        no = (max(req.attempt_numbers) + 1) if req.attempt_numbers else 1
        # 备用切换优先 small/fallback 以保住主池？排序默认 full 优先；但故障
        # 场景下原 worker 被 avoid，主池其他槽通常也满。直接用排序结果即可。
        att = self._create_attempt(req, no, placements[0], at_ms, prepared=True)
        self._trace("backup_prepared_because", at_ms=at_ms, request_id=req.request_id,
                    attempt_no=no, caused_by=caused_by,
                    delivery_gate="blocked_until_old_attempt_confirmed_empty_and_closed")
        return att.key

    def _launch_or_prepare_backup(self, req: Request, at_ms: int, *, caused_by: str) -> None:
        key = self.prepared_backups.get(req.request_id)
        att = self.attempts.get(key) if key else None
        if att is not None and att.status is AttemptStatus.PREPARED:
            att.status = AttemptStatus.DISPATCHED
            att.dispatched_at_ms = at_ms
            self.prepared_backups.pop(req.request_id, None)
            self._act("dispatch_prepared_backup", at_ms=at_ms, request_id=req.request_id,
                      attempt_no=att.attempt_no, worker=att.worker,
                      bound_units=att.bound_units, caused_by=caused_by,
                      delivery_gate_open=True)
            self._trace("backup_launched", at_ms=at_ms, request_id=req.request_id,
                        attempt_no=att.attempt_no, caused_by=caused_by)
            return
        # 没有提前预留：若已有别的在制尝试则不重复创建，否则直接发起新尝试。
        other_live = [a for a in self.capacity.tenant_charges(req.tenant)
                      if a.request_id == req.request_id and a.key in self.capacity.charges
                      and a.status is not AttemptStatus.PREPARED]
        if other_live:
            return
        self._try_admit(req, at_ms, force_any=True)

    def _abort_prepared_backup(self, req: Request, at_ms: int, *, reason: str) -> None:
        key = self.prepared_backups.pop(req.request_id, None)
        if not key:
            return
        att = self.attempts.get(key)
        if att and att.status is AttemptStatus.PREPARED and key in self.capacity.charges:
            self.capacity.release(key)  # 从未发起：预留可立即撤销，不涉及副本实占
            att.status = AttemptStatus.CANCEL_REQUESTED
            self._trace("prepared_backup_aborted", at_ms=at_ms,
                        request_id=req.request_id, attempt_key=key, reason=reason,
                        note="reservation removed; no replica occupancy existed")

    def _ensure_continuation(self, req: Request, at_ms: int, *, caused_by: str) -> None:
        # 若还有 prepared 备用，旧尝试 released 本身就是“已退出”的强证据，可发起。
        key = self.prepared_backups.get(req.request_id)
        if key and key in self.attempts:
            att = self.attempts[key]
            if att.status is AttemptStatus.PREPARED:
                att.status = AttemptStatus.DISPATCHED
                att.dispatched_at_ms = at_ms
                self.prepared_backups.pop(req.request_id, None)
                self._act("dispatch_prepared_backup", at_ms=at_ms,
                          request_id=req.request_id, attempt_no=att.attempt_no,
                          worker=att.worker, caused_by=caused_by)
                return
        # 是否还有其他仍持容量的尝试（例如在别的 worker 上跑着）
        live_others = [a for a in self.capacity.tenant_charges(req.tenant)
                       if a.request_id == req.request_id and a.key in self.capacity.charges]
        if live_others:
            return
        if len(req.attempt_numbers) >= self.config.policy.max_attempts:
            req.status = RequestStatus.FAILED
            req.status_reason = "exhausted_attempts_without_credible_result"
            self._trace("request_failed", at_ms=at_ms, request_id=req.request_id,
                        reason=req.status_reason, caused_by=caused_by)
            return
        if self._try_admit(req, at_ms):
            self._trace("retry_after_release", at_ms=at_ms, request_id=req.request_id,
                        caused_by=caused_by)
        else:
            fastest = self._fastest_p95(req)
            if fastest is not None and self._misses_deadline(at_ms, fastest, req.deadline_ms):
                req.status = RequestStatus.FAILED
                req.status_reason = "no_deadline_feasible_retry_after_release"
                self._trace("request_failed", at_ms=at_ms, request_id=req.request_id,
                            reason=req.status_reason, caused_by=caused_by)

    # ======================================================================
    # 排队调度 / 截止时间
    # ======================================================================
    def _schedule_queued(self, at_ms: int) -> None:
        self._schedule_waiting(at_ms)

    def _schedule_waiting(self, at_ms: int) -> None:
        """接纳排队请求，并为“尝试已释放但尚无交付”的 admitted 请求续跑。"""
        waiting: list[Request] = []
        for req in self.requests.values():
            if req.tenant == "__probe__":
                continue
            if req.status is RequestStatus.QUEUED:
                waiting.append(req)
            elif req.status is RequestStatus.ADMITTED and req.delivered_attempt is None:
                has_charge = any(a.request_id == req.request_id
                                 for a in self.capacity.charges.values())
                has_prepared = req.request_id in self.prepared_backups
                if not has_charge and not has_prepared:
                    waiting.append(req)
        waiting.sort(key=lambda r: (r.deadline_ms, r.at_ms, r.request_id))
        for req in waiting:
            if req.status not in (RequestStatus.QUEUED, RequestStatus.ADMITTED):
                continue
            self._try_admit(req, at_ms)

    def _reject_infeasible_queued(self, at_ms: int) -> None:
        if not self.config.policy.auto_reject_late:
            return
        for req in list(self.requests.values()):
            if req.status is not RequestStatus.QUEUED:
                continue
            fastest = self._fastest_p95(req)
            if fastest is None:
                continue
            if self._misses_deadline(at_ms, fastest, req.deadline_ms):
                req.status = RequestStatus.REJECTED
                req.status_reason = "deadline_infeasible_waiting_for_capacity"
                req.decided_at_ms = at_ms
                self._trace("rejected", at_ms=at_ms, request_id=req.request_id,
                            reason=req.status_reason, fastest_p95_ms=fastest,
                            latest_start_ms=req.deadline_ms - fastest)

    def _expire_past_deadline(self, at_ms: int) -> None:
        for req in self.requests.values():
            if at_ms < req.deadline_ms:
                continue
            if req.status is RequestStatus.QUEUED:
                req.status = RequestStatus.REJECTED
                req.status_reason = "deadline_reached_while_queued"
                req.decided_at_ms = at_ms
                self._trace("rejected", at_ms=at_ms, request_id=req.request_id,
                            reason=req.status_reason)
            elif req.status is RequestStatus.ADMITTED and req.delivered_attempt is None:
                req.status = RequestStatus.FAILED
                req.status_reason = "deadline_passed_without_credible_result"
                req.decided_at_ms = at_ms
                self._trace("request_failed", at_ms=at_ms, request_id=req.request_id,
                            reason=req.status_reason)

    # ======================================================================
    # 恢复探测（真实占容量）
    # ======================================================================
    def submit_probe(self, worker_name: str, *, at_ms: Optional[int] = None,
                     units: Optional[int] = None) -> dict[str, Any]:
        at_ms = self.clock if at_ms is None else int(at_ms)
        self.advance(at_ms)
        if worker_name not in self.config.workers:
            raise KeyError(f"未知副本 {worker_name}")
        w = self.config.workers[worker_name]
        probe_units = int(units if units is not None else self.config.policy.probe_units)
        self._probe_seq += 1
        rid = f"__probe__:{worker_name}:{self._probe_seq}"
        no = 1
        if not self.capacity.can_fit(worker_name, probe_units, self.config.policy.probe_slots):
            self._trace("probe_rejected_no_capacity", at_ms=at_ms, worker=worker_name,
                        units=probe_units)
            return {"admitted": False, "reason": "no_capacity"}
        choice = {
            "worker": worker_name, "pool": w.pool, "variant": w.variant,
            "estimated_units": probe_units, "bound_units": probe_units,
            "p95_ms": w.p95_ms, "finish_ms": at_ms + w.p95_ms,
            "slack_ms": 0, "slice": {"reserved": 0, "borrowed": 0, "spare": probe_units},
            "loans": [],
        }
        req = Request(request_id=rid, tenant="__probe__", model_family="text",
                      at_ms=at_ms, deadline_ms=at_ms + w.p95_ms, units=probe_units,
                      allow_small=True, units_ceiling=probe_units)
        self.requests[rid] = req
        att = self._create_attempt(req, no, choice, at_ms, prepared=False, is_probe=True)
        self._trace("probe_started_real_capacity", at_ms=at_ms, worker=worker_name,
                    attempt_key=att.key, units=probe_units,
                    blast_radius="probe charge counts against worker until released")
        return {"admitted": True, "attempt_key": att.key, "request_id": rid}

    def release_probe(self, attempt_key_: str, *, at_ms: Optional[int] = None) -> bool:
        at_ms = self.clock if at_ms is None else int(at_ms)
        att = self.attempts.get(attempt_key_)
        if not att:
            return False
        att.status = AttemptStatus.RELEASED
        att.release_observed_at_ms = at_ms
        return self.capacity.release(attempt_key_) is not None

    # ======================================================================
    # 保留额催还：原租户新需求 -> 在 return_ms 内实际拿回份额
    # ======================================================================
    def reclaim_reservation(
        self,
        tenant: str,
        *,
        pool: str,
        model_family: str = "text",
        units: Optional[int] = None,
        at_ms: Optional[int] = None,
    ) -> dict[str, Any]:
        """原租户索要被借出的保留额。

        账面只登记催还 deadline 并（在策略允许时）通知借用尝试取消；份额
        真正可用仍要等对应 attempt 的 ``released`` 反馈，绝不会直接改配额
        假装容量回来。返回催还单与涉及的 attempt。
        """
        at_ms = self.clock if at_ms is None else int(at_ms)
        if units is None:
            units = self.config.reserved_units(tenant, pool, model_family, at_ms)
        loans = self.ledger.reclaim(tenant, pool, model_family, int(units), at_ms)
        affected_attempts = []
        for loan in loans:
            att = self.attempts.get(loan.attempt_key)
            if att is None:
                continue
            affected_attempts.append(att.key)
            if self.config.policy.preempt_borrowers and att.key in self.capacity.charges \
                    and att.status not in (AttemptStatus.RELEASED, AttemptStatus.PREPARED):
                att.status = AttemptStatus.CANCEL_REQUESTED
                self._act("preempt_borrowed_capacity", at_ms=at_ms,
                          request_id=att.request_id, attempt_no=att.attempt_no,
                          worker=att.worker, lender=tenant, units=loan.units,
                          reclaim_deadline_ms=loan.reclaim_deadline_ms)
        deadline = self.config.pool_return_ms(pool)
        result = {
            "lender": tenant, "pool": pool, "model_family": model_family,
            "units_requested": int(units),
            "reclaim_return_ms": deadline,
            "reclaim_deadline_ms": at_ms + deadline if deadline is not None else None,
            "loans": [l.to_dict() for l in loans],
            "affected_attempts": affected_attempts,
            "note": "capacity becomes reusable only after released callbacks",
        }
        self._trace("reservation_reclaim_requested", at_ms=at_ms, request_id=None, **result)
        return result

    reclaim = reclaim_reservation

    # ======================================================================
    # 自检：任何时刻都不能突破池/worker 上限（接纳按 ceiling 安全预留）
    # ======================================================================
    def check_invariants(self) -> list[str]:
        violations: list[str] = []
        for name, w in self.config.workers.items():
            bound = self.capacity.worker_bound_used(name)
            slots = self.capacity.worker_slots_used(name)
            if bound > w.work_capacity:
                violations.append(
                    f"worker {name} bound {bound} > work_capacity {w.work_capacity}")
            if slots > w.slots:
                violations.append(f"worker {name} slots {slots} > {w.slots}")
        for key in self.capacity.charges:
            if key not in self.attempts:
                violations.append(f"capacity charge {key} missing attempt")
        # 同一逻辑请求至多一个交付归属
        seen: dict[str, int] = {}
        for key_, receipt in self.egress.outbox.items():
            if receipt.get("kind") != "result_claim":
                continue
            rid = receipt.get("request_id")
            no = receipt.get("attempt_no")
            if rid in seen and seen[rid] != no:
                violations.append(f"request {rid} has multiple result claims")
            seen[rid] = no
        return violations

    # ======================================================================
    # 配置热替换（值班员新配置也可作为事件到达）
    # ======================================================================
    def apply_config(self, config: dict | RehearsalConfig, *, at_ms: Optional[int] = None,
                     merge: bool = True) -> dict[str, Any]:
        at_ms = self.clock if at_ms is None else int(at_ms)
        if isinstance(config, RehearsalConfig):
            new_cfg = config
        elif merge:
            new_cfg = self.config.overlay(config, merge=True)
        else:
            new_cfg = RehearsalConfig.from_dict(config)
        # 保留运行时 worker 健康状态
        for name, old_w in self.config.workers.items():
            if name in new_cfg.workers:
                new_cfg.workers[name].up = old_w.up
                new_cfg.workers[name].down_at_ms = old_w.down_at_ms
                new_cfg.workers[name].up_at_ms = old_w.up_at_ms
        self.config = new_cfg
        self.capacity.config = new_cfg
        self.ledger.config = new_cfg
        self._trace("config_applied", at_ms=at_ms, name=new_cfg.name,
                    workers=list(new_cfg.workers),
                    reservations=len(new_cfg.reservations))
        return {"applied": True, "name": new_cfg.name}

    # ======================================================================
    # 查询：请求追查 / 占用 / 对账
    # ======================================================================
    def request_report(self, request_id: str) -> dict[str, Any]:
        req = self.requests.get(request_id)
        if req is None:
            return {"request_id": request_id, "found": False}
        attempts = [self.attempts[k].to_dict() for k in
                    (attempt_key(request_id, n) for n in req.attempt_numbers)
                    if k in self.attempts]
        timeline = [e for e in self.trace if e.get("request_id") == request_id]
        promises = [a.get("promise") for a in attempts if a.get("promise")]
        feedback_changes = [
            e for e in timeline
            if e["kind"].startswith("feedback_")
            or e["kind"] in ("delivery_claimed_single_credible_result",
                             "old_attempt_confirmed_empty_and_closed",
                             "backup_launched", "retry_after_release",
                             "prepared_backup_aborted")
        ]
        egress_attempts = {
            k: v.to_dict() for k, v in self.egress.attempts.items()
            if v.request_id == request_id
        }
        return {
            "request_id": request_id,
            "found": True,
            "status": req.status.value,
            "status_reason": req.status_reason,
            "tenant": req.tenant,
            "deadline_ms": req.deadline_ms,
            "submitted_at_ms": req.at_ms,
            "allow_small": req.allow_small,
            "units": req.units,
            "units_ceiling": req.units_ceiling,
            "delivered_attempt": req.delivered_attempt,
            "admission_promises": promises,
            "attempts": attempts,
            "egress": egress_attempts,
            "feedback_decisions": feedback_changes,
            "timeline": timeline,
        }

    trace_for = request_report
    explain = request_report
    request_trace = request_report

    # --- 更细粒度的兼容 API ---
    def status(self, request_id: str) -> Optional[str]:
        req = self.requests.get(request_id)
        return req.status.value if req else None

    def get_request(self, request_id: str) -> Optional[dict[str, Any]]:
        req = self.requests.get(request_id)
        return req.to_dict() if req else None

    def get_attempt(self, request_id: str, attempt_no: int = 1) -> Optional[dict[str, Any]]:
        att = self.attempts.get(attempt_key(request_id, int(attempt_no)))
        return att.to_dict() if att else None

    def can_admit(self, row: dict[str, Any], at_ms: Optional[int] = None) -> bool:
        """不改状态地判断一条 submit 现在是否存在 deadline 安全的 placement。"""
        probe_row = dict(row)
        if at_ms is not None:
            probe_row["at_ms"] = at_ms
        req = Request.from_row(probe_row)
        choice, _ = self._select_now(req, int(probe_row["at_ms"]), force_any=True)
        return choice is not None

    def admission_plan(self, row: dict[str, Any]) -> Optional[dict[str, Any]]:
        req = Request.from_row(row)
        choice, defer = self._select_now(req, int(row["at_ms"]), force_any=True)
        if choice is None:
            return {"admit": False, "defer_reason": defer}
        return {"admit": True, "defer_reason": defer, "placement": {
            k: v for k, v in choice.items() if k != "loans"}}

    def query_first_chunk(self, request_id: str, attempt_no: int) -> Optional[dict]:
        return self.egress.received_first_chunk(request_id, int(attempt_no))

    def stopped_receiving(self, request_id: str, attempt_no: int) -> bool:
        return self.egress.stopped_receiving(request_id, int(attempt_no))

    def trace_timeline(self, request_id: str) -> list[dict[str, Any]]:
        return [e for e in self.trace if e.get("request_id") == request_id]

    def occupancy(self, by: str = "pool") -> dict[str, Any]:
        snap = self.capacity.occupancy(by=by, at_ms=self.clock)
        # 补上“仍持容量的尝试”与“已结束用户请求”的区分汇总
        live_keys = set(self.capacity.charges)
        live_requests = sorted({a.request_id for a in self.capacity.charges.values()
                                if not a.is_probe})
        ended_user_requests = sorted(
            r.request_id for r in self.requests.values()
            if r.status in (RequestStatus.COMPLETED, RequestStatus.REJECTED,
                            RequestStatus.CANCELLED, RequestStatus.FAILED,
                            RequestStatus.DELIVERED)
        )
        snap["clock_ms"] = self.clock
        snap["live_charge_keys"] = sorted(live_keys)
        snap["requests_with_live_charges"] = live_requests
        snap["ended_user_requests"] = ended_user_requests
        snap["request_status_counts"] = self._status_counts()
        return snap

    def occupancy_by_pool(self):
        return self.occupancy("pool")

    def occupancy_by_tenant(self):
        return self.occupancy("tenant")

    def occupancy_by_worker(self):
        return self.occupancy("worker")

    def _status_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for r in self.requests.values():
            if r.tenant == "__probe__":
                continue
            counts[r.status.value] = counts.get(r.status.value, 0) + 1
        return counts

    def tenant_quota(self, at_ms: Optional[int] = None) -> list[dict[str, Any]]:
        return self.ledger.snapshot(self.clock if at_ms is None else at_ms)

    def reconcile(self) -> dict[str, Any]:
        from .persist import reconcile_state
        return reconcile_state(self)

    # ======================================================================
    # 持久化 / 恢复
    # ======================================================================
    def to_state(self) -> dict[str, Any]:
        return {
            "version": 1,
            "clock": self.clock,
            "config": self.config.to_dict(),
            "requests": {k: v.to_dict() for k, v in self.requests.items()},
            "attempts": {k: v.to_dict() for k, v in self.attempts.items()},
            "prepared_backups": self.prepared_backups,
            "live_charge_keys": sorted(self.capacity.charges.keys()),
            "wait_noted": self._wait_noted,
            "trace": self.trace,
            "actions": self.actions,
            "seq": self._seq,
            "probe_seq": self._probe_seq,
            "egress": self.egress.to_dict(),
            "loans": [l.to_dict() for l in self.ledger.loans],
        }

    @classmethod
    def from_state(cls, state: dict[str, Any]) -> "RehearsalEngine":
        cfg = RehearsalConfig.from_dict(state["config"]) if state.get("config") else RehearsalConfig()
        eng = cls(cfg, clock=state.get("clock", 0))
        eng.egress = EgressBook.from_dict(state.get("egress", {}))
        for rid, rd in state.get("requests", {}).items():
            rd = dict(rd)
            status = rd.pop("status", "queued")
            req = Request(**{k: v for k, v in rd.items() if k in Request.__dataclass_fields__})
            req.status = RequestStatus(status)
            eng.requests[rid] = req

        live_keys = state.get("live_charge_keys")
        live_set = set(live_keys) if live_keys is not None else None
        for key, ad in state.get("attempts", {}).items():
            ad = dict(ad)
            status = ad.pop("status", "prepared")
            ad.pop("charge_units", None)
            ad.pop("holds_charge", None)
            att = Attempt(**{k: v for k, v in ad.items() if k in Attempt.__dataclass_fields__})
            att.status = AttemptStatus(status)
            eng.attempts[key] = att
            # 以落盘时容量账的真实键集合为准，避免晚到 sink 状态把已释放
            # 的占用“复活”。旧快照没有该字段时退回状态判断。
            should_charge = (key in live_set) if live_set is not None else att.holds_charge
            if should_charge:
                eng.capacity.add(att)

        eng.prepared_backups = {
            r: k for r, k in dict(state.get("prepared_backups", {})).items()
            if k in eng.capacity.charges  # 已撤销/释放的预留不再恢复
        }
        eng._wait_noted = dict(state.get("wait_noted", {}))
        eng.trace = list(state.get("trace", []))
        eng.actions = list(state.get("actions", []))
        eng._seq = int(state.get("seq", len(eng.trace)))
        eng._probe_seq = int(state.get("probe_seq", 0))
        from .books import Loan
        eng.ledger.loans = [Loan(**loan) for loan in state.get("loans", [])]
        return eng

    def save(self, directory) -> dict[str, str]:
        from .persist import Store
        return Store(directory).save(self)

    @classmethod
    def restore(cls, directory) -> "RehearsalEngine":
        from .persist import Store
        return Store(directory).load()


# 常见方法命名别名（放在类外补充，保持类体可读）
for _alias in ("snapshot", "state"):
    setattr(RehearsalEngine, _alias, RehearsalEngine.to_state)
