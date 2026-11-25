"""两份账的落盘、控制程序重启恢复与对账。

产物（一个运行目录）：

* ``state.json``          —— 引擎完整状态原子快照（重启入口）
* ``control_book.json``   —— 控制面容量/租借账（便于值班员直接查看）
* ``egress_book.json``    —— 出口交付账（首段/关闭确认/幂等 outbox）
* ``trace.jsonl``         —— 决策追踪
* 以及 replay 写出的报告文件。

写 ``state.json`` 采用 “临时文件 + os.replace” 原子替换，控制程序在写盘
中途被杀也不会留下半截 JSON。
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from .engine import RehearsalEngine


def _atomic_write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(data, stream, ensure_ascii=False, indent=2, sort_keys=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp_name, path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise


class Store:
    def __init__(self, directory: str | Path):
        self.dir = Path(directory)
        self.state_path = self.dir / "state.json"

    def save(self, engine: "RehearsalEngine") -> dict[str, str]:
        self.dir.mkdir(parents=True, exist_ok=True)
        state = engine.to_state()
        _atomic_write_json(self.state_path, state)
        _atomic_write_json(self.dir / "egress_book.json", engine.egress.to_dict())
        control_book = {
            "clock_ms": engine.clock,
            "workers": {
                name: {
                    "pool": w.pool, "variant": w.variant, "slots": w.slots,
                    "work_capacity": w.work_capacity, "up": w.up,
                    "down_at_ms": w.down_at_ms, "up_at_ms": w.up_at_ms,
                    "bound_used": engine.capacity.worker_bound_used(name),
                    "observed_used": engine.capacity.worker_observed_used(name),
                    "slots_used": engine.capacity.worker_slots_used(name),
                }
                for name, w in engine.config.workers.items()
            },
            "live_charges": [a.to_dict() for a in engine.capacity.charges.values()],
            "loans": [l.to_dict() for l in engine.ledger.loans],
        }
        _atomic_write_json(self.dir / "control_book.json", control_book)
        with (self.dir / "trace.jsonl").open("w", encoding="utf-8") as stream:
            for event in engine.trace:
                stream.write(json.dumps(event, ensure_ascii=False) + "\n")
        return {
            "state": str(self.state_path),
            "control_book": str(self.dir / "control_book.json"),
            "egress_book": str(self.dir / "egress_book.json"),
            "trace": str(self.dir / "trace.jsonl"),
        }

    def load_state(self) -> dict[str, Any]:
        return json.loads(self.state_path.read_text(encoding="utf-8"))

    def load(self) -> "RehearsalEngine":
        from .engine import RehearsalEngine
        return RehearsalEngine.from_state(self.load_state())

    def exists(self) -> bool:
        return self.state_path.exists()


def reconcile_state(engine: "RehearsalEngine") -> dict[str, Any]:
    """对账控制账与出口账。

    权威方向：**出口账（调用方是否已经拿到结果）优先**。凡是 outbox 中已有
    ``result_claim`` 的请求，控制账必须标记为同一 delivered_attempt；重复
    交付由幂等键在 ``EgressBook.publish`` 处挡下。

    容量侧不做任何“账面释放”：缺少 ``released`` 证据的尝试一律列为
    ``awaiting_actual_release``，这正是“不能靠改账面配额假装有容量”的底线。
    """
    repairs: list[dict[str, Any]] = []
    mismatches: list[dict[str, Any]] = []

    # 1) 出口 -> 控制
    for key, receipt in engine.egress.outbox.items():
        if receipt.get("kind") != "result_claim":
            continue
        rid = receipt.get("request_id")
        no = receipt.get("attempt_no")
        req = engine.requests.get(rid)
        if req is None:
            mismatches.append({"type": "egress_delivery_for_unknown_request",
                               "request_id": rid, "key": key})
            continue
        if req.delivered_attempt is None:
            req.delivered_attempt = no
            from .model import RequestStatus
            req.status = RequestStatus.DELIVERED
            repairs.append({"type": "control_marked_delivered_from_egress",
                            "request_id": rid, "attempt_no": no})
        elif req.delivered_attempt != no:
            mismatches.append({"type": "delivered_attempt_conflict",
                               "request_id": rid,
                               "control_attempt": req.delivered_attempt,
                               "egress_attempt": no})

    # 2) 控制 -> 出口（控制声称交付但出口没有幂等凭据：只报告，不伪造）
    for req in engine.requests.values():
        if req.delivered_attempt is None:
            continue
        key = f"result:{req.request_id}"
        if key not in engine.egress.outbox:
            mismatches.append({"type": "control_delivered_without_egress_receipt",
                               "request_id": req.request_id,
                               "attempt_no": req.delivered_attempt})

    # 3) outbox 幂等键唯一性（dict 天然唯一，这里复核交付条数）
    keys = list(engine.egress.outbox.keys())
    duplicate_keys = sorted({k for k in keys if keys.count(k) > 1})

    # 4) 容量悬挂：没有 released 证据就仍在容量账上
    awaiting = [
        {"attempt_key": a.key, "request_id": a.request_id, "tenant": a.tenant,
         "worker": a.worker, "status": a.status.value, "bound_units": a.bound_units}
        for a in engine.capacity.charges.values()
    ]

    # 5) 同请求多个出口首段：只允许一个 delivered_attempt
    first_by_request: dict[str, list[int]] = {}
    for st in engine.egress.attempts.values():
        if st.first_received:
            first_by_request.setdefault(st.request_id, []).append(st.attempt_no)
    multi_first = {rid: nos for rid, nos in first_by_request.items() if len(nos) > 1}

    report = {
        "ok": not mismatches and not duplicate_keys,
        "clock_ms": engine.clock,
        "repairs": repairs,
        "mismatches": mismatches,
        "duplicate_idempotency_keys": duplicate_keys,
        "multiple_first_chunks": multi_first,
        "awaiting_actual_release": awaiting,
        "awaiting_count": len(awaiting),
        "outbox_size": len(engine.egress.outbox),
    }
    engine._trace("reconciled", **{k: v for k, v in report.items() if k != "ok"})
    return report


def reconcile_directory(directory: str | Path) -> dict[str, Any]:
    engine = Store(directory).load()
    report = engine.reconcile()
    _atomic_write_json(Path(directory) / "reconcile_report.json", report)
    Store(directory).save(engine)
    return report
