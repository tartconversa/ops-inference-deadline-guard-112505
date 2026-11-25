"""读取本地演练资料，合并到达时间线，驱动引擎并用新配置重演。

时间线按 ``at_ms``（控制面收到时刻）稳定排序后顺序喂给引擎，保证晚到回调
（例如 c21 先见 timeout、后见 first_chunk）在决策时确实尚未可知。
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from .config import RehearsalConfig
from .engine import RehearsalEngine
from .persist import Store, _atomic_write_json


def merge_timeline(
    request_rows: list[dict[str, Any]],
    callback_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for row in request_rows:
        events.append({"at_ms": int(row["at_ms"]), "kind": "request", "row": row})
    for row in callback_rows:
        events.append({"at_ms": int(row["at_ms"]), "kind": "callback", "row": row})
    # 稳定排序：同 at_ms 时保留“请求/回调各自资料顺序 + 合并顺序”。
    events.sort(key=lambda e: e["at_ms"])
    return events


@dataclass
class RehearsalResult:
    engine: RehearsalEngine
    config: RehearsalConfig
    events_processed: int = 0
    out_dir: Optional[Path] = None
    reconcile_report: dict[str, Any] = field(default_factory=dict)

    # 便捷透传
    def trace(self, request_id: str) -> dict[str, Any]:
        return self.engine.request_report(request_id)

    def occupancy(self, by: str = "pool") -> dict[str, Any]:
        return self.engine.occupancy(by)

    @property
    def requests(self) -> dict[str, Any]:
        return self.engine.requests

    def summary(self) -> dict[str, Any]:
        eng = self.engine
        return {
            "config_name": self.config.name,
            "clock_ms": eng.clock,
            "events_processed": self.events_processed,
            "request_status_counts": eng._status_counts(),
            "requests": {
                rid: {
                    "status": r.status.value,
                    "reason": r.status_reason,
                    "attempts": r.attempt_numbers,
                    "delivered_attempt": r.delivered_attempt,
                }
                for rid, r in eng.requests.items() if r.tenant != "__probe__"
            },
            "live_charge_count": len(eng.capacity.charges),
            "trace_events": len(eng.trace),
            "reconcile_ok": self.reconcile_report.get("ok"),
        }


def _resolve_config(
    config: Optional[RehearsalConfig | dict | str],
    materials_dir: str | Path,
) -> RehearsalConfig:
    if isinstance(config, RehearsalConfig):
        return config
    base = RehearsalConfig.from_materials(materials_dir)
    if config is None:
        return base
    if isinstance(config, (str, Path)):
        path = Path(config)
        if path.exists():
            override = RehearsalConfig.from_json(path)
            return base.overlay(override, merge=True)
        # 也允许直接传 JSON 字符串
        return base.overlay(json.loads(str(config)), merge=True)
    if isinstance(config, dict):
        return base.overlay(config, merge=bool(config.get("merge", True)))
    raise TypeError(f"不支持的 config 类型：{type(config)}")


def load_feed_rows(materials_dir: str | Path, name: str) -> list[dict[str, Any]]:
    import sys
    root = Path(materials_dir)
    repo_root = root.resolve().parent if root.name == "materials" else root.resolve()
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from materials import read_feed  # type: ignore
    return list(read_feed(name, root))


def run_rehearsal(
    materials_dir: str | Path = "materials",
    *,
    config: Optional[RehearsalConfig | dict | str] = None,
    out_dir: Optional[str | Path] = None,
    until_ms: Optional[int] = None,
    write: bool = True,
    state_dir: Optional[str | Path] = None,
) -> RehearsalResult:
    """按资料重演一遍压力。

    :param materials_dir: 资料目录（fleet/requests/callbacks/reservations）
    :param config: 新配置：RehearsalConfig / JSON 路径 / 覆盖 dict / None
    :param out_dir: 若给出，写出全套 JSON/JSONL 报告
    :param until_ms: 重演结束后把虚拟时钟推进到该时刻（触发超时拒绝/失败）；
                     默认停在最后一条事件的 ``at_ms``，保留“仍在工作”的尝试。
    :param state_dir: 若给出，额外写可恢复状态（默认等同 out_dir）。
    """
    materials_dir = Path(materials_dir)
    cfg = _resolve_config(config, materials_dir)

    request_rows = load_feed_rows(materials_dir, "requests")
    callback_rows = load_feed_rows(materials_dir, "callbacks")
    timeline = merge_timeline(request_rows, callback_rows)

    engine = RehearsalEngine(cfg)
    processed = 0
    for item in timeline:
        row = item["row"]
        if item["kind"] == "request":
            if row["op"] == "submit":
                engine.submit_request(row)
            else:
                engine.cancel_request(row=row)
        else:
            engine.handle_callback(row)
        processed += 1

    if until_ms is not None:
        engine.advance(int(until_ms))

    report = engine.reconcile()
    result = RehearsalResult(engine=engine, config=cfg, events_processed=processed,
                             reconcile_report=report)

    target = Path(out_dir) if out_dir else (Path(state_dir) if state_dir else None)
    if write and target is not None:
        write_outputs(result, target)
    return result


def write_outputs(result: RehearsalResult, out_dir: str | Path) -> Path:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    eng = result.engine

    _atomic_write_json(out / "summary.json", result.summary())
    _atomic_write_json(out / "requests.json",
                       [eng.request_report(rid) for rid in sorted(eng.requests)
                        if eng.requests[rid].tenant != "__probe__"
                        or rid.startswith("__probe__")])
    _atomic_write_json(out / "occupancy_pool.json", eng.occupancy("pool"))
    _atomic_write_json(out / "occupancy_tenant.json", eng.occupancy("tenant"))
    _atomic_write_json(out / "occupancy_worker.json", eng.occupancy("worker"))
    _atomic_write_json(out / "tenant_quota.json", eng.tenant_quota())
    _atomic_write_json(out / "reconcile_report.json", result.reconcile_report)
    _atomic_write_json(out / "config_used.json", result.config.to_dict())

    with (out / "trace.jsonl").open("w", encoding="utf-8") as stream:
        for event in eng.trace:
            stream.write(json.dumps(event, ensure_ascii=False) + "\n")
    with (out / "actions.jsonl").open("w", encoding="utf-8") as stream:
        for action in eng.actions:
            stream.write(json.dumps(action, ensure_ascii=False) + "\n")

    # state.json / control_book.json / egress_book.json
    Store(out).save(eng)
    result.out_dir = out
    return out
