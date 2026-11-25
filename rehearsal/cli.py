"""值班员命令行：重演、持续接收、追查请求、查占用、对账。

示例
----
python -m rehearsal run --materials materials --out runs/baseline
python -m rehearsal replay --config configs/low_cost.json --out runs/cfg
python -m rehearsal trace c23 --run runs/baseline
python -m rehearsal occupancy --run runs/baseline --by tenant
python -m rehearsal reconcile --run runs/baseline
... | python -m rehearsal stream --state-dir state/live
python -m rehearsal serve --port 8765 --state-dir state/http
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Optional

from .engine import RehearsalEngine
from .replay import run_rehearsal
from .persist import Store, reconcile_directory


def _print(obj: Any) -> None:
    json.dump(obj, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")


def _load_engine(run_dir: str | Path) -> RehearsalEngine:
    path = Path(run_dir)
    if (path / "state.json").exists():
        return RehearsalEngine.restore(path)
    raise SystemExit(f"目录 {run_dir} 下没有 state.json，请先 run/replay")


def _compact_trace(report: dict) -> dict:
    return {
        "request_id": report["request_id"],
        "status": report["status"],
        "status_reason": report.get("status_reason"),
        "delivered_attempt": report.get("delivered_attempt"),
        "admission_promises": report.get("admission_promises"),
        "attempts": [
            {k: a[k] for k in ("key", "attempt_no", "worker", "pool", "variant",
                               "status", "estimated_units", "bound_units",
                               "observed_units") if k in a}
            for a in report.get("attempts", [])
        ],
        "feedback_decisions": report.get("feedback_decisions"),
    }


def cmd_run(args: argparse.Namespace) -> int:
    result = run_rehearsal(
        args.materials,
        config=args.config,
        out_dir=args.out,
        until_ms=args.until,
    )
    _print(result.summary())
    if args.out:
        sys.stderr.write(f"报告已写入 {Path(args.out).resolve()}\n")
    return 0


def cmd_trace(args: argparse.Namespace) -> int:
    eng = _load_engine(args.run)
    report = eng.request_report(args.request_id)
    if not report.get("found"):
        raise SystemExit(f"未知请求 {args.request_id}")
    if args.full:
        _print(report)
    else:
        _print(_compact_trace(report))
    return 0


def cmd_occupancy(args: argparse.Namespace) -> int:
    eng = _load_engine(args.run)
    snap = eng.occupancy(args.by)
    if args.summary:
        for name, g in snap["groups"].items():
            print(f"[{args.by}] {name}: working={g['working_attempts']} "
                  f"unresolved={g['unresolved_attempts']} "
                  f"bound={g['bound_units']} observed={g['observed_units']}/"
                  f"{g['work_capacity']} slots={g['slots_used']}/{g['slots_capacity']}")
        print("live requests:", snap["requests_with_live_charges"])
        print("ended requests:", snap["ended_user_requests"])
    else:
        _print(snap)
    return 0


def cmd_quota(args: argparse.Namespace) -> int:
    eng = _load_engine(args.run)
    _print(eng.tenant_quota(args.at))
    return 0


def cmd_reconcile(args: argparse.Namespace) -> int:
    report = reconcile_directory(args.run)
    _print(report)
    return 0 if report["ok"] else 2


def cmd_stream(args: argparse.Namespace) -> int:
    """持续从 stdin 读 JSONL 事件，每条输出决策 JSONL，并可落盘恢复。"""
    state_dir = Path(args.state_dir) if args.state_dir else None
    if state_dir and Store(state_dir).exists():
        eng = RehearsalEngine.restore(state_dir)
        sys.stderr.write(f"已从 {state_dir} 恢复控制程序状态，clock={eng.clock}\n")
    elif args.materials:
        from .config import RehearsalConfig
        eng = RehearsalEngine(RehearsalConfig.from_materials(args.materials))
    else:
        eng = RehearsalEngine()

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
            before = len(eng.trace)
            eng.ingest(event)
            new_events = eng.trace[before:]
            sys.stdout.write(json.dumps(
                {"accepted": True, "trace": new_events}, ensure_ascii=False) + "\n")
            sys.stdout.flush()
            if state_dir:
                Store(state_dir).save(eng)
        except Exception as exc:  # 流式入口不能因一条坏消息整体退出
            sys.stdout.write(json.dumps(
                {"accepted": False, "error": str(exc), "line": line},
                ensure_ascii=False) + "\n")
            sys.stdout.flush()
    if state_dir:
        Store(state_dir).save(eng)
    return 0


def cmd_serve(args: argparse.Namespace) -> int:
    from .server import serve
    serve(host=args.host, port=args.port, state_dir=args.state_dir,
          materials_dir=args.materials)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="rehearsal", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="按资料时间线重演")
    p_run.add_argument("--materials", default="materials")
    p_run.add_argument("--config", default=None, help="值班员新配置 JSON（覆盖资料配置）")
    p_run.add_argument("--out", default="runs/baseline")
    p_run.add_argument("--until", type=int, default=None)
    p_run.set_defaults(func=cmd_run)

    p_replay = sub.add_parser("replay", help="run 的别名：用新配置重演同样压力")
    p_replay.add_argument("--materials", default="materials")
    p_replay.add_argument("--config", default=None)
    p_replay.add_argument("--out", default="runs/replay")
    p_replay.add_argument("--until", type=int, default=None)
    p_replay.set_defaults(func=cmd_run)

    p_trace = sub.add_parser("trace", help="按请求追查承诺/交付/反馈改变的决策")
    p_trace.add_argument("request_id")
    p_trace.add_argument("--run", default="runs/baseline")
    p_trace.add_argument("--full", action="store_true")
    p_trace.set_defaults(func=cmd_trace)

    p_occ = sub.add_parser("occupancy", help="按 pool/tenant/worker 查占用")
    p_occ.add_argument("--run", default="runs/baseline")
    p_occ.add_argument("--by", choices=["pool", "tenant", "worker"], default="pool")
    p_occ.add_argument("--summary", action="store_true")
    p_occ.set_defaults(func=cmd_occupancy)

    p_q = sub.add_parser("quota", help="租户保留额/租借额度")
    p_q.add_argument("--run", default="runs/baseline")
    p_q.add_argument("--at", type=int, default=None)
    p_q.set_defaults(func=cmd_quota)

    p_rec = sub.add_parser("reconcile", help="两本账对账并修复出口->控制标记")
    p_rec.add_argument("--run", default="runs/baseline")
    p_rec.set_defaults(func=cmd_reconcile)

    p_stream = sub.add_parser("stream", help="从 stdin 持续接收 JSONL 事件")
    p_stream.add_argument("--state-dir", default=None)
    p_stream.add_argument("--materials", default=None)
    p_stream.set_defaults(func=cmd_stream)

    p_serve = sub.add_parser("serve", help="启动标准库 HTTP 事件入口")
    p_serve.add_argument("--host", default="127.0.0.1")
    p_serve.add_argument("--port", type=int, default=8765)
    p_serve.add_argument("--state-dir", default=None)
    p_serve.add_argument("--materials", default=None)
    p_serve.set_defaults(func=cmd_serve)
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
