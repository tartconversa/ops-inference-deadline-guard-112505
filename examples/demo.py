"""端到端 API 演示：重演资料、追 c23、查占用、重启对账。

运行：python examples/demo.py
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from rehearsal import run_rehearsal, RehearsalEngine  # noqa: E402



def main() -> None:
    out = Path(tempfile.mkdtemp(prefix="rehearsal-demo-"))
    result = run_rehearsal(ROOT / "materials", out_dir=out)
    print("== summary ==")
    print(json.dumps(result.summary(), ensure_ascii=False, indent=2))

    print("\n== c23 承诺（admission promises） ==")
    for p in result.trace("c23")["admission_promises"]:
        print(f"  t={p['at_ms']} #{p['attempt_no']} -> {p['worker']}/"
              f"{p['variant']} bound={p['bound_units']} finish~{p['projected_finish_ms']} "
              f"slice={p['reservation_slice']} prepared={p['prepared']}")

    print("\n== pool occupancy summary ==")
    for name, g in result.occupancy("pool")["groups"].items():
        print(f"  {name}: working={g['working_attempts']} unresolved={g['unresolved_attempts']} "
              f"bound={g['bound_units']}/{g['work_capacity']} slots={g['slots_used']}/{g['slots_capacity']}")

    print("\n== restart + reconcile ==")
    restored = RehearsalEngine.restore(out)
    print("  charges after restart:", len(restored.capacity.charges))
    print("  reconcile ok:", restored.reconcile()["ok"])
    print("  artifacts in", out)


if __name__ == "__main__":
    main()
