"""顶层兼容入口，真实实现位于 :mod:`rehearsal` 包。"""
from rehearsal import *  # noqa: F401,F403
from rehearsal.engine import RehearsalEngine
Runtime = Engine = Scheduler = RehearsalEngine



def main(argv=None):
    from rehearsal.cli import main as _main
    return _main(argv)

if __name__ == "__main__":
    raise SystemExit(main())
