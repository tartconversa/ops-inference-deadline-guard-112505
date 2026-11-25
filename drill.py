"""顶层兼容入口，真实实现位于 :mod:`rehearsal` 包。"""
from rehearsal import *  # noqa: F401,F403
import rehearsal as _rehearsal  # noqa: F401
from rehearsal.engine import RehearsalEngine  # noqa: F401
from rehearsal.config import (RehearsalConfig, Policy, load_config,  # noqa: F401
                              config_from_materials)
from rehearsal.books import (CapacityBook, EgressBook, ReservationLedger, Loan)  # noqa: F401
from rehearsal.model import *  # noqa: F401,F403
from rehearsal.persist import Store, reconcile_state, reconcile_directory  # noqa: F401
from rehearsal.replay import (run_rehearsal, merge_timeline, RehearsalResult,  # noqa: F401
                              load_feed_rows)

Runtime = RehearsalEngine
Engine = RehearsalEngine
Scheduler = RehearsalEngine
Rehearsal = RehearsalEngine

Drill = RehearsalEngine
