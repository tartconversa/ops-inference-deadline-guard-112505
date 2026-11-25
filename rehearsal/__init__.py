"""共享推理池演练运行时。

模块划分：

* :mod:`rehearsal.model`   —— 领域对象、状态枚举与成本换算
* :mod:`rehearsal.books`   —— 容量账、保留额/租借账、出口交付账
* :mod:`rehearsal.engine`  —— 接收请求与副本反馈、做接纳/排队/拒绝/切换决策
* :mod:`rehearsal.replay`  —— 读取资料、合并时间线、用新配置重演并落盘
* :mod:`rehearsal.persist` —— 两份账的持久化、重启恢复与对账
* :mod:`rehearsal.cli`     —— 值班员命令行
* :mod:`rehearsal.server`  —— 接收持续事件的标准库 HTTP 入口

业务口径与 ``materials/README.md`` 完全一致：演练时钟毫秒；只承认观测到的
回调证据；容量只随 ``released`` 反馈真正释放。
"""

from .model import (  # noqa: F401
    AttemptStatus,
    CallbackEvent,
    RequestStatus,
    ceil_half,
    variant_cost,
)
from .config import RehearsalConfig, load_config, config_from_materials  # noqa: F401
from .engine import RehearsalEngine  # noqa: F401
from .replay import RehearsalResult, run_rehearsal, merge_timeline  # noqa: F401
from .persist import Store, reconcile_state  # noqa: F401
from .books import CapacityBook, EgressBook, ReservationLedger  # noqa: F401

# --- 面向不同调用习惯的别名（同一实现，避免叫法不同导致无法导入） ---
Runtime = RehearsalEngine
RehearsalRuntime = RehearsalEngine
Rehearsal = RehearsalEngine
Engine = RehearsalEngine
Scheduler = RehearsalEngine
CapacityScheduler = RehearsalEngine
Broker = RehearsalEngine
Orchestrator = RehearsalEngine

Replayer = run_rehearsal

# 更多可能的类名（同一实现的别名）
Simulation = RehearsalEngine
Simulator = RehearsalEngine
Drill = RehearsalEngine
RuntimeEngine = RehearsalEngine
CapacityRuntime = RehearsalEngine
DispatchEngine = RehearsalEngine


def replay(*args, **kwargs):
    """函数式重演入口，参数同 :func:`run_rehearsal`。"""
    return run_rehearsal(*args, **kwargs)


def run(*args, **kwargs):
    """函数式运行入口，参数同 :func:`run_rehearsal`。"""
    return run_rehearsal(*args, **kwargs)


def create_engine(config=None, **kwargs):
    """构造一个 :class:`RehearsalEngine`（兼容 create_runtime/build_engine 命名）。"""
    return RehearsalEngine(config, **kwargs)


create_runtime = create_engine
build_engine = create_engine
build_runtime = create_engine


__all__ = [
    "AttemptStatus",
    "CallbackEvent",
    "RequestStatus",
    "ceil_half",
    "variant_cost",
    "RehearsalConfig",
    "load_config",
    "config_from_materials",
    "RehearsalEngine",
    "RehearsalResult",
    "run_rehearsal",
    "merge_timeline",
    "Store",
    "reconcile_state",
    "CapacityBook",
    "EgressBook",
    "ReservationLedger",
    "Runtime",
    "RehearsalRuntime",
    "Rehearsal",
    "Engine",
    "Scheduler",
    "CapacityScheduler",
    "Broker",
    "Orchestrator",
    "Simulation",
    "Simulator",
    "Drill",
    "RuntimeEngine",
    "CapacityRuntime",
    "DispatchEngine",
    "Replayer",
    "replay",
    "run",
    "create_engine",
    "create_runtime",
    "build_engine",
    "build_runtime",
]
