"""领域模型、状态枚举与成本换算。

全部使用标准库；时间单位为演练时钟毫秒，成本单位与资料中的 ``units`` 一致。
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Optional


def ceil_half(units: float | int) -> int:
    """降级 small 时使用的“向上取整的一半”。

    资料口径：估计成本与 ``units_ceiling`` 都分别向上取整减半，因此
    ``ceil_half(43) == 22``、``ceil_half(57) == 29``。
    """
    return (int(units) + 1) // 2


def variant_cost(units: float | int, variant: str) -> int:
    """请求成本在目标 variant 下的记账值。

    ``full`` 同值；``small`` 向上取整减半。fleet 中 full/small 同属 text 族。
    """
    if variant == "small":
        return ceil_half(units)
    return int(units)


class RequestStatus(str, Enum):
    """逻辑请求的对外状态。"""

    QUEUED = "queued"            # 已接收，等待容量/可行性
    ADMITTED = "admitted"        # 已接纳，至少有一个尝试在兑现承诺
    REJECTED = "rejected"        # 拒绝（容量/额度/截止时间不可行）
    CANCELLED = "cancelled"     # 调用方主动取消
    DELIVERED = "delivered"      # 已出现对调用方的首段交付证据，结果被锁定
    COMPLETED = "completed"      # sink_finished，完整交付得到确认
    FAILED = "failed"            # 所有尝试结束且无法在截止前给出可采信结果

    @classmethod
    def parse(cls, value: Any) -> "RequestStatus":
        if isinstance(value, RequestStatus):
            return value
        return cls(str(value))


class AttemptStatus(str, Enum):
    """单次尝试的内部状态。

    容量账上的占用只有在收到 ``released`` 证据后才真正移除；``finished`` /
    ``closed_empty`` 只说明出口侧结束，不代表副本已经释放卡槽。
    """

    PREPARED = "prepared"              # 备用尝试：已预留容量，但尚未向副本发起
    DISPATCHED = "dispatched"          # 已发起，尚未收到 started
    STARTED = "started"                # 收到 started
    TIMED_OUT = "timed_out"            # 观察端超时（仍可能有晚到首段/终止）
    CLOSED_EMPTY = "closed_empty"      # 出口原子关闭并确认没有首段、不再接收
    FINISHED = "finished"              # 出口确认完整交付
    RELEASED = "released"              # 副本已退出：容量真正释放
    NODE_DOWN = "node_down"            # 所在副本通告故障，尝试影响范围之内
    CANCEL_REQUESTED = "cancel_requested"  # 已通知取消，等待 released 证据

    @property
    def holds_charge(self) -> bool:
        """该状态是否仍在容量账上持有预留/占用。"""
        return self is not AttemptStatus.RELEASED


class CallbackEvent(str, Enum):
    STARTED = "started"
    USAGE = "usage"
    TIMEOUT = "timeout"
    FIRST_CHUNK = "first_chunk"
    RELEASED = "released"
    SINK_CLOSED_EMPTY = "sink_closed_empty"
    SINK_FINISHED = "sink_finished"
    NODE_DOWN = "node_down"
    NODE_UP = "node_up"


# 仍可能产生交付/仍占资源的“在制”尝试状态（用于占用查询）。
WORKING_STATUSES = frozenset({
    AttemptStatus.PREPARED,
    AttemptStatus.DISPATCHED,
    AttemptStatus.STARTED,
    AttemptStatus.TIMED_OUT,
    AttemptStatus.CANCEL_REQUESTED,
})

# 已被出口确认不会再产出、但可能尚未 released 的悬挂状态。
UNRESOLVED_STATUSES = frozenset({
    AttemptStatus.TIMED_OUT,
    AttemptStatus.NODE_DOWN,
    AttemptStatus.CLOSED_EMPTY,
    AttemptStatus.FINISHED,
    AttemptStatus.CANCEL_REQUESTED,
})


@dataclass
class Worker:
    worker: str
    pool: str
    slots: int
    work_capacity: int
    variant: str
    p95_ms: int
    return_ms: int
    up: bool = True
    down_at_ms: Optional[int] = None
    up_at_ms: Optional[int] = None

    @classmethod
    def from_row(cls, row: dict) -> "Worker":
        return cls(
            worker=row["worker"],
            pool=row["pool"],
            slots=int(row["slots"]),
            work_capacity=int(row["work_capacity"]),
            variant=row["variant"],
            p95_ms=int(row["p95_ms"]),
            return_ms=int(row["return_ms"]),
        )


@dataclass
class Reservation:
    tenant: str
    pool: str
    model_family: str
    start_ms: int
    end_ms: int
    units: int

    @classmethod
    def from_row(cls, row: dict) -> "Reservation":
        return cls(
            tenant=row["tenant"],
            pool=row["pool"],
            model_family=row["model_family"],
            start_ms=int(row["start_ms"]),
            end_ms=int(row["end_ms"]),
            units=int(row["units"]),
        )

    def active_at(self, at_ms: int) -> bool:
        return self.start_ms <= at_ms < self.end_ms


@dataclass
class Request:
    request_id: str
    tenant: str
    model_family: str
    at_ms: int
    deadline_ms: int
    units: int
    allow_small: bool
    units_ceiling: int
    status: RequestStatus = RequestStatus.QUEUED
    status_reason: Optional[str] = None
    attempt_numbers: list[int] = field(default_factory=list)
    delivered_attempt: Optional[int] = None
    created_at_ms: int = 0
    decided_at_ms: Optional[int] = None
    finished_at_ms: Optional[int] = None

    @classmethod
    def from_row(cls, row: dict) -> "Request":
        at = int(row["at_ms"])
        return cls(
            request_id=row["request_id"],
            tenant=row["tenant"],
            model_family=row["model_family"],
            at_ms=at,
            deadline_ms=int(row["deadline_ms"]),
            units=int(row["units"]),
            allow_small=bool(row["allow_small"]),
            units_ceiling=int(row["units_ceiling"]),
            created_at_ms=at,
        )

    @property
    def full_p95_budget_ms(self) -> int:
        return max(0, self.deadline_ms - self.at_ms)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["status"] = self.status.value
        return data


@dataclass
class Promise:
    """接纳时向请求做出的容量/交付承诺，供事后追查。"""

    at_ms: int
    request_id: str
    attempt_no: int
    worker: str
    pool: str
    variant: str
    estimated_units: int
    bound_units: int
    slots: int
    p95_ms: int
    deadline_ms: int
    projected_finish_ms: int
    deadline_slack_ms: int
    reservation_slice: dict[str, int] = field(default_factory=dict)
    prepared: bool = False
    note: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class Attempt:
    key: str                 # f"{request_id}#{attempt_no}"
    request_id: str
    attempt_no: int
    tenant: str
    worker: str
    pool: str
    variant: str
    estimated_units: int
    bound_units: int
    status: AttemptStatus = AttemptStatus.PREPARED
    observed_units: Optional[int] = None
    started_at_ms: Optional[int] = None
    dispatched_at_ms: Optional[int] = None
    released_at_ms: Optional[int] = None
    release_observed_at_ms: Optional[int] = None
    prepared: bool = False
    is_probe: bool = False
    promise: Optional[dict] = None
    # 该次尝试的容量成本由哪些额度层承担：reserved/borrowed/spare
    slice: dict[str, int] = field(default_factory=lambda: {
        "reserved": 0, "borrowed": 0, "spare": 0})

    @property
    def charge_units(self) -> int:
        """当前实际占用估计：有反馈用反馈值，否则用估计值。"""
        return self.observed_units if self.observed_units is not None else self.estimated_units

    @property
    def holds_charge(self) -> bool:
        return self.status.holds_charge

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["status"] = self.status.value
        data["charge_units"] = self.charge_units
        data["holds_charge"] = self.holds_charge
        return data


def attempt_key(request_id: str, attempt_no: int) -> str:
    return f"{request_id}#{attempt_no}"
