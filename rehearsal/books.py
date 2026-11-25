"""容量账、保留额/租借账与出口交付账。

设计要点
========

* 容量账只承认两类占用：接纳时按 ``units_ceiling`` 预留的安全上界
  （``bound``），以及副本反馈修正的实际占用（``observed``）。``released``
  证据到达前，取消/超时/节点掉线都不会移除占用。
* 保留额账把每次接纳的 bound 拆成 ``reserved``（本租户有效保留额）、
  ``borrowed``（其他租户当前空闲、正式借出的保留额）和 ``spare``（池中未
  保留的空闲容量，尽力而为）三层；租借与催还单独留痕。
* 出口账按 attempt 记录“是否已收到首段”“是否已停止接收”，并用幂等键
  outbox 保证同一逻辑结果不会向调用方重复交付。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field, asdict
from typing import Any, Optional

from .model import Attempt, attempt_key


# ---------------------------------------------------------------------------
# 容量账
# ---------------------------------------------------------------------------
class CapacityBook:
    def __init__(self, config):
        self.config = config
        # 仍持有容量的尝试：key -> Attempt（attempt.status 必须 holds_charge）
        self.charges: dict[str, Attempt] = {}

    # ---- 基础聚合 ----
    def worker(self, worker_name: str):
        return self.config.workers[worker_name]

    def worker_charges(self, worker_name: str) -> list[Attempt]:
        return [a for a in self.charges.values() if a.worker == worker_name]

    def worker_bound_used(self, worker_name: str) -> int:
        return sum(a.bound_units for a in self.worker_charges(worker_name))

    def worker_observed_used(self, worker_name: str) -> int:
        return sum(a.charge_units for a in self.worker_charges(worker_name))

    def worker_slots_used(self, worker_name: str) -> int:
        return sum(max(1, getattr(a, "slots", 1) or 1) for a in self.worker_charges(worker_name))

    def pool_charges(self, pool: str) -> list[Attempt]:
        return [a for a in self.charges.values() if a.pool == pool]

    def tenant_charges(self, tenant: str) -> list[Attempt]:
        return [a for a in self.charges.values() if a.tenant == tenant]

    # ---- 可行性 / 预留 / 释放 ----
    def can_fit(
        self,
        worker_name: str,
        bound_units: int,
        slots: int = 1,
        *,
        ignore_key: Optional[str] = None,
        require_up: bool = True,
    ) -> bool:
        w = self.config.workers[worker_name]
        if require_up and not w.up:
            return False
        used_bound = sum(
            a.bound_units for a in self.worker_charges(worker_name)
            if a.key != ignore_key
        )
        used_slots = sum(
            1 for a in self.worker_charges(worker_name) if a.key != ignore_key
        )
        return (
            used_bound + int(bound_units) <= w.work_capacity
            and used_slots + int(slots) <= w.slots
        )

    def add(self, attempt: Attempt) -> None:
        if attempt.key in self.charges:
            raise ValueError(f"容量账中已存在尝试 {attempt.key}")
        self.charges[attempt.key] = attempt

    def release(self, key: str) -> Optional[Attempt]:
        return self.charges.pop(key, None)

    def get(self, key: str) -> Optional[Attempt]:
        return self.charges.get(key)

    # ---- 占用快照 ----
    def occupancy(self, by: str = "worker", at_ms: Optional[int] = None) -> dict[str, Any]:
        if by not in ("worker", "pool", "tenant"):
            raise ValueError("by 只能是 worker/pool/tenant")
        groups: dict[str, dict[str, Any]] = {}

        def bucket(name: str, *, capacity: Optional[int] = None, slots: Optional[int] = None):
            b = groups.setdefault(name, {
                "name": name,
                "live_attempts": 0,
                "working_attempts": 0,
                "unresolved_attempts": 0,
                "probe_attempts": 0,
                "bound_units": 0,
                "observed_units": 0,
                "slots_used": 0,
                "work_capacity": capacity,
                "slots_capacity": slots,
                "up": True,
                "attempts": [],
            })
            return b

        if by == "worker":
            for name, w in self.config.workers.items():
                b = bucket(name, capacity=w.work_capacity, slots=w.slots)
                b["pool"] = w.pool
                b["variant"] = w.variant
                b["up"] = w.up
        for a in self.charges.values():
            if by == "worker":
                name = a.worker
            elif by == "pool":
                name = a.pool
            else:
                name = a.tenant
            b = bucket(name)
            b["live_attempts"] += 1
            b["bound_units"] += a.bound_units
            b["observed_units"] += a.charge_units
            b["slots_used"] += 1
            if a.is_probe:
                b["probe_attempts"] += 1
            if a.status.value in ("timed_out", "node_down", "closed_empty",
                                  "finished", "cancel_requested"):
                b["unresolved_attempts"] += 1
            else:
                b["working_attempts"] += 1
            b["attempts"].append({
                "key": a.key,
                "request_id": a.request_id,
                "attempt_no": a.attempt_no,
                "tenant": a.tenant,
                "worker": a.worker,
                "status": a.status.value,
                "bound_units": a.bound_units,
                "observed_units": a.charge_units,
                "is_probe": a.is_probe,
            })
        # 容量/使用率
        for name, b in groups.items():
            if b["work_capacity"]:
                b["bound_utilization"] = round(b["bound_units"] / b["work_capacity"], 4)
            if by == "pool":
                workers = [w for w in self.config.workers.values() if w.pool == name]
                b["work_capacity"] = sum(w.work_capacity for w in workers)
                b["slots_capacity"] = sum(w.slots for w in workers)
                b["healthy_workers"] = [w.worker for w in workers if w.up]
                b["down_workers"] = [w.worker for w in workers if not w.up]
                b["up"] = all(w.up for w in workers) if workers else True
                b["bound_utilization"] = (
                    round(b["bound_units"] / b["work_capacity"], 4)
                    if b["work_capacity"] else None
                )
        return {"by": by, "at_ms": at_ms, "groups": groups}


# ---------------------------------------------------------------------------
# 保留额 / 租借账
# ---------------------------------------------------------------------------
@dataclass
class Loan:
    key: str
    lender: str
    borrower: str
    pool: str
    model_family: str
    units: int
    attempt_key: str
    created_at_ms: int
    reclaim_deadline_ms: Optional[int] = None
    reclaim_requested_at_ms: Optional[int] = None
    returned_at_ms: Optional[int] = None
    met_deadline: Optional[bool] = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class ReservationLedger:
    def __init__(self, config, capacity: CapacityBook):
        self.config = config
        self.capacity = capacity
        self.loans: list[Loan] = []

    def own_reserved(self, tenant: str, pool: str, family: str, at_ms: int) -> int:
        return self.config.reserved_units(tenant, pool, family, at_ms)

    def owner_live_bound(self, tenant: str, pool: str, family: str) -> int:
        family_variants = {
            w.variant for w in self.config.workers.values()
            if w.pool == pool and getattr(w, "model_family", "text") == family
        } or {"full", "small"}  # 资料中 full/small 均属 text 族
        return sum(
            a.bound_units for a in self.capacity.charges.values()
            if a.tenant == tenant and a.pool == pool and a.variant in family_variants
        )

    def idle_own_reservation(self, tenant: str, pool: str, family: str, at_ms: int) -> int:
        """该租户保留额中当前真正空闲的部分。

        空闲 = 有效保留额 − 自己在制尝试的 bound − 已经正式借给别人且尚未
        归还的 Loan 额度。已借出的部分不能重复出借。
        """
        reserved = self.own_reserved(tenant, pool, family, at_ms)
        used_self = sum(
            a.bound_units for a in self.capacity.tenant_charges(tenant)
            if a.pool == pool
        )
        lent_out = sum(
            l.units for l in self.active_loans(lender=tenant)
            if l.pool == pool and l.model_family == family
        )
        return max(0, reserved - used_self - lent_out)

    def borrowable_units(
        self, borrower: str, pool: str, family: str, at_ms: int
    ) -> list[dict[str, Any]]:
        if not self.config.policy.lend_idle_reservation:
            return []
        lenders = {
            r.tenant for r in self.config.reservations
            if r.pool == pool and r.model_family == family and r.tenant != borrower
            and r.active_at(at_ms)
        }
        return [
            {"lender": lender, "units": self.idle_own_reservation(lender, pool, family, at_ms)}
            for lender in sorted(lenders)
            if self.idle_own_reservation(lender, pool, family, at_ms) > 0
        ]

    def plan_slice(
        self,
        tenant: str,
        pool: str,
        family: str,
        at_ms: int,
        bound_units: int,
        attempt_key_: str,
    ) -> tuple[dict[str, int], list[Loan]]:
        """决定 bound 由 reserved/borrowed/spare 各承担多少，并生成 Loan。"""
        slice_ = {"reserved": 0, "borrowed": 0, "spare": 0}
        new_loans: list[Loan] = []
        remaining = int(bound_units)

        own_available = self.idle_own_reservation(tenant, pool, family, at_ms)
        take = min(remaining, own_available)
        slice_["reserved"] += take
        remaining -= take

        for offer in self.borrowable_units(tenant, pool, family, at_ms):
            if remaining <= 0:
                break
            take = min(remaining, offer["units"])
            if take <= 0:
                continue
            slice_["borrowed"] += take
            remaining -= take
            deadline = self.config.pool_return_ms(pool)
            new_loans.append(Loan(
                key=f"loan:{attempt_key_}:{offer['lender']}",
                lender=offer["lender"], borrower=tenant, pool=pool,
                model_family=family, units=take, attempt_key=attempt_key_,
                created_at_ms=at_ms,
                reclaim_deadline_ms=(at_ms + deadline) if deadline is not None else None,
            ))

        slice_["spare"] += remaining
        return slice_, new_loans

    def add_loans(self, loans: list[Loan]) -> None:
        self.loans.extend(loans)

    def active_loans(self, *, lender: Optional[str] = None) -> list[Loan]:
        return [
            l for l in self.loans
            if l.returned_at_ms is None and (lender is None or l.lender == lender)
        ]

    def reclaim(
        self, lender: str, pool: str, family: str, units: int, at_ms: int
    ) -> list[Loan]:
        """原租户索要被借出的保留额。

        返回需要让出的 Loan 列表；真正的容量可用时间仍以对应 attempt 的
        ``released`` 反馈为准，由 :meth:`mark_loan_returned_on_release` 核销。
        """
        need = units
        chosen: list[Loan] = []
        for loan in self.active_loans(lender=lender):
            if loan.pool != pool or loan.model_family != family:
                continue
            if need <= 0:
                break
            loan.reclaim_requested_at_ms = at_ms
            deadline = self.config.pool_return_ms(pool)
            loan.reclaim_deadline_ms = at_ms + deadline if deadline is not None else None
            chosen.append(loan)
            need -= loan.units
        return chosen

    def mark_released(self, attempt_key_: str, at_ms: int) -> list[Loan]:
        """尝试 released 时核销关联租借，并判定是否满足返还时限。"""
        affected = [l for l in self.loans if l.attempt_key == attempt_key_
                    and l.returned_at_ms is None]
        for loan in affected:
            loan.returned_at_ms = at_ms
            if loan.reclaim_deadline_ms is not None:
                loan.met_deadline = at_ms <= loan.reclaim_deadline_ms
        return affected

    def snapshot(self, at_ms: int) -> list[dict[str, Any]]:
        rows = []
        tenants = {r.tenant for r in self.config.reservations} | {a.tenant for a in self.capacity.charges.values()}
        pools = {r.pool for r in self.config.reservations} | {a.pool for a in self.capacity.charges.values()}
        families = {r.model_family for r in self.config.reservations} | {"text"}
        for tenant in sorted(tenants):
            for pool in sorted(pools):
                for family in sorted(families):
                    reserved = self.own_reserved(tenant, pool, family, at_ms)
                    if not reserved and not any(
                        a.tenant == tenant and a.pool == pool for a in self.capacity.charges.values()
                    ):
                        continue
                    live_bound = sum(
                        a.bound_units for a in self.capacity.charges.values()
                        if a.tenant == tenant and a.pool == pool
                    )
                    borrower_loans = [
                        l.to_dict() for l in self.active_loans()
                        if l.borrower == tenant and l.pool == pool
                        and l.model_family == family
                    ]
                    lender_loans = [
                        l.to_dict() for l in self.active_loans(lender=tenant)
                        if l.pool == pool and l.model_family == family
                    ]
                    rows.append({
                        "tenant": tenant,
                        "pool": pool,
                        "model_family": family,
                        "at_ms": at_ms,
                        "reserved_units": reserved,
                        "live_bound_units": live_bound,
                        "idle_reserved_units": max(0, reserved - live_bound),
                        "active_loans_as_borrower": borrower_loans,
                        "active_loans_as_lender": lender_loans,
                    })
        return rows


# ---------------------------------------------------------------------------
# 出口账（按 attempt 的首段/停止接收证据 + 幂等 outbox）
# ---------------------------------------------------------------------------
@dataclass
class EgressAttemptState:
    key: str
    request_id: str
    attempt_no: int
    first_received: bool = False
    first_received_at_ms: Optional[int] = None
    first_occurred_at_ms: Optional[int] = None
    sink: Optional[str] = None          # closed_empty / finished
    stopped: bool = False               # 已确认不再接收
    empty: bool = False                 # 关闭时确认没有首段
    finished: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class EgressBook:
    """出口/收件端账本。

    线程安全仅为将来的 HTTP 流式入口准备；演练本身是单线程虚拟时钟。
    """

    def __init__(self):
        self._lock = threading.RLock()
        self.attempts: dict[str, EgressAttemptState] = {}
        # 幂等键 -> 交付回执
        self.outbox: dict[str, dict[str, Any]] = {}
        self.deliveries: list[dict[str, Any]] = []
        self.connected: bool = True
        self.pending_acks: list[str] = []

    # ---- attempt 证据 ----
    def _state(self, request_id: str, attempt_no: int) -> EgressAttemptState:
        key = attempt_key(request_id, attempt_no)
        st = self.attempts.get(key)
        if st is None:
            st = EgressAttemptState(key=key, request_id=request_id, attempt_no=attempt_no)
            self.attempts[key] = st
        return st

    def has_first_chunk(self, request_id: str, attempt_no: Optional[int] = None) -> bool:
        with self._lock:
            if attempt_no is not None:
                st = self.attempts.get(attempt_key(request_id, attempt_no))
                return bool(st and st.first_received)
            return any(
                st.request_id == request_id and st.first_received
                for st in self.attempts.values()
            )

    def first_chunk_attempt(self, request_id: str) -> Optional[int]:
        for st in self.attempts.values():
            if st.request_id == request_id and st.first_received:
                return st.attempt_no
        return None

    def received_first_chunk(self, request_id: str, attempt_no: int) -> Optional[dict[str, Any]]:
        st = self.attempts.get(attempt_key(request_id, attempt_no))
        if st and st.first_received_at_ms is not None:
            return {"at_ms": st.first_received_at_ms, "occurred_ms": st.first_occurred_at_ms}
        return None

    def observe_first_chunk(
        self, request_id: str, attempt_no: int, at_ms: int, occurred_ms: Optional[int] = None
    ) -> dict[str, Any]:
        with self._lock:
            st = self._state(request_id, attempt_no)
            duplicate = st.first_received
            if not duplicate:
                st.first_received = True
                st.first_received_at_ms = at_ms
                st.first_occurred_at_ms = occurred_ms
            return {"duplicate": duplicate, "state": st.to_dict()}

    def confirm_sink(
        self, request_id: str, attempt_no: int, sink: str, at_ms: int,
        occurred_ms: Optional[int] = None,
    ) -> dict[str, Any]:
        with self._lock:
            st = self._state(request_id, attempt_no)
            repeated = st.sink == sink
            st.sink = sink
            st.stopped = True
            if sink == "sink_closed_empty":
                # 原子关闭：以“是否已收到首段”为准，empty 必须确实无首段。
                st.empty = not st.first_received
            elif sink == "sink_finished":
                st.finished = True
                st.empty = False
            return {"repeated": repeated, "state": st.to_dict()}

    def stopped_receiving(self, request_id: str, attempt_no: int) -> bool:
        st = self.attempts.get(attempt_key(request_id, attempt_no))
        return bool(st and st.stopped)

    def is_empty_and_closed(self, request_id: str, attempt_no: int) -> bool:
        """既确认没有交付首段、又确认不会再接收 —— 此后备用尝试才可交付。"""
        st = self.attempts.get(attempt_key(request_id, attempt_no))
        return bool(st and st.stopped and st.empty and not st.first_received)

    # ---- 对调用方交付（幂等） ----
    def publish(
        self,
        idempotency_key: str,
        payload: dict[str, Any],
        at_ms: int,
    ) -> dict[str, Any]:
        """记录一次对调用方的交付意图/事实。重复键返回原回执，不再交付。"""
        with self._lock:
            existing = self.outbox.get(idempotency_key)
            if existing is not None:
                return {"delivered": False, "duplicate": True, "receipt": existing}
            receipt = {
                "idempotency_key": idempotency_key,
                "at_ms": at_ms,
                "connected": self.connected,
                "acked": self.connected,
                **payload,
            }
            self.outbox[idempotency_key] = receipt
            self.deliveries.append(receipt)
            if not self.connected:
                self.pending_acks.append(idempotency_key)
            return {"delivered": self.connected, "duplicate": False, "receipt": receipt}

    def disconnect(self) -> None:
        self.connected = False

    def reconnect(self) -> list[dict[str, Any]]:
        """重连后重放未确认交付；幂等键保证调用方不会得到两份结果。"""
        with self._lock:
            self.connected = True
            flushed = []
            for key in list(self.pending_acks):
                receipt = self.outbox[key]
                receipt["acked"] = True
                receipt["flushed_after_reconnect_ms"] = receipt.get("_reconnect_at_ms")
                flushed.append(receipt)
            self.pending_acks.clear()
            return flushed

    # ---- 序列化 ----
    def to_dict(self) -> dict[str, Any]:
        with self._lock:
            return {
                "connected": self.connected,
                "attempts": {k: v.to_dict() for k, v in self.attempts.items()},
                "outbox": self.outbox,
                "deliveries": self.deliveries,
                "pending_acks": self.pending_acks,
            }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "EgressBook":
        book = cls()
        book.connected = data.get("connected", True)
        for k, v in data.get("attempts", {}).items():
            book.attempts[k] = EgressAttemptState(**v)
        book.outbox = dict(data.get("outbox", {}))
        book.deliveries = list(data.get("deliveries", []))
        book.pending_acks = list(data.get("pending_acks", []))
        return book
