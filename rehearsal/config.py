"""演练配置：fleet 副本、reservations 时段保留额与调度策略开关。

值班员可以提供一份新的 JSON 配置重演同一批 requests/callbacks。配置支持
只给覆盖项（``merge=True`` 时按 worker/保留额主键合并到资料配置），也可以
完整替换。
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Optional

from .model import Worker, Reservation


@dataclass
class Policy:
    # 接纳时是否用 units_ceiling 作为安全预留（防止多个任务同时增长压垮池）
    reserve_ceiling: bool = True
    # 截止时间可行性是否使用 p95 估计
    deadline_check: bool = True
    # 首次接纳的降级策略：
    #  deadline = 只要等主池 full 仍能在 latest_start 前接纳，就排队等 full，
    #             超过 latest_start 才使用 small/备用池（避免无谓占用低余量备用池）；
    #  immediate = 只要有任何截止可行的 placement（含 small）就立即接纳。
    downgrade_mode: str = "deadline"
    # 严格模式：now + p95 > deadline 即判定不可行；否则要求 <=
    deadline_strict: bool = False
    # 排队请求超过 latest_start 后是否自动拒绝
    auto_reject_late: bool = True
    # 超时后是否立即“准备”备用尝试（预留容量但不发起、不交付）
    prepare_backup_on_timeout: bool = True
    # 只有在出口确认 old attempt 空且关闭后才发起备用尝试
    launch_backup_after_empty_close: bool = True
    # 原租户索要被借出的保留额时，是否允许通知借用者让出（仍等 released 实证）
    preempt_borrowers: bool = True
    # node_up 后是否自动发恢复探测（探测真实占容量）
    probe_on_up: bool = False
    # 恢复探测的成本/槽位
    probe_units: int = 1
    probe_slots: int = 1
    # 同一逻辑请求最多发起的尝试数
    max_attempts: int = 3
    # 是否允许把空闲保留额借给其他租户
    lend_idle_reservation: bool = True
    # 若为 true，租户安全记账不得超过 自有+可借 额度（默认 false：
    # 超出部分记入 spare 池间未保留容量，仍受 worker 物理上限约束）
    reservation_is_hard_cap: bool = False
    # 低成本 variant 名称（新配置预留的低成本模型）
    low_cost_variant: str = "small"
    # 标记为“专门预留的低成本池”的 pool 名（空表示所有 small variant）
    low_cost_pools: list[str] = field(default_factory=list)
    # 选副本时是否优先同租户有保留额的池
    prefer_reserved_pool: bool = True

    @classmethod
    def from_dict(cls, data: Optional[dict]) -> "Policy":
        if not data:
            return cls()
        known = {f for f in cls.__dataclass_fields__}  # type: ignore[attr-defined]
        return cls(**{k: v for k, v in dict(data).items() if k in known})

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class RehearsalConfig:
    workers: dict[str, Worker] = field(default_factory=dict)
    reservations: list[Reservation] = field(default_factory=list)
    policy: Policy = field(default_factory=Policy)
    name: str = "baseline"

    # ---- 构造 ----
    @classmethod
    def from_materials(
        cls,
        directory: str | Path = "materials",
        *,
        policy: Optional[dict | Policy] = None,
    ) -> "RehearsalConfig":
        # 延迟导入，避免包初始化阶段依赖仓库根目录的 materials.py
        import sys

        root = Path(directory)
        repo_root = root.resolve().parent if root.name == "materials" else root.resolve()
        if str(repo_root) not in sys.path:
            sys.path.insert(0, str(repo_root))
        from materials import read_feed  # type: ignore

        workers = {r["worker"]: Worker.from_row(r) for r in read_feed("fleet", root)}
        reservations = [Reservation.from_row(r) for r in read_feed("reservations", root)]
        pol = policy if isinstance(policy, Policy) else Policy.from_dict(policy)
        return cls(workers=workers, reservations=reservations, policy=pol, name="materials")

    @classmethod
    def from_dict(cls, data: dict) -> "RehearsalConfig":
        workers = {
            row["worker"]: Worker.from_row(row)
            for row in data.get("fleet", data.get("workers", []))
        }
        reservations = [
            Reservation.from_row(row)
            for row in data.get("reservations", [])
        ]
        policy = Policy.from_dict(data.get("policy"))
        return cls(
            workers=workers,
            reservations=reservations,
            policy=policy,
            name=data.get("name", "config"),
        )

    @classmethod
    def from_json(cls, path: str | Path) -> "RehearsalConfig":
        return cls.from_dict(json.loads(Path(path).read_text(encoding="utf-8")))

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "fleet": [w.__dict__ for w in self.workers.values()],
            "reservations": [r.__dict__ for r in self.reservations],
            "policy": self.policy.to_dict(),
        }

    # ---- 覆盖/合并（值班员新配置重演） ----
    def overlay(self, override: dict | "RehearsalConfig", *, merge: bool = True) -> "RehearsalConfig":
        if isinstance(override, RehearsalConfig):
            other = override
        else:
            other = RehearsalConfig.from_dict(override)
        if not merge:
            return other
        workers = dict(self.workers)
        workers.update(other.workers)
        # 保留额按完整业务主键合并
        res_key = lambda r: (r.tenant, r.pool, r.model_family, r.start_ms, r.end_ms)
        reservations = {res_key(r): r for r in self.reservations}
        for r in other.reservations:
            reservations[res_key(r)] = r
        policy_data = self.policy.to_dict()
        policy_data.update(other.policy.to_dict() if isinstance(override, RehearsalConfig)
                           else (override.get("policy", {}) if isinstance(override, dict) else {}))
        return RehearsalConfig(
            workers=workers,
            reservations=list(reservations.values()),
            policy=Policy.from_dict(policy_data),
            name=other.name if other.name != "config" else self.name + "+overlay",
        )

    # ---- 查询 ----
    def pools(self) -> dict[str, list[Worker]]:
        out: dict[str, list[Worker]] = {}
        for w in self.workers.values():
            out.setdefault(w.pool, []).append(w)
        return out

    def pool_return_ms(self, pool: str) -> Optional[int]:
        vals = [w.return_ms for w in self.workers.values() if w.pool == pool]
        return min(vals) if vals else None

    def active_reservations(
        self, tenant: str, pool: str, family: str, at_ms: int
    ) -> list[Reservation]:
        return [
            r for r in self.reservations
            if r.tenant == tenant and r.pool == pool
            and r.model_family == family and r.active_at(at_ms)
        ]

    def reserved_units(self, tenant: str, pool: str, family: str, at_ms: int) -> int:
        return sum(r.units for r in self.active_reservations(tenant, pool, family, at_ms))


def config_from_materials(directory: str | Path = "materials", **kwargs) -> RehearsalConfig:
    """便捷函数，兼容常见命名。"""
    return RehearsalConfig.from_materials(directory, **kwargs)


def load_config(
    path: Optional[str | Path] = None,
    *,
    materials_dir: str | Path = "materials",
    merge: bool = True,
) -> RehearsalConfig:
    """加载配置：不给 path 时完全使用资料配置；给 path 时按 merge 叠加。"""
    base = RehearsalConfig.from_materials(materials_dir)
    if path is None:
        return base
    override = RehearsalConfig.from_json(path)
    return base.overlay(override, merge=merge)
