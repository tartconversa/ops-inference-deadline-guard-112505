# 共享推理池演练运行时

纯 Python 3.11 标准库实现，不连接任何生产推理服务。它接收**持续到达的请求与副本反馈**，
按演练时钟毫秒做确定性调度；值班员可以拿一份新配置，对同一批 requests/callbacks
**重演同样的压力**，并按请求追查“承诺了什么、交付了什么、哪条反馈改变了下一步”。

```sh
python -m unittest discover -s tests -v          # 仓库原有资料测试
python -m unittest discover -s selftests -v       # 运行时行为自测（26 例）

python -m rehearsal run --materials materials --out runs/baseline
python -m rehearsal trace c23 --run runs/baseline
python -m rehearsal occupancy --run runs/baseline --by pool --summary
python -m rehearsal reconcile --run runs/baseline
python -m rehearsal replay --config configs/reserved_low_cost.json --out runs/lowcost
```

也可以 `python runtime.py run ...`（顶层 `runtime.py` 是兼容入口），或
`from rehearsal import RehearsalEngine, run_rehearsal`。

---

## 1. 它如何理解资料里的成本与时间

* 每个尝试同时记两个成本：
  * `estimated_units`：提交预估（full 用 `units`，small 用 `ceil(units/2)`）；
  * `bound_units`：本次尝试的安全上界（full 用 `units_ceiling`，small 用
    `ceil(units_ceiling/2)`）。副本保证不会突破上界。
* 接纳安全检查对所有在制尝试（含已预留的备用尝试、探测）的 **bound 求和**：
  `Σ bound ≤ worker.work_capacity` 且 `尝试数 ≤ slots`。因此多个任务同时从预估值
  增长到上界也不会压垮副本。`usage` 反馈只把“实际占用”修正为反馈值（如 r11 43→54），
  **安全预留仍保留 bound=57 直到 `released`**。
* 取消/超时/节点掉线都**不会**立刻归还容量；只有 `released`（“副本已退出的证据”）
  到达控制面后才把 bound/实际占用同时移除。别的租户此时才看得到真实释放的容量。
* 所有决策按事件的 `at_ms`（控制面**收到**时刻）顺序进行；`occurred_ms` 只作证据
  时间记录。因此 c21 “28 先见 timeout、31 才收到发生在 22 的 first_chunk” 的晚到
  首段陷阱可以被正确重演。

## 2. 接纳、排队、拒绝与“何时才用备用池”

请求状态：`queued / admitted / rejected / cancelled / delivered / completed / failed`。

选副本时同时满足：worker 健康、槽位与 bound 安全、`at + p95_ms ≤ deadline_ms`。
排序优先：不降级 full → 租户有保留额的池 → 在制尝试更少（分散故障域）→ p95 更快。

默认 `policy.downgrade_mode="deadline"` 下：

1. 有 full/主池可行位置就立即接纳（r11→a1、r12→a2、c21→a1）。
2. 只有 small/备用池可行时：
   * 若主池槽位是被 **`cancel_requested`（已通知取消、等实证释放）** 的尝试占着，
     且 `at ≤ deadline - p95_full`，就短暂排队等真实释放——c23 在 33 排队，
     35 收到 r12 `released` 后接纳到 **a2**，与 callbacks 完全一致；
   * 若主池被超时 / `sink_closed_empty` / 掉线等**释放时间不明**的尝试占住，
     则**及早**使用仍有余量的备用池（r13→b2、r11#2→b1），不把请求堵到截止边缘
     才去挤占低余量备用池。
3. 即使最乐观副本的 p95 都无法在截止前完成，排队请求会在
   `latest_start = deadline - fastest_p95` 之后被拒绝。c22（不接受 small）15 排队，
   `latest_start=20`，24 判 `deadline_infeasible` 拒绝。

`policy.downgrade_mode="immediate"` 可切到“只要有截止可行位置（含 small）立即接纳”，
用于值班员对比策略（见 `configs/immediate_downgrade.json`）。

### 低成本模型门控

small/低成本副本只对 `allow_small=true` 且 `at+p95≤deadline` 的请求开放；
不接受降级（c22、c24）永远不会被放到 small。新配置里 `policy.low_cost_pools`
可标记“专门预留的低成本池”，门控同样适用。参见 `configs/reserved_low_cost.json`
与 `selftests` 中的 `test_low_cost_only_for_explicit_degradation_and_feasible`。

## 3. 保留额、空闲出借与按时限收回

`Reservation` 按 `tenant/pool/model_family/[start_ms,end_ms)`（左含右不含）生效。
每次接纳的 bound 被拆成三层并写进承诺 `reservation_slice`：

* `reserved`：本租户当前有效保留额的空闲部分；
* `borrowed`：其他租户保留额中当前空闲、正式出借的部分（生成 `Loan`）；
* `spare`：池中未保留的空闲容量（尽力而为）。

已借出且未归还的额度不能重复出借。原租户有新需求时调用
`engine.reclaim_reservation("caption", pool="main", units=..., at_ms=t)`：
按该池 `return_ms`（main 26 / fallback 18）登记 `reclaim_deadline_ms`，在
`policy.preempt_borrowers=true` 时通知借用尝试让出（发 `preempt_borrowed_capacity`
动作），但份额**只有在对应尝试 `released` 后才真正可用**；Loan 核销时记录
`met_deadline`。系统不会用“把配额改回去”的方式假装有容量。

> 说明：资料中的既有接纳（review 的 r11/r12 bound 合计超过其 8 单位保留额）正是
> 通过“8 自有 + 借入 caption 空闲 50 + spare”分层承担的；物理 worker 上限始终是硬约束。
> 若希望租户额度也成为硬门槛，设置 `policy.reservation_is_hard_cap=true`。

## 4. 主池故障、唯一可采信结果与出口门控

* 每个逻辑请求在出口 outbox 中以幂等键 `result:<request_id>` 至多产生一条结果凭据；
  重复发布返回 `duplicate=true`，不会二次交付给调用方。
* 出口账 `EgressBook` 按 `request_id#attempt_no` 记录：
  * `observe_first_chunk` / `received_first_chunk(rid,no)` /
    `query_first_chunk(rid,no)`：按尝试查询是否已收到首段；
  * `confirm_sink(... sink_closed_empty | sink_finished)` 与
    `stopped_receiving(rid,no)`、`is_empty_and_closed(rid,no)`：确认停止接收/空且关闭。
* **首个被出口实际收到的首段**锁定 `delivered_attempt` 与模型：之后其他尝试的首段
  会被记为 `cross_attempt_delivery_suppressed`，绝不产生第二份上游结果，也不能再
  换模型补后半段（c21 晚到首段胜出，已预备的 #2 立即撤销预留）。
* 超时本身不证明“无交付”。此时可以 `_prepare_backup`：选好备用位置并**预留安全容量**
  （trace 中有 `backup_prepared`/promise，但**没有** `dispatch_attempt` 动作，因此不会
  产生副本尝试记录），交付被 `delivery_gate` 挡住。只有收到
  `sink_closed_empty`——既无首段、又原子关闭不再接收——才真正发起备用尝试
  （c23：39 预备 b1#2，42 空且关闭后发起，57 首段经 #2 交付，63 `sink_finished`）。
* 重复的 `sink_closed_empty` / `released` / `first_chunk` 回调都是幂等的，不会再生成
  额外尝试或结果。

## 5. 恢复探测与故障波及面

`engine.submit_probe(worker, at_ms=..., units=...)` 发起的探测是带 `is_probe`
标记的真实 charge：占一槽和 `probe_units` bound，直到 `release_probe(key)` 或对应
`released` 反馈。`policy.probe_on_up=true` 时 `node_up` 自动发探测。

`node_down` 的 `node_down_blast_radius` 只列**该 worker 上的在制尝试**，并给出同池
未受影响副本。基线末尾 83 `node_down b1` 的波及面恰为 `["r11#2"]`，同池 b2 上的
r13 出现在 `same_pool_unaffected_workers=["b2"]`，范围与同池其他副本分开。

## 6. 值班员追查与占用查询

```sh
python -m rehearsal trace c23 --run runs/baseline --full   # 完整时间线
python -m rehearsal occupancy --run runs/baseline --by tenant
python -m rehearsal quota --run runs/baseline
```

或 Python：

```python
rep = engine.request_report("c23")
rep["admission_promises"]   # 接纳时承诺：worker/variant/bound/p95/预计完成/额度层
rep["attempts"]             # 每次尝试与真实状态/占用
rep["egress"]               # 出口按尝试的首段/关闭证据
rep["feedback_decisions"]   # 哪条 feedback 触发了预备/发起/锁定/重试/拒绝
engine.occupancy("pool" | "tenant" | "worker")
```

占用快照把每个分组分为 `working_attempts`（dispatched/started/prepared 等）与
`unresolved_attempts`（timeout/node_down/closed_empty/finished/cancel_requested
但尚未 released），并单独给出 `requests_with_live_charges` 与
`ended_user_requests`。基线结束时钟 83 上仍有 3 个真实 charge：
`c24#1@a1(59)`、`r13#1@b2(17)`、`r11#2@b1(node_down,29)`，其余用户请求已结束。

## 7. 两份账、重启恢复与对账

每次 `Store(dir).save(engine)` 原子写：

* `state.json`：完整状态（含**真实 `live_charge_keys`**，恢复时不会让晚到 sink
  把已 released 的占用“复活”）；
* `control_book.json`：worker 健康、bound/observed/slots、live charges、Loan；
* `egress_book.json`：按尝试的首段/sink 状态与幂等 outbox；
* `trace.jsonl`。

`RehearsalEngine.restore(dir)`（或 `GET`/CLI）后，`engine.reconcile()` 以**出口账为
权威**把已有 `result_claim` 的请求在控制账标记为同一 `delivered_attempt`，报告
`mismatches/repairs/awaiting_actual_release`。已经发给调用方的结果由幂等键保护，
重启或出口断线重连（`egress.disconnect()/reconnect()`）都不会重复交付。容量侧不会
被对账“做平”：缺 `released` 证据的尝试一律留在 `awaiting_actual_release`。

## 8. 持续接收（流式 / HTTP）

```sh
# stdin JSONL：每行一条 materials 风格事件；--state-dir 每条后落盘、重启可恢复
tail -f events.jsonl | python -m rehearsal stream --materials materials --state-dir state/live

# 标准库 HTTP
python -m rehearsal serve --host 127.0.0.1 --port 8765 --state-dir state/http
# POST /ingest  (单对象或数组)   GET /requests/<id>  GET /trace/<id>
# GET /occupancy?by=tenant       GET /quota          POST /reconcile
```

## 9. 模块与兼容入口

| 模块 | 职责 |
| --- | --- |
| `rehearsal/model.py` | 枚举、Request/Attempt/Promise/Worker/Reservation、成本换算 |
| `rehearsal/config.py` | `RehearsalConfig`/`Policy`、资料加载、新配置覆盖 |
| `rehearsal/books.py` | `CapacityBook`、`ReservationLedger`(Loan/催还)、`EgressBook` |
| `rehearsal/engine.py` | `RehearsalEngine`：接纳/排队/拒绝、预备与切换、故障、追查 |
| `rehearsal/replay.py` | 合并时间线、`run_rehearsal`、报告产物 |
| `rehearsal/persist.py` | `Store` 原子落盘、恢复、`reconcile_state` |
| `rehearsal/cli.py` / `__main__.py` | 值班员命令 |
| `rehearsal/server.py` | 标准库 HTTP 入口 |

根目录另提供 `runtime.py / engine.py / scheduler.py / replay.py / broker.py /
ledger.py / egress.py / store.py / simulator.py` 等薄兼容模块，并把主引擎别名为
`Runtime / Rehearsal / Engine / Scheduler / Broker / Orchestrator / Simulation`。

## 10. 资料基线决策一览（默认策略实际重演结果）

| at | 请求/事件 | 决策 |
| --- | --- | --- |
| 0 | r11 review 43/57 d90 | admit a1/full，slice 8 reserved+49 borrowed |
| 3 | r12 review 31/45 d109 | admit a2/full |
| 12 | c21 caption 36/50 d61 | admit a1（57+50=107≤110，第 2 槽） |
| 15 | c22 25/39 不接受 small | queued |
| 19 | r11 usage=54 | 实际占用修正为 54，bound 仍 57 |
| 24 | cancel r12 / c22 reject | a2 容量仍 held；c22 `latest_start=20` 拒绝 |
| 26 | a1 node_down | 波及 {r11#1,c21#1}，a2 不受影响 |
| 28/31 | c21 timeout → 晚到 first_chunk | 预备 b1#2 未发起；#1 首段锁定，撤销 #2 |
| 33/35 | c23 到达 / r12 released | 先等 a2 取消释放，35 admit c23#1→a2 |
| 39/42 | c23 timeout / closed_empty | 39 预备 b1#2，42 确认空且关闭后发起 |
| 41 | r13 allow_small | 主池被超时尝试占住，及早 admit b2 small(17) |
| 43 | r11#1 released 无交付 | retry → b1 small#2(29)，与 c23#2 合槽 47≤56 |
| 57/63 | c23#2 first/finished | 经 #2 唯一交付并完成 |
| 66 | c23#1 晚到 released | a2 实际释放 |
| 78 | c24 45/59 不接受 small | admit a1/full |
| 83 | b1 node_down | 波及 {r11#2}，b2/r13 隔离不受影响 |
