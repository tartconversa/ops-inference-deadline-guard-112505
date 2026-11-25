# 共享推理池的截止时间承诺与故障降级

字幕和工单提要的合成调度演练资料。请求时间、回调时间、配置时刻均为演练时钟毫秒。副本回调是可观测消息，第三方没发回调时不存在隐含完成信号。

## 在本地查看资料

运行环境使用 Python 3.11.2 和标准库。启动资料浏览器无需安装额外包。

```sh
python materials.py
python materials.py fleet
python -m unittest discover -s tests -v
```

资料浏览器也接受下方的其他资料名。JSONL 使用 UTF-8 编码，每行是一个对象。表内的顺序对应来源记录，事件时间与接收时间的区别以资料说明为准。各入口可以在无外网的环境中运行。

资料字段和业务口径见[演练资料格式](materials/README.md)。

## 演练运行时

仓库现在包含一个纯标准库的调度运行时（包 `rehearsal/`，兼容入口 `runtime.py`），
完整说明与需求对照见 [RUNTIME.md](RUNTIME.md)。

```sh
# 用现有资料重演，产出 trace/两本账/占用/对账报告到 runs/baseline/
python -m rehearsal run --materials materials --out runs/baseline

# 值班员用新配置重演同样压力
python -m rehearsal replay --config configs/reserved_low_cost.json --out runs/lowcost

# 按请求追查、按池/租户查占用、两本账对账
python -m rehearsal trace c23 --run runs/baseline --full
python -m rehearsal occupancy --run runs/baseline --by tenant --summary
python -m rehearsal reconcile --run runs/baseline

# 持续接收 JSONL 事件（可重启恢复）与 HTTP 入口
... | python -m rehearsal stream --materials materials --state-dir state/live
python -m rehearsal serve --port 8765 --state-dir state/http

python -m unittest discover -s selftests -v   # 运行时行为自测
```

它兑现的关键口径：按成本上界而非请求数接纳；取消/掉线只在 `released` 反馈后真正
释放容量；保留额空闲可出借、按 `return_ms` 催还；超时只预备备用尝试，`sink_closed_empty`
确认空且关闭后才发起；首个收到的首段锁定唯一可采信结果，跨尝试重复首段被压制；
恢复探测真实占容量，`node_down` 波及面严格限定同一 worker；出口幂等账与控制账在
断线/重启后对账且不重复交付。

