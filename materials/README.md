# 演练资料格式

字幕和工单提要的合成调度演练资料。请求时间、回调时间、配置时刻均为演练时钟毫秒。副本回调是可观测消息，第三方没发回调时不存在隐含完成信号。

## `fleet` 容量和模型成本估算

卡槽同时运行数。work_capacity 为并发成本单位上限。p95_ms 是按 variant 的一次尝试耗时估计。return_ms 是高级租户需取回借出容量的时限。同一 worker 的单次尝试占一槽和预计成本，后续用反馈修正成本。 full 和 small 均属于 text 模型族。配额单位可在同池可运行该模型的副本间分配。

字段为 `worker`、`pool`、`slots`、`work_capacity`、`variant`、`p95_ms`、`return_ms`。

## `reservations` 租户时段承诺

start_ms 含端点，end_ms 不含端点。units 为同一模型族在对应池的保留成本单位，其他租户可以暂借。premium 的保留有效时，必须按对应池返还时限实际可用。model_family 用于对应请求。

字段为 `tenant`、`pool`、`model_family`、`start_ms`、`end_ms`、`units`。

## `requests` 不同租户的到达和主动撤销

submit 行带调用方稳定 request_id、绝对截止时间和预估成本。allow_small 代表调用方接受小模型。cancel 复用 request_id，其余提交字段留空。 units_ceiling 是副本确保不会突破的本请求成本上界，降级的 small 使用向上取整的一半成本和上界。

字段为 `at_ms`、`op`、`request_id`、`tenant`、`model_family`、`deadline_ms`、`units`、`allow_small`、`units_ceiling`。

## `callbacks` 副本和调用方回执的投递记录

at_ms 是收到消息的时刻，occurred_ms 是副本记录的发生时刻。模板中 request_id 标注相关尝试，attempt_no 从 1 开始。started/usage 带占用成本，timeout 只说明观察端超时，first_chunk 为对调用方的交付证据，released 是副本已退出的证据。节点和出口回执可以晚到。试用实现发起对应尝试时才投递相应尝试记录，node_down、node_up 属独立通告。 sink_closed_empty 表示收件端原子关闭该尝试并确认没有首段，sink_finished 表示收件端确认完整交付。关闭回执同一尝试可重复查询。

字段为 `at_ms`、`occurred_ms`、`worker`、`request_id`、`attempt_no`、`event`、`units`。
