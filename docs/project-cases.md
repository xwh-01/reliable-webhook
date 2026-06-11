# Reliable Webhook 项目案例

这个项目不要讲成"我写了一个 Webhook 平台"。更准确的讲法是：

> 我做了一个可靠 Webhook 投递服务，用来解决业务系统向外部系统发送事件通知时，网络失败、对方服务异常、进程重启导致通知丢失或无法追踪的问题。

---

## 案例一：订单创建后通知第三方系统

### 真实问题

电商系统创建订单后，需要通知第三方 CRM、ERP 或商家系统。

如果订单服务直接调用第三方接口，会遇到几个问题：

- 第三方服务短暂不可用，订单系统不能一直阻塞。
- HTTP 请求超时后，不知道对方到底有没有收到。
- 服务重启后，内存里的待通知任务会丢失。
- 出问题后只能查日志，很难知道某个订单通知到底失败了几次。

### 这个项目怎么解决

订单服务只负责把事件写入 `events` 表，并在同一个事务里创建 `deliveries` 投递任务。

后台 worker 异步投递：

```text
pending -> running -> succeeded
pending -> running -> pending   (重试)
pending -> running -> dead      (不可重试或达到上限)
```

具体实现要点：

- **事务原子性**：`EventRepository.CreateEventWithDelivery()` 中开启 MySQL 事务，先 `INSERT events`，取到 `LastInsertId` 后紧接 `INSERT deliveries`，最后 `tx.Commit()`。事务回滚时两个表都不会残留数据。
- **重试策略分离**：`WebhookClient.Send()` 对结果做三元判断——2xx 成功、网络错误/超时/429/5xx 可重试、其他 4xx 不可重试。`DeliveryWorker.Process()` 根据 `Retryable` 标志决定下一步。
- **指数退避**：`backoff()` 函数按重试次数返回 5s / 15s / 30s 的间隔，通过 `next_retry_at` 字段控制任务再次被领取的时间。
- **完整审计**：每次投递都会在 `delivery_attempts` 表中插入一条记录，包含 `attempt_no`、`status`、`error_message`、`response_status`，出问题时可以直接查表定位。

### 能体现的能力

- 能把同步调用改造成异步可靠投递。
- 能用数据库事务保证 event 和 delivery 一起创建。
- 能根据 HTTP 状态码区分可重试失败和不可重试失败。
- 能通过状态机建模复杂异步流程。

---

## 案例二：支付成功事件不能丢

### 真实问题

支付成功后，系统需要通知会员系统、订单系统或营销系统。

支付事件通常不能丢，但下游系统可能会临时失败。

直接发 HTTP 请求的问题是：

- 失败后没有统一重试机制。
- 重试次数和失败原因不可见。
- 多个系统各自实现重试，逻辑重复且质量不稳定。
- 上游重试时可能重复创建同一事件的投递任务。

### 这个项目怎么解决

支付服务把 `payment.succeeded` 作为事件通过 `POST /events` 写入投递服务。

具体实现要点：

- **幂等接收**：`events` 表有 `UNIQUE KEY uk_event_key`。`CreateEventWithDelivery()` 在遇到 MySQL 1062 重复键错误时，识别为 `ErrEventKeyConflict`，上层 `EventService.CreateEvent()` 再通过 `GetEventIDByKey()` 查出已有事件 ID 返回 `{EventID, Created: false}`，而非报错。
- **失败记录可查询**：`MarkDead()` 将 `last_error` 写入 deliveries 表；`RecordAttempt()` 将每次尝试的 `error_message` 和 `response_status` 写入 delivery_attempts 表。运维人员可以直接查 SQL，不必翻日志。
- **人工兜底 replay**：`POST /events/{id}/replay` 调用 `CreateReplayDelivery()`，在同一事务中：先 `SELECT ... FOR UPDATE` 检查是否有活跃投递任务（避免重复创建），再取最新 delivery 的 `target_url`，插入新 delivery 并标记 `replay_of_delivery_id`。最终交付的 worker pool 无需改动即可执行 replay 任务。
- **Prometheus 可观测**：`webhook_events_received_total` 按 `result` 维度（created/duplicate/invalid/error）区分，`webhook_delivery_final_state_total` 按 `state` 维度（succeeded/dead_non_retryable/dead_max_attempts/retry_scheduled）区分，配合 `webhook_delivery_duration_seconds` 直方图，可以在 Grafana 上直接看投递成功率、耗时分布和失败原因占比。

### 能体现的能力

- 能设计幂等键，避免重复提交导致重复投递。
- 能把失败信息沉淀为结构化数据库记录，而不是只依赖日志。
- 能考虑人工介入的兜底机制（replay），不假设系统永远自动恢复。
- 能用 Prometheus 多维度指标量化系统的可靠性。

---

## 案例三：worker 重启后任务不能卡死

### 真实问题

worker 从数据库领取任务后，会用 `UPDATE deliveries SET status = 'running', locked_until = ?` 标记任务。

如果进程刚好在发送 HTTP 请求时崩溃，这个任务可能永远停留在 `running`，后续不会再被投递。

这是很多简单任务系统容易忽略的问题——状态机只有正常流转，没有异常恢复路径。

### 这个项目怎么解决

核心在 `ClaimOneReadyPending()` 这条 SQL 查询：

```sql
SELECT d.id, d.event_id, d.target_url, e.payload, d.attempt_count, d.max_attempts
FROM deliveries d
JOIN events e ON e.id = d.event_id
WHERE (
    d.status = 'pending'
    AND (d.next_retry_at IS NULL OR d.next_retry_at <= NOW())
) OR (
    d.status = 'running'
    AND d.locked_until IS NOT NULL
    AND d.locked_until <= NOW()
)
ORDER BY d.id ASC
LIMIT 1
FOR UPDATE
```

关键设计：

- **双重领取条件**：不仅要领 `pending` 任务，也会领 `running` 但 `locked_until <= NOW()` 的任务。这意味着即使上一个 worker 崩溃没有释放锁，超时后任务也会被重新认领。
- **事务内锁定**：`SELECT ... FOR UPDATE` + `UPDATE` + `COMMIT` 在同一个事务内执行。MySQL 行锁保证同一时刻只有一个 worker 能认领到这条任务。
- **租约自动续期**：每次 `ClaimOneReadyPending()` 时传入 `time.Now().Add(claimLease)`，如果本次投递成功会 `locked_until = NULL`；如果调度重试则连 `locked_until` 一起清除。
- **优雅关闭**：`DeliveryPool.Start()` 中通过 `context.WithCancel` 传播关闭信号，`select` 监听 `<-ctx.Done()`，关闭 channel 后 `wg.Wait()` 等待所有 worker 完成当前任务再退出。

### 能体现的能力

- 能考虑进程异常退出，不只考虑正常流程。
- 能用租约机制解决分布式任务卡死问题。
- 能解释为什么 `running` 状态必须有过期恢复路径，以及为什么用 `FOR UPDATE` 行锁而非应用层锁。
- Worker Pool 的启停控制体现了对 Go 并发原语（goroutine、channel、context、WaitGroup）的熟练运用。

---

## 面试讲法

可以按这个顺序讲（5 分钟版本）：

1. **问题定义**（30s）：Webhook 不是简单发 HTTP——请求失败、服务重启、下游异常都会导致通知不可控。
2. **事务保底**（1min）：我把一次通知拆成 event 和 delivery，两者在同一个 MySQL 事务里创建。event 有唯一键保证幂等，重复提交返回已有 ID 不报错。
3. **状态机 + 重试**（1.5min）：后台 worker pool（4 goroutine + buffered channel）通过状态机处理投递任务——pending→running→succeeded/dead。重试策略按状态码区分：网络超时、429、5xx 可重试，4xx 直接 dead，避免无效重试。采用指数退避 5s/15s/30s，最多重试 3 次。
4. **租约恢复**（1min）：我特别处理了 worker 崩溃导致任务卡死的问题。`locked_until` 租约机制 + `FOR UPDATE` 行锁，超时任务会被其他 worker 接管。`SELECT` 语句同时匹配 `pending` 和超时的 `running` 任务。
5. **可观测 + 兜底**（1min）：每次投递记录到 `delivery_attempts` 表，接入 Prometheus（事件接收量、投递耗时、状态分布、队列深度等 6 个指标）。dead 任务支持人工 GET 查询和 POST replay，不依赖上游重发。

一句话总结：

> 这个项目不是为了做一个大平台，而是聚焦解决"业务事件通过 Webhook 可靠通知外部系统"这个具体问题——从事务保底、状态机流转、租约恢复到可观测闭环，形成了一条小而完整的可靠投递链路。

---

## 简历一句话版本

> 用 Go + MySQL 实现可靠 Webhook 投递服务：通过事务保证 event 与 delivery 原子创建，状态机驱动异步投递与智能重试，`locked_until` 租约防止 worker 崩溃任务卡死，Prometheus 多维度指标 + delivery_attempts 审计表实现故障可观测。支持幂等接收、死信重放与 Worker Pool 优雅启停。

---

## 技术点速查

| 设计要求 | 实现位置 | 关键技术点 |
|---|---|---|
| 事务原子性 | `event_repository.go:30-69` | `BEGIN` → `INSERT events` → `INSERT deliveries` → `COMMIT` |
| 幂等接收 | `event_service.go:61-77` | `UNIQUE KEY uk_event_key` + 1062 错误捕获 |
| 状态机 | `delivery_worker.go:37-172` | pending → running → succeeded / dead / pending(retry) |
| 重试策略 | `webhook_client.go:29-74` | 网络错误/超时/429/5xx → retryable；4xx → non-retryable |
| 指数退避 | `delivery_worker.go:174-183` | 第 1 次 5s / 第 2 次 15s / 后续 30s |
| 租约恢复 | `delivery_repository.go:31-93` | `FOR UPDATE` + `locked_until <= NOW()` 双重匹配 |
| Worker Pool | `delivery_pool.go:46-134` | goroutine + buffered channel + context 取消传播 |
| 审计记录 | `delivery_repository.go:139-156` | `delivery_attempts` 表记录每次投递 |
| 人工重放 | `delivery_repository.go:158-224` | `FOR UPDATE` 防并发 + `replay_of_delivery_id` 链路追踪 |
| Prometheus | `observability/metrics.go:14-69` | 6 个指标：CounterVec × 3 + HistogramVec + Gauge × 2 |
