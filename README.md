# 业务事件可靠通知系统

这是一个轻量级 Webhook 投递服务，用来解决业务系统向外部系统发送事件通知时，直接 HTTP 调用容易失败、阻塞主流程、失败不可追踪的问题。

典型场景：

- 订单创建后通知商家系统
- 支付成功后通知财务或会员系统
- 库存变化后通知外部业务方

项目重点不在于做一个完整商业化 Webhook 平台，而是实现一条小而完整的可靠投递链路：事件接收、任务落库、异步投递、失败重试、死亡状态、人工重放和基础可观测。

## 核心流程

1. 业务系统调用 `POST /events` 提交事件。
2. 服务将事件写入 `events` 表，并在同一个事务中创建一条 `deliveries` 投递任务。
3. 后台 worker pool 轮询可投递任务，将任务标记为 `running`。
4. worker 向 `target_url` 发起 HTTP POST 回调。
5. 根据投递结果更新任务状态：
   - 2xx：标记为 `succeeded`
   - 网络超时、429、5xx：进入下一轮重试
   - 其他 4xx 或达到最大重试次数：标记为 `dead`
6. 每次投递尝试会记录到 `delivery_attempts`，用于问题排查。

## 解决的问题

### 1. 避免主业务流程被外部系统阻塞

订单、支付等主业务只需要提交事件，不需要等待第三方系统响应。实际 HTTP 投递由后台 worker 异步完成。

### 2. 避免事件接收后任务丢失

事件和投递任务在同一个数据库事务中创建，避免出现“事件已接收，但没有对应投递任务”的情况。

### 3. 失败可重试、可追踪

系统会区分可重试失败和不可重试失败：

- 网络错误、超时、429、5xx：可重试
- 其他 4xx：不可重试

每次投递都会记录 attempt，包括状态、错误信息和响应状态码。

### 4. worker 异常退出后的任务恢复

任务被 worker 领取后会进入 `running` 状态。如果进程此时异常退出，任务可能长期卡住。

为了解决这个问题，系统引入 `locked_until` 租约机制。`running` 任务超过租约时间后，可以被其他 worker 重新领取执行。

## 投递状态机

```text
pending -> running -> succeeded
pending -> running -> pending
pending -> running -> dead
running -> running
```

状态说明：

- `pending`：等待投递，或等待下一次重试
- `running`：已被 worker 领取，正在投递
- `succeeded`：投递成功
- `dead`：不可重试失败，或达到最大重试次数

## 技术栈

- Go
- Gin
- MySQL
- Prometheus Client
- Docker / Docker Compose

## 项目结构

```text
cmd/server              程序入口
internal/api            HTTP API
internal/service        业务逻辑
internal/repository     数据库访问
internal/worker         异步投递 worker
internal/observability  Prometheus 指标
sql/init.sql            数据库初始化脚本
docs/project-cases.md   项目场景说明
```

## API 示例

### 创建事件

```bash
curl -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{
    "event_key": "order-1001-created",
    "event_type": "order.created",
    "payload": "{\"order_id\":1001}",
    "target_url": "http://localhost:8080/mock-downstream"
  }'
```

### 查询事件和投递状态

```bash
curl http://localhost:8080/events/1
```

### 人工重放事件

```bash
curl -X POST http://localhost:8080/events/1/replay
```

### 健康检查和指标

```bash
curl http://localhost:8080/healthz
curl http://localhost:8080/metrics
```

## 本地运行

先创建 MySQL 数据库，然后执行初始化脚本：

```bash
mysql -u root -p webhook_platform < sql/init.sql
```

修改 `config.yml` 中的 MySQL 配置后启动服务：

```bash
go run ./cmd/server
```

## Docker 运行

使用 Docker Compose 启动服务和 MySQL：

```bash
docker compose up --build
```

服务启动后访问：

```text
http://localhost:8080
```

停止服务：

```bash
docker compose down
```

## 配置项

服务默认读取 `config.yml`，也可以通过环境变量覆盖：

- `HTTP_ADDR`
- `MYSQL_DSN`
- `REQUEST_TIMEOUT`
- `DELIVERY_TIMEOUT`
- `SHUTDOWN_TIMEOUT`
- `WORKER_COUNT`
- `QUEUE_SIZE`
- `POLL_INTERVAL`

## 测试

```bash
go test ./...
```

## 项目说明

这个项目不是为了替代 Svix、Hookdeck 等成熟 Webhook 基础设施，而是抽取其中最核心的可靠投递模型，做一个适合学习和展示的轻量实现。

它重点体现：

- 异步任务设计
- 数据库事务
- 状态机建模
- 失败重试
- worker 异常恢复
- 投递记录和基础可观测

更多实际场景说明见：[docs/project-cases.md](docs/project-cases.md)。
