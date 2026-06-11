# Reliable Webhook Demo Console

这个 demo console 是一个轻量级本地展示工具，用来更直观地演示 Reliable Webhook 的投递、失败重试和超时行为。它不属于核心业务链路，不新增数据库表，也不改变 Go 服务的业务逻辑。

## 1. 启动主服务

先确认 MySQL 已经启动，并且已经执行过初始化脚本：

```bash
mysql -u root -p webhook_platform < sql/init.sql
```

根据本地环境检查 `config.yml` 中的 MySQL 配置，然后在项目根目录启动主服务：

```bash
go run ./cmd/server
```

默认情况下，主服务监听：

```text
http://127.0.0.1:8080
```

也可以使用 Docker Compose 启动：

```bash
docker compose up --build
```

## 2. 启动本地 webhook receiver

`scripts/webhook_receiver.py` 使用 Python 标准库实现，不需要安装第三方依赖。它监听：

```text
http://127.0.0.1:9000/webhook
```

成功场景：

```bash
python scripts/webhook_receiver.py success
```

失败场景：

```bash
python scripts/webhook_receiver.py fail
```

超时场景：

```bash
python scripts/webhook_receiver.py slow
```

不传参数时默认是 `success`：

```bash
python scripts/webhook_receiver.py
```

receiver 收到 `POST /webhook` 后会在终端打印请求 path、headers 和 body。

## 3. 打开 demo console

用浏览器打开：

```text
web/demo.html
```

也可以在 receiver 启动后打开下面的地址，它加载的是同一个 `web/demo.html`：

```text
http://127.0.0.1:9000/demo.html
```

页面标题是 `Reliable Webhook Demo Console`。页面会调用主服务的 `POST /events` 创建事件，并把 `target_url` 设置为：

```text
http://127.0.0.1:9000/webhook
```

页面创建事件时会自动生成 `event_key`，例如：

```text
demo_success_1710000000000
```

事件内容固定为：

```json
{
  "event_type": "order.paid",
  "payload": {
    "order_id": 10001,
    "amount": 99
  }
}
```

实际提交给 `POST /events` 时，`payload` 会按当前后端接口要求转换为 JSON 字符串。

为了避免浏览器直接打开本地 HTML 文件时遇到跨域限制，demo 页面会先请求 `http://127.0.0.1:9000/events`，再由 `webhook_receiver.py` 代理到主服务的 `http://127.0.0.1:8080/events`。最终使用的仍然是现有主服务接口。

说明：当前后端查询接口路径是 `GET /events/{id}`，创建接口返回 `event_id`。demo console 会在浏览器本地保存本页创建过的 `event_key -> event_id` 映射，所以你仍然可以在输入框里填 `event_key` 并刷新状态。

## 4. 演示 success 场景

1. 启动主服务。
2. 启动 receiver：

```bash
python scripts/webhook_receiver.py success
```

3. 打开 `web/demo.html`。
4. 点击“创建成功投递事件”。
5. 等待 1-2 秒，点击“刷新事件状态”。

应该观察到：

- receiver 终端打印了一次 `POST /webhook` 请求。
- 页面查询结果中的 delivery 状态最终变为 `succeeded`。

## 5. 演示 fail 场景

1. 停止当前 receiver。
2. 以失败模式重新启动：

```bash
python scripts/webhook_receiver.py fail
```

3. 点击“创建失败投递事件”。
4. 多次点击“刷新事件状态”。

应该观察到：

- receiver 每次收到请求都会返回 HTTP 500。
- delivery 状态可能先是 `pending`，随后被 worker 再次投递。
- 重试过程中可以观察到 `attempt_count` 增加。
- 达到最大重试次数后，状态预期进入 `dead`。

## 6. 演示 slow 场景

1. 停止当前 receiver。
2. 以慢响应模式重新启动：

```bash
python scripts/webhook_receiver.py slow
```

3. 点击“创建超时投递事件”。
4. 多次点击“刷新事件状态”。

应该观察到：

- receiver 会 sleep 10 秒后返回 HTTP 200。
- Go webhook client 默认 5 秒超时，因此本次投递会先表现为 timeout。
- delivery 状态通常会回到 `pending` 等待下一轮 retry。
- 多次刷新时可以观察到 `last_error` 和 `attempt_count` 的变化。

## 7. 预期结果速查

- success 场景预期 `succeeded`
- fail 场景预期 `pending` / `retry` / `dead`
- slow 场景预期 `timeout` / `retry`

这个 demo console 只用于本地展示和手动观察，不是生产功能，也不参与核心业务链路。

## 8. 使用脚本做真实链路测试

如果不想用页面手动点，可以直接运行端到端测试脚本：

```bash
python scripts/demo_test.py
```

这个脚本会：

- 在本机启动一个临时 webhook receiver，监听 `127.0.0.1:9000/webhook`
- 调用主服务 `POST /events` 创建真实事件
- 接收 worker 发出的真实 webhook 投递
- 调用主服务 `GET /events/{id}` 轮询状态
- 对 success / fail / slow 三个场景输出 PASS 或失败原因

也可以只测试单个场景：

```bash
python scripts/demo_test.py --scenario success
python scripts/demo_test.py --scenario fail
python scripts/demo_test.py --scenario slow
```

运行脚本前需要先启动主服务：

```bash
go run ./cmd/server
```

如果主服务不是默认地址，可以指定：

```bash
python scripts/demo_test.py --api http://127.0.0.1:8080
```

注意：fail 和 slow 会等待系统完成真实重试，默认每个场景最多等待 60 秒。
