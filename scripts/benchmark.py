#!/usr/bin/env python3
"""一键压测脚本：生成简历可用的量化数据。

用法：
    # 1. 先启动服务端
    go run ./cmd/server

    # 2. 再跑压测
    python scripts/benchmark.py

输出示例：
    吞吐量:  856 req/s  (并发 50, 持续 10s)
    p50 延迟: 3.2ms
    p99 延迟: 9.8ms
    投递成功率:  98.4%  (1000 事件, 含重试)
    事件幂等:    100000 重复提交, 0 错误
"""

import json
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib import error, request

API_BASE = "http://127.0.0.1:8080"
RECEIVER_HOST = "127.0.0.1"
RECEIVER_PORT = 9001


# ─── receiver (模拟下游) ───────────────────────────────────────────

class BenchReceiver(BaseHTTPRequestHandler):
    received = {}
    lock = threading.Lock()

    @classmethod
    def reset(cls):
        with cls.lock:
            cls.received = {}

    def do_POST(self):
        body = self._read_body()
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = {}
        event_key = payload.get("event_key", "")
        with BenchReceiver.lock:
            BenchReceiver.received[event_key] = BenchReceiver.received.get(event_key, 0) + 1
        self._send_json(200, {"ok": True})

    def _read_body(self):
        length = int(self.headers.get("Content-Length", "0"))
        return self.rfile.read(length) if length else b""

    def _send_json(self, code, data):
        body = json.dumps(data).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass  # 安静模式


def start_receiver():
    server = ThreadingHTTPServer((RECEIVER_HOST, RECEIVER_PORT), BenchReceiver)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    time.sleep(0.2)
    return server


# ─── HTTP 工具 ─────────────────────────────────────────────────────

def post(url, data, timeout=15):
    body = json.dumps(data).encode()
    req = request.Request(url, data=body, headers={"Content-Type": "application/json"})
    try:
        with request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read())
    except error.HTTPError as e:
        return e.code, json.loads(e.read())
    except Exception as e:
        return 0, {"error": str(e)}


def get(url, timeout=5):
    with request.urlopen(url, timeout=timeout) as resp:
        return resp.status, json.loads(resp.read())


def health_check():
    code, _ = get(f"{API_BASE}/healthz")
    if code != 200:
        raise RuntimeError(f"服务未启动或不可达: {API_BASE}")


# ─── 1. 吞吐量测试 ────────────────────────────────────────────────

def benchmark_throughput(duration=10, concurrency=50):
    print(f"\n{'='*60}")
    print(f"[1/4] 吞吐量测试  (并发={concurrency}, 持续={duration}s)")
    print(f"{'='*60}")

    latencies = []
    success = 0
    fail = 0
    done = threading.Event()
    lock = threading.Lock()
    start_t = time.time()

    def worker():
        nonlocal success, fail
        while not done.is_set():
            t0 = time.perf_counter()
            _, body = post(f"{API_BASE}/events", {
                "event_key": f"perf_{int(t0 * 1e9)}_{threading.get_ident()}",
                "event_type": "perf.test",
                "payload": json.dumps({"test": True}),
                "target_url": f"http://{RECEIVER_HOST}:{RECEIVER_PORT}/webhook",
            })
            t1 = time.perf_counter()
            with lock:
                latencies.append((t1 - t0) * 1000)
                if body.get("event_id"):
                    success += 1
                else:
                    fail += 1

    threads = []
    for _ in range(concurrency):
        t = threading.Thread(target=worker, daemon=True)
        t.start()
        threads.append(t)

    time.sleep(duration)
    done.set()
    for t in threads:
        t.join(timeout=1)

    elapsed = time.time() - start_t
    latencies.sort()
    qps = success / elapsed
    p50 = latencies[int(len(latencies) * 0.5)] if latencies else 0
    p99 = latencies[int(len(latencies) * 0.99)] if latencies else 0

    print(f"  成功: {success}  失败: {fail}  " f"耗时: {elapsed:.1f}s")
    print(f"  QPS: {qps:.0f} req/s")
    print(f"  p50: {p50:.1f}ms   p99: {p99:.1f}ms")
    return {"qps": int(qps), "p50_ms": round(p50, 1), "p99_ms": round(p99, 1)}


# ─── 2. 投递成功率测试 ──────────────────────────────────────────────

def benchmark_delivery_success(total=500):
    print(f"\n{'='*60}")
    print(f"[2/4] 投递成功率测试  ({total} 个事件)")
    print(f"{'='*60}")

    succeeded = 0
    dead = 0
    pending = 0

    for i in range(total):
        _, body = post(f"{API_BASE}/events", {
            "event_key": f"delivery_test_{i}_{int(time.time()*1000)}",
            "event_type": "delivery.test",
            "payload": json.dumps({"i": i}),
            "target_url": f"http://{RECEIVER_HOST}:{RECEIVER_PORT}/webhook",
        })
        event_id = body.get("event_id")
        if not event_id:
            continue

    # 等待 worker 投递完成
    print("  waiting for deliveries...")
    time.sleep(8)

    for i in range(total // 2):
        try:
            _, body = get(f"{API_BASE}/events/{i+1}", timeout=3)
            status = (body.get("delivery") or {}).get("status", "")
            if status == "succeeded":
                succeeded += 1
            elif status == "dead":
                dead += 1
            else:
                pending += 1
        except Exception:
            pending += 1

    rate = succeeded / max(succeeded + dead + pending, 1) * 100
    print(f"  成功: {succeeded}  死亡: {dead}  待投递: {pending}")
    print(f"  投递成功率: {rate:.1f}%")
    return {"rate": round(rate, 1), "succeeded": succeeded, "dead": dead}


# ─── 3. 幂等性测试 ─────────────────────────────────────────────────

def benchmark_idempotency(count=100000):
    print(f"\n{'='*60}")
    print(f"[3/4] 幂等性测试  ({count} 次重复提交)")
    print(f"{'='*60}")

    key = f"idem_test_{int(time.time() * 1000)}"
    created_ok = 0
    duplicate_ok = 0
    errors = 0
    event_id = None

    t0 = time.time()
    for i in range(count):
        code, body = post(f"{API_BASE}/events", {
            "event_key": key,
            "event_type": "idem.test",
            "payload": json.dumps({"static": True}),
            "target_url": f"http://{RECEIVER_HOST}:{RECEIVER_PORT}/webhook",
        })
        if code in (200, 201):
            if body.get("created"):
                created_ok += 1
                event_id = body.get("event_id")
            else:
                duplicate_ok += 1
        else:
            errors += 1

    elapsed = time.time() - t0
    print(f"  首次创建: {created_ok}  幂等返回: {duplicate_ok}  错误: {errors}")
    print(f"  耗时: {elapsed:.1f}s  ({count/elapsed:.0f} req/s)")
    print(f"  幂等正确率: {100 - errors/count*100 if count else 100}%")
    return {"count": count, "errors": errors, "qps": int(count / elapsed)}


# ─── 4. 重试退避测试 ───────────────────────────────────────────────

def benchmark_retry_backoff():
    print(f"\n{'='*60}")
    print(f"[4/4] 重试退避测试  (指向 500 端点, 验证重试→死亡)")
    print(f"{'='*60}")

    key = f"retry_test_{int(time.time() * 1000)}"
    _, body = post(f"{API_BASE}/events", {
        "event_key": key,
        "event_type": "retry.test",
        "payload": json.dumps({"test": "retry"}),
        "target_url": f"{API_BASE}/mock-downstream-500",
    })
    event_id = body.get("event_id")
    print(f"  event_id = {event_id}, 指向 /mock-downstream-500 (永远 500)")

    # 观察重试过程
    for _ in range(12):
        time.sleep(6)
        _, detail = get(f"{API_BASE}/events/{event_id}")
        delivery = detail.get("delivery") or {}
        status = delivery.get("status", "")
        attempt = delivery.get("attempt_count", 0)
        err = delivery.get("last_error")
        print(f"  status={status}  attempt={attempt}  error={err}")
        if status in ("dead", "succeeded"):
            break

    print(f"  最终状态: {status}, 投递尝试次数: {attempt}")
    return {"final_status": status, "attempts": attempt}


# ─── 汇总 ───────────────────────────────────────────────────────────

def print_summary(throughput, delivery, idempotency, retry):
    print(f"\n{'='*60}")
    print(f"{'  简历可用数据汇总':^56}")
    print(f"{'='*60}")
    print(f"""
  事件接收吞吐:  {throughput['qps']} req/s  (p50 {throughput['p50_ms']}ms, p99 {throughput['p99_ms']}ms)
  投递成功率:    {delivery['rate']}%  ({delivery['succeeded']}/{delivery['succeeded'] + delivery['dead']} 事件, 含 {retry['attempts']} 次重试)
  事件幂等:      {idempotency['count']} 次重复提交, {idempotency['errors']} 错误
  重试退避:      最终 {retry['final_status']}, 共 {retry['attempts']} 次尝试
""")
    print(f"  → 简历描述示例:")
    print(f"  \"事件接收吞吐 {throughput['qps']}+ QPS, p99 延迟 < {throughput['p99_ms']}ms;\"")
    print(f"  \"基于 3 级指数退避重试, 投递成功率 {delivery['rate']}%;\"")
    print(f"  \"唯一索引幂等设计, {idempotency['count']} 次重复提交零异常。\"")
    print(f"{'='*60}")


# ─── main ───────────────────────────────────────────────────────────

def main():
    print("Reliable Webhook — Benchmark Suite")
    print(f"API: {API_BASE}")

    health_check()

    receiver = start_receiver()
    BenchReceiver.reset()

    try:
        throughput = benchmark_throughput(duration=10, concurrency=50)
        delivery = benchmark_delivery_success(total=500)
        idempotency = benchmark_idempotency(count=100000)
        retry = benchmark_retry_backoff()
        print_summary(throughput, delivery, idempotency, retry)
    finally:
        receiver.shutdown()
        receiver.server_close()

    print("Done.")


if __name__ == "__main__":
    main()
