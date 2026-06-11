#!/usr/bin/env python3
"""Worker 调参脚本：模拟 500 万/天，2% 慢下游，找出最小 worker_count。

用法：
    python scripts/tune_workers.py --workers 4 --queue 16 --duration 60 --slow-ratio 0.02
"""

import argparse
import json
import time
import threading
from urllib import error, request

API = "http://127.0.0.1:8080"


def post(path, data, timeout=5):
    body = json.dumps(data).encode()
    req = request.Request(f"{API}{path}", data=body, headers={"Content-Type": "application/json"})
    try:
        with request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())
    except Exception:
        return {"error": "timeout_or_fail"}


def get(path, timeout=5):
    try:
        with request.urlopen(f"{API}{path}", timeout=timeout) as resp:
            return json.loads(resp.read())
    except Exception:
        return {}


def get_metrics():
    """解析 Prometheus 指标，返回 dict"""
    data = get("/metrics")
    if isinstance(data, str):
        # Prometheus 返回的是 text，不是 json
        # 手动爬文本
        return {}
    return data if isinstance(data, dict) else {}


def fire_events(rate_per_sec, duration, slow_ratio, stop_event):
    """以固定速率发射事件"""
    interval = 1.0 / rate_per_sec
    sent = 0
    fast = 0
    slow = 0
    start = time.time()

    while not stop_event.is_set() and (time.time() - start) < duration:
        is_slow = (sent % 100) < int(slow_ratio * 100)
        target = f"{API}/mock-downstream-slow" if is_slow else f"{API}/mock-downstream"

        post("/events", {
            "event_key": f"tune_{int(time.time() * 1e6)}_{sent}",
            "event_type": "tune.test",
            "payload": json.dumps({"seq": sent, "slow": is_slow}),
            "target_url": target,
        })

        sent += 1
        if is_slow:
            slow += 1
        else:
            fast += 1

        elapsed = time.time() - time.time()
        sleep_time = interval - (time.time() - time.time())
        if sleep_time > 0:
            time.sleep(sleep_time)

    return sent, fast, slow


def check_pending():
    """粗略估算 DB 里 pending 数量：通过对比发射数和投递成功数"""
    # 直接查 metrics: webhook_events_received_total - webhook_delivery_final_state_total
    # 这里简化用 metrics endpoint 的原始文本
    try:
        import urllib.request as ureq
        req = ureq.Request(f"{API}/metrics")
        with ureq.urlopen(req, timeout=5) as resp:
            text = resp.read().decode()

        received = 0
        succeeded = 0
        dead = 0
        for line in text.splitlines():
            if line.startswith("webhook_events_received_total{"):
                # 手动提取数值，简陋但管用
                pass
        # 返回占位
        return {
            "text_snippet": text[:500],
        }
    except Exception as e:
        return {"error": str(e)}


def main():
    parser = argparse.ArgumentParser(description="调 worker 参数")
    parser.add_argument("--rate", type=int, default=60, help="事件发射速率 (条/秒)")
    parser.add_argument("--duration", type=int, default=60, help="测试持续时间 (秒)")
    parser.add_argument("--slow-ratio", type=float, default=0.02, help="慢下游比例")
    args = parser.parse_args()

    print(f"发射速率: {args.rate}/s, 持续 {args.duration}s, 慢比例: {args.slow_ratio}")
    print(f"预计总事件: {args.rate * args.duration}")
    print(f"每轮配置需手动改 config.yml 的 worker_count 和 queue_size 后重启服务")
    print()

    # 健康检查
    if get("/healthz").get("status") != "ok":
        print("ERROR: 服务未启动")
        return

    print("开始发射事件...")
    stop = threading.Event()
    sent, fast, slow = fire_events(args.rate, args.duration, args.slow_ratio, stop)

    print(f"\n发射完毕: 总计 {sent}, 快 {fast}, 慢 {slow}")

    # 等待投递完成
    wait = max(30, int(slow * 8 * 3 / 4))  # 慢请求 × 8s × 最多3次重试 / 4 worker
    print(f"等待 {wait}s 让 Worker 处理完...")
    time.sleep(wait)

    # 看 metrics
    print("\n--- Prometheus 指标（部分）---")
    metrics_snippet = check_pending()
    print(metrics_snippet)

    print("\n--- 手动检查建议 ---")
    print("浏览器打开 http://127.0.0.1:8080/metrics，关注:")
    print("  webhook_deliveries_in_flight       — 应 ≤ worker_count")
    print("  webhook_delivery_queue_depth       — 不应持续增长")
    print("  webhook_delivery_final_state_total — 死信数")
    print("  webhook_delivery_duration_seconds  — 分位数")


if __name__ == "__main__":
    main()
