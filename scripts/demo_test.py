#!/usr/bin/env python3
"""End-to-end local test for the Reliable Webhook delivery flow."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import argparse
import json
import sys
import threading
import time
from urllib import error, parse, request


DEFAULT_API_BASE_URL = "http://127.0.0.1:8080"
DEFAULT_RECEIVER_HOST = "127.0.0.1"
DEFAULT_RECEIVER_PORT = 9000


class ReceiverState:
    def __init__(self):
        self.lock = threading.Lock()
        self.requests = []

    def record(self, item):
        with self.lock:
            self.requests.append(item)

    def count_for(self, event_key):
        with self.lock:
            return sum(1 for item in self.requests if item.get("event_key") == event_key)


class DemoWebhookHandler(BaseHTTPRequestHandler):
    state = ReceiverState()

    def do_POST(self):
        if self.path != "/webhook":
            self.send_json(404, {"error": "not found"})
            return

        raw_body = self.read_body()
        body_text = raw_body.decode("utf-8", errors="replace")
        payload = parse_json_object(body_text)
        scenario = payload.get("scenario", "success")
        event_key = payload.get("event_key", "")

        self.state.record(
            {
                "event_key": event_key,
                "scenario": scenario,
                "path": self.path,
                "headers": dict(self.headers.items()),
                "body": body_text,
                "received_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
        )

        print("\n--- webhook received ---", flush=True)
        print(f"path: {self.path}", flush=True)
        print(f"scenario: {scenario}", flush=True)
        print(f"event_key: {event_key}", flush=True)
        print("headers:", flush=True)
        for key, value in self.headers.items():
            print(f"  {key}: {value}", flush=True)
        print("body:", flush=True)
        print(body_text, flush=True)
        print("--- end webhook ---\n", flush=True)

        if scenario == "fail":
            self.send_json(500, {"status": "fail"})
            return

        if scenario == "slow":
            time.sleep(10)

        self.send_json(200, {"status": "ok"})

    def log_message(self, fmt, *args):
        sys.stdout.write(
            "%s - - [%s] %s\n"
            % (self.address_string(), self.log_date_time_string(), fmt % args)
        )
        sys.stdout.flush()

    def read_body(self):
        content_length = int(self.headers.get("Content-Length", "0"))
        return self.rfile.read(content_length) if content_length else b""

    def send_json(self, status_code, payload):
        data = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        try:
            self.wfile.write(data)
        except BrokenPipeError:
            print("client disconnected before receiver response was written", flush=True)


def parse_json_object(text):
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def start_receiver(host, port):
    try:
        server = ThreadingHTTPServer((host, port), DemoWebhookHandler)
    except OSError as exc:
        raise RuntimeError(
            f"cannot listen on {host}:{port}: {exc}. "
            "Stop any existing receiver or pass --receiver-port."
        ) from exc

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"local webhook receiver: http://{host}:{port}/webhook")
    return server


def request_json(method, url, payload=None, timeout=10):
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = request.Request(url, data=data, method=method, headers=headers)
    try:
        with request.urlopen(req, timeout=timeout) as response:
            return response.status, read_response_json(response)
    except error.HTTPError as exc:
        return exc.code, read_response_json(exc)
    except error.URLError as exc:
        raise RuntimeError(f"request failed: {url}: {exc.reason}") from exc


def read_response_json(response):
    raw = response.read().decode("utf-8", errors="replace")
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"raw": raw}


def check_main_service(api_base_url):
    status, body = request_json("GET", f"{api_base_url}/healthz", timeout=3)
    if status != 200:
        raise RuntimeError(f"main service health check failed: HTTP {status}: {body}")


def create_event(api_base_url, receiver_url, scenario):
    event_key = f"demo_{scenario}_{int(time.time() * 1000)}"
    payload = {
        "event_key": event_key,
        "scenario": scenario,
        "order_id": 10001,
        "amount": 99,
    }
    body = {
        "event_key": event_key,
        "event_type": "order.paid",
        "payload": json.dumps(payload, separators=(",", ":")),
        "target_url": receiver_url,
    }

    status, response_body = request_json("POST", f"{api_base_url}/events", body)
    if status not in (200, 201):
        raise RuntimeError(f"create event failed: HTTP {status}: {response_body}")

    event_id = response_body.get("event_id")
    if not event_id:
        raise RuntimeError(f"create event response missing event_id: {response_body}")

    return event_key, event_id


def get_event(api_base_url, event_id):
    status, body = request_json("GET", f"{api_base_url}/events/{event_id}")
    if status != 200:
        raise RuntimeError(f"get event failed: HTTP {status}: {body}")
    return body


def delivery_status(detail):
    delivery = detail.get("delivery") or {}
    return delivery.get("status", ""), delivery.get("attempt_count", 0), delivery.get("last_error")


def wait_for_result(api_base_url, event_id, scenario, timeout_seconds):
    deadline = time.time() + timeout_seconds
    last_detail = None
    target_status = "succeeded" if scenario == "success" else "dead"

    while time.time() < deadline:
        detail = get_event(api_base_url, event_id)
        last_detail = detail
        status, attempt_count, last_error = delivery_status(detail)
        print(
            f"poll: scenario={scenario} event_id={event_id} "
            f"delivery_status={status} attempt_count={attempt_count} "
            f"last_error={last_error}"
        )

        if status == target_status:
            return detail

        time.sleep(2)

    pretty = json.dumps(last_detail, ensure_ascii=False, indent=2)
    raise RuntimeError(
        f"scenario {scenario} did not reach {target_status} "
        f"within {timeout_seconds}s. last detail:\n{pretty}"
    )


def run_scenario(args, scenario):
    receiver_url = f"http://{args.receiver_host}:{args.receiver_port}/webhook"
    print(f"\n=== scenario: {scenario} ===")
    event_key, event_id = create_event(args.api, receiver_url, scenario)
    print(f"created event: event_key={event_key} event_id={event_id}")

    detail = wait_for_result(args.api, event_id, scenario, args.timeout)
    status, attempt_count, last_error = delivery_status(detail)
    received_count = DemoWebhookHandler.state.count_for(event_key)

    print(
        f"PASS {scenario}: status={status}, "
        f"attempt_count={attempt_count}, received_count={received_count}"
    )
    if last_error:
        print(f"last_error: {last_error}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run a real local end-to-end test against the Reliable Webhook service."
    )
    parser.add_argument(
        "--api",
        default=DEFAULT_API_BASE_URL,
        help=f"main service base URL, default: {DEFAULT_API_BASE_URL}",
    )
    parser.add_argument(
        "--receiver-host",
        default=DEFAULT_RECEIVER_HOST,
        help=f"local receiver host, default: {DEFAULT_RECEIVER_HOST}",
    )
    parser.add_argument(
        "--receiver-port",
        type=int,
        default=DEFAULT_RECEIVER_PORT,
        help=f"local receiver port, default: {DEFAULT_RECEIVER_PORT}",
    )
    parser.add_argument(
        "--scenario",
        choices=("all", "success", "fail", "slow"),
        default="all",
        help="which scenario to run, default: all",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="seconds to wait for each scenario, default: 60",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    scenarios = ["success", "fail", "slow"] if args.scenario == "all" else [args.scenario]

    server = None
    try:
        server = start_receiver(args.receiver_host, args.receiver_port)
        check_main_service(args.api)
        for scenario in scenarios:
            run_scenario(args, scenario)
    except RuntimeError as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        if server is not None:
            server.shutdown()
            server.server_close()

    print("\nALL PASS")


if __name__ == "__main__":
    main()
