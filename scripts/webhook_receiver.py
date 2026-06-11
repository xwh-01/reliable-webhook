#!/usr/bin/env python3
"""Local webhook receiver for the Reliable Webhook demo."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import argparse
import json
from pathlib import Path
import sys
import time
from urllib import error, request


HOST = "127.0.0.1"
PORT = 9000
API_BASE_URL = "http://127.0.0.1:8080"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEMO_HTML = PROJECT_ROOT / "web" / "demo.html"


class WebhookHandler(BaseHTTPRequestHandler):
    mode = "success"

    def do_OPTIONS(self):
        self.send_response(204)
        self.end_headers()

    def do_GET(self):
        if self.path in ("/", "/demo.html"):
            self.serve_demo_html()
            return

        if self.path.startswith("/events/"):
            self.proxy_to_api("GET")
            return

        self.send_json(404, {"error": "not found"})

    def do_POST(self):
        if self.path == "/events":
            self.proxy_to_api("POST")
            return

        if self.path != "/webhook":
            self.send_json(404, {"error": "not found"})
            return

        body = self.read_body()
        self.print_webhook_request(body)

        if self.mode == "fail":
            self.send_json(500, {"status": "fail"})
            return

        if self.mode == "slow":
            time.sleep(10)

        self.send_json(200, {"status": "ok"})

    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        super().end_headers()

    def read_body(self):
        content_length = int(self.headers.get("Content-Length", "0"))
        return self.rfile.read(content_length) if content_length else b""

    def print_webhook_request(self, body):
        print("\n--- webhook received ---", flush=True)
        print(f"path: {self.path}", flush=True)
        print("headers:", flush=True)
        for key, value in self.headers.items():
            print(f"  {key}: {value}", flush=True)
        print("body:", flush=True)
        print(body.decode("utf-8", errors="replace"), flush=True)
        print("--- end webhook ---\n", flush=True)

    def log_message(self, fmt, *args):
        sys.stdout.write(
            "%s - - [%s] %s\n"
            % (self.address_string(), self.log_date_time_string(), fmt % args)
        )
        sys.stdout.flush()

    def serve_demo_html(self):
        try:
            data = DEMO_HTML.read_bytes()
        except OSError as exc:
            self.send_json(500, {"error": f"read demo.html failed: {exc}"})
            return

        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def proxy_to_api(self, method):
        body = self.read_body() if method == "POST" else None
        api_request = request.Request(
            f"{API_BASE_URL}{self.path}",
            data=body,
            method=method,
            headers={"Content-Type": self.headers.get("Content-Type", "application/json")},
        )

        try:
            with request.urlopen(api_request, timeout=10) as response:
                self.send_proxy_response(response.status, response.headers, response.read())
        except error.HTTPError as exc:
            self.send_proxy_response(exc.code, exc.headers, exc.read())
        except error.URLError as exc:
            self.send_json(502, {"error": f"proxy to main service failed: {exc.reason}"})

    def send_proxy_response(self, status_code, headers, data):
        self.send_response(status_code)
        content_type = headers.get("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def send_json(self, status_code, payload):
        data = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Local webhook receiver for Reliable Webhook demos."
    )
    parser.add_argument(
        "mode",
        nargs="?",
        default="success",
        choices=("success", "fail", "slow"),
        help="Response mode: success=200, fail=500, slow=sleep 10s then 200.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    WebhookHandler.mode = args.mode

    server = ThreadingHTTPServer((HOST, PORT), WebhookHandler)
    print(f"webhook receiver listening on http://{HOST}:{PORT}/webhook")
    print(f"demo console available at http://{HOST}:{PORT}/demo.html")
    print(f"proxying demo API calls to {API_BASE_URL}")
    print(f"mode: {args.mode}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nshutting down webhook receiver")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
