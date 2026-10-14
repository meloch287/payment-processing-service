import json
import threading
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


@dataclass(frozen=True, slots=True)
class ReceivedWebhook:
    path: str
    headers: dict[str, str]
    body: bytes
    status: int
    received_at: float

    @property
    def payload(self) -> dict[str, Any]:
        result: dict[str, Any] = json.loads(self.body)
        return result


class WebhookReceiver:
    def __init__(self, host: str = "127.0.0.1", port: int = 0) -> None:
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._received: list[ReceivedWebhook] = []
        self._hits: dict[str, int] = {}
        receiver = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                status = receiver._respond(self.path)
                receiver._record(
                    ReceivedWebhook(
                        path=self.path,
                        headers={key.lower(): value for key, value in self.headers.items()},
                        body=body,
                        status=status,
                        received_at=time.monotonic(),
                    )
                )
                self.send_response(status)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, format: str, *args: Any) -> None:
                return

        self._server = ThreadingHTTPServer((host, port), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def port(self) -> int:
        return int(self._server.server_address[1])

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()

    def _respond(self, path: str) -> int:
        with self._lock:
            hit = self._hits.get(path, 0) + 1
            self._hits[path] = hit
        mode = path.strip("/").split("/")[0]
        if mode == "fail":
            return 500
        if mode.startswith("flaky-"):
            return 503 if hit <= int(mode.removeprefix("flaky-")) else 200
        return 200

    def _record(self, webhook: ReceivedWebhook) -> None:
        with self._condition:
            self._received.append(webhook)
            self._condition.notify_all()

    def received(self, path: str) -> list[ReceivedWebhook]:
        with self._lock:
            return [webhook for webhook in self._received if webhook.path == path]

    def wait_for(self, path: str, count: int, timeout: float) -> list[ReceivedWebhook]:
        deadline = time.monotonic() + timeout
        with self._condition:
            while True:
                matched = [webhook for webhook in self._received if webhook.path == path]
                remaining = deadline - time.monotonic()
                if len(matched) >= count or remaining <= 0:
                    return matched
                self._condition.wait(remaining)
