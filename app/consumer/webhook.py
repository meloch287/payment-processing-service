import asyncio
import hashlib
import hmac
import json
import time
import uuid
from typing import Any

import httpx

from app.models import Payment
from app.serializers import to_read

SIGNATURE_HEADER = "X-Webhook-Signature"
TIMESTAMP_HEADER = "X-Webhook-Timestamp"
EVENT_ID_HEADER = "X-Webhook-Id"


class WebhookDeliveryError(Exception):
    pass


def webhook_event_id(payment: Payment) -> uuid.UUID:
    return uuid.uuid5(payment.id, f"payment.{payment.status}")


def build_payload(payment: Payment) -> dict[str, Any]:
    return {
        "event_id": str(webhook_event_id(payment)),
        "event_type": f"payment.{payment.status}",
        "data": to_read(payment).model_dump(mode="json", exclude={"webhook"}),
    }


def sign(secret: str, timestamp: str, body: bytes) -> str:
    digest = hmac.new(secret.encode(), timestamp.encode() + b"." + body, hashlib.sha256)
    return f"sha256={digest.hexdigest()}"


class WebhookSender:
    def __init__(
        self, client: httpx.AsyncClient, secret: str | None, timeout: float = 10.0
    ) -> None:
        self._client = client
        self._secret = secret
        self._timeout = timeout

    async def send(self, url: str, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()
        timestamp = str(int(time.time()))
        headers = {
            "Content-Type": "application/json",
            EVENT_ID_HEADER: str(payload["event_id"]),
            TIMESTAMP_HEADER: timestamp,
        }
        if self._secret:
            headers[SIGNATURE_HEADER] = sign(self._secret, timestamp, body)
        try:
            async with (
                asyncio.timeout(self._timeout),
                self._client.stream("POST", url, content=body, headers=headers) as response,
            ):
                status_code = response.status_code
        except TimeoutError as exc:
            raise WebhookDeliveryError(f"Timeout after {self._timeout:g}s") from exc
        except httpx.HTTPError as exc:
            raise WebhookDeliveryError(f"{type(exc).__name__}: {exc}") from exc
        if not 200 <= status_code < 300:
            raise WebhookDeliveryError(f"HTTP {status_code}")
