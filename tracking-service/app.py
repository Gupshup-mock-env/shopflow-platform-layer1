"""Tracking service.

Keeps customer-facing shipment tracking state in sync with fulfilment events.
"""

from __future__ import annotations

import json
import os
import signal
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

SERVICE_NAME: str = os.environ.get("SERVICE_NAME", "tracking-service")
SQS_QUEUE_URL: str = os.environ["TRACKING_SQS_URL"]
AWS_ENDPOINT_URL: str | None = os.environ.get("AWS_ENDPOINT_URL")
AWS_REGION: str = os.environ.get("AWS_REGION", "us-east-1")
HEALTH_PORT: int = int(os.environ.get("HEALTH_PORT", "8080"))
WAIT_TIME_SECONDS: int = int(os.environ.get("WAIT_TIME_SECONDS", "10"))
VISIBILITY_TIMEOUT_SECONDS: int = int(os.environ.get("VISIBILITY_TIMEOUT_SECONDS", "30"))
STARTUP_TIMEOUT_SECONDS: float = float(os.environ.get("STARTUP_TIMEOUT_SECONDS", "60"))
MAX_MESSAGES_PER_POLL: int = int(os.environ.get("MAX_MESSAGES_PER_POLL", "10"))

_shutdown = threading.Event()


def log(service: str, event: str, **fields: object) -> None:
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": service,
        "event": event,
        **fields,
    }
    print(json.dumps(record), flush=True)


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/healthz":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *args: object) -> None:
        return


def start_health_server(port: int = 8080) -> None:
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()


def _handle_sigterm(signum: int, frame: object) -> None:
    _shutdown.set()


signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)


def build_sqs_client():
    return boto3.client(
        "sqs",
        endpoint_url=AWS_ENDPOINT_URL,
        region_name=AWS_REGION,
        config=Config(retries={"max_attempts": 3, "mode": "standard"}),
    )


def wait_for_queue(client, timeout_seconds: float) -> None:
    deadline = time.monotonic() + timeout_seconds
    delay = 0.5
    last_error: Exception | None = None
    while time.monotonic() < deadline and not _shutdown.is_set():
        try:
            client.get_queue_attributes(QueueUrl=SQS_QUEUE_URL, AttributeNames=["QueueArn"])
            return
        except (ClientError, BotoCoreError) as exc:
            last_error = exc
            log(SERVICE_NAME, "waiting_for_broker", retry_in_seconds=delay)
            if _shutdown.wait(delay):
                return
            delay = min(delay * 2, 5.0)
    raise RuntimeError(f"SQS queue not reachable within {timeout_seconds}s: {last_error}")


def unwrap(message: dict[str, Any]) -> tuple[str, str, dict[str, Any]]:
    """Return (message_id, source, payload) for one received message."""
    body: Any = json.loads(message["Body"])
    if isinstance(body, dict) and body.get("Type") == "Notification" and "Message" in body:
        return (
            str(body.get("MessageId", message["MessageId"])),
            str(body.get("TopicArn", SQS_QUEUE_URL)),
            json.loads(body["Message"]),
        )
    payload = body if isinstance(body, dict) else {"raw": body}
    return str(message["MessageId"]), SQS_QUEUE_URL, payload


def handle(client, message: dict[str, Any]) -> None:
    message_id, source, payload = unwrap(message)
    log(
        SERVICE_NAME,
        "consumed",
        topic=source,
        message_id=message_id,
        shipment_id=payload.get("shipment_id"),
        carrier=payload.get("carrier"),
        tracking_number=payload.get("tracking_number"),
        status=payload.get("status"),
        estimated_delivery=payload.get("estimated_delivery"),
    )
    client.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=message["ReceiptHandle"])


def poll_loop(client) -> None:
    while not _shutdown.is_set():
        try:
            response = client.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=MAX_MESSAGES_PER_POLL,
                WaitTimeSeconds=WAIT_TIME_SECONDS,
                VisibilityTimeout=VISIBILITY_TIMEOUT_SECONDS,
                MessageAttributeNames=["All"],
            )
        except (ClientError, BotoCoreError) as exc:
            log(SERVICE_NAME, "receive_failed", error=str(exc))
            _shutdown.wait(2.0)
            continue

        for message in response.get("Messages", []):
            try:
                handle(client, message)
            except (ValueError, KeyError) as exc:
                log(SERVICE_NAME, "malformed_message", error=str(exc))
                client.delete_message(
                    QueueUrl=SQS_QUEUE_URL, ReceiptHandle=message["ReceiptHandle"]
                )
            except (ClientError, BotoCoreError) as exc:
                log(SERVICE_NAME, "ack_failed", error=str(exc))


def main() -> None:
    start_health_server(HEALTH_PORT)
    log(SERVICE_NAME, "started", health_port=HEALTH_PORT)

    client = build_sqs_client()
    wait_for_queue(client, STARTUP_TIMEOUT_SECONDS)

    try:
        poll_loop(client)
    finally:
        log(SERVICE_NAME, "stopping")


if __name__ == "__main__":
    main()
