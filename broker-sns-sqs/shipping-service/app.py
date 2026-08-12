"""Shipping service.

Emits shipment dispatch events for the ShopFlow fulfilment pipeline.
"""

from __future__ import annotations

import json
import os
import signal
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

SERVICE_NAME: str = os.environ.get("SERVICE_NAME", "shipping-service")
SNS_TOPIC_ARN: str = os.environ["SHIPMENT_SNS_ARN"]
AWS_ENDPOINT_URL: str | None = os.environ.get("AWS_ENDPOINT_URL")
AWS_REGION: str = os.environ.get("AWS_REGION", "us-east-1")
HEALTH_PORT: int = int(os.environ.get("HEALTH_PORT", "8080"))
PUBLISH_INTERVAL_SECONDS: float = float(os.environ.get("PUBLISH_INTERVAL_SECONDS", "2"))
STARTUP_TIMEOUT_SECONDS: float = float(os.environ.get("STARTUP_TIMEOUT_SECONDS", "60"))

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


SAMPLE_SHIPMENTS: list[dict[str, str]] = [
    {
        "shipment_id": "shp_8f21c4a7",
        "carrier": "ups",
        "tracking_number": "1Z999AA10123456784",
        "status": "dispatched",
        "estimated_delivery": "2024-11-06",
    },
    {
        "shipment_id": "shp_1b90de33",
        "carrier": "fedex",
        "tracking_number": "778899001122",
        "status": "dispatched",
        "estimated_delivery": "2024-11-05",
    },
    {
        "shipment_id": "shp_4c07ab19",
        "carrier": "dhl",
        "tracking_number": "JD014600003914521104",
        "status": "in_transit",
        "estimated_delivery": "2024-11-08",
    },
    {
        "shipment_id": "shp_2ed5f860",
        "carrier": "usps",
        "tracking_number": "9400111899223197428490",
        "status": "dispatched",
        "estimated_delivery": "2024-11-07",
    },
    {
        "shipment_id": "shp_63aa7c02",
        "carrier": "ups",
        "tracking_number": "1Z999AA10123456799",
        "status": "out_for_delivery",
        "estimated_delivery": "2024-11-04",
    },
]


def build_sns_client():
    return boto3.client(
        "sns",
        endpoint_url=AWS_ENDPOINT_URL,
        region_name=AWS_REGION,
        config=Config(retries={"max_attempts": 3, "mode": "standard"}),
    )


def wait_for_topic(client, timeout_seconds: float) -> None:
    deadline = time.monotonic() + timeout_seconds
    delay = 0.5
    last_error: Exception | None = None
    while time.monotonic() < deadline and not _shutdown.is_set():
        try:
            client.get_topic_attributes(TopicArn=SNS_TOPIC_ARN)
            return
        except (ClientError, BotoCoreError) as exc:
            last_error = exc
            log(SERVICE_NAME, "waiting_for_broker", retry_in_seconds=delay)
            if _shutdown.wait(delay):
                return
            delay = min(delay * 2, 5.0)
    raise RuntimeError(f"SNS topic not reachable within {timeout_seconds}s: {last_error}")


def publish(client, shipment: dict[str, str]) -> str:
    response = client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject="shipment-dispatched",
        Message=json.dumps(shipment),
        MessageAttributes={
            "event_type": {"DataType": "String", "StringValue": "shipment.dispatched"},
            "carrier": {"DataType": "String", "StringValue": shipment["carrier"]},
        },
    )
    return str(response["MessageId"])


def main() -> None:
    start_health_server(HEALTH_PORT)
    log(SERVICE_NAME, "started", health_port=HEALTH_PORT)

    client = build_sns_client()
    wait_for_topic(client, STARTUP_TIMEOUT_SECONDS)

    for shipment in SAMPLE_SHIPMENTS:
        if _shutdown.is_set():
            break
        try:
            message_id = publish(client, shipment)
        except (ClientError, BotoCoreError) as exc:
            log(SERVICE_NAME, "publish_failed", shipment_id=shipment["shipment_id"], error=str(exc))
            continue
        log(
            SERVICE_NAME,
            "published",
            topic=SNS_TOPIC_ARN,
            message_id=message_id,
            shipment_id=shipment["shipment_id"],
            carrier=shipment["carrier"],
        )
        _shutdown.wait(PUBLISH_INTERVAL_SECONDS)

    _shutdown.wait()
    log(SERVICE_NAME, "stopping")


if __name__ == "__main__":
    main()
