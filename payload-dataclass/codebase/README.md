# ShopFlow Inventory

Stock movements recorded at the warehouses, streamed to the services that need
to react to them.

| Service | Role |
| --- | --- |
| `inventory-service` | Publishes a `StockUpdate` to Kafka for every stock movement. |
| `warehouse-service` | Consumes the stream and keeps its on-hand counters current. |

## Wire format

`shared/models.py` holds the event definitions used by both sides. Events go on
the wire as UTF-8 JSON produced by `dataclasses.asdict()`.

## Build

Images build from the repository root so both stages can see `shared/`:

    docker build -f inventory-service/Dockerfile -t inventory-service:latest .
    docker build -f warehouse-service/Dockerfile -t warehouse-service:latest .

## Configuration

| Variable | Default |
| --- | --- |
| `KAFKA_BOOTSTRAP` | `localhost:9092` |
| `SERVICE_NAME` | the service name |
| `HEALTH_PORT` | `8080` |
| `KAFKA_CONSUMER_GROUP` (warehouse-service only) | `warehouse-service` |

Both services answer `GET /healthz` on `HEALTH_PORT`.
