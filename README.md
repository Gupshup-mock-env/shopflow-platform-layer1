# ShopFlow Analytics

Storefront behavioural events and the collector that rolls them up.

| Service | Role |
| --- | --- |
| `analytics-service` | Emits a Kafka event every time a shopper opens a product page. |
| `collector-service` | Consumes the stream and maintains per-product view counters. |

## Build

Each service is self-contained and builds from its own directory:

    docker build -t analytics-service:latest analytics-service
    docker build -t collector-service:latest collector-service

## Configuration

| Variable | Default |
| --- | --- |
| `KAFKA_BOOTSTRAP` | `localhost:9092` |
| `SERVICE_NAME` | the service name |
| `HEALTH_PORT` | `8080` |
| `KAFKA_CONSUMER_GROUP` (collector-service only) | `collector-service` |

Both services answer `GET /healthz` on `HEALTH_PORT`.
