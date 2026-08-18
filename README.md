# ShopFlow — Legacy Order Sync

Two services that carry orders from the legacy fulfilment stack into the modern
pipeline.

| Service | Role | Description |
|---|---|---|
| `legacy-service` | producer | Drains the pending order batch out of the legacy export and puts it on the bus. |
| `processor-service` | consumer | Stages incoming orders for the modern fulfilment pipeline. |

`legacy_models.py` is mirrored in both service directories on purpose: the sync
payloads are pickled, so both sides must resolve the same dotted module path.
Edit the two copies together (SHOP-2984).

## Local development

Each service is self-contained. Build and run it from its own directory:

```bash
cd legacy-service
pip install -r requirements.txt
KAFKA_BOOTSTRAP=localhost:9092 python -u app.py
```

Docker:

```bash
docker build -t shopflow-legacy-service ./legacy-service
docker build -t shopflow-processor-service ./processor-service
```

## Configuration

| Variable | Default | Notes |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka bootstrap servers |
| `KAFKA_CONSUMER_GROUP` | `processor-service` | Consumer group, `processor-service` only |
| `SERVICE_NAME` | per service | Used in the structured log records |
| `HEALTH_PORT` | `8080` | `GET /healthz` |

Both services log one JSON object per line to stdout.
