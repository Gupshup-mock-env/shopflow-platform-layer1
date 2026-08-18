# ShopFlow — Pricing & Discounts

Two FastStream services from the ShopFlow merchandising domain.

| Service | Role | Description |
|---|---|---|
| `pricing-service` | producer | Owns the effective sell price of every catalogue product. Runs the nightly repricing job and applies merchandiser clearance overrides. |
| `discount-service` | consumer | Rebuilds the promotional discount ladder for a product whenever its price changes. |

Both services are `AsgiFastStream` applications on a `KafkaBroker`, so the same
process serves the Kubernetes probe on `/healthz` and runs the broker workers.

## Local development

Each service is self-contained. Build and run it from its own directory:

```bash
cd pricing-service
pip install -r requirements.txt
KAFKA_BOOTSTRAP=localhost:9092 faststream run app:app --port 8080
```

Docker:

```bash
docker build -t shopflow-pricing-service ./pricing-service
docker build -t shopflow-discount-service ./discount-service
```

## Configuration

| Variable | Default | Notes |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka bootstrap servers |
| `KAFKA_CONSUMER_GROUP` | `discount-service` | Consumer group, `discount-service` only |
| `SERVICE_NAME` | per service | Used in the structured log records |

FastStream's own logger is disabled in both services; each one writes a single
JSON object per line to stdout instead.
