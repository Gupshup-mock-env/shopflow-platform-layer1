# ShopFlow — Catalogue Enrichment

Two services from the ShopFlow catalogue domain.

| Service | Role | Description |
|---|---|---|
| `enrichment-service` | producer | Derives presentation attributes for merchandising products and emits the enriched record. |
| `store-service` | consumer | Keeps the storefront product index in sync with enriched records. |

## Local development

Each service is self-contained. Build and run it from its own directory:

```bash
cd enrichment-service
pip install -r requirements.txt
KAFKA_BOOTSTRAP=localhost:9092 python -u app.py
```

Docker:

```bash
docker build -t shopflow-enrichment-service ./enrichment-service
docker build -t shopflow-store-service ./store-service
```

## Configuration

| Variable | Default | Notes |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka bootstrap servers |
| `KAFKA_CONSUMER_GROUP` | `store-service` | Consumer group, `store-service` only |
| `SERVICE_NAME` | per service | Used in the structured log records |
| `HEALTH_PORT` | `8080` | `GET /healthz` |

Both services log one JSON object per line to stdout.
