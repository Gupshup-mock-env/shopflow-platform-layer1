# ShopFlow — Catalogue & Search

Two services from the ShopFlow product domain.

| Service | Role | Description |
|---|---|---|
| `catalog-service` | producer | Owns the product catalogue. Emits an event whenever a merchandiser edits a product. |
| `search-service`  | consumer | Maintains the storefront search index from catalogue events. |

## Local development

Each service is self-contained. Build and run it from its own directory:

```bash
cd catalog-service
pip install -r requirements.txt
KAFKA_BOOTSTRAP=localhost:9092 python -u app.py
```

Docker:

```bash
docker build -t shopflow-catalog-service ./catalog-service
docker build -t shopflow-search-service ./search-service
```

## Configuration

| Variable | Default | Notes |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka bootstrap servers |
| `KAFKA_CONSUMER_GROUP` | `search-service` | Consumer group, search-service only |
| `SERVICE_NAME` | per service | Used in the structured log records |
| `HEALTH_PORT` | `8080` | `GET /healthz` |

Both services log one JSON object per line to stdout.
