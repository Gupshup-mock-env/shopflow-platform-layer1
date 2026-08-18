# ShopFlow — Clickstream & Storefront Metrics

Two Faust stream-processing services from the ShopFlow analytics domain.

| Service | Role | Description |
|---|---|---|
| `clickstream-service` | producer | Collects storefront interactions from the web tier and writes them onto the raw clickstream. |
| `metrics-service` | consumer | Aggregates the raw clickstream into the counters the merchandising dashboards read. |

Both services are Faust applications, so each one runs as a Faust worker and
serves its Kubernetes probe from Faust's built-in web server.

## Local development

Each service is self-contained. Build and run it from its own directory:

```bash
cd clickstream-service
pip install -r requirements.txt
KAFKA_BOOTSTRAP=localhost:9092 faust -A app worker -l info
```

Docker:

```bash
docker build -t shopflow-clickstream-service ./clickstream-service
docker build -t shopflow-metrics-service ./metrics-service
```

## Configuration

| Variable | Default | Notes |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka bootstrap servers |
| `SERVICE_NAME` | per service | Faust application id, and the consumer group that follows from it |
| `HEALTH_PORT` | `8080` | Faust web port; serves `GET /healthz` |

Faust's own logging is routed to stderr and stdout redirection is disabled, so
stdout carries one JSON object per line and nothing else.

## Web endpoints

| Path | Service | Purpose |
|---|---|---|
| `/healthz` | both | Probe endpoint |
| `/metrics/actions` | `metrics-service` | Running action counts |
