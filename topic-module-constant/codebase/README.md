# ShopFlow — Cart & Pricing

Monorepo holding the two services that sit on either side of the shopping-cart
event stream, plus the `shared/` package they both depend on.

```
shared/            topic names and event models used by both services
cart-service/      producer - owns cart state
pricing-service/   consumer - recalculates promotions and shipping thresholds
```

`shared/` is not published to an index. Each image copies it in at build time
and puts it on `PYTHONPATH`, so **all Docker builds use the repository root as
their build context**:

```bash
docker build -f cart-service/Dockerfile     -t shopflow-cart-service     .
docker build -f pricing-service/Dockerfile  -t shopflow-pricing-service  .
```

For a local run, do the same from the repository root:

```bash
pip install -r cart-service/requirements.txt
PYTHONPATH=. KAFKA_BOOTSTRAP=localhost:9092 python -u cart-service/app.py
```

## Configuration

| Variable | Default | Notes |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka bootstrap servers |
| `KAFKA_CONSUMER_GROUP` | `pricing-service` | Consumer group, pricing-service only |
| `SERVICE_NAME` | per service | Used in the structured log records |
| `HEALTH_PORT` | `8080` | `GET /healthz` |
