# ShopFlow — Risk

Two services from the ShopFlow risk domain.

| Service | Role | Description |
|---|---|---|
| `fraud-service`  | producer | Scores settled orders and asks for a fraud decision on the ones that trip a rule. |
| `review-service` | consumer | Picks up those requests and routes them to an analyst queue or to auto-screening. |

## Local development

Each service is self-contained. Build and run it from its own directory:

```bash
cd fraud-service
pip install -r requirements.txt
python -u app.py
```

Docker:

```bash
docker build -t shopflow-fraud-service ./fraud-service
docker build -t shopflow-review-service ./review-service
```

## Configuration

Nothing is baked into the images. Both services read their wiring from the
process environment, and both refuse to start when a required key is missing —
a misconfigured deployment fails loudly at boot instead of silently writing to
the wrong place.

| Variable | Default | Notes |
|---|---|---|
| `FRAUD_CHECK_TOPIC` | **required** | Set by the platform from the risk domain's messaging ConfigMap. Deliberately has no in-repo default: the value differs per environment and is owned by the infrastructure repository, not by this one. |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka bootstrap servers |
| `KAFKA_CONSUMER_GROUP` | `review-service` | Consumer group, `review-service` only |
| `SERVICE_NAME` | per service | Used in the structured log records |
| `HEALTH_PORT` | `8080` | `GET /healthz` |

Both services log one JSON object per line to stdout.
