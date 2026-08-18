# ShopFlow — Identity

| Service | Role | Description |
|---|---|---|
| `user-service` | producer | Owns registration and account records. Announces new accounts. |
| `profile-service` | consumer | Builds the customer profile projection from account events. |

Each service wraps its Kafka client in a small class (`UserEventPublisher` /
`UserEventConsumer`) so that the transport can be swapped without touching the
handlers.

## Local development

```bash
cd user-service
pip install -r requirements.txt
KAFKA_BOOTSTRAP=localhost:9092 python -u app.py
```

Docker:

```bash
docker build -t shopflow-user-service ./user-service
docker build -t shopflow-profile-service ./profile-service
```

## Configuration

| Variable | Default | Notes |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka bootstrap servers |
| `KAFKA_CONSUMER_GROUP` | `profile-service` | Consumer group, profile-service only |
| `SERVICE_NAME` | per service | Used in the structured log records |
| `HEALTH_PORT` | `8080` | `GET /healthz` |
