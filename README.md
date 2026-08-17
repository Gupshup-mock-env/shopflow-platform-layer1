# ShopFlow — Domain Event Bus

Two services that sit either side of the ShopFlow AMQP event bus. Both speak
AMQP through [Kombu](https://docs.celeryq.dev/projects/kombu/), so the exchange,
queue and binding are declared as Python objects rather than in broker config.

| Service | Role | Description |
|---|---|---|
| `event-service` | producer | Turns committed transactions into event envelopes and publishes them onto the bus. |
| `audit-service` | consumer | Binds the whole `event.#` space and writes every envelope to the compliance audit log. |

## Topology

`bus.py` in each service holds that service's view of the topology. The exchange
declaration is identical on both sides, which keeps the declaration idempotent
and lets either service be deployed first.

## Local development

Each service is self-contained. Build and run it from its own directory:

```bash
cd event-service
pip install -r requirements.txt
RABBITMQ_HOST=localhost python -u app.py
```

Docker:

```bash
docker build -t shopflow-event-service ./event-service
docker build -t shopflow-audit-service ./audit-service
```

## Configuration

| Variable | Default | Notes |
|---|---|---|
| `RABBITMQ_HOST` | `localhost` | Broker hostname |
| `RABBITMQ_PORT` | `5672` | Broker port |
| `RABBITMQ_USER` | `guest` | Broker username |
| `RABBITMQ_PASSWORD` | `guest` | Broker password |
| `RABBITMQ_VHOST` | `/` | Virtual host |
| `SERVICE_NAME` | per service | Used in the structured log records |
| `HEALTH_PORT` | `8080` | `GET /healthz` |

Both services log one JSON object per line to stdout.
