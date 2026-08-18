# shopflow-orders

Order intake and payment capture for ShopFlow.

## Layout

```
order-service/     HTTP-facing service that accepts placed orders
payment-worker/    Celery worker that captures authorised payments
```

Both deployables ship the same `orders/` package (Celery app, `celeryconfig`
and the task registry) so that the caller and the executor agree on task names
and signatures. The package is vendored into each image because each service
builds from its own directory; keep the two copies in sync when you change a
task signature.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `SERVICE_NAME` | per service | value used in the structured log stream |
| `RABBITMQ_HOST` | `localhost` | AMQP host |
| `RABBITMQ_PORT` | `5672` | AMQP port |
| `RABBITMQ_USER` | `guest` | AMQP user |
| `RABBITMQ_PASSWORD` | `guest` | AMQP password |
| `RABBITMQ_VHOST` | `/` | AMQP virtual host |
| `HEALTH_PORT` | `8080` | port serving `GET /healthz` |

Everything else lives in `celeryconfig.py`.

## Running locally

```
docker run -d --rm -p 5672:5672 rabbitmq:3.13-management

cd payment-worker && pip install -r requirements.txt
celery -A orders.worker worker --loglevel=INFO

cd order-service && pip install -r requirements.txt
python -u app.py
```

Both processes emit one JSON object per line on stdout.
