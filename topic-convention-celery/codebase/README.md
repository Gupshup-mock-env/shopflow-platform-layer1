# ShopFlow billing

Invoice generation, split across a producer and a Celery worker.

```
billing/            shared library: Celery app, tasks, telemetry
celeryconfig.py     Celery settings, loaded via config_from_object
billing-service/    enqueues invoice work for settled orders
invoice-worker/     runs the Celery worker that executes it
```

## Running the services

Both images are built from the repository root so the shared `billing`
package and `celeryconfig.py` land in the build context:

```sh
docker build -f billing-service/Dockerfile -t shopflow/billing-service .
docker build -f invoice-worker/Dockerfile  -t shopflow/invoice-worker  .
```

Locally, with a broker already listening:

```sh
export RABBITMQ_HOST=localhost RABBITMQ_USER=guest RABBITMQ_PASSWORD=guest
PYTHONPATH=. python -u billing-service/app.py
PYTHONPATH=. python -u invoice-worker/app.py
```

`invoice-worker/app.py` is a thin wrapper: it adds the `/healthz` endpoint,
waits for the broker, then hands control to `celery -A billing.celery_app
worker`, subscribed to every queue the configured task routes point at.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `RABBITMQ_HOST` | `localhost` | AMQP host |
| `RABBITMQ_PORT` | `5672` | AMQP port |
| `RABBITMQ_USER` | `guest` | AMQP username |
| `RABBITMQ_PASSWORD` | `guest` | AMQP password |
| `RABBITMQ_VHOST` | `/` | AMQP virtual host |
| `SERVICE_NAME` | per service | identifies the process in logs and to the broker |
| `HEALTH_PORT` | `8080` | port the health endpoint binds to |
| `WORKER_CONCURRENCY` | `1` | worker pool size |
