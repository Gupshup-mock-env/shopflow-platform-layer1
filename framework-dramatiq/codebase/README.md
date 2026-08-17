# shopflow-alerting

Merchandising alert delivery for ShopFlow.

## Layout

```
alert-service/    inventory sweep front end that raises alerts
alert-worker/     Dramatiq worker that delivers them
```

Both deployables ship the same `alerting/` package (broker wiring plus the
actor definitions) so that the caller and the executor agree on actor names,
signatures and queues. The package is vendored into each image because each
service builds from its own directory; keep the two copies in sync when you
change an actor signature.

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

## Running locally

```
docker run -d --rm -p 5672:5672 rabbitmq:3.13-management

cd alert-worker && pip install -r requirements.txt
dramatiq alerting.actors --processes 1 --threads 2

cd alert-service && pip install -r requirements.txt
python -u app.py
```

Both processes emit one JSON object per line on stdout.
