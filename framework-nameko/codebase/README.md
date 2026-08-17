# ShopFlow — Accounts

Two [Nameko](https://nameko.readthedocs.io/) microservices from the ShopFlow
accounts domain. Both run under the Nameko service runner and communicate over
the service event bus rather than by calling each other directly.

| Service | Role | Description |
|---|---|---|
| `registration-service` | producer | Owns account activation. Drains the registration outbox and dispatches an event per activated account. |
| `welcome-service` | consumer | Handles the registration event and sends the onboarding email. |

## Running a service

Each service is self-contained and started by the Nameko runner:

```bash
cd registration-service
pip install -r requirements.txt
RABBITMQ_HOST=localhost nameko run --config config.yaml registration_service:RegistrationService
```

```bash
cd welcome-service
pip install -r requirements.txt
RABBITMQ_HOST=localhost nameko run --config config.yaml welcome_service:WelcomeService
```

Docker:

```bash
docker build -t shopflow-registration-service ./registration-service
docker build -t shopflow-welcome-service ./welcome-service
```

## Configuration

`config.yaml` is the Nameko configuration file. It interpolates the following
environment variables into `AMQP_URI`:

| Variable | Default | Notes |
|---|---|---|
| `RABBITMQ_HOST` | `localhost` | Broker hostname |
| `RABBITMQ_PORT` | `5672` | Broker port |
| `RABBITMQ_USER` | `guest` | Broker username |
| `RABBITMQ_PASSWORD` | `guest` | Broker password |
| `SERVICE_NAME` | per service | Used in the structured log records |
| `HEALTH_PORT` | `8080` | `GET /healthz` |

Nameko's own runner logs go to stderr. Each service logs one JSON object per
line to stdout.
