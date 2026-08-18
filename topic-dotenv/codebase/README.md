# ShopFlow Returns Platform

| Service | Role | Description |
| --- | --- | --- |
| `returns-service` | producer | Opens a return authorisation and announces it to the rest of the platform. |
| `refund-service` | consumer | Reserves the refund amount for each authorised return. |

## Configuration

Both services call `python-dotenv` at startup. `load_dotenv` walks up from the working
directory, finds the `.env` at the repository root, and fills in anything the process
environment does not already define, so real deployments can override any key.

Keys used:

| Key | Meaning |
| --- | --- |
| `KAFKA_BOOTSTRAP` | Kafka bootstrap servers |
| `RETURNS_TOPIC` | Topic carrying return authorisations |
| `RETURNS_CONSUMER_GROUP` | Consumer group used by `refund-service` |
| `PUBLISH_INTERVAL_SECONDS` | Gap between sample publishes on startup |
| `POLL_TIMEOUT_SECONDS` | Consumer poll timeout |
| `HEALTH_PORT` | Port serving `GET /healthz` |

## Running locally

```bash
cd returns-service && pip install -r requirements.txt && python -u app.py
cd refund-service && pip install -r requirements.txt && python -u app.py
```
