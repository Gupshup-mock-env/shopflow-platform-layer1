# ShopFlow Loyalty Platform

| Service | Role | Description |
| --- | --- | --- |
| `loyalty-service` | producer | Awards loyalty points when an order completes and announces the award. |
| `rewards-service` | consumer | Keeps each customer's redeemable balance up to date. |

## Configuration

Each service owns a `Settings` model built on `pydantic-settings`. Every field has a
default that is correct for the platform, and any field can be overridden by an
environment variable of the same name (case-insensitive), for example `KAFKA_BOOTSTRAP`
or `SERVICE_NAME`.

## Running locally

```bash
cd loyalty-service && pip install -r requirements.txt && python -u app.py
cd rewards-service && pip install -r requirements.txt && python -u app.py
```

Both services expose `GET /healthz` on port 8080.
