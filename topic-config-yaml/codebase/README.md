# ShopFlow Inventory Platform

Two services that keep warehouse stock levels in step with the inventory ledger.

| Service | Role | Description |
|---|---|---|
| `inventory-service` | producer | Emits a stock movement event whenever the ledger changes. |
| `warehouse-service` | consumer | Applies stock movements to the per-warehouse on-hand counts. |

## Configuration

Both services read `config.yaml` from the repository root. In a container the file is
supplied by the platform and its location is given by `SHOPFLOW_CONFIG_PATH`; the loader
falls back to `/etc/shopflow/config.yaml` and then to the copy checked in here.

Broker connection details are environment-only:

| Variable | Default |
|---|---|
| `KAFKA_BOOTSTRAP` | `localhost:9092` |
| `SERVICE_NAME` | the service's own name |
| `SHOPFLOW_CONFIG_PATH` | unset |

## Running locally

Point `KAFKA_BOOTSTRAP` at any single-node Kafka, then:

```bash
cd inventory-service && pip install -r requirements.txt && python -u app.py
cd warehouse-service && pip install -r requirements.txt && python -u app.py
```

Each service exposes `GET /healthz` on port 8080.
