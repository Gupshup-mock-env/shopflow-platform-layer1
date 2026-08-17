# ShopFlow — Analytics

Two services from the ShopFlow analytics domain.

| Service | Role | Description |
|---|---|---|
| `analytics-service`  | producer | Collects client-side interactions and fans them out onto the regional streams. |
| `aggregator-service` | consumer | Folds every regional stream back into per-session counters for the warehouse. |

## Stream naming

Each region owns its own set of per-event-type streams, so a regional outage
cannot back traffic up for the rest of the fleet. `topics.py` (identical in both
services) is the only place that knows the naming scheme:

```python
def topic_for(region: str, event_type: str) -> str:
    return f"shopflow.analytics.{region}.{event_type}"
```

`TRACKED_EVENT_TYPES` lists the event types the pipeline carries. The producer
composes the destination per event; the aggregator subscribes to the full set
for its region. Adding an event type means adding it to that tuple and
redeploying both services — nothing else references the stream names.

## Local development

Each service is self-contained. Build and run it from its own directory:

```bash
cd analytics-service
pip install -r requirements.txt
python -u app.py
```

Docker:

```bash
docker build -t shopflow-analytics-service ./analytics-service
docker build -t shopflow-aggregator-service ./aggregator-service
```

## Configuration

| Variable | Default | Notes |
|---|---|---|
| `REGION` | `us-east-1` | Region this replica is pinned to. Both services must agree. |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka bootstrap servers |
| `KAFKA_CONSUMER_GROUP` | `aggregator-service` | Consumer group, `aggregator-service` only |
| `SERVICE_NAME` | per service | Used in the structured log records |
| `HEALTH_PORT` | `8080` | `GET /healthz` |

Both services log one JSON object per line to stdout.
