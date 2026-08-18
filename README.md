# ShopFlow Logistics

Two services that keep the ShopFlow store network in step with carrier movements.

| Service | Role |
| --- | --- |
| `logistics-service` | Publishes a `ShipmentUpdate` to Kafka every time a carrier scans a parcel. |
| `route-service` | Consumes the stream and keeps the routing table for each shipment current. |

## Wire format

`proto/shipment.proto` is the source of truth for what goes on the wire. Both
images generate `shipment_pb2.py` from it during the build; the generated module
is not committed.

## Build

Images build from the repository root so the code-generation stage can see
`proto/`:

    docker build -f logistics-service/Dockerfile -t logistics-service:latest .
    docker build -f route-service/Dockerfile -t route-service:latest .

## Configuration

| Variable | Default |
| --- | --- |
| `KAFKA_BOOTSTRAP` | `localhost:9092` |
| `SERVICE_NAME` | the service name |
| `HEALTH_PORT` | `8080` |
| `KAFKA_CONSUMER_GROUP` (route-service only) | `route-service` |

Both services answer `GET /healthz` on `HEALTH_PORT`.
