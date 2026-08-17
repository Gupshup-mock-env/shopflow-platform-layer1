# Build from the repository root so the code-generation stage can see proto/:
#   docker build -f logistics-service/Dockerfile -t shopflow/logistics-service .
FROM python:3.11-slim AS protogen

WORKDIR /build

COPY proto/requirements.txt ./proto/requirements.txt
RUN pip install --no-cache-dir -r proto/requirements.txt

COPY proto/shipment.proto ./proto/shipment.proto
RUN mkdir -p generated \
    && python -m grpc_tools.protoc --proto_path=proto --python_out=generated shipment.proto


FROM python:3.11-slim

WORKDIR /app

COPY logistics-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY logistics-service/ .
COPY --from=protogen /build/generated/shipment_pb2.py ./shipment_pb2.py

EXPOSE 8080

CMD ["python", "-u", "app.py"]
