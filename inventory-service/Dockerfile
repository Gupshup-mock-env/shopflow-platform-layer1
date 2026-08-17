# Build from the repository root so the shared/ package is in the build context:
#   docker build -f inventory-service/Dockerfile -t shopflow/inventory-service .
FROM python:3.11-slim

WORKDIR /app

COPY inventory-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ ./shared/
COPY inventory-service/ .

EXPOSE 8080

CMD ["python", "-u", "app.py"]
