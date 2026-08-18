# Build from the repository root so the shared/ package is in the build context:
#   docker build -f warehouse-service/Dockerfile -t shopflow/warehouse-service .
FROM python:3.11-slim

WORKDIR /app

COPY warehouse-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ ./shared/
COPY warehouse-service/ .

EXPOSE 8080

CMD ["python", "-u", "app.py"]
