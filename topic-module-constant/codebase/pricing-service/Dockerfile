# Build from the repository root:
#   docker build -f pricing-service/Dockerfile -t shopflow-pricing-service .
FROM python:3.11-slim

WORKDIR /app

COPY pricing-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ ./shared/
COPY pricing-service/ ./

ENV PYTHONPATH=/app

EXPOSE 8080

CMD ["python", "-u", "app.py"]
