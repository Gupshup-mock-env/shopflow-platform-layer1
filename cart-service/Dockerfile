# Build from the repository root:
#   docker build -f cart-service/Dockerfile -t shopflow-cart-service .
FROM python:3.11-slim

WORKDIR /app

COPY cart-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ ./shared/
COPY cart-service/ ./

ENV PYTHONPATH=/app

EXPOSE 8080

CMD ["python", "-u", "app.py"]
