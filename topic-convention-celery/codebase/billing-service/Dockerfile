# Build from the repository root so the shared `billing` package and
# `celeryconfig.py` are inside the build context:
#
#   docker build -f billing-service/Dockerfile -t shopflow/billing-service .
FROM python:3.11-slim

WORKDIR /app

COPY billing-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY celeryconfig.py ./celeryconfig.py
COPY billing/ ./billing/
COPY billing-service/ ./billing-service/

ENV PYTHONPATH=/app

EXPOSE 8080

CMD ["python", "-u", "billing-service/app.py"]
