# Build from the repository root so the shared `billing` package and
# `celeryconfig.py` are inside the build context:
#
#   docker build -f invoice-worker/Dockerfile -t shopflow/invoice-worker .
FROM python:3.11-slim

WORKDIR /app

COPY invoice-worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY celeryconfig.py ./celeryconfig.py
COPY billing/ ./billing/
COPY invoice-worker/ ./invoice-worker/

ENV PYTHONPATH=/app

EXPOSE 8080

CMD ["python", "-u", "invoice-worker/app.py"]
