FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY etl/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy rule engines, analytics, and ETL from project root
COPY rules/ ./rules/
COPY analytics/ ./analytics/
COPY etl/intelligence_etl.py .

CMD ["python", "intelligence_etl.py"]