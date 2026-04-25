FROM python:3.11-slim

# Install Java (required for Spark)
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jdk-headless procps curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install PySpark (lightweight) + PostgreSQL driver
RUN pip install --no-cache-dir pyspark==3.5.0 psycopg2-binary

WORKDIR /app
COPY etl_job.py .

CMD ["python", "etl_job.py"]
