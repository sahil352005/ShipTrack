"""
Simple ETL - Consumes from Kafka and writes to PostgreSQL
Replaces heavy PySpark with lightweight Python consumer
"""

import json
import time
import logging
from datetime import datetime, timezone
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────────────
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "shipment-events"
KAFKA_GROUP = "etl-consumer"

PG_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "database": "logistics",
    "user": "logistics_user",
    "password": "logistics_pass",
}

BATCH_SIZE = 50
POLL_TIMEOUT = 10  # seconds


def get_db_connection():
    """Create PostgreSQL connection."""
    return psycopg2.connect(**PG_CONFIG)


def process_event(event: dict) -> dict:
    """Process and enrich a single shipment event."""
    estimated = event.get("estimated_delivery_hours", 0)
    actual = event.get("actual_delivery_hours")
    
    # Determine if delayed (actual > estimated + 2 hours)
    is_delayed = False
    if actual is not None and estimated:
        is_delayed = (actual - estimated) > 2
    
    # Parse timestamp
    event_timestamp = event.get("timestamp")
    if event_timestamp:
        try:
            event_timestamp = datetime.fromisoformat(event_timestamp.replace("Z", "+00:00"))
        except:
            event_timestamp = datetime.now(timezone.utc)
    else:
        event_timestamp = datetime.now(timezone.utc)
    
    created_at = event.get("created_at")
    if created_at:
        try:
            created_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        except:
            created_at = datetime.now(timezone.utc)
    
    return {
        "shipment_id": event.get("shipment_id"),
        "vehicle_id": event.get("vehicle_id"),
        "warehouse_id": event.get("warehouse_id"),
        "region": event.get("region"),
        "latitude": event.get("latitude"),
        "longitude": event.get("longitude"),
        "status": event.get("status"),
        "weight_kg": event.get("weight_kg"),
        "distance_km": event.get("distance_km"),
        "is_delayed": is_delayed,
        "created_at": created_at,
        "estimated_delivery_hours": estimated,
        "actual_delivery_hours": actual,
        "event_timestamp": event_timestamp,
    }


def insert_shipments(conn, records: list):
    """Bulk insert shipment records."""
    if not records:
        return
    
    query = """
        INSERT INTO shipments (
            shipment_id, vehicle_id, warehouse_id, region,
            latitude, longitude, status, weight_kg, distance_km,
            is_delayed, created_at, estimated_delivery_hours,
            actual_delivery_hours, event_timestamp, processed_at
        ) VALUES %s
        ON CONFLICT (shipment_id) DO UPDATE SET
            status = EXCLUDED.status,
            is_delayed = EXCLUDED.is_delayed,
            actual_delivery_hours = EXCLUDED.actual_delivery_hours,
            processed_at = NOW()
    """
    
    values = [
        (
            r["shipment_id"],
            r["vehicle_id"],
            r["warehouse_id"],
            r["region"],
            r["latitude"],
            r["longitude"],
            r["status"],
            r["weight_kg"],
            r["distance_km"],
            r["is_delayed"],
            r["created_at"],
            r["estimated_delivery_hours"],
            r["actual_delivery_hours"],
            r["event_timestamp"],
            datetime.now(timezone.utc),
        )
        for r in records
    ]
    
    with conn.cursor() as cur:
        execute_values(cur, query, values)
    conn.commit()
    logger.info(f"Inserted/updated {len(records)} shipment records")


def update_metrics(conn):
    """Update delivery_metrics table with latest data."""
    query = """
        INSERT INTO delivery_metrics (
            shipment_id, region, status,
            estimated_delivery_hours, actual_delivery_hours,
            delay_hours, is_delayed, on_time, processed_at
        )
        SELECT 
            s.shipment_id,
            s.region,
            s.status,
            s.estimated_delivery_hours,
            s.actual_delivery_hours,
            s.actual_delivery_hours - s.estimated_delivery_hours,
            s.is_delayed,
            NOT s.is_delayed,
            NOW()
        FROM shipments s
        WHERE NOT EXISTS (
            SELECT 1 FROM delivery_metrics dm 
            WHERE dm.shipment_id = s.shipment_id
        )
        ON CONFLICT (shipment_id) DO UPDATE SET
            status = EXCLUDED.status,
            is_delayed = EXCLUDED.is_delayed,
            actual_delivery_hours = EXCLUDED.actual_delivery_hours,
            delay_hours = EXCLUDED.delay_hours,
            on_time = EXCLUDED.on_time,
            processed_at = NOW()
    """
    
    with conn.cursor() as cur:
        cur.execute(query)
    conn.commit()
    logger.info("Updated delivery_metrics table")


def run_etl():
    """Main ETL loop."""
    logger.info("Starting Simple ETL...")
    
    # Wait for Kafka to be ready
    logger.info("Waiting for Kafka to be ready...")
    max_retries = 30
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                group_id=KAFKA_GROUP,
                auto_offset_reset="earliest",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=5000,
            )
            logger.info("Connected to Kafka!")
            break
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1}/{max_retries}: {e}")
            time.sleep(2)
    else:
        logger.error("Failed to connect to Kafka after max retries")
        return
    
    # Test database connection
    logger.info("Testing database connection...")
    for attempt in range(10):
        try:
            conn = get_db_connection()
            conn.close()
            logger.info("Database connection successful!")
            break
        except Exception as e:
            logger.warning(f"DB attempt {attempt + 1}/10: {e}")
            time.sleep(2)
    else:
        logger.error("Failed to connect to database")
        return
    
    logger.info("ETL is running! Consuming from Kafka and writing to PostgreSQL...")
    
    batch = []
    last_metrics_update = time.time()
    
    try:
        while True:
            # Poll for messages
            messages = consumer.poll(timeout_ms=1000)
            
            for topic_partition, records in messages.items():
                for record in records:
                    try:
                        processed = process_event(record.value)
                        batch.append(processed)
                        logger.debug(f"Processed: {processed['shipment_id']}")
                    except Exception as e:
                        logger.error(f"Error processing event: {e}")
            
            # Insert batch when full
            if len(batch) >= BATCH_SIZE:
                conn = get_db_connection()
                try:
                    insert_shipments(conn, batch)
                    batch = []
                finally:
                    conn.close()
            
            # Update metrics every 30 seconds
            if time.time() - last_metrics_update > 30:
                conn = get_db_connection()
                try:
                    update_metrics(conn)
                    last_metrics_update = time.time()
                finally:
                    conn.close()
                    
    except KeyboardInterrupt:
        logger.info("ETL stopped by user")
    except Exception as e:
        logger.error(f"ETL error: {e}")
    finally:
        consumer.close()
        logger.info("ETL shutdown complete")


if __name__ == "__main__":
    run_etl()