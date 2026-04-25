"""
Logistics Shipment Event Producer
Simulates real-time shipment events and sends them to Kafka topic: shipment-events
"""

import json
import time
import random
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BROKER = "kafka:9092"
TOPIC = "shipment-events"

# Simulated regions with bounding boxes [lat_min, lat_max, lon_min, lon_max]
REGIONS = {
    "north": [40.0, 45.0, -80.0, -70.0],
    "south": [25.0, 30.0, -90.0, -80.0],
    "east":  [35.0, 40.0, -75.0, -65.0],
    "west":  [34.0, 38.0, -120.0, -110.0],
    "central": [38.0, 42.0, -95.0, -85.0],
}

STATUSES = ["created", "in_transit", "delivered", "delayed"]
VEHICLES = [f"VH-{str(i).zfill(3)}" for i in range(1, 21)]  # VH-001 to VH-020
WAREHOUSES = ["WH-NYC", "WH-LAX", "WH-CHI", "WH-HOU", "WH-PHX"]


def get_region_coords(region: str) -> tuple:
    bounds = REGIONS[region]
    lat = round(random.uniform(bounds[0], bounds[1]), 6)
    lon = round(random.uniform(bounds[2], bounds[3]), 6)
    return lat, lon


def generate_shipment_event(shipment_counter: int) -> dict:
    region = random.choice(list(REGIONS.keys()))
    lat, lon = get_region_coords(region)
    status = random.choices(
        STATUSES,
        weights=[0.2, 0.4, 0.3, 0.1]  # weighted: more in_transit and delivered
    )[0]

    created_offset = random.randint(0, 72)   # hours ago
    delivery_offset = created_offset + random.randint(1, 48)

    event = {
        "shipment_id": f"SHP-{str(shipment_counter).zfill(6)}",
        "vehicle_id": random.choice(VEHICLES),
        "warehouse_id": random.choice(WAREHOUSES),
        "region": region,
        "latitude": lat,
        "longitude": lon,
        "status": status,
        "weight_kg": round(random.uniform(0.5, 500.0), 2),
        "distance_km": round(random.uniform(10.0, 2000.0), 2),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "estimated_delivery_hours": delivery_offset,
        "actual_delivery_hours": delivery_offset + random.randint(-5, 10) if status == "delivered" else None,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    return event


def create_producer() -> KafkaProducer:
    retries = 10
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                retries=3,
            )
            logger.info("Connected to Kafka broker at %s", KAFKA_BROKER)
            return producer
        except KafkaError as e:
            logger.warning("Kafka not ready (attempt %d/%d): %s", attempt + 1, retries, e)
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka after multiple retries")


def on_send_success(record_metadata):
    logger.debug("Sent to %s [partition %d] offset %d",
                 record_metadata.topic, record_metadata.partition, record_metadata.offset)


def on_send_error(exc):
    logger.error("Failed to send message: %s", exc)


def main():
    producer = create_producer()
    counter = 1
    logger.info("Starting shipment event simulation...")

    try:
        while True:
            event = generate_shipment_event(counter)
            producer.send(
                TOPIC,
                key=event["shipment_id"],
                value=event
            ).add_callback(on_send_success).add_errback(on_send_error)

            logger.info("Produced: %s | status=%s | region=%s",
                        event["shipment_id"], event["status"], event["region"])

            counter += 1
            # Flush every 10 messages
            if counter % 10 == 0:
                producer.flush()

            time.sleep(1)  # 1 event per second

    except KeyboardInterrupt:
        logger.info("Shutting down producer...")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
