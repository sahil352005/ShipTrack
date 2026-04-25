"""
Intelligence ETL - Runs Rule Engines and Updates Intelligence Tables
Consumes from Kafka → applies rules → writes to PostgreSQL + intelligence tables
"""

import json
import time
import logging
from datetime import datetime, timezone
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import execute_values

# Import rule engines
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from rules.delay_risk_engine import DelayRiskEngine
from rules.warehouse_bottleneck import WarehouseBottleneckDetector
from rules.alerts import AlertEngine
from analytics.region_performance import RegionPerformanceAnalyzer
from analytics.sla_monitor import SLAMonitor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────────────
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "shipment-events"
KAFKA_GROUP = "intelligence-etl"

PG_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "database": "logistics",
    "user": "logistics_user",
    "password": "logistics_pass",
}

BATCH_SIZE = 50
INTELLIGENCE_UPDATE_INTERVAL = 30  # seconds


def get_db_connection():
    """Create PostgreSQL connection."""
    return psycopg2.connect(**PG_CONFIG)


def process_event(event: dict) -> dict:
    """Process and enrich a single shipment event."""
    estimated = event.get("estimated_delivery_hours", 0)
    actual = event.get("actual_delivery_hours")
    
    # Determine if delayed
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


def calculate_and_store_risk(conn, shipment: dict):
    """Calculate risk score and store in shipment_risk table."""
    risk_engine = DelayRiskEngine()
    
    # Get warehouse stats
    warehouse_stats = get_warehouse_stats(conn)
    
    # Get region stats
    region_stats = get_region_stats(conn)
    
    # Assess risk
    risk = risk_engine.assess_risk(shipment, warehouse_stats, region_stats)
    
    # Store in database
    query = """
        INSERT INTO shipment_risk (shipment_id, risk_score, risk_level, reason, rules_triggered, updated_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (shipment_id) DO UPDATE SET
            risk_score = EXCLUDED.risk_score,
            risk_level = EXCLUDED.risk_level,
            reason = EXCLUDED.reason,
            rules_triggered = EXCLUDED.rules_triggered,
            updated_at = NOW()
    """
    
    with conn.cursor() as cur:
        cur.execute(query, (
            risk.shipment_id,
            risk.risk_score,
            risk.risk_level,
            risk.reason,
            risk.rules_triggered
        ))
    conn.commit()


def get_warehouse_stats(conn) -> dict:
    """Get warehouse statistics from database."""
    stats = {}
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT warehouse_id, delay_rate FROM warehouse_health")
            for row in cur.fetchall():
                stats[row[0]] = {"delay_rate": row[1]}
    except Exception as e:
        logger.warning(f"Could not get warehouse stats: {e}")
    return stats


def get_region_stats(conn) -> dict:
    """Get region statistics from database."""
    stats = {}
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT region, total_shipments, on_time_rate FROM region_performance")
            for row in cur.fetchall():
                stats[row[0]] = {"volume": row[1], "on_time_rate": row[2]}
    except Exception as e:
        logger.warning(f"Could not get region stats: {e}")
    return stats


def update_warehouse_health(conn):
    """Update warehouse health based on current data."""
    detector = WarehouseBottleneckDetector()
    
    # Get warehouse stats from shipments
    query = """
        SELECT 
            warehouse_id,
            COUNT(*) as total,
            SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) as delay_rate,
            AVG(estimated_delivery_hours) as avg_hours
        FROM shipments
        WHERE created_at > NOW() - INTERVAL '24 hours'
        GROUP BY warehouse_id
    """
    
    with conn.cursor() as cur:
        cur.execute(query)
        warehouses = []
        for row in cur.fetchall():
            warehouses.append({
                "warehouse_id": row[0],
                "total_shipments": row[1],
                "delay_rate": row[2] or 0,
                "avg_processing_hours": row[3] or 0,
                "capacity": 100
            })
    
    # Detect bottlenecks
    results = detector.detect_bottlenecks(warehouses)
    
    # Update database
    for r in results:
        query = """
            INSERT INTO warehouse_health (warehouse_id, bottleneck, severity, reason, avg_processing_hours, delay_rate, total_shipments, volume_ratio, last_checked)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (warehouse_id) DO UPDATE SET
                bottleneck = EXCLUDED.bottleneck,
                severity = EXCLUDED.severity,
                reason = EXCLUDED.reason,
                avg_processing_hours = EXCLUDED.avg_processing_hours,
                delay_rate = EXCLUDED.delay_rate,
                total_shipments = EXCLUDED.total_shipments,
                volume_ratio = EXCLUDED.volume_ratio,
                last_checked = NOW()
        """
        with conn.cursor() as cur:
            cur.execute(query, (
                r.warehouse_id,
                r.bottleneck,
                r.severity,
                r.reason,
                r.metrics.get("avg_processing_hours", 0),
                r.metrics.get("delay_rate", 0),
                r.metrics.get("volume", 0),
                r.metrics.get("volume", 0) / 100 if r.metrics.get("capacity") else 0
            ))
    conn.commit()
    logger.info(f"Updated warehouse health for {len(results)} warehouses")


def update_region_performance(conn):
    """Update region performance metrics."""
    analyzer = RegionPerformanceAnalyzer()
    
    # Get shipments
    with conn.cursor() as cur:
        cur.execute("""
            SELECT shipment_id, region, status, actual_delivery_hours
            FROM shipments
            WHERE created_at > NOW() - INTERVAL '24 hours'
        """)
        shipments = []
        for row in cur.fetchall():
            shipments.append({
                "shipment_id": row[0],
                "region": row[1],
                "status": row[2],
                "actual_delivery_hours": row[3]
            })
    
    # Analyze
    results = analyzer.analyze_regions(shipments)
    
    # Update database
    for r in results:
        query = """
            INSERT INTO region_performance (region, total_shipments, delivered, delayed, in_transit, on_time_rate, avg_delivery_hours, trend, calculated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (region) DO UPDATE SET
                total_shipments = EXCLUDED.total_shipments,
                delivered = EXCLUDED.delivered,
                delayed = EXCLUDED.delayed,
                in_transit = EXCLUDED.in_transit,
                on_time_rate = EXCLUDED.on_time_rate,
                avg_delivery_hours = EXCLUDED.avg_delivery_hours,
                trend = EXCLUDED.trend,
                calculated_at = NOW()
        """
        with conn.cursor() as cur:
            cur.execute(query, (
                r.region,
                r.total_shipments,
                r.delivered,
                r.delayed,
                r.in_transit,
                r.on_time_rate,
                r.avg_delivery_hours,
                r.trend
            ))
    conn.commit()
    logger.info(f"Updated region performance for {len(results)} regions")


def update_sla_violations(conn):
    """Check and store SLA violations."""
    monitor = SLAMonitor()
    
    # Get shipments with actual delivery hours
    with conn.cursor() as cur:
        cur.execute("""
            SELECT shipment_id, warehouse_id, region, estimated_delivery_hours, actual_delivery_hours
            FROM shipments
            WHERE actual_delivery_hours IS NOT NULL
            AND created_at > NOW() - INTERVAL '24 hours'
        """)
        shipments = []
        for row in cur.fetchall():
            shipments.append({
                "shipment_id": row[0],
                "warehouse_id": row[1],
                "region": row[2],
                "estimated_delivery_hours": row[3],
                "actual_delivery_hours": row[4]
            })
    
    # Analyze violations
    metrics = monitor.analyze_violations(shipments)
    
    # Store violations
    for shipment in shipments:
        violation = monitor.check_violation(shipment)
        if violation:
            query = """
                INSERT INTO sla_violations (shipment_id, warehouse_id, region, estimated_hours, actual_hours, violation_hours, severity)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            with conn.cursor() as cur:
                cur.execute(query, (
                    violation.shipment_id,
                    violation.warehouse_id,
                    violation.region,
                    violation.estimated_hours,
                    violation.actual_hours,
                    violation.violation_hours,
                    violation.severity
                ))
    conn.commit()
    logger.info(f"Stored {metrics.total_violations} SLA violations")


def generate_alerts(conn):
    """Generate alerts based on current metrics."""
    alert_engine = AlertEngine()
    
    # Get metrics
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*), 
                   SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END)
            FROM shipments
        """)
        row = cur.fetchone()
        total = row[0] or 0
        delayed = row[1] or 0
    
    metrics = {
        "total_shipments": total,
        "delayed_count": delayed
    }
    
    # Get warehouse bottlenecks
    with conn.cursor() as cur:
        cur.execute("SELECT warehouse_id, severity FROM warehouse_health WHERE bottleneck = TRUE")
        from rules.warehouse_bottleneck import WarehouseBottleneck
        warehouse_bottlenecks = []
        for row in cur.fetchall():
            wb = WarehouseBottleneck(
                warehouse_id=row[0],
                bottleneck=True,
                severity=row[1],
                reason="",
                metrics={}
            )
            warehouse_bottlenecks.append(wb)
    
    # Get critical risk shipments
    with conn.cursor() as cur:
        cur.execute("SELECT shipment_id, risk_score FROM shipment_risk WHERE risk_score >= 80")
        from rules.delay_risk_engine import RiskAssessment
        risk_assessments = []
        for row in cur.fetchall():
            ra = RiskAssessment(
                shipment_id=row[0],
                risk_score=row[1],
                risk_level="",
                reason="",
                rules_triggered=[]
            )
            risk_assessments.append(ra)
    
    # Generate alerts
    alerts = alert_engine.generate_alerts(metrics, warehouse_bottlenecks, risk_assessments)
    
    # Store alerts
    for alert in alerts:
        query = """
            INSERT INTO alerts (type, severity, message, shipment_id, warehouse_id, region)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        with conn.cursor() as cur:
            cur.execute(query, (
                alert.type,
                alert.severity,
                alert.message,
                alert.shipment_id,
                alert.warehouse_id,
                alert.region
            ))
    conn.commit()
    logger.info(f"Generated {len(alerts)} alerts")


def run_intelligence_etl():
    """Main Intelligence ETL loop."""
    logger.info("Starting Intelligence ETL...")
    
    # Wait for Kafka
    logger.info("Waiting for Kafka to be ready...")
    for attempt in range(30):
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
            logger.warning(f"Attempt {attempt + 1}/30: {e}")
            time.sleep(2)
    else:
        logger.error("Failed to connect to Kafka")
        return
    
    # Test database connection
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
    
    logger.info("Intelligence ETL is running!")
    
    batch = []
    last_intelligence_update = time.time()
    
    try:
        while True:
            # Poll for messages
            messages = consumer.poll(timeout_ms=1000)
            
            for topic_partition, records in messages.items():
                for record in records:
                    try:
                        processed = process_event(record.value)
                        batch.append(processed)
                    except Exception as e:
                        logger.error(f"Error processing event: {e}")
            
            # Insert batch when full
            if len(batch) >= BATCH_SIZE:
                conn = get_db_connection()
                try:
                    insert_shipments(conn, batch)
                    
                    # Calculate risk for each shipment
                    for shipment in batch:
                        calculate_and_store_risk(conn, shipment)
                    
                    batch = []
                finally:
                    conn.close()
            
            # Update intelligence tables every 30 seconds
            if time.time() - last_intelligence_update > INTELLIGENCE_UPDATE_INTERVAL:
                conn = get_db_connection()
                try:
                    update_warehouse_health(conn)
                    update_region_performance(conn)
                    update_sla_violations(conn)
                    generate_alerts(conn)
                    last_intelligence_update = time.time()
                except Exception as e:
                    logger.error(f"Intelligence update error: {e}")
                finally:
                    conn.close()
                    
    except KeyboardInterrupt:
        logger.info("ETL stopped by user")
    except Exception as e:
        logger.error(f"ETL error: {e}")
    finally:
        consumer.close()
        logger.info("Intelligence ETL shutdown complete")


if __name__ == "__main__":
    run_intelligence_etl()