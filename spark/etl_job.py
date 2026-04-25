"""
PySpark Structured Streaming — Logistics ETL Pipeline
Consumes from Kafka → transforms → writes to PostgreSQL
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, BooleanType
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────────────
KAFKA_BROKER      = "kafka:9092"
KAFKA_TOPIC       = "shipment-events"
CHECKPOINT_BASE   = "/tmp/checkpoints"

PG_URL  = "jdbc:postgresql://postgres:5432/logistics"
PG_PROPS = {
    "user":     "logistics_user",
    "password": "logistics_pass",
    "driver":   "org.postgresql.Driver",
}

# ── Schema ───────────────────────────────────────────────────────────────────
SHIPMENT_SCHEMA = StructType([
    StructField("shipment_id",               StringType(),  True),
    StructField("vehicle_id",                StringType(),  True),
    StructField("warehouse_id",              StringType(),  True),
    StructField("region",                    StringType(),  True),
    StructField("latitude",                  DoubleType(),  True),
    StructField("longitude",                 DoubleType(),  True),
    StructField("status",                    StringType(),  True),
    StructField("weight_kg",                 DoubleType(),  True),
    StructField("distance_km",               DoubleType(),  True),
    StructField("created_at",                StringType(),  True),
    StructField("estimated_delivery_hours",  IntegerType(), True),
    StructField("actual_delivery_hours",     IntegerType(), True),
    StructField("timestamp",                 StringType(),  True),
])


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("LogisticsStreamingETL")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_and_transform(raw_df):
    """Parse JSON, apply ETL transformations, return enriched DataFrame."""

    parsed = (
        raw_df
        .select(F.from_json(F.col("value").cast("string"), SHIPMENT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_timestamp", F.to_timestamp("timestamp"))
        .withColumn("created_at",      F.to_timestamp("created_at"))
        .drop("timestamp")
    )

    # ETL: delay detection — delayed if actual > estimated by more than 2 hours
    enriched = (
        parsed
        .withColumn(
            "delay_hours",
            F.when(
                F.col("actual_delivery_hours").isNotNull(),
                F.col("actual_delivery_hours") - F.col("estimated_delivery_hours")
            ).otherwise(F.lit(None).cast(DoubleType()))
        )
        .withColumn(
            "is_delayed",
            F.when(F.col("status") == "delayed", F.lit(True))
             .when(
                 F.col("actual_delivery_hours").isNotNull() &
                 (F.col("actual_delivery_hours") - F.col("estimated_delivery_hours") > 2),
                 F.lit(True)
             )
             .otherwise(F.lit(False))
        )
        .withColumn(
            "on_time",
            F.when(F.col("is_delayed"), F.lit(False)).otherwise(F.lit(True))
        )
    )

    return enriched


def write_shipments(batch_df, batch_id: int):
    """Write raw enriched shipments to PostgreSQL."""
    if batch_df.isEmpty():
        return

    out = batch_df.select(
        "shipment_id", "vehicle_id", "warehouse_id", "region",
        "latitude", "longitude", "status", "weight_kg", "distance_km",
        "is_delayed", "created_at", "estimated_delivery_hours",
        "actual_delivery_hours", "event_timestamp"
    )

    out.write.jdbc(url=PG_URL, table="shipments", mode="append", properties=PG_PROPS)
    logger.info("Batch %d → shipments: %d rows", batch_id, out.count())


def write_delivery_metrics(batch_df, batch_id: int):
    """Write per-shipment delivery metrics to PostgreSQL."""
    if batch_df.isEmpty():
        return

    metrics = (
        batch_df
        .filter(F.col("status").isin("delivered", "delayed"))
        .select(
            "shipment_id", "region", "status",
            "estimated_delivery_hours", "actual_delivery_hours",
            "delay_hours", "is_delayed", "on_time"
        )
        .dropDuplicates(["shipment_id"])
    )

    if not metrics.isEmpty():
        metrics.write.jdbc(url=PG_URL, table="delivery_metrics", mode="append", properties=PG_PROPS)
        logger.info("Batch %d → delivery_metrics: %d rows", batch_id, metrics.count())


def write_route_performance(batch_df, batch_id: int):
    """Aggregate per-region window stats and write to route_performance."""
    if batch_df.isEmpty():
        return

    windowed = (
        batch_df
        .withWatermark("event_timestamp", "10 minutes")
        .groupBy(
            "region",
            F.window("event_timestamp", "5 minutes").alias("win")
        )
        .agg(
            F.count("*").alias("total_shipments"),
            F.sum(F.col("is_delayed").cast("int")).alias("delayed_shipments"),
            F.sum(F.when(F.col("status") == "delivered", 1).otherwise(0)).alias("delivered_shipments"),
            F.avg("actual_delivery_hours").alias("avg_delivery_hours"),
            F.avg("distance_km").alias("avg_distance_km"),
        )
        .withColumn("window_start", F.col("win.start"))
        .withColumn("window_end",   F.col("win.end"))
        .withColumn(
            "on_time_rate",
            F.round(
                (1 - F.col("delayed_shipments") / F.col("total_shipments")) * 100, 2
            )
        )
        .drop("win")
    )

    windowed.write.jdbc(url=PG_URL, table="route_performance", mode="append", properties=PG_PROPS)
    logger.info("Batch %d → route_performance written", batch_id)


def process_batch(batch_df, batch_id: int):
    """Micro-batch processor — called by foreachBatch."""
    batch_df.cache()
    write_shipments(batch_df, batch_id)
    write_delivery_metrics(batch_df, batch_id)
    write_route_performance(batch_df, batch_id)
    batch_df.unpersist()


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Reading from Kafka topic: %s", KAFKA_TOPIC)
    raw = read_kafka_stream(spark)
    enriched = parse_and_transform(raw)

    query = (
        enriched.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/logistics_etl")
        .trigger(processingTime="10 seconds")
        .start()
    )

    logger.info("Streaming query started. Awaiting termination...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
