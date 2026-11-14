from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

# GLOBAL VAR

KAFKA_BOOTSTRAP = "localhost:9092"
SOURCE_TOPIC = "transactions"
VALID_TOPIC = "transactions_valid"
DLQ_TOPIC = "transactions_dlq"
CHECKPOINT_BASE = "/tmp/checkpoints/spark_streaming_job2"

SCHEMA = StructType([
    StructField("user_id", StringType(), True),
    StructField("amount", StringType(), True),  
    StructField("timestamp", StringType(), True),
    StructField("source", StringType(), True),
])

VALID_SOURCES = ["mobile", "web", "pos"]


def create_spark():
    return (
        SparkSession.builder
        .appName("spark-streaming-job2")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.5"
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")  # timestamp producer dalam UTC ('Z')
        .getOrCreate()
    )


def read_from_kafka(spark):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", SOURCE_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )


def parse_json(df):
    """Deserialize JSON ke spark df"""
    return (
        df.selectExpr("CAST(value AS STRING) AS json_str")
        .select(F.from_json("json_str", SCHEMA).alias("data"))
        .select("data.*")
    )


def validate(df):
    """5 validasi wajib untuk cek apakah is_valid"""
    # 1. mandatory field check
    missing = (
        F.col("user_id").isNull() | (F.trim("user_id") == "")
        | F.col("amount").isNull()
        | F.col("timestamp").isNull()
    )

    # 2. type validation: amount harus bisa cast ke long, timestamp harus bisa diparse
    bad_type = (
        (F.col("amount").isNotNull() & F.col("amount_long").isNull())
        | (F.col("timestamp").isNotNull() & F.col("ts").isNull())
    )

    # 3. range validation: 1 - 10.000.000
    out_of_range = F.col("amount_long").isNotNull() & ~F.col("amount_long").between(1, 10_000_000)

    # 4. source validation
    bad_source = F.col("source").isNotNull() & ~F.col("source").isin(VALID_SOURCES)

    # 5. Duplicate detection (user_id + timestamp) dilakukan di process_batch, karena window function non-time tidak didukung di streaming DataFrame.

    reason = (
        F.when(missing, "missing field")
        .when(bad_type, "invalid type (amount/timestamp)")
        .when(out_of_range, "amount out of range")
        .when(bad_source, "unknown source")
        .otherwise(F.lit(None))
    )

    return (
        df.withColumn("amount_long", F.expr("try_cast(amount AS BIGINT)"))
        .withColumn("is_valid", reason.isNull())
        .withColumn("error_reason", reason)
    )


def send_to_kafka(df_json, topic):
    (
        df_json.write.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("topic", topic)
            .save()
    )


def process_batch(batch_df, batch_id, state):
    """Routing valid/invalid + window monitoring + console output."""
    if not batch_df.take(1):
        return

    # 5. Duplicate detection (user_id + timestamp), lintas micro-batch
    #    memakai set "seen" di driver state.
    key_col = F.concat_ws("|", "user_id", "ts")
    batch_keys = [
        row["key"] for row in
        batch_df.withColumn("key", key_col).select("key").collect()
    ]
    dup_keys = {
        key for key in batch_keys
        if key in state["seen"] or batch_keys.count(key) > 1
    }
    state["seen"].update(batch_keys)

    # late event: timestamp lebih dari 3 menit -> invalid (DLQ).
    threshold = F.current_timestamp() - F.expr("interval 3 minutes")
    batch_df = batch_df.withColumn("dup_key", key_col)
    batch_df = (
        batch_df
        .withColumn(
            "is_dup",
            F.col("dup_key").isin(list(dup_keys)) if dup_keys else F.lit(False),
        )
        .withColumn(
            "is_late",
            F.col("ts") < threshold,
        )
        .withColumn(
            "is_valid",
            F.col("is_valid") & ~F.col("is_dup") & ~F.col("is_late"),
        )
        .withColumn(
            "error_reason",
            F.when(F.col("is_dup"), F.coalesce(F.col("error_reason"), F.lit("duplicate event")))
            .when(F.col("is_late"), F.coalesce(F.col("error_reason"), F.lit("late event beyond watermark")))
            .otherwise(F.col("error_reason")),
        )
    )

    # routing: kalo valid masuk ke transactions_valid, kalo invalid ke transactions_dlq
    valid_df = batch_df.filter(F.col("is_valid"))
    invalid_df = batch_df.filter(~F.col("is_valid"))

    send_to_kafka(
        valid_df.select(
            F.to_json(
                F.struct("user_id", "amount", "timestamp", "source")
            ).alias("value")
        ),
        VALID_TOPIC,
    )
    send_to_kafka(
        invalid_df.select(
            F.to_json(
                F.struct("user_id", "amount", "timestamp", "source", "error_reason")
            ).alias("value")
        ),
        DLQ_TOPIC,
    )

    # Tumbling window 1 menit: hitung total transaksi valid per window.
    window_df = (
        valid_df.groupBy(F.window("ts", "1 minute"))
        .agg(F.count("*").alias("total_transactions"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "total_transactions",
        )
    )
    rows = window_df.orderBy("window_start").collect()
    for row in rows:
        state["running_total"] += row["total_transactions"]

    # Console output: timestamp + running_total
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(
        f"timestamp={now} running_total={state['running_total']}",
        flush=True,
    )


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = read_from_kafka(spark)
    events = parse_json(raw)

    # Watermark 3 menit pada event_time
    events = (
        events.withColumn("ts", F.try_to_timestamp("timestamp", F.lit("yyyy-MM-dd'T'HH:mm:ss'Z'")))
        .withWatermark("ts", "3 minutes")
    )
    validated = validate(events)

    state = {"running_total": 0, "seen": set()}

    query = (
        validated.writeStream
        .foreachBatch(lambda df, bid: process_batch(df, bid, state))
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/route")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
