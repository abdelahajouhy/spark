# Standard library imports
from pathlib import Path

# Third-party imports (PySpark)
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import to_avro
from pyspark.sql.functions import col, decode, explode, struct, timestamp_millis
from pyspark.sql.protobuf.functions import from_protobuf

# Local application imports

"""
====================== STREAMING FOR SILVER ====================

Reads raw Sparkplug B messages from Kafka, decodes Protobuf payloads,
extracts individual metrics into a structured DataFrame, removes duplicates,
and displays the results for validation.

============== Author : AJOUHY ABDELAH - DatAbde ============

"""
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DESC_PATH = PROJECT_ROOT / "proto" / "sparkplugb_poseidon.desc"

# Configuration parameters
KAFKA_BROKERS = "192.168.112.138:9092,192.168.112.139:9092,192.168.112.140:9092"
KAFKA_TOPIC_SRC = "prod.oilgas.bronze"
KAFKA_TOPIC_DST = "prod.oilgas.silver"

# ==============================================================================
# SPARK SESSION & DATA INGESTION
# ==============================================================================

# Initialize Spark Session (Spark Connect)
spark = (
    SparkSession.builder
    .remote("sc://192.168.112.139:15002")
    .appName(KAFKA_TOPIC_DST+"-streaming")
    .getOrCreate()
)

# Tuning configuration
spark.conf.set("spark.sql.shuffle.partitions", "1")

# Read streaming data from Kafka
df_bronze = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKERS)
    .option("subscribe", KAFKA_TOPIC_SRC)
    .option("startingOffsets", "earliest")
    .load()
)

# ==============================================================================
# df_raw = RAW DATA : decode key value protobuff + explode metrics
# ==============================================================================
df_raw = (
    df_bronze
    .select(
        col("timestamp").alias("kafka_ts"),
        col("topic").alias("kafka_topic"),
        "key",
        "value"
    )
    .select(
        "kafka_ts",
        "kafka_topic",
        decode(col("key"), "UTF-8").alias("topic_mqtt"),
        from_protobuf(
            data=col("value"), 
            messageName="org.eclipse.tahu.protobuf.Payload",
            descFilePath=str(DESC_PATH),
            options={"recursive.fields.max.depth": "3"}
        ).alias("data"),
    )
    .select(
        "kafka_ts",
        "kafka_topic",
        "topic_mqtt",
        explode("data.metrics").alias("raw_metrics")
    )
)

# ==============================================================================
# df_metrics = METRICS DATA : DEDUPLICATE
# ==============================================================================
df_metrics = (
    df_raw
    .select(
        "kafka_topic",
        "kafka_ts",
        "topic_mqtt",
        timestamp_millis(col('raw_metrics.timestamp')).alias('metric_ts'),
        col('raw_metrics.name').alias('metric_name'),
        col('raw_metrics.int_value').alias('int_value'),
        col('raw_metrics.long_value').alias('long_value'),
        col('raw_metrics.float_value').alias('float_value'),
        col('raw_metrics.double_value').alias('double_value'),
        col('raw_metrics.boolean_value').alias('boolean_value'),
        col('raw_metrics.string_value').alias('string_value'),
        col('raw_metrics.datatype').alias('datatype')
    )
    .withWatermark('metric_ts', '1 minute')
    .dropDuplicates([
        'kafka_ts',
        'topic_mqtt',
        'metric_ts',
        'metric_name',
        'int_value',
        'long_value',
        'float_value',
        'double_value',
        'boolean_value',
        'string_value',
    ])
)

# ==============================================================================
# DATA WRITING (SILVER STREAM)
# ==============================================================================
df_silver_kafka = (
    df_metrics
    .select(
        to_avro(
            struct("*")
        ).alias("value")
    )
)

silver_stream = (
    df_silver_kafka.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKERS)
    .option("topic", KAFKA_TOPIC_DST)
    .option(
        "checkpointLocation", 
        f"/opt/spark/work-dir/checkpoints/{KAFKA_TOPIC_DST.replace('.', '_')}"
        )
    .outputMode("append")
    .start()
)

try:
    silver_stream.awaitTermination(60)
finally:
    print(f"Status : {silver_stream.status}")
    print(f"Progression : {silver_stream.recentProgress}")
    print(f"Last progression : {silver_stream.lastProgress}")
    silver_stream.stop()
