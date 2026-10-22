"""
====================== BATCH FOR BRONZE ====================

Reads raw Sparkplug B messages from Kafka, decodes Protobuf payloads,
extracts individual metrics into a structured DataFrame, removes duplicates,
and displays the results for validation.

============== Author : AJOUHY ABDELAH - DatAbde ============

"""

from pyspark.sql import SparkSession
from pyspark.sql.protobuf.functions import from_protobuf
from pyspark.sql.functions import col, hex, length, decode, explode, flatten, timestamp_millis, coalesce, split, first, round, window, avg
from sparkplug_types import cast_datatype
import time
import inspect

"""
                                SPARK SESSION
"""
spark = (
        SparkSession.builder
        .remote("sc://192.168.112.139:15002")
        .appName("prod.oilgas.streaming")
        .getOrCreate()
)

"""
                                READ FROM TOPIC
"""
topic_raw = spark \
  .read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "192.168.112.138:9092,192.168.112.139:9092,192.168.112.140:9092") \
  .option("subscribe", "prod.oilgas.bronze") \
  .load()

"""
                                df_raw = RAW DATA : decode key value protobuff + explode metrics
"""
df_raw= topic_raw.select(
    col("timestamp").alias("kafka_ts"),
    col("topic").alias("kafka_topic"),
    "key",
    "value"
    ).select(
    "kafka_ts",
    "kafka_topic",
    decode(col("key"),"UTF-8").alias("topic_mqtt"),
    from_protobuf(
        data=col("value"), 
        messageName="org.eclipse.tahu.protobuf.Payload",
        descFilePath="sparkplugb_poseidon.desc",
        options={"recursive.fields.max.depth": "3"}
        ).alias("data"),
    ).select(
      "kafka_ts",
      "kafka_topic",
      "topic_mqtt",
      explode("data.metrics").alias("raw_metrics")
    )


"""
                                df_metrics = METRICS DATA : DEDUPLICATE
"""
df_metrics= df_raw.select(
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
).dropDuplicates([
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

"""
                                df_pivot = METRICS DATA : PIVOT + GROUPBY + AGG
"""
df_pivot = df_metrics.select(
    "kafka_topic",
    "kafka_ts",
    "topic_mqtt",
    "metric_ts",
    "metric_name",
    coalesce(
        col("int_value").cast("string"),
        col("long_value").cast("string"),
        col("float_value").cast("string"),
        col("double_value").cast("string"),
        col("boolean_value").cast("string"),
        col("string_value").cast("string")
    ).alias("metric_value"),
    split(col("topic_mqtt"), "/")[0].alias("namespace"),
    split(col("topic_mqtt"), "/")[1].alias("group_id"),
    split(col("topic_mqtt"), "/")[2].alias("message_type"),
    split(col("topic_mqtt"), "/")[3].alias("edge_node_id")
).groupBy(
    window('metric_ts','3000 milliseconds').alias('metric_ts_per_3s'),
    col('kafka_topic'),
    col("topic_mqtt"),
    col('namespace'),
    col('group_id'),
    col("edge_node_id")
).pivot(
    "metric_name"
    ).agg(
        first('metric_value') 
    ).select(
        col('metric_ts_per_3s.start').alias('metric_ts'),
        '*'
    ).drop(
        col('metric_ts_per_3s')
        )

df_pivot.show(n=50,truncate=False,vertical=True)