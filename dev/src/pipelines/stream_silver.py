"""
====================== STREAM FOR BRONZE ====================

Reads raw Sparkplug B messages from Kafka, decodes Protobuf payloads,
extracts individual metrics into a structured DataFrame, removes duplicates,
and displays the results for validation.

============== Author : AJOUHY ABDELAH - DatAbde ============

"""
from pyspark.sql import SparkSession
from pyspark.sql.protobuf.functions import from_protobuf
from pyspark.sql.functions import col, hex, length, decode, explode, flatten, timestamp_millis
import time
import inspect

# 1.intitialization : load and config the Spark session
spark = (
        SparkSession.builder
        .remote("sc://192.168.112.139:15002")
        .appName("prod.oilgas.streaming")
        .getOrCreate()
)

# Creating the Kafka datafram for Batch Queries or Straming Queries
# 2.Read the data : subscribe to the Kafa bronze topic
topic_raw = spark \
  .read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "192.168.112.138:9092,192.168.112.139:9092,192.168.112.140:9092") \
  .option("subscribe", "prod.oilgas.raw") \
  .load()

# DF brut
df_raw= topic_raw.select(
    col("timestamp").alias("kafka_ts"),
    col("topic").alias("kafka_topic"),
    "key",
    "value"       # payload de mqtt   => à décoder.
    )

print("********************************** AVANT LE MENAGE CE QUI EST ENVOYE PAR LES SITES PETROLIERS**********************************")
df_raw.show(n=3, truncate=False, vertical=False)
#decode key and value
df_decode = df_raw.select(
    "kafka_ts",
    "kafka_topic",
    decode(col("key"),"UTF-8").alias("topic_mqtt"), # ici on a toute la data, metrics + timestamps + topics mqtt.
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

#df_decode.show(n=1, truncate=False, vertical=False)

df_metrics= df_decode.select(
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


df_metrics.dropDuplicates([
  'topic_mqtt',
  'metric_ts',
  'metric_name',
  'int_value',
  'long_value',
  'float_value',
  'double_value',
  'boolean_value',
  'string_value'
  ]).show(n=3, truncate=False, vertical=False)


