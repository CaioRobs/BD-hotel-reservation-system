from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, from_unixtime, col, window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType
)

spark = SparkSession.builder \
    .appName("ReservationAnalytics") \
    .getOrCreate()

kafka_bootstrap_servers = "kafka:9092"
reservation_topic = "reservation-events"

reservation_schema = StructType([
    StructField("type", StringType(), True),
    StructField("roomId", IntegerType(), True),
    StructField("roomType", StringType(), True),
    StructField("floor", StringType(), True),
    StructField("startDate", StringType(), True),
    StructField("endDate", StringType(), True),
    StructField("timestamp", LongType(), True)
])

# READ FROM KAFKA
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", reservation_topic) \
    .load()

json_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), reservation_schema).alias("data")) \
    .select("data.*")


json_df = json_df.withColumn(
    "event_time",
    from_unixtime(col("timestamp") / 1000).cast("timestamp")
)

json_df_with_watermark = json_df.withWatermark("event_time", "10 minutes")

aggregated_df = json_df_with_watermark.groupBy(
    window(col("event_time"), "10 minutes"),
    col("roomType"),
    col("floor")
).count()

hadoop = "namenode:8020"
hdfs_file = "historical-reservations"

# WRITE TO HDFS
query = aggregated_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path",
            f"hdfs://{hadoop}/data/{hdfs_file}") \
    .option("checkpointLocation",
            f"hdfs://{hadoop}/checkpoint/{hdfs_file}") \
    .partitionBy("event_time") \
    .start()

query.awaitTermination()
