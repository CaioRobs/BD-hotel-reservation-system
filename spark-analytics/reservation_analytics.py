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

reservation_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", reservation_topic) \
    .load()

reservation_df = reservation_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), reservation_schema).alias("data")) \
    .select("data.*")


reservation_df = reservation_df.withColumn(
    "event_time", 
    from_unixtime(col("timestamp") / 1000).cast("timestamp")
)

aggregated_df = reservation_df.groupBy(
    window(col("event_time"), "10 minutes"),  
    col("roomType"),
    col("floor")
).count()  

aggregated_df = aggregated_df.withColumnRenamed("count", "reservation_count")

query = aggregated_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
