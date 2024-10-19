from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StringType,
    IntegerType,
    TimestampType
)

spark = SparkSession.builder \
    .appName("ReservationAnalytics") \
    .getOrCreate()

kafka_bootstrap_servers = "kafka:9092"
reservation_topic = "reservation-events"

reservation_schema = StructType() \
    .add("type", StringType()) \
    .add("roomId", IntegerType()) \
    .add("roomType", IntegerType()) \
    .add("floor", IntegerType()) \
    .add("startDate", TimestampType()) \
    .add("endDate", TimestampType()) \
    .add("timestamp", IntegerType())

reservation_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", reservation_topic) \
    .load()

reservation_df = reservation_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), reservation_schema).alias("data")) \
    .select("data.*")

# Print the data to the console (for debugging purposes)
query = reservation_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
query.awaitTermination()
