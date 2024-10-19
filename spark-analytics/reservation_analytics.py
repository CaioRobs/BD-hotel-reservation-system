from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType, 
    StringType, 
    IntegerType, 
    TimestampType
)

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("ReservationAnalytics") \
    .getOrCreate()

# Define Kafka parameters
kafka_bootstrap_servers = "kafka:9092"
reservation_topic = "reservation-events"

# Define schema for reservation events
reservation_schema = StructType() \
    .add("eventType", StringType()) \
    .add("reservation", StructType()
         .add("roomId", IntegerType())
         .add("userId", IntegerType())
         .add("startDate", TimestampType())
         .add("endDate", TimestampType()))

# Read data from Kafka topic
reservation_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", reservation_topic) \
    .load()

# Convert binary "value" column to string and parse JSON
reservation_df = reservation_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), reservation_schema).alias("data")) \
    .select("data.*")

# Print the data to the console (for debugging purposes)
query = reservation_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
