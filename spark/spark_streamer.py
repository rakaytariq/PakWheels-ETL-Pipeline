# spark_streamer.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

# Define schema for incoming JSON data
schema = StructType() \
    .add("title", StringType()) \
    .add("price", StringType()) \
    .add("location", StringType()) \
    .add("mileage", StringType()) \
    .add("engine", StringType()) \
    .add("year", StringType()) \
    .add("url", StringType()) \
    .add("image", StringType())

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PakwheelsKafkaConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read from Kafka topic
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "pakwheels_listings") \
    .option("startingOffsets", "latest") \
    .load()

# Parse the value field (JSON)
json_df = raw_df.selectExpr("CAST(value AS STRING) as json")
parsed_df = json_df.select(from_json(col("json"), schema).alias("data")).select("data.*")

# Write to Parquet sink
query = parsed_df.writeStream \
    .format("parquet") \
    .option("path", "parquet_output/") \
    .option("checkpointLocation", "checkpoint/") \
    .outputMode("append") \
    .start()

query.awaitTermination()
