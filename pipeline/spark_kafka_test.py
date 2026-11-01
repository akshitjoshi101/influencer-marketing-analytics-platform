import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType
)
from pyspark.sql.functions import from_json, col, to_timestamp

# ============================================================
# Create Spark session with Kafka and Delta Lake support
# ============================================================
spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("KafkaJSONParser")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,"
        "io.delta:delta-spark_2.13:4.4.0"
    )
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# Define the Kafka JSON schema
# ============================================================
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("request_id", IntegerType(), True),
    StructField("campaign_id", IntegerType(), True),
    StructField("influencer_id", IntegerType(), True),
    StructField("sponsor_id", IntegerType(), True),
    StructField("payment_amount", DoubleType(), True),
    StructField("status", StringType(), True)
])


# ============================================================
# Read messages from Kafka
# ============================================================
kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "ad_requests")
    .option("startingOffsets", "earliest")
    .load()
)


# ============================================================
# Convert Kafka value from binary to string
# ============================================================
messages = kafka_stream.select(
    col("value").cast("string").alias("json_value")
)


# ============================================================
# Parse JSON into separate columns
# ============================================================
parsed_messages = messages.select(
    from_json(col("json_value"), schema).alias("data")
).select(
    "data.*"
)

# ============================================================
# Keep only valid ad request events
# ============================================================
clean_messages = parsed_messages.filter(
    (col("event_id").isNotNull()) &
    (col("event_time").isNotNull()) &
    (col("request_id").isNotNull()) &
    (col("campaign_id").isNotNull()) &
    (col("influencer_id").isNotNull()) &
    (col("sponsor_id").isNotNull()) &
    (col("payment_amount").isNotNull()) &
    (col("payment_amount") >= 0) &
    (col("status").isNotNull())
)


# ============================================================
# Remove duplicate ad request events
# ============================================================
deduplicated_messages = (clean_messages
    .withColumn("event_timestamp", to_timestamp("event_time"))
    .withWatermark("event_timestamp", "1 day")
    .dropDuplicates(["event_id"]))

# ============================================================
# Write cleaned Kafka events to Silver Delta
# ============================================================
query = (
    deduplicated_messages.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        os.getenv("SPARK_CHECKPOINT_PATH", "spark_checkpoint_silver_events")   
    )
    .start(
        os.getenv("SPARK_SILVER_PATH", "data/silver/fact_ad_request_events")     
    )
)

query.awaitTermination()
