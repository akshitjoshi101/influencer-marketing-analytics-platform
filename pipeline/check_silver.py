from pyspark.sql import SparkSession

# ============================================================
# Create Spark session with Delta Lake support
# ============================================================
spark = (
    SparkSession.builder
    .appName("CheckSilverDelta")
    .config(
        "spark.jars.packages",
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
# Read the Silver Delta table
# ============================================================
silver = spark.read.format("delta").load(
    "D:/old/MAD1-PROJECT/data/silver/fact_ad_request"
)

# ============================================================
# Display Silver records
# ============================================================
silver.orderBy("request_id", ascending=False).show(20, truncate=False)

spark.stop()