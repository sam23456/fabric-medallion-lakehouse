# Fabric notebook: 01_bronze_ingestion
# Attach this notebook to the lh_retail_analytics Lakehouse.

from pyspark.sql import functions as F

LANDING_PATH = "Files/landing/orders/*.csv"
BRONZE_TABLE = "bronze_orders"

raw_orders = (
    spark.read.option("header", True)
    .option("inferSchema", False)
    .csv(LANDING_PATH)
)

bronze_orders = (
    raw_orders
    .withColumn("_source_file", F.input_file_name())
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_ingestion_date", F.current_date())
)

# Append-only capture preserves source history and enables replay.
(
    bronze_orders.write.format("delta")
    .mode("append")
    .partitionBy("_ingestion_date")
    .saveAsTable(BRONZE_TABLE)
)

display(spark.table(BRONZE_TABLE).orderBy(F.desc("_ingested_at")).limit(20))
