# Fabric notebook: 02_silver_transform
# Attach this notebook to lh_retail_analytics.

from pyspark.sql import functions as F
from pyspark.sql.window import Window

bronze = spark.table("bronze_orders")
typed = (bronze.select(
    F.trim("order_id").alias("order_id"),
    F.trim("customer_id").alias("customer_id"),
    F.to_timestamp("order_ts", "yyyy-MM-dd HH:mm:ss").alias("order_ts"),
    F.trim("product_category").alias("product_category"),
    F.col("quantity").cast("int").alias("quantity"),
    F.col("unit_price").cast("decimal(18,2)").alias("unit_price"),
    F.to_timestamp("source_updated_at", "yyyy-MM-dd HH:mm:ss").alias("source_updated_at"),
    "_source_file", "_ingested_at"
).withColumn("order_date", F.to_date("order_ts"))
 .withColumn("order_amount", F.round(F.col("quantity") * F.col("unit_price"), 2)))

latest = Window.partitionBy("order_id").orderBy(F.col("source_updated_at").desc_nulls_last(), F.col("_ingested_at").desc())
deduped = typed.withColumn("_rank", F.row_number().over(latest)).filter("_rank = 1").drop("_rank")

reason = (F.when(F.col("order_id").isNull() | (F.trim("order_id") == ""), "MISSING_ORDER_ID")
 .when(F.col("customer_id").isNull() | (F.trim("customer_id") == ""), "MISSING_CUSTOMER_ID")
 .when(F.col("order_ts").isNull(), "INVALID_ORDER_TIMESTAMP")
 .when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "INVALID_QUANTITY")
 .when(F.col("unit_price").isNull() | (F.col("unit_price") < 0), "INVALID_UNIT_PRICE"))

checked = deduped.withColumn("quality_reason", reason).withColumn("_silver_processed_at", F.current_timestamp())
valid = checked.filter(F.col("quality_reason").isNull()).drop("quality_reason")
invalid = checked.filter(F.col("quality_reason").isNotNull())

valid.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_orders")
invalid.write.format("delta").mode("append").saveAsTable("quarantine_orders")
spark.sql("OPTIMIZE silver_orders ZORDER BY (order_date, customer_id)")
display(spark.table("silver_orders"))
