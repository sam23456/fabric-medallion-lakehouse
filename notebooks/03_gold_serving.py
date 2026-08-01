# Fabric notebook: 03_gold_serving
# Attach this notebook to lh_retail_analytics.

from pyspark.sql import functions as F

orders = spark.table("silver_orders")

daily_sales = (
    orders.groupBy("order_date", "product_category")
    .agg(
        F.countDistinct("order_id").alias("order_count"),
        F.sum("quantity").alias("units_sold"),
        F.round(F.sum("order_amount"), 2).alias("revenue"),
    )
    .withColumn("_gold_refreshed_at", F.current_timestamp())
)

customer_sales = (
    orders.groupBy("customer_id")
    .agg(
        F.countDistinct("order_id").alias("lifetime_orders"),
        F.round(F.sum("order_amount"), 2).alias("lifetime_revenue"),
        F.max("order_date").alias("last_order_date"),
    )
    .withColumn("_gold_refreshed_at", F.current_timestamp())
)

daily_sales.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_daily_sales")
customer_sales.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_customer_sales")
display(spark.table("gold_daily_sales").orderBy("order_date", "product_category"))
