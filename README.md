# Microsoft Fabric Lakehouse: Medallion Data Pipeline

A production-style Microsoft Fabric portfolio project that ingests retail orders, standardizes and validates them in a Lakehouse, and publishes analytics-ready Gold tables.

## Architecture

Bronze (raw Delta capture) -> Silver (typed, deduplicated, quality-checked) -> Gold (BI-ready sales aggregates) -> Power BI

## Demonstrated capabilities

- Microsoft Fabric Lakehouse and OneLake
- PySpark, Spark SQL and Delta Lake
- Bronze/Silver/Gold Medallion Architecture
- Window-based deduplication and quality quarantine
- Reconciliation checks and Gold serving tables
- Power BI-ready sales and customer aggregates

## Run in Fabric

1. Create a Lakehouse named `lh_retail_analytics`.
2. Upload the sample CSV to `Files/landing/orders/`.
3. Attach the three notebooks to the Lakehouse and run them in numerical order.
4. Run the SQL validation queries in the Lakehouse SQL analytics endpoint.
5. Build a Power BI semantic model using `gold_daily_sales` and `gold_customer_sales`.

## Data quality

The Silver layer rejects missing business keys, invalid timestamps, non-positive quantities, and negative prices. Invalid records are retained in a quarantine Delta table with a reason; they are never silently discarded.

## Stack

Microsoft Fabric | OneLake | Lakehouse | PySpark | Spark SQL | Delta Lake | Power BI | Data Quality

Sample data is fictional and exists only to demonstrate the pipeline.
