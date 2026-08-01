-- Run in the Fabric Lakehouse SQL analytics endpoint.

-- Records must be visible at every layer.
SELECT 'bronze' AS layer, COUNT(*) AS records FROM dbo.bronze_orders
UNION ALL SELECT 'silver', COUNT(*) FROM dbo.silver_orders
UNION ALL SELECT 'quarantine', COUNT(*) FROM dbo.quarantine_orders;

-- Quality failures are retained and explainable.
SELECT quality_reason, COUNT(*) AS rejected_records
FROM dbo.quarantine_orders
GROUP BY quality_reason
ORDER BY rejected_records DESC;

-- Gold revenue reconciles to Silver revenue.
SELECT
  (SELECT SUM(order_amount) FROM dbo.silver_orders) AS silver_revenue,
  (SELECT SUM(revenue) FROM dbo.gold_daily_sales) AS gold_revenue;

-- Silver should have one row per business order.
SELECT order_id, COUNT(*) AS duplicate_count
FROM dbo.silver_orders
GROUP BY order_id
HAVING COUNT(*) > 1;
