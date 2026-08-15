-- ============================================================================
-- Einkaufpark Retail Platform — Gold Quality Contract
-- Critical failures stop the Lakeflow pipeline update.
-- ============================================================================

USE CATALOG workspace;
USE SCHEMA retail_dev_gold;

CREATE OR REFRESH MATERIALIZED VIEW gold_quality_checks
COMMENT 'Machine-readable Gold grain, reconciliation, and business-value checks.'
AS
-- Grain checks ---------------------------------------------------------------
SELECT
'daily_sales_unique_grain' AS check_name,
'CRITICAL' AS severity,
cast(0 AS DECIMAL(20,4)) AS expected_value,
cast(count(*) - count(DISTINCT concat_ws('||',
cast(order_date AS STRING),
store_id,
category,
subcategory)) AS DECIMAL(20,
4)) AS actual_value,
CASE WHEN count(*) = count(DISTINCT concat_ws('||',
cast(order_date AS STRING),
store_id,
category,
subcategory)) THEN 'PASSED' ELSE 'FAILED' END AS status,
'Daily sales must contain one row per date-store-category-subcategory grain.' AS description,
current_timestamp() AS checked_at
FROM daily_sales

UNION ALL
SELECT
'store_performance_unique_grain', 'CRITICAL', cast(0 AS DECIMAL(20,4)),
cast(count(*) - count(DISTINCT store_id) AS DECIMAL(20,4)),
CASE WHEN count(*) = count(DISTINCT store_id) THEN 'PASSED' ELSE 'FAILED' END,
'Store performance must contain one row per trusted store.', current_timestamp()
FROM store_performance

UNION ALL
SELECT
'product_performance_unique_grain', 'CRITICAL', cast(0 AS DECIMAL(20,4)),
cast(count(*) - count(DISTINCT product_id) AS DECIMAL(20,4)),
CASE WHEN count(*) = count(DISTINCT product_id) THEN 'PASSED' ELSE 'FAILED' END,
'Product performance must contain one row per trusted product.',
current_timestamp()
FROM product_performance

UNION ALL
SELECT
'customer_ltv_unique_grain', 'CRITICAL', cast(0 AS DECIMAL(20,4)),
cast(count(*) - count(DISTINCT customer_id) AS DECIMAL(20,4)),
CASE WHEN count(*) = count(DISTINCT customer_id) THEN 'PASSED' ELSE 'FAILED' END,
'Customer LTV must contain one row per trusted customer.', current_timestamp()
FROM customer_ltv

UNION ALL
SELECT
'basket_analysis_unique_grain', 'CRITICAL', cast(0 AS DECIMAL(20,4)),
cast(count(*) - count(DISTINCT basket_id) AS DECIMAL(20,4)),
CASE WHEN count(*) = count(DISTINCT basket_id) THEN 'PASSED' ELSE 'FAILED' END,
'Basket analysis must contain one row per trusted basket.', current_timestamp()
FROM basket_analysis

UNION ALL
SELECT
'return_analysis_unique_grain', 'CRITICAL', cast(0 AS DECIMAL(20,4)),
cast(count(*) - count(DISTINCT concat_ws('||',
product_id,
reason_code)) AS DECIMAL(20,
4)),
CASE WHEN count(*) = count(DISTINCT concat_ws('||',
product_id,
reason_code)) THEN 'PASSED' ELSE 'FAILED' END,
'Return analysis must contain one row per product-reason combination.',
current_timestamp()
FROM return_analysis

UNION ALL
SELECT
'hourly_traffic_unique_grain', 'CRITICAL', cast(0 AS DECIMAL(20,4)),
cast(count(*) - count(DISTINCT concat_ws('||',
cast(order_hour AS STRING),
cast(weekday_number AS STRING),
store_size_class)) AS DECIMAL(20,
4)),
CASE WHEN count(*) = count(DISTINCT concat_ws('||',
cast(order_hour AS STRING),
cast(weekday_number AS STRING),
store_size_class)) THEN 'PASSED' ELSE 'FAILED' END,
'Hourly traffic must contain one row per hour-weekday-store-size combination.',
current_timestamp()
FROM hourly_traffic

-- Revenue reconciliations ----------------------------------------------------
UNION ALL
SELECT
'daily_sales_revenue_reconciliation', 'CRITICAL',
cast(round(s.silver_value, 2) AS DECIMAL(20,4)),
cast(round(g.gold_value, 2) AS DECIMAL(20,4)),
CASE WHEN abs(s.silver_value - g.gold_value) <= cast(:revenue_tolerance_eur AS DECIMAL(10,
2)) THEN 'PASSED' ELSE 'FAILED' END,
'Daily sales revenue must reconcile to trusted Silver sales.',
current_timestamp()
FROM (
SELECT coalesce(sum(net_sales_eur), 0) AS silver_value
FROM workspace.retail_dev_silver.fact_sales
) s
CROSS JOIN (
SELECT coalesce(sum(net_sales_eur), 0) AS gold_value FROM daily_sales
) g

UNION ALL
SELECT
'basket_revenue_reconciliation', 'CRITICAL',
cast(round(s.silver_value,
2) AS DECIMAL(20,
4)),
cast(round(g.gold_value,
2) AS DECIMAL(20,
4)),
CASE WHEN abs(s.silver_value - g.gold_value) <= cast(:revenue_tolerance_eur AS DECIMAL(10,
2)) THEN 'PASSED' ELSE 'FAILED' END,
'Basket-level revenue must reconcile to trusted Silver sales.',
current_timestamp()
FROM (SELECT coalesce(sum(net_sales_eur),
0) AS silver_value FROM workspace.retail_dev_silver.fact_sales) s
CROSS JOIN (SELECT coalesce(sum(net_sales_eur),
0) AS gold_value FROM basket_analysis) g

UNION ALL
SELECT
'store_revenue_reconciliation', 'CRITICAL',
cast(round(s.silver_value,
2) AS DECIMAL(20,
4)),
cast(round(g.gold_value,
2) AS DECIMAL(20,
4)),
CASE WHEN abs(s.silver_value - g.gold_value) <= cast(:revenue_tolerance_eur AS DECIMAL(10,
2)) THEN 'PASSED' ELSE 'FAILED' END,
'Store performance revenue must reconcile to trusted Silver sales.',
current_timestamp()
FROM (SELECT coalesce(sum(net_sales_eur),
0) AS silver_value FROM workspace.retail_dev_silver.fact_sales) s
CROSS JOIN (SELECT coalesce(sum(net_sales_eur),
0) AS gold_value FROM store_performance) g

UNION ALL
SELECT
'product_revenue_reconciliation', 'CRITICAL',
cast(round(s.silver_value,
2) AS DECIMAL(20,
4)),
cast(round(g.gold_value,
2) AS DECIMAL(20,
4)),
CASE WHEN abs(s.silver_value - g.gold_value) <= cast(:revenue_tolerance_eur AS DECIMAL(10,
2)) THEN 'PASSED' ELSE 'FAILED' END,
'Product performance revenue must reconcile to trusted Silver sales.',
current_timestamp()
FROM (SELECT coalesce(sum(net_sales_eur),
0) AS silver_value FROM workspace.retail_dev_silver.fact_sales) s
CROSS JOIN (SELECT coalesce(sum(net_sales_eur),
0) AS gold_value FROM product_performance) g

UNION ALL
SELECT
'hourly_revenue_reconciliation', 'CRITICAL',
cast(round(s.silver_value,
2) AS DECIMAL(20,
4)),
cast(round(g.gold_value,
2) AS DECIMAL(20,
4)),
CASE WHEN abs(s.silver_value - g.gold_value) <= cast(:revenue_tolerance_eur AS DECIMAL(10,
2)) THEN 'PASSED' ELSE 'FAILED' END,
'Hourly traffic revenue must reconcile to trusted Silver sales.',
current_timestamp()
FROM (SELECT coalesce(sum(net_sales_eur),
0) AS silver_value FROM workspace.retail_dev_silver.fact_sales) s
CROSS JOIN (SELECT coalesce(sum(net_sales_eur),
0) AS gold_value FROM hourly_traffic) g

UNION ALL
SELECT
'identified_customer_revenue_reconciliation', 'CRITICAL',
cast(round(s.silver_value,
2) AS DECIMAL(20,
4)),
cast(round(g.gold_value,
2) AS DECIMAL(20,
4)),
CASE WHEN abs(s.silver_value - g.gold_value) <= cast(:revenue_tolerance_eur AS DECIMAL(10,
2)) THEN 'PASSED' ELSE 'FAILED' END,
'Customer LTV sales must reconcile to Silver sales for identified customers only.',
current_timestamp()
FROM (
SELECT coalesce(sum(net_sales_eur), 0) AS silver_value
FROM workspace.retail_dev_silver.fact_sales
WHERE customer_id IS NOT NULL
) s
CROSS JOIN (SELECT coalesce(sum(lifetime_sales_eur),
0) AS gold_value FROM customer_ltv) g

-- Return reconciliations -----------------------------------------------------
UNION ALL
SELECT
'product_refund_reconciliation', 'CRITICAL',
cast(round(s.silver_value,
2) AS DECIMAL(20,
4)),
cast(round(g.gold_value,
2) AS DECIMAL(20,
4)),
CASE WHEN abs(s.silver_value - g.gold_value) <= cast(:revenue_tolerance_eur AS DECIMAL(10,
2)) THEN 'PASSED' ELSE 'FAILED' END,
'Product performance refunds must reconcile to trusted Silver returns.',
current_timestamp()
FROM (SELECT coalesce(sum(refund_amount_eur),
0) AS silver_value FROM workspace.retail_dev_silver.fact_returns) s
CROSS JOIN (SELECT coalesce(sum(refund_amount_eur),
0) AS gold_value FROM product_performance) g

UNION ALL
SELECT
'return_reason_refund_reconciliation', 'CRITICAL',
cast(round(s.silver_value,
2) AS DECIMAL(20,
4)),
cast(round(g.gold_value,
2) AS DECIMAL(20,
4)),
CASE WHEN abs(s.silver_value - g.gold_value) <= cast(:revenue_tolerance_eur AS DECIMAL(10,
2)) THEN 'PASSED' ELSE 'FAILED' END,
'Return reason refunds must reconcile to trusted Silver returns without repeated sales denominators.',
current_timestamp()
FROM (SELECT coalesce(sum(refund_amount_eur),
0) AS silver_value FROM workspace.retail_dev_silver.fact_returns) s
CROSS JOIN (SELECT coalesce(sum(refund_amount_eur),
0) AS gold_value FROM return_analysis) g

UNION ALL
SELECT
'return_reason_unit_reconciliation', 'CRITICAL',
cast(s.silver_value AS DECIMAL(20,4)), cast(g.gold_value AS DECIMAL(20,4)),
CASE WHEN s.silver_value = g.gold_value THEN 'PASSED' ELSE 'FAILED' END,
'Return reason units must reconcile to trusted Silver returns.',
current_timestamp()
FROM (SELECT coalesce(sum(return_quantity),
0) AS silver_value FROM workspace.retail_dev_silver.fact_returns) s
CROSS JOIN (SELECT coalesce(sum(returned_units),
0) AS gold_value FROM return_analysis) g

-- Business-value and ranking checks -----------------------------------------
UNION ALL
SELECT
'gold_non_negative_values',
'CRITICAL',
cast(0 AS DECIMAL(20,
4)),
cast(count(*) AS DECIMAL(20,
4)),
CASE WHEN count(*) = 0 THEN 'PASSED' ELSE 'FAILED' END,
'Gold financial and quantity metrics must not become negative.',
current_timestamp()
FROM (
SELECT basket_id AS business_key FROM basket_analysis
WHERE net_sales_eur < 0 OR total_units <= 0 OR discount_amount_eur < 0
UNION ALL
SELECT product_id FROM product_performance
WHERE net_sales_eur < 0 OR units_sold < 0 OR refund_amount_eur < 0
UNION ALL
SELECT store_id FROM store_performance
WHERE net_sales_eur < 0 OR basket_count < 0 OR refund_amount_eur < 0
) invalid_values

UNION ALL
SELECT
'customer_top_1000_rank', 'CRITICAL',
cast(least(1000, count(*)) AS DECIMAL(20,4)),
cast(count(ltv_rank_top_1000) AS DECIMAL(20,4)),
CASE WHEN count(ltv_rank_top_1000) = least(1000,
count(*)) THEN 'PASSED' ELSE 'FAILED' END,
'Top-1000 LTV rank must contain exactly min(1000, trusted customer count) populated ranks.',
current_timestamp()
FROM customer_ltv

UNION ALL
SELECT
'customer_rank_unique', 'CRITICAL', cast(0 AS DECIMAL(20,4)),
cast(count(overall_ltv_rank) - count(DISTINCT overall_ltv_rank) AS DECIMAL(20,
4)),
CASE WHEN count(overall_ltv_rank) = count(DISTINCT overall_ltv_rank) THEN 'PASSED' ELSE 'FAILED' END,
'Overall customer LTV rank must be unique.', current_timestamp()
FROM customer_ltv

UNION ALL
SELECT
'return_reason_shares',
'WARNING',
cast(0 AS DECIMAL(20,
4)),
cast(count(*) AS DECIMAL(20,
4)),
CASE WHEN count(*) = 0 THEN 'PASSED' ELSE 'FAILED' END,
'For every product with returns, return-event shares should sum to approximately 100 percent.',
current_timestamp()
FROM (
SELECT product_id
FROM return_analysis
GROUP BY product_id
HAVING abs(sum(product_return_event_share_pct) - 100) > 0.10
) invalid_return_shares

UNION ALL
SELECT
'gold_has_positive_sales', 'WARNING', cast(1 AS DECIMAL(20,4)),
cast(CASE WHEN coalesce(sum(net_sales_eur),
0) > 0 THEN 1 ELSE 0 END AS DECIMAL(20,
4)),
CASE WHEN coalesce(sum(net_sales_eur), 0) > 0 THEN 'PASSED' ELSE 'FAILED' END,
'A demo or portfolio run should produce positive Gold revenue.',
current_timestamp()
FROM daily_sales;

CREATE OR REFRESH MATERIALIZED VIEW gold_quality_gate (
CONSTRAINT all_critical_gold_checks_pass
EXPECT (failed_critical_checks = 0)
ON VIOLATION FAIL UPDATE
)
COMMENT 'Pipeline quality gate. Any failed CRITICAL Gold check fails the medallion pipeline update.'
AS
SELECT
count_if(severity = 'CRITICAL' AND status = 'FAILED') AS failed_critical_checks,
count_if(severity = 'WARNING' AND status = 'FAILED') AS failed_warning_checks,
count(*) AS total_checks,
current_timestamp() AS evaluated_at
FROM gold_quality_checks;
