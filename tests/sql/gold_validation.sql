-- ============================================================================
-- Einkaufpark Gold post-run validation queries
-- Replace retail_dev_gold / retail_dev_silver with release schemas when needed.
-- Every reconciliation difference should be zero (or within EUR 0.02).
-- ============================================================================

USE CATALOG workspace;
USE SCHEMA retail_dev_gold;

-- 1. Gold quality contract
SELECT *
FROM gold_quality_checks
ORDER BY
  CASE severity WHEN 'CRITICAL' THEN 1 ELSE 2 END,
  check_name;

SELECT * FROM gold_quality_gate;

-- 2. Intended grains
SELECT COUNT(*) AS rows, COUNT(DISTINCT basket_id) AS business_keys
FROM basket_analysis;

SELECT COUNT(*) AS rows, COUNT(DISTINCT store_id) AS business_keys
FROM store_performance;

SELECT COUNT(*) AS rows, COUNT(DISTINCT product_id) AS business_keys
FROM product_performance;

SELECT COUNT(*) AS rows, COUNT(DISTINCT customer_id) AS business_keys
FROM customer_ltv;

SELECT
  COUNT(*) AS rows,
  COUNT(DISTINCT concat_ws('||', product_id, reason_code)) AS business_keys
FROM return_analysis;

-- 3. Revenue reconciliation
SELECT
  (SELECT ROUND(SUM(net_sales_eur), 2) FROM workspace.retail_dev_silver.fact_sales) AS silver_revenue,
  (SELECT ROUND(SUM(net_sales_eur), 2) FROM daily_sales) AS daily_sales_revenue,
  (SELECT ROUND(SUM(net_sales_eur), 2) FROM basket_analysis) AS basket_revenue,
  (SELECT ROUND(SUM(net_sales_eur), 2) FROM store_performance) AS store_revenue,
  (SELECT ROUND(SUM(net_sales_eur), 2) FROM product_performance) AS product_revenue,
  (SELECT ROUND(SUM(net_sales_eur), 2) FROM hourly_traffic) AS hourly_revenue;

-- 4. Refund reconciliation
SELECT
  (SELECT ROUND(SUM(refund_amount_eur), 2) FROM workspace.retail_dev_silver.fact_returns) AS silver_refunds,
  (SELECT ROUND(SUM(refund_amount_eur), 2) FROM product_performance) AS product_refunds,
  (SELECT ROUND(SUM(refund_amount_eur), 2) FROM return_analysis) AS reason_refunds;

-- 5. Top-1000 ranking
SELECT
  COUNT(*) AS customer_count,
  COUNT(ltv_rank_top_1000) AS populated_top_1000_ranks,
  COUNT(DISTINCT ltv_rank_top_1000) AS distinct_top_1000_ranks,
  MIN(ltv_rank_top_1000) AS minimum_rank,
  MAX(ltv_rank_top_1000) AS maximum_rank
FROM customer_ltv;

-- 6. Product Pareto progression
SELECT
  product_id,
  product_name,
  net_sales_eur,
  revenue_rank,
  revenue_share_pct,
  cumulative_revenue_share_pct,
  pareto_class
FROM product_performance
ORDER BY revenue_rank
LIMIT 25;

-- 7. Return reasons do not repeat sales denominators
SELECT
  product_id,
  SUM(product_return_event_share_pct) AS event_share_pct,
  SUM(product_refund_share_pct) AS refund_share_pct
FROM return_analysis
GROUP BY product_id
HAVING ABS(SUM(product_return_event_share_pct) - 100) > 0.10
    OR ABS(SUM(product_refund_share_pct) - 100) > 0.10;
