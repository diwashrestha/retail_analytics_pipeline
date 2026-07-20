-- Einkaufpark Silver validation queries
-- Run these in the target Silver catalog/schema after a successful update.
-- Example:
--   USE CATALOG workspace;
--   USE SCHEMA retail_dev_silver;

-- 1. Machine-readable quality contract. All CRITICAL checks should pass.
SELECT
  check_name,
  severity,
  expected_value,
  actual_value,
  status,
  description,
  checked_at
FROM silver_quality_checks
ORDER BY
  CASE severity WHEN 'CRITICAL' THEN 1 ELSE 2 END,
  check_name;

-- 2. Verify mutually exclusive routing of transaction rows.
SELECT *
FROM silver_transaction_reconciliation;

-- 3. Verify mutually exclusive routing of return rows.
SELECT *
FROM silver_return_reconciliation;

-- 4. Review why completed sales were excluded.
SELECT
  reason,
  count(*) AS affected_rows
FROM (
  SELECT explode(split(silver_review_reasons, '\\|')) AS reason
  FROM fact_sales_review
)
WHERE reason <> ''
GROUP BY reason
ORDER BY affected_rows DESC, reason;

-- 5. Review why returns were excluded.
SELECT
  reason,
  count(*) AS affected_rows
FROM (
  SELECT explode(split(silver_review_reasons, '\\|')) AS reason
  FROM fact_returns_review
)
WHERE reason <> ''
GROUP BY reason
ORDER BY affected_rows DESC, reason;

-- 6. Confirm exact retries were removed without deleting the canonical row.
WITH routed_canonical_rows AS (
  SELECT sales_line_sk AS bronze_record_fingerprint, 'fact_sales' AS destination FROM fact_sales
  UNION ALL
  SELECT sales_line_sk, 'fact_sales_review' FROM fact_sales_review
  UNION ALL
  SELECT void_line_sk, 'fact_voids' FROM fact_voids
  UNION ALL
  SELECT void_line_sk, 'fact_voids_review' FROM fact_voids_review
)
SELECT
  d.record_hash,
  count(*) AS removed_retry_rows,
  count(DISTINCT d.canonical_bronze_record_fingerprint) AS canonical_rows_referenced,
  count(DISTINCT r.bronze_record_fingerprint) AS canonical_rows_found,
  collect_set(r.destination) AS canonical_destinations
FROM duplicate_transactions d
LEFT JOIN routed_canonical_rows r
  ON d.canonical_bronze_record_fingerprint = r.bronze_record_fingerprint
GROUP BY d.record_hash
ORDER BY removed_retry_rows DESC
LIMIT 50;

-- 7. Dimension grains.
SELECT 'dim_store' AS dataset_name, count(*) AS rows, count(DISTINCT store_id) AS distinct_keys FROM dim_store
UNION ALL
SELECT 'dim_customer', count(*), count(DISTINCT customer_id) FROM dim_customer
UNION ALL
SELECT 'dim_product', count(*), count(DISTINCT product_id) FROM dim_product
UNION ALL
SELECT 'dim_terminal', count(*), count(DISTINCT terminal_id) FROM dim_terminal;

-- 8. SCD2 source issues. The generated dataset should have no overlaps.
SELECT
  review_reasons,
  count(*) AS affected_intervals
FROM dim_product_scd2_review
GROUP BY review_reasons
ORDER BY affected_intervals DESC;

-- 9. Trusted sales financial totals.
SELECT
  count(*) AS sales_lines,
  count(DISTINCT basket_id) AS baskets,
  count(DISTINCT transaction_id) AS transactions,
  round(sum(pre_discount_sales_eur), 2) AS pre_discount_sales_eur,
  round(sum(discount_amount_eur), 2) AS discount_amount_eur,
  round(sum(net_sales_eur), 2) AS net_sales_eur,
  round(sum(net_sales_ex_vat_eur), 2) AS net_sales_ex_vat_eur,
  round(sum(vat_amount_eur), 2) AS vat_amount_eur
FROM fact_sales;

-- 10. Financial arithmetic must reconcile per row.
SELECT count(*) AS non_reconciling_sales_lines
FROM fact_sales
WHERE abs(pre_discount_sales_eur - discount_amount_eur - net_sales_eur) > 0.03
   OR abs(net_sales_ex_vat_eur + vat_amount_eur - net_sales_eur) > 0.03;

-- 11. Customer-type distribution should include walk-ins and registered users.
SELECT
  customer_type,
  count(DISTINCT basket_id) AS baskets,
  round(sum(net_sales_eur), 2) AS net_sales_eur
FROM fact_sales
GROUP BY customer_type
ORDER BY baskets DESC;

-- 12. Return limits and refunds.
SELECT
  count(*) AS trusted_returns,
  sum(return_quantity) AS returned_units,
  round(sum(refund_amount_eur), 2) AS refund_amount_eur,
  count_if(cumulative_return_quantity > sold_quantity) AS quantity_violations,
  count_if(cumulative_refund_amount_eur > original_net_sales_eur + 0.02) AS refund_violations
FROM fact_returns;

-- 13. Late-arriving records are retained, not rejected.
SELECT
  'sales' AS dataset_name,
  count_if(is_late_arrival) AS late_rows,
  max(arrival_delay_days) AS maximum_delay_days
FROM fact_sales
UNION ALL
SELECT
  'returns',
  count_if(is_late_arrival),
  max(arrival_delay_days)
FROM fact_returns;

-- 14. Review-table samples for debugging.
SELECT * FROM fact_sales_review LIMIT 20;
SELECT * FROM fact_returns_review LIMIT 20;
