-- ============================================================================
-- Einkaufpark Retail Platform — Silver Quality Contract
-- Produces machine-readable checks for orchestration and release validation.
-- Critical FAILED checks should cause the parent Lakeflow Job to fail.
-- ============================================================================

-- Publish all unqualified datasets in the parameterized Silver target.
-- The USE statements are scoped to this source file.
USE CATALOG workspace;
USE SCHEMA retail_dev_silver;

CREATE OR REFRESH MATERIALIZED VIEW silver_quality_checks
COMMENT 'Machine-readable Silver quality contract. A downstream task should fail the job when a CRITICAL check has status FAILED.'
AS
SELECT
'transaction_routing_reconciliation' AS check_name,
'CRITICAL' AS severity,
cast(0 AS DECIMAL(20,4)) AS expected_value,
cast(reconciliation_difference AS DECIMAL(20,4)) AS actual_value,
CASE WHEN reconciliation_difference = 0 THEN 'PASSED' ELSE 'FAILED' END AS status,
'Bronze transactions must be routed exactly once to hash-conflict review, duplicate removal, trusted sales, sales review, trusted voids, or void review.' AS description,
current_timestamp() AS checked_at
FROM silver_transaction_reconciliation

UNION ALL
SELECT
'return_routing_reconciliation',
'CRITICAL',
cast(0 AS DECIMAL(20,4)),
cast(reconciliation_difference AS DECIMAL(20,4)),
CASE WHEN reconciliation_difference = 0 THEN 'PASSED' ELSE 'FAILED' END,
'Bronze returns must be routed exactly once to ID-conflict review, duplicate removal, trusted returns, or return review.',
current_timestamp()
FROM silver_return_reconciliation

UNION ALL
SELECT
'dim_store_unique_key',
'CRITICAL',
cast(0 AS DECIMAL(20,4)),
cast(count(*) - count(DISTINCT store_id) AS DECIMAL(20,4)),
CASE WHEN count(*) = count(DISTINCT store_id) THEN 'PASSED' ELSE 'FAILED' END,
'Trusted store dimension must contain one row per store_id.',
current_timestamp()
FROM dim_store

UNION ALL
SELECT
'dim_customer_unique_key',
'CRITICAL',
cast(0 AS DECIMAL(20,4)),
cast(count(*) - count(DISTINCT customer_id) AS DECIMAL(20,4)),
CASE WHEN count(*) = count(DISTINCT customer_id) THEN 'PASSED' ELSE 'FAILED' END,
'Trusted customer dimension must contain one row per customer_id.',
current_timestamp()
FROM dim_customer

UNION ALL
SELECT
'dim_product_unique_key',
'CRITICAL',
cast(0 AS DECIMAL(20,4)),
cast(count(*) - count(DISTINCT product_id) AS DECIMAL(20,4)),
CASE WHEN count(*) = count(DISTINCT product_id) THEN 'PASSED' ELSE 'FAILED' END,
'Trusted product dimension must contain one row per product_id.',
current_timestamp()
FROM dim_product

UNION ALL
SELECT
'dim_terminal_unique_key',
'CRITICAL',
cast(0 AS DECIMAL(20,4)),
cast(count(*) - count(DISTINCT terminal_id) AS DECIMAL(20,4)),
CASE WHEN count(*) = count(DISTINCT terminal_id) THEN 'PASSED' ELSE 'FAILED' END,
'Trusted terminal dimension must contain one row per terminal_id.',
current_timestamp()
FROM dim_terminal

UNION ALL
SELECT
'trusted_scd2_no_overlaps',
'CRITICAL',
cast(0 AS DECIMAL(20,4)),
cast(count(*) AS DECIMAL(20,4)),
CASE WHEN count(*) = 0 THEN 'PASSED' ELSE 'FAILED' END,
'Trusted SCD2 intervals must not overlap for the same product.',
current_timestamp()
FROM dim_product_scd2 a
JOIN dim_product_scd2 b
ON a.product_id = b.product_id
AND a.price_version_sk < b.price_version_sk
AND a.effective_from <= b.effective_to
AND b.effective_from <= a.effective_to

UNION ALL
SELECT
'source_scd2_overlap_rows',
'WARNING',
cast(0 AS DECIMAL(20,4)),
cast(count(*) AS DECIMAL(20,4)),
CASE WHEN count(*) = 0 THEN 'PASSED' ELSE 'FAILED' END,
'Source SCD2 overlaps are excluded from the trusted dimension and retained in review.',
current_timestamp()
FROM dim_product_scd2_review
WHERE review_reasons LIKE '%OVERLAPPING_SCD2_INTERVAL%'

UNION ALL
SELECT
'fact_sales_unique_line_key',
'CRITICAL',
cast(0 AS DECIMAL(20,4)),
cast(count(*) - count(DISTINCT sales_line_sk) AS DECIMAL(20,4)),
CASE WHEN count(*) = count(DISTINCT sales_line_sk) THEN 'PASSED' ELSE 'FAILED' END,
'Trusted sales must contain one row per canonical source sales line.',
current_timestamp()
FROM fact_sales

UNION ALL
SELECT
'fact_sales_positive_business_values',
'CRITICAL',
cast(0 AS DECIMAL(20,4)),
cast(count(*) AS DECIMAL(20,4)),
CASE WHEN count(*) = 0 THEN 'PASSED' ELSE 'FAILED' END,
'Trusted completed sales must have positive quantity, unit price, and non-negative revenue.',
current_timestamp()
FROM fact_sales
WHERE quantity <= 0
OR unit_price_eur <= 0
OR net_sales_eur < 0

UNION ALL
SELECT
'fact_sales_foreign_keys_resolved',
'CRITICAL',
cast(0 AS DECIMAL(20,4)),
cast(count(*) AS DECIMAL(20,4)),
CASE WHEN count(*) = 0 THEN 'PASSED' ELSE 'FAILED' END,
'Trusted sales must resolve store, terminal, product, and effective price keys; customer may be null only for walk-ins.',
current_timestamp()
FROM fact_sales
WHERE store_sk IS NULL
OR terminal_sk IS NULL
OR product_sk IS NULL
OR price_version_sk IS NULL
OR (customer_id IS NOT NULL AND customer_sk IS NULL)

UNION ALL
SELECT
'fact_sales_scd2_price_match',
'CRITICAL',
cast(0 AS DECIMAL(20,4)),
cast(count(*) AS DECIMAL(20,4)),
CASE WHEN count(*) = 0 THEN 'PASSED' ELSE 'FAILED' END,
'Trusted sales unit price must match the effective SCD2 price within the configured tolerance.',
current_timestamp()
FROM fact_sales
WHERE abs(unit_price_variance_eur) > cast(:price_tolerance_eur AS DECIMAL(10,2))

UNION ALL
SELECT
'fact_voids_zero_values',
'CRITICAL',
cast(0 AS DECIMAL(20,4)),
cast(count(*) AS DECIMAL(20,4)),
CASE WHEN count(*) = 0 THEN 'PASSED' ELSE 'FAILED' END,
'Trusted void lines must have zero quantity, discount, and revenue.',
current_timestamp()
FROM fact_voids
WHERE quantity <> 0
OR discount_pct <> 0
OR net_revenue_eur <> 0

UNION ALL
SELECT
'fact_returns_unique_return_id',
'CRITICAL',
cast(0 AS DECIMAL(20,4)),
cast(count(*) - count(DISTINCT return_id) AS DECIMAL(20,4)),
CASE WHEN count(*) = count(DISTINCT return_id) THEN 'PASSED' ELSE 'FAILED' END,
'Trusted returns must contain one row per return_id.',
current_timestamp()
FROM fact_returns

UNION ALL
SELECT
'fact_returns_within_original_sale',
'CRITICAL',
cast(0 AS DECIMAL(20,4)),
cast(count(*) AS DECIMAL(20,4)),
CASE WHEN count(*) = 0 THEN 'PASSED' ELSE 'FAILED' END,
'Trusted cumulative return quantity and refund must not exceed the trusted original sale.',
current_timestamp()
FROM fact_returns
WHERE cumulative_return_quantity > sold_quantity
OR cumulative_refund_amount_eur > original_net_sales_eur + cast(:revenue_tolerance_eur AS DECIMAL(10,
2))

UNION ALL
SELECT
'trusted_sales_revenue_nonzero',
'WARNING',
cast(1 AS DECIMAL(20,4)),
cast(CASE WHEN coalesce(sum(net_sales_eur),
0) > 0 THEN 1 ELSE 0 END AS DECIMAL(20,
4)),
CASE WHEN coalesce(sum(net_sales_eur), 0) > 0 THEN 'PASSED' ELSE 'FAILED' END,
'A non-empty demo or portfolio run should produce positive trusted revenue.',
current_timestamp()
FROM fact_sales;

CREATE OR REFRESH MATERIALIZED VIEW silver_quality_gate (
CONSTRAINT all_critical_silver_checks_pass
EXPECT (failed_critical_checks = 0)
ON VIOLATION FAIL UPDATE
)
COMMENT 'Pipeline quality gate. Any failed CRITICAL Silver check fails the Silver pipeline update.'
AS
SELECT
count_if(severity = 'CRITICAL' AND status = 'FAILED') AS failed_critical_checks,
count_if(severity = 'WARNING' AND status = 'FAILED') AS failed_warning_checks,
count(*) AS total_checks,
current_timestamp() AS evaluated_at
FROM silver_quality_checks;
