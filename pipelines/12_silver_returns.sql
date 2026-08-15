-- ============================================================================
-- Einkaufpark Retail Platform — Silver Returns
-- Databricks Lakeflow Spark Declarative Pipeline (SQL)
--
-- Required pipeline parameters:
--   :bronze_catalog
--   :bronze_schema
--   :silver_catalog
--   :silver_schema
--   :max_return_window_days
--   :price_tolerance_eur
--   :revenue_tolerance_eur
--
-- A return becomes trusted only when it links to a trusted Silver sale and its
-- cumulative quantity/refund remain within the amount originally purchased.
-- ============================================================================

-- Publish all unqualified datasets in the parameterized Silver target.
-- The USE statements are scoped to this source file.

USE CATALOG workspace;
USE SCHEMA retail_dev_silver;

-- ---------------------------------------------------------------------------
-- 1. RETURN ID INTEGRITY AND EXACT-DUPLICATE REMOVAL
-- ---------------------------------------------------------------------------

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW return_payloads
AS
SELECT
b.*,
sha2(concat_ws('||',
coalesce(return_id, ''),
coalesce(original_transaction_id, ''),
coalesce(original_basket_id, ''),
coalesce(cast(return_date AS STRING), ''),
coalesce(cast(return_timestamp AS STRING), ''),
coalesce(store_id, ''),
coalesce(customer_id, 'WALK_IN'),
coalesce(product_id, ''),
coalesce(cast(original_quantity AS STRING), ''),
coalesce(cast(return_quantity AS STRING), ''),
coalesce(cast(original_unit_price_eur AS STRING), ''),
coalesce(cast(original_discount_pct AS STRING), ''),
coalesce(cast(net_unit_price_eur AS STRING), ''),
coalesce(cast(refund_amount_eur AS STRING), ''),
coalesce(reason_code, ''),
coalesce(cashier_id, '')
), 256) AS return_payload_hash
FROM workspace.retail_dev_bronze.fact_returns b;

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW return_id_stats
AS
SELECT
return_id,
count(*) AS source_row_count,
count(DISTINCT return_payload_hash) AS distinct_payload_count
FROM return_payloads
GROUP BY return_id;

CREATE OR REFRESH MATERIALIZED VIEW return_id_conflict_review
COMMENT 'Return IDs associated with more than one business payload.'
AS
SELECT
p.*,
s.source_row_count,
s.distinct_payload_count,
'RETURN_ID_REUSED_WITH_DIFFERENT_PAYLOAD' AS review_reason
FROM return_payloads p
JOIN return_id_stats s USING (return_id)
WHERE s.distinct_payload_count > 1;

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW return_nonconflicting_ranked
AS
SELECT
p.*,
row_number() OVER (
PARTITION BY p.return_id, p.return_payload_hash
ORDER BY
p.ingestion_date,
p._source_file_modified_at,
p._bronze_ingested_at,
p._bronze_record_fingerprint
) AS exact_retry_rank,
first_value(p._bronze_record_fingerprint) OVER (
PARTITION BY p.return_id, p.return_payload_hash
ORDER BY
p.ingestion_date,
p._source_file_modified_at,
p._bronze_ingested_at,
p._bronze_record_fingerprint
) AS canonical_bronze_record_fingerprint
FROM return_payloads p
JOIN return_id_stats s USING (return_id)
WHERE s.distinct_payload_count = 1;

CREATE OR REFRESH MATERIALIZED VIEW duplicate_returns
COMMENT 'Exact duplicate return events removed from the trusted return flow.'
AS
SELECT
return_id,
original_transaction_id,
original_basket_id,
product_id,
return_payload_hash,
_bronze_record_fingerprint,
canonical_bronze_record_fingerprint,
exact_retry_rank,
ingestion_date,
_source_file_path,
_bronze_ingested_at,
'EXACT_RETURN_RETRY' AS duplicate_reason
FROM return_nonconflicting_ranked
WHERE exact_retry_rank > 1;

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW returns_deduplicated
AS
SELECT *
FROM return_nonconflicting_ranked
WHERE exact_retry_rank = 1;

-- ---------------------------------------------------------------------------
-- 2. TRUSTED ORIGINAL SALE AT BASKET-PRODUCT GRAIN
-- Aggregation makes the validation robust even if a future source sends split
-- lines for the same product. Current generator output normally has one line.
-- ---------------------------------------------------------------------------

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW original_sale_by_basket_product
AS
SELECT
basket_id,
product_id,
min(transaction_id) AS transaction_id,
min(order_date) AS order_date,
min(store_id) AS store_id,
min(customer_id) AS customer_id,
min(product_sk) AS product_sk,
min(customer_sk) AS customer_sk,
min(store_sk) AS store_sk,
sum(quantity) AS sold_quantity,
max(unit_price_eur) AS original_unit_price_eur,
max(discount_pct) AS original_discount_pct,
round(sum(net_sales_eur), 2) AS original_net_sales_eur,
round(sum(net_sales_eur) / nullif(sum(quantity),
0),
4) AS original_net_unit_price_eur,
count(*) AS source_sales_line_count
FROM fact_sales
GROUP BY basket_id, product_id;

-- ---------------------------------------------------------------------------
-- 3. LINK, SEQUENCE, AND CLASSIFY RETURNS
-- ---------------------------------------------------------------------------

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW returns_linked
AS
SELECT
r.*,
s.transaction_id AS sale_transaction_id,
s.order_date AS sale_order_date,
s.store_id AS sale_store_id,
s.customer_id AS sale_customer_id,
s.product_sk,
s.customer_sk,
s.store_sk,
s.sold_quantity,
s.original_unit_price_eur AS sale_unit_price_eur,
s.original_discount_pct AS sale_discount_pct,
s.original_net_sales_eur,
s.original_net_unit_price_eur AS sale_net_unit_price_eur,
s.source_sales_line_count
FROM returns_deduplicated r
LEFT JOIN original_sale_by_basket_product s
ON r.original_basket_id = s.basket_id
AND r.product_id = s.product_id;

-- First apply independent linkage and arithmetic rules. Invalid base records
-- are not allowed to consume cumulative return quantity or refund capacity.
CREATE OR REFRESH PRIVATE MATERIALIZED VIEW returns_base_classified
AS
SELECT
r.*,
concat_ws('|',
CASE WHEN sale_transaction_id IS NULL THEN 'ORIGINAL_TRUSTED_SALE_NOT_FOUND' END,
CASE
WHEN sale_transaction_id IS NOT NULL
AND original_transaction_id <> sale_transaction_id
THEN 'ORIGINAL_TRANSACTION_ID_MISMATCH'
END,
CASE WHEN sale_transaction_id IS NOT NULL AND store_id <> sale_store_id THEN 'RETURN_STORE_MISMATCH' END,
CASE
WHEN sale_transaction_id IS NOT NULL
AND NOT (customer_id <=> sale_customer_id)
THEN 'RETURN_CUSTOMER_MISMATCH'
END,
CASE WHEN sale_transaction_id IS NOT NULL AND return_date < sale_order_date THEN 'RETURN_BEFORE_SALE' END,
CASE
WHEN sale_transaction_id IS NOT NULL
AND datediff(return_date,
sale_order_date) > cast(:max_return_window_days AS INT)
THEN 'RETURN_WINDOW_EXCEEDED'
END,
CASE WHEN dayofweek(return_date) = 1 THEN 'SUNDAY_RETURN' END,
CASE WHEN cashier_id IS NULL THEN 'MISSING_RETURN_CASHIER' END,
CASE WHEN return_quantity <= 0 THEN 'NON_POSITIVE_RETURN_QUANTITY' END,
CASE
WHEN sale_transaction_id IS NOT NULL
AND original_quantity <> sold_quantity
THEN 'SOURCE_ORIGINAL_QUANTITY_MISMATCH'
END,
CASE
WHEN sale_transaction_id IS NOT NULL
AND abs(original_unit_price_eur - sale_unit_price_eur)
> cast(:price_tolerance_eur AS DECIMAL(10,2))
THEN 'SOURCE_ORIGINAL_UNIT_PRICE_MISMATCH'
END,
CASE
WHEN sale_transaction_id IS NOT NULL
AND abs(original_discount_pct - sale_discount_pct) > 0.05
THEN 'SOURCE_ORIGINAL_DISCOUNT_MISMATCH'
END,
CASE
WHEN sale_transaction_id IS NOT NULL
AND abs(net_unit_price_eur - sale_net_unit_price_eur)
> cast(:price_tolerance_eur AS DECIMAL(10,2))
THEN 'NET_UNIT_PRICE_MISMATCH'
END,
CASE
WHEN abs(refund_amount_eur - round(net_unit_price_eur * return_quantity, 2))
> cast(:revenue_tolerance_eur AS DECIMAL(10,2))
THEN 'REFUND_CALCULATION_MISMATCH'
END
) AS base_review_reasons,
concat_ws('|',
CASE WHEN bronze_warning_codes <> '' THEN bronze_warning_codes END,
CASE WHEN ingestion_date > return_date THEN 'INFO:LATE_ARRIVAL' END
) AS silver_warning_codes
FROM returns_linked r;

-- Only independently valid candidates participate in cumulative controls.
-- This prevents one malformed return from poisoning all later valid returns
-- for the same basket-product purchase.
CREATE OR REFRESH PRIVATE MATERIALIZED VIEW return_cumulative_candidates
AS
SELECT
_bronze_record_fingerprint,
sum(return_quantity) OVER (
PARTITION BY original_basket_id, product_id
ORDER BY return_timestamp, return_id
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS cumulative_return_quantity,
round(sum(refund_amount_eur) OVER (
PARTITION BY original_basket_id, product_id
ORDER BY return_timestamp, return_id
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
), 2) AS cumulative_refund_amount_eur
FROM returns_base_classified
WHERE base_review_reasons = '';

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW returns_classified
AS

WITH normalized AS (
SELECT
r.*,
coalesce(trim(r.base_review_reasons), '') AS normalized_base_review_reasons
FROM returns_base_classified r
)

SELECT
r.* EXCEPT (
base_review_reasons,
normalized_base_review_reasons
),

c.cumulative_return_quantity,
c.cumulative_refund_amount_eur,

concat_ws(
'|',

nullif(r.normalized_base_review_reasons, ''),

CASE
WHEN r.normalized_base_review_reasons = ''
AND c.cumulative_return_quantity > r.sold_quantity
THEN 'CUMULATIVE_RETURN_QUANTITY_EXCEEDS_SALE'
END,

CASE
WHEN r.normalized_base_review_reasons = ''
AND c.cumulative_refund_amount_eur >
r.original_net_sales_eur
+ cast(:revenue_tolerance_eur AS DECIMAL(10,2))
THEN 'CUMULATIVE_REFUND_EXCEEDS_AMOUNT_PAID'
END

) AS silver_review_reasons

FROM normalized r

LEFT JOIN return_cumulative_candidates c
ON r._bronze_record_fingerprint =
c._bronze_record_fingerprint;

CREATE OR REFRESH MATERIALIZED VIEW fact_returns
COMMENT 'Trusted return events linked to trusted sales with cumulative quantity and refund controls.'
CLUSTER BY AUTO
AS
SELECT
_bronze_record_fingerprint AS return_sk,
return_id,
original_transaction_id,
original_basket_id,
product_sk,
product_id,
store_sk,
store_id,
customer_sk,
customer_id,
sale_order_date AS original_order_date,
return_date,
return_time,
return_timestamp,
datediff(return_date, sale_order_date) AS days_to_return,
ingestion_date,
datediff(ingestion_date, return_date) AS arrival_delay_days,
ingestion_date > return_date AS is_late_arrival,
sold_quantity,
original_quantity,
return_quantity,
sale_unit_price_eur AS original_unit_price_eur,
sale_discount_pct AS original_discount_pct,
sale_net_unit_price_eur AS net_unit_price_eur,
refund_amount_eur,
cumulative_return_quantity,
cumulative_refund_amount_eur,
original_net_sales_eur,
reason_code,
cashier_id,
silver_warning_codes,
_source_file_path,
_source_file_name,
_source_file_modified_at,
_bronze_ingested_at,
_bronze_processed_at
FROM returns_classified
WHERE coalesce(trim(silver_review_reasons), '') = '';

CREATE OR REFRESH MATERIALIZED VIEW fact_returns_review
COMMENT 'Return events excluded from trusted analytics, with explicit linkage, timing, quantity, and refund review reasons.'
CLUSTER BY AUTO
AS
SELECT
_bronze_record_fingerprint AS return_sk,
return_id,
original_transaction_id,
original_basket_id,
store_id,
customer_id,
product_id,
return_date,
return_timestamp,
ingestion_date,
original_quantity,
return_quantity,
original_unit_price_eur,
original_discount_pct,
net_unit_price_eur,
refund_amount_eur,
sale_transaction_id,
sale_order_date,
sale_store_id,
sale_customer_id,
sold_quantity,
sale_unit_price_eur,
sale_discount_pct,
sale_net_unit_price_eur,
original_net_sales_eur,
cumulative_return_quantity,
cumulative_refund_amount_eur,
reason_code,
silver_review_reasons,
silver_warning_codes,
_source_file_path,
_source_file_name,
_bronze_ingested_at
FROM returns_classified
WHERE silver_review_reasons <> '';

-- ---------------------------------------------------------------------------
-- 4. RETURN RECONCILIATION AND QUALITY SUMMARY
-- Categories are mutually exclusive and must reconcile to accepted Bronze
-- return rows.
-- ---------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW silver_return_reconciliation
COMMENT 'Mutually exclusive Silver routing counts for accepted Bronze returns.'
AS
SELECT
bronze_rows,
id_conflict_rows,
exact_duplicate_rows,
trusted_return_rows,
return_review_rows,
bronze_rows
- id_conflict_rows
- exact_duplicate_rows
- trusted_return_rows
- return_review_rows AS reconciliation_difference,
current_timestamp() AS measured_at
FROM (
SELECT
(SELECT count(*) FROM workspace.retail_dev_bronze.fact_returns) AS bronze_rows,
(SELECT count(*) FROM return_id_conflict_review) AS id_conflict_rows,
(SELECT count(*) FROM duplicate_returns) AS exact_duplicate_rows,
(SELECT count(*) FROM fact_returns) AS trusted_return_rows,
(SELECT count(*) FROM fact_returns_review) AS return_review_rows
) routing_counts;

CREATE OR REFRESH MATERIALIZED VIEW silver_return_quality_summary
COMMENT 'Operational Silver return quality metrics.'
AS
SELECT 'bronze_return_rows' AS metric_name,
cast(bronze_rows AS DECIMAL(20,
2)) AS metric_value,
measured_at
FROM silver_return_reconciliation
UNION ALL
SELECT 'return_id_conflict_rows',
cast(id_conflict_rows AS DECIMAL(20,
2)),
measured_at
FROM silver_return_reconciliation
UNION ALL
SELECT 'exact_duplicate_returns_removed',
cast(exact_duplicate_rows AS DECIMAL(20,
2)),
measured_at
FROM silver_return_reconciliation
UNION ALL
SELECT 'trusted_return_rows',
cast(trusted_return_rows AS DECIMAL(20,
2)),
measured_at
FROM silver_return_reconciliation
UNION ALL
SELECT 'return_review_rows',
cast(return_review_rows AS DECIMAL(20,
2)),
measured_at
FROM silver_return_reconciliation
UNION ALL
SELECT 'return_reconciliation_difference',
cast(reconciliation_difference AS DECIMAL(20,
2)),
measured_at
FROM silver_return_reconciliation
UNION ALL
SELECT 'trusted_refund_amount_eur',
cast(round(sum(refund_amount_eur),
2) AS DECIMAL(20,
2)),
current_timestamp()
FROM fact_returns;
