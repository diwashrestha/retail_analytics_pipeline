-- ============================================================================
-- Einkaufpark Retail Platform — Silver Sales and Voids
-- Databricks Lakeflow Spark Declarative Pipeline (SQL)
--
-- Required pipeline parameters:
--   :bronze_catalog
--   :bronze_schema
--   :silver_catalog
--   :silver_schema
--   :price_tolerance_eur
--   :revenue_tolerance_eur
--
-- Processing order:
--   Bronze accepted rows
--     -> detect record-hash conflicts
--     -> remove exact retry duplicates
--     -> detect transaction/basket conflicts
--     -> enrich with trusted dimensions and effective SCD2 price
--     -> split valid sales, review sales, valid voids, review voids
-- ============================================================================

-- Publish all unqualified datasets in the parameterized Silver target.
-- The USE statements are scoped to this source file.
USE CATALOG IDENTIFIER(:silver_catalog);
USE SCHEMA IDENTIFIER(:silver_schema);

-- ---------------------------------------------------------------------------
-- 1. SOURCE PAYLOAD AND RECORD-HASH INTEGRITY
-- record_hash identifies a generated POS line. Repeated rows are exact retries
-- only when all business values also match. A repeated record_hash with a
-- different payload is a conflict and must never be silently deduplicated.
-- ---------------------------------------------------------------------------

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW transaction_payloads
AS
SELECT
  b.*,
  sha2(concat_ws('||',
    coalesce(transaction_id, ''),
    coalesce(basket_id, ''),
    coalesce(source_system, ''),
    coalesce(cast(order_date AS STRING), ''),
    coalesce(cast(order_timestamp AS STRING), ''),
    coalesce(sales_channel, ''),
    coalesce(order_status, ''),
    coalesce(store_id, ''),
    coalesce(customer_id, 'WALK_IN'),
    coalesce(cast(membership_active AS STRING), ''),
    coalesce(cast(loyalty_points_earned AS STRING), ''),
    coalesce(cast(coupon_applied AS STRING), ''),
    coalesce(coupon_code, ''),
    coalesce(product_id, ''),
    coalesce(cast(is_private_label AS STRING), ''),
    coalesce(brand, ''),
    coalesce(cast(quantity AS STRING), ''),
    coalesce(cast(unit_price_eur AS STRING), ''),
    coalesce(cast(discount_pct AS STRING), ''),
    coalesce(transaction_currency, ''),
    coalesce(cast(net_revenue_eur AS STRING), ''),
    coalesce(payment_type, ''),
    coalesce(pos_terminal_id, ''),
    coalesce(terminal_type, ''),
    coalesce(cast(is_self_checkout AS STRING), ''),
    coalesce(cashier_id, ''),
    coalesce(promo_week_id, ''),
    coalesce(cast(is_promo_period AS STRING), '')
  ), 256) AS transaction_payload_hash
FROM IDENTIFIER(:bronze_catalog || '.' || :bronze_schema || '.fact_transactions') b;

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW transaction_record_hash_stats
AS
SELECT
  record_hash,
  count(*) AS source_row_count,
  count(DISTINCT transaction_payload_hash) AS distinct_payload_count
FROM transaction_payloads
GROUP BY record_hash;

CREATE OR REFRESH MATERIALIZED VIEW transaction_hash_conflict_review
COMMENT 'All Bronze transaction rows whose record_hash is associated with more than one business payload.'
AS
SELECT
  p.*,
  s.source_row_count,
  s.distinct_payload_count,
  'RECORD_HASH_REUSED_WITH_DIFFERENT_PAYLOAD' AS review_reason
FROM transaction_payloads p
JOIN transaction_record_hash_stats s USING (record_hash)
WHERE s.distinct_payload_count > 1;

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW transaction_nonconflicting_ranked
AS
SELECT
  p.*,
  s.source_row_count,
  row_number() OVER (
    PARTITION BY p.record_hash, p.transaction_payload_hash
    ORDER BY
      CASE WHEN p.source_data_quality_flag LIKE '%INFO:DUPLICATE_TXN%' THEN 1 ELSE 0 END,
      p.ingestion_date,
      p._source_file_modified_at,
      p._bronze_ingested_at,
      p._bronze_record_fingerprint
  ) AS exact_retry_rank,
  first_value(p._bronze_record_fingerprint) OVER (
    PARTITION BY p.record_hash, p.transaction_payload_hash
    ORDER BY
      CASE WHEN p.source_data_quality_flag LIKE '%INFO:DUPLICATE_TXN%' THEN 1 ELSE 0 END,
      p.ingestion_date,
      p._source_file_modified_at,
      p._bronze_ingested_at,
      p._bronze_record_fingerprint
  ) AS canonical_bronze_record_fingerprint
FROM transaction_payloads p
JOIN transaction_record_hash_stats s USING (record_hash)
WHERE s.distinct_payload_count = 1;

CREATE OR REFRESH MATERIALIZED VIEW duplicate_transactions
COMMENT 'Exact POS retry rows removed from the trusted Silver transaction flow. One earliest canonical copy is retained.'
AS
SELECT
  transaction_id,
  basket_id,
  record_hash,
  transaction_payload_hash,
  _bronze_record_fingerprint,
  canonical_bronze_record_fingerprint,
  exact_retry_rank,
  batch_id,
  ingestion_date,
  source_data_quality_flag,
  bronze_warning_codes,
  _source_file_path,
  _source_file_name,
  _bronze_ingested_at,
  'EXACT_POS_RETRY' AS duplicate_reason
FROM transaction_nonconflicting_ranked
WHERE exact_retry_rank > 1;

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW transactions_deduplicated
AS
SELECT *
FROM transaction_nonconflicting_ranked
WHERE exact_retry_rank = 1;

-- ---------------------------------------------------------------------------
-- 2. TRANSACTION, BASKET, AND BASKET-PRODUCT CONTEXT INTEGRITY
-- Multiple line items in a basket are expected. The surrounding business
-- context must remain stable across all lines belonging to the same IDs.
-- ---------------------------------------------------------------------------

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW transaction_context_stats
AS
WITH contexts AS (
  SELECT
    transaction_id,
    sha2(concat_ws('||',
      basket_id,
      store_id,
      cast(order_date AS STRING),
      coalesce(customer_id, 'WALK_IN'),
      source_system,
      order_status,
      pos_terminal_id,
      payment_type,
      cast(membership_active AS STRING),
      cast(coupon_applied AS STRING),
      coalesce(coupon_code, '')
    ), 256) AS context_hash
  FROM transactions_deduplicated
)
SELECT
  transaction_id,
  count(DISTINCT context_hash) AS distinct_context_count
FROM contexts
GROUP BY transaction_id;

CREATE OR REFRESH MATERIALIZED VIEW transaction_id_conflicts
COMMENT 'Transaction IDs associated with multiple basket/customer/store/date contexts.'
AS
SELECT
  t.transaction_id,
  collect_set(t.basket_id) AS basket_ids,
  collect_set(t.store_id) AS store_ids,
  collect_set(cast(t.order_date AS STRING)) AS order_dates,
  collect_set(coalesce(t.customer_id, 'WALK_IN')) AS customer_ids,
  collect_set(t.pos_terminal_id) AS terminal_ids,
  max(s.distinct_context_count) AS distinct_context_count,
  'TRANSACTION_ID_CONTEXT_CONFLICT' AS review_reason
FROM transactions_deduplicated t
JOIN transaction_context_stats s USING (transaction_id)
WHERE s.distinct_context_count > 1
GROUP BY t.transaction_id;

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW basket_context_stats
AS
WITH contexts AS (
  SELECT
    basket_id,
    sha2(concat_ws('||',
      transaction_id,
      store_id,
      cast(order_date AS STRING),
      coalesce(customer_id, 'WALK_IN'),
      source_system,
      order_status,
      pos_terminal_id
    ), 256) AS context_hash
  FROM transactions_deduplicated
)
SELECT
  basket_id,
  count(DISTINCT context_hash) AS distinct_context_count
FROM contexts
GROUP BY basket_id;

CREATE OR REFRESH MATERIALIZED VIEW basket_id_conflicts
COMMENT 'Basket IDs associated with multiple transaction/customer/store/date contexts.'
AS
SELECT
  t.basket_id,
  collect_set(t.transaction_id) AS transaction_ids,
  collect_set(t.store_id) AS store_ids,
  collect_set(cast(t.order_date AS STRING)) AS order_dates,
  collect_set(coalesce(t.customer_id, 'WALK_IN')) AS customer_ids,
  max(s.distinct_context_count) AS distinct_context_count,
  'BASKET_ID_CONTEXT_CONFLICT' AS review_reason
FROM transactions_deduplicated t
JOIN basket_context_stats s USING (basket_id)
WHERE s.distinct_context_count > 1
GROUP BY t.basket_id;

CREATE OR REFRESH MATERIALIZED VIEW basket_product_conflicts
COMMENT 'Unexpected repeated product lines inside a deduplicated basket.'
AS
SELECT
  basket_id,
  product_id,
  count(*) AS line_count,
  collect_set(record_hash) AS record_hashes,
  'DUPLICATE_PRODUCT_LINE_WITHIN_BASKET' AS review_reason
FROM transactions_deduplicated
GROUP BY basket_id, product_id
HAVING count(*) > 1;

-- ---------------------------------------------------------------------------
-- 3. COMPLETED SALES ENRICHMENT AND QUALITY CLASSIFICATION
-- The SCD2 join uses inclusive effective dates, matching the generator.
-- A source row is valid only when exactly one trusted price version matches.
-- ---------------------------------------------------------------------------

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW sales_enrichment_candidates
AS
SELECT
  t.*,
  s.store_sk,
  s.source_system AS store_source_system,
  s.size_class AS store_size_class,
  c.customer_sk,
  c.is_member AS customer_master_is_member,
  c.age_group AS customer_age_group,
  c.age_quality_status AS customer_age_quality_status,
  p.product_sk,
  p.default_brand AS product_default_brand,
  p.price_band,
  p.vat_rate AS product_vat_rate,
  trm.terminal_sk,
  trm.store_id AS terminal_store_id,
  trm.terminal_type AS master_terminal_type,
  trm.is_self_checkout AS master_is_self_checkout,
  pv.price_version_sk,
  pv.effective_price_eur,
  pv.effective_from AS price_effective_from,
  pv.effective_to AS price_effective_to,
  pv.is_promo_price,
  pv.vat_rate AS effective_vat_rate,
  count(pv.price_version_sk) OVER (
    PARTITION BY t._bronze_record_fingerprint
  ) AS scd2_match_count,
  row_number() OVER (
    PARTITION BY t._bronze_record_fingerprint
    ORDER BY pv.effective_from DESC NULLS LAST, pv.price_version_sk
  ) AS scd2_match_rank,
  CASE WHEN tx.transaction_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_transaction_id_conflict,
  CASE WHEN bx.basket_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_basket_id_conflict,
  CASE WHEN bp.basket_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_basket_product_conflict
FROM transactions_deduplicated t
LEFT JOIN dim_store s
  ON t.store_id = s.store_id
LEFT JOIN dim_customer c
  ON t.customer_id = c.customer_id
LEFT JOIN dim_product p
  ON t.product_id = p.product_id
LEFT JOIN dim_terminal trm
  ON t.pos_terminal_id = trm.terminal_id
LEFT JOIN dim_product_scd2 pv
  ON t.product_id = pv.product_id
 AND t.order_date BETWEEN pv.effective_from AND pv.effective_to
LEFT JOIN transaction_id_conflicts tx
  ON t.transaction_id = tx.transaction_id
LEFT JOIN basket_id_conflicts bx
  ON t.basket_id = bx.basket_id
LEFT JOIN basket_product_conflicts bp
  ON t.basket_id = bp.basket_id
 AND t.product_id = bp.product_id
WHERE t.order_status = 'Completed';

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW sales_classified
AS
SELECT
  e.*,
  round(unit_price_eur * quantity, 2) AS pre_discount_sales_eur,
  round(unit_price_eur * quantity * (discount_pct / 100), 2) AS calculated_discount_eur,
  round(unit_price_eur * quantity * (1 - discount_pct / 100), 2) AS calculated_net_sales_eur,
  round(unit_price_eur * quantity - net_revenue_eur, 2) AS source_discount_amount_eur,
  round(effective_price_eur * quantity, 2) AS effective_list_amount_eur,
  round(unit_price_eur - effective_price_eur, 2) AS unit_price_variance_eur,
  round(net_revenue_eur / (1 + effective_vat_rate), 2) AS net_sales_ex_vat_eur,
  round(net_revenue_eur - (net_revenue_eur / (1 + effective_vat_rate)), 2) AS vat_amount_eur,
  concat_ws('|',
    CASE WHEN has_transaction_id_conflict THEN 'TRANSACTION_ID_CONTEXT_CONFLICT' END,
    CASE WHEN has_basket_id_conflict THEN 'BASKET_ID_CONTEXT_CONFLICT' END,
    CASE WHEN has_basket_product_conflict THEN 'DUPLICATE_PRODUCT_LINE_WITHIN_BASKET' END,
    CASE WHEN store_sk IS NULL THEN 'STORE_NOT_IN_TRUSTED_DIMENSION' END,
    CASE WHEN store_sk IS NOT NULL AND source_system <> store_source_system THEN 'STORE_SOURCE_SYSTEM_MISMATCH' END,
    CASE WHEN customer_id IS NOT NULL AND customer_sk IS NULL THEN 'CUSTOMER_NOT_IN_TRUSTED_DIMENSION' END,
    CASE WHEN customer_id IS NULL AND membership_active THEN 'WALK_IN_MARKED_AS_MEMBER' END,
    CASE WHEN membership_active AND customer_master_is_member = FALSE THEN 'ACTIVE_MEMBER_NOT_MEMBER_IN_MASTER' END,
    CASE WHEN product_sk IS NULL THEN 'PRODUCT_NOT_IN_TRUSTED_DIMENSION' END,
    CASE
      WHEN product_sk IS NOT NULL
       AND (CASE WHEN lower(brand) = 'bulk' THEN 'EKP-Classic' ELSE brand END) <> product_default_brand
      THEN 'TRANSACTION_BRAND_MISMATCH'
    END,
    CASE WHEN terminal_sk IS NULL THEN 'TERMINAL_NOT_IN_TRUSTED_DIMENSION' END,
    CASE WHEN terminal_sk IS NOT NULL AND terminal_store_id <> store_id THEN 'TERMINAL_STORE_MISMATCH' END,
    CASE WHEN terminal_sk IS NOT NULL AND master_terminal_type <> terminal_type THEN 'TERMINAL_TYPE_MISMATCH' END,
    CASE WHEN terminal_sk IS NOT NULL AND NOT (master_is_self_checkout <=> is_self_checkout) THEN 'SELF_CHECKOUT_FLAG_MISMATCH' END,
    CASE WHEN dayofweek(order_date) = 1 THEN 'SUNDAY_SALE' END,
    CASE WHEN quantity <= 0 THEN 'NON_POSITIVE_COMPLETED_QUANTITY' END,
    CASE WHEN unit_price_eur <= 0 THEN 'NON_POSITIVE_UNIT_PRICE' END,
    CASE WHEN discount_pct < 0 OR discount_pct > 100 THEN 'DISCOUNT_OUT_OF_RANGE' END,
    CASE WHEN net_revenue_eur < 0 THEN 'NEGATIVE_NET_REVENUE' END,
    CASE
      WHEN abs(net_revenue_eur - round(unit_price_eur * quantity * (1 - discount_pct / 100), 2))
           > cast(:revenue_tolerance_eur AS DECIMAL(10,2))
      THEN 'REVENUE_CALCULATION_MISMATCH'
    END,
    CASE WHEN scd2_match_count = 0 THEN 'NO_EFFECTIVE_SCD2_PRICE' END,
    CASE WHEN scd2_match_count > 1 THEN 'MULTIPLE_EFFECTIVE_SCD2_PRICES' END,
    CASE
      WHEN scd2_match_count = 1
       AND abs(unit_price_eur - effective_price_eur) > cast(:price_tolerance_eur AS DECIMAL(10,2))
      THEN 'UNIT_PRICE_DOES_NOT_MATCH_SCD2'
    END,
    CASE WHEN scd2_match_count = 1 AND effective_vat_rate <> product_vat_rate THEN 'SCD2_VAT_RATE_MISMATCH' END
  ) AS silver_review_reasons,
  concat_ws('|',
    CASE WHEN bronze_warning_codes <> '' THEN bronze_warning_codes END,
    CASE WHEN ingestion_date > order_date THEN 'INFO:LATE_ARRIVAL' END,
    CASE WHEN customer_id IS NOT NULL AND customer_age_quality_status <> 'VALID' THEN 'INFO:CUSTOMER_AGE_NOT_VALID' END
  ) AS silver_warning_codes
FROM sales_enrichment_candidates e
WHERE scd2_match_rank = 1;

CREATE OR REFRESH MATERIALIZED VIEW fact_sales
COMMENT 'Trusted completed POS line items after exact-retry removal, context validation, dimensional enrichment, and effective-price validation.'
CLUSTER BY AUTO
AS
SELECT
  _bronze_record_fingerprint AS sales_line_sk,
  transaction_id,
  basket_id,
  batch_id,
  record_hash,
  transaction_payload_hash,
  order_date,
  order_time,
  order_timestamp,
  ingestion_date,
  datediff(ingestion_date, order_date) AS arrival_delay_days,
  ingestion_date > order_date AS is_late_arrival,
  store_sk,
  store_id,
  terminal_sk,
  pos_terminal_id,
  customer_sk,
  customer_id,
  CASE
    WHEN customer_id IS NULL THEN 'Walk-in'
    WHEN membership_active THEN 'Loyalty Member'
    ELSE 'Registered Non-member'
  END AS customer_type,
  product_sk,
  product_id,
  price_version_sk,
  quantity,
  effective_price_eur AS effective_list_price_eur,
  unit_price_eur,
  discount_pct,
  pre_discount_sales_eur,
  source_discount_amount_eur AS discount_amount_eur,
  net_revenue_eur AS net_sales_eur,
  net_sales_ex_vat_eur,
  vat_amount_eur,
  effective_vat_rate AS vat_rate,
  effective_list_amount_eur,
  unit_price_variance_eur,
  payment_type,
  membership_active,
  loyalty_points_earned,
  coupon_applied,
  coupon_code,
  is_private_label,
  brand,
  terminal_type,
  is_self_checkout,
  cashier_id,
  promo_week_id,
  is_promo_period,
  is_promo_price,
  source_system,
  sales_channel,
  source_data_quality_flag,
  silver_warning_codes,
  _source_file_path,
  _source_file_name,
  _source_file_modified_at,
  _bronze_ingested_at,
  _bronze_processed_at
FROM sales_classified
WHERE silver_review_reasons = '';

CREATE OR REFRESH MATERIALIZED VIEW fact_sales_review
COMMENT 'Completed transaction lines excluded from trusted sales, with explicit and independently testable review reasons.'
CLUSTER BY AUTO
AS
SELECT
  _bronze_record_fingerprint AS sales_line_sk,
  transaction_id,
  basket_id,
  batch_id,
  record_hash,
  transaction_payload_hash,
  order_date,
  order_timestamp,
  ingestion_date,
  store_id,
  pos_terminal_id,
  customer_id,
  product_id,
  quantity,
  unit_price_eur,
  discount_pct,
  net_revenue_eur,
  effective_price_eur,
  scd2_match_count,
  silver_review_reasons,
  silver_warning_codes,
  source_data_quality_flag,
  bronze_warning_codes,
  _source_file_path,
  _source_file_name,
  _bronze_ingested_at
FROM sales_classified
WHERE silver_review_reasons <> '';

-- ---------------------------------------------------------------------------
-- 4. VOID EVENTS
-- Voided baskets are operational events, not sales. Trusted void lines must
-- have zero quantity and zero revenue and must resolve to trusted dimensions.
-- ---------------------------------------------------------------------------

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW voids_classified
AS
SELECT
  t.*,
  s.store_sk,
  c.customer_sk,
  p.product_sk,
  trm.terminal_sk,
  concat_ws('|',
    CASE WHEN tx.transaction_id IS NOT NULL THEN 'TRANSACTION_ID_CONTEXT_CONFLICT' END,
    CASE WHEN bx.basket_id IS NOT NULL THEN 'BASKET_ID_CONTEXT_CONFLICT' END,
    CASE WHEN bp.basket_id IS NOT NULL THEN 'DUPLICATE_PRODUCT_LINE_WITHIN_BASKET' END,
    CASE WHEN s.store_id IS NULL THEN 'STORE_NOT_IN_TRUSTED_DIMENSION' END,
    CASE WHEN s.store_id IS NOT NULL AND t.source_system <> s.source_system THEN 'STORE_SOURCE_SYSTEM_MISMATCH' END,
    CASE WHEN t.customer_id IS NOT NULL AND c.customer_id IS NULL THEN 'CUSTOMER_NOT_IN_TRUSTED_DIMENSION' END,
    CASE WHEN p.product_id IS NULL THEN 'PRODUCT_NOT_IN_TRUSTED_DIMENSION' END,
    CASE WHEN trm.terminal_id IS NULL THEN 'TERMINAL_NOT_IN_TRUSTED_DIMENSION' END,
    CASE WHEN trm.terminal_id IS NOT NULL AND trm.store_id <> t.store_id THEN 'TERMINAL_STORE_MISMATCH' END,
    CASE WHEN dayofweek(t.order_date) = 1 THEN 'SUNDAY_VOID' END,
    CASE WHEN t.quantity <> 0 THEN 'VOID_QUANTITY_NOT_ZERO' END,
    CASE WHEN t.net_revenue_eur <> 0 THEN 'VOID_REVENUE_NOT_ZERO' END,
    CASE WHEN t.discount_pct <> 0 THEN 'VOID_DISCOUNT_NOT_ZERO' END
  ) AS silver_review_reasons,
  concat_ws('|',
    CASE WHEN t.bronze_warning_codes <> '' THEN t.bronze_warning_codes END,
    CASE WHEN t.ingestion_date > t.order_date THEN 'INFO:LATE_ARRIVAL' END
  ) AS silver_warning_codes
FROM transactions_deduplicated t
LEFT JOIN dim_store s ON t.store_id = s.store_id
LEFT JOIN dim_customer c ON t.customer_id = c.customer_id
LEFT JOIN dim_product p ON t.product_id = p.product_id
LEFT JOIN dim_terminal trm ON t.pos_terminal_id = trm.terminal_id
LEFT JOIN transaction_id_conflicts tx ON t.transaction_id = tx.transaction_id
LEFT JOIN basket_id_conflicts bx ON t.basket_id = bx.basket_id
LEFT JOIN basket_product_conflicts bp
  ON t.basket_id = bp.basket_id
 AND t.product_id = bp.product_id
WHERE t.order_status = 'Voided';

CREATE OR REFRESH MATERIALIZED VIEW fact_voids
COMMENT 'Trusted voided POS line events. These rows must never contribute to sales KPIs.'
AS
SELECT
  _bronze_record_fingerprint AS void_line_sk,
  transaction_id,
  basket_id,
  batch_id,
  record_hash,
  order_date,
  order_timestamp,
  ingestion_date,
  store_sk,
  store_id,
  terminal_sk,
  pos_terminal_id,
  customer_sk,
  customer_id,
  product_sk,
  product_id,
  unit_price_eur,
  quantity,
  discount_pct,
  net_revenue_eur,
  source_system,
  source_data_quality_flag,
  silver_warning_codes,
  _source_file_path,
  _bronze_ingested_at
FROM voids_classified
WHERE silver_review_reasons = '';

CREATE OR REFRESH MATERIALIZED VIEW fact_voids_review
COMMENT 'Voided POS lines with invalid financial values, conflicting IDs, or unresolved dimensions.'
AS
SELECT
  _bronze_record_fingerprint AS void_line_sk,
  transaction_id,
  basket_id,
  record_hash,
  order_date,
  store_id,
  customer_id,
  product_id,
  pos_terminal_id,
  quantity,
  discount_pct,
  net_revenue_eur,
  silver_review_reasons,
  silver_warning_codes,
  _source_file_path,
  _bronze_ingested_at
FROM voids_classified
WHERE silver_review_reasons <> '';

-- ---------------------------------------------------------------------------
-- 5. TRANSACTION RECONCILIATION AND QUALITY SUMMARY
-- The category counts below are mutually exclusive and must reconcile exactly
-- to accepted Bronze transaction rows.
-- ---------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW silver_transaction_reconciliation
COMMENT 'Mutually exclusive Silver routing counts for accepted Bronze transactions.'
AS
SELECT
  bronze_rows,
  hash_conflict_rows,
  exact_duplicate_rows,
  valid_sales_rows,
  sales_review_rows,
  valid_void_rows,
  void_review_rows,
  bronze_rows
    - hash_conflict_rows
    - exact_duplicate_rows
    - valid_sales_rows
    - sales_review_rows
    - valid_void_rows
    - void_review_rows AS reconciliation_difference,
  current_timestamp() AS measured_at
FROM (
  SELECT
    (SELECT count(*) FROM IDENTIFIER(:bronze_catalog || '.' || :bronze_schema || '.fact_transactions')) AS bronze_rows,
    (SELECT count(*) FROM transaction_hash_conflict_review) AS hash_conflict_rows,
    (SELECT count(*) FROM duplicate_transactions) AS exact_duplicate_rows,
    (SELECT count(*) FROM fact_sales) AS valid_sales_rows,
    (SELECT count(*) FROM fact_sales_review) AS sales_review_rows,
    (SELECT count(*) FROM fact_voids) AS valid_void_rows,
    (SELECT count(*) FROM fact_voids_review) AS void_review_rows
) routing_counts;

CREATE OR REFRESH MATERIALIZED VIEW silver_sales_quality_summary
COMMENT 'Operational Silver sales quality metrics for dashboards and release checks.'
AS
SELECT 'bronze_transaction_rows' AS metric_name, cast(bronze_rows AS DECIMAL(20,2)) AS metric_value, measured_at
FROM silver_transaction_reconciliation
UNION ALL
SELECT 'record_hash_conflict_rows', cast(hash_conflict_rows AS DECIMAL(20,2)), measured_at
FROM silver_transaction_reconciliation
UNION ALL
SELECT 'exact_duplicate_rows_removed', cast(exact_duplicate_rows AS DECIMAL(20,2)), measured_at
FROM silver_transaction_reconciliation
UNION ALL
SELECT 'trusted_sales_rows', cast(valid_sales_rows AS DECIMAL(20,2)), measured_at
FROM silver_transaction_reconciliation
UNION ALL
SELECT 'sales_review_rows', cast(sales_review_rows AS DECIMAL(20,2)), measured_at
FROM silver_transaction_reconciliation
UNION ALL
SELECT 'trusted_void_rows', cast(valid_void_rows AS DECIMAL(20,2)), measured_at
FROM silver_transaction_reconciliation
UNION ALL
SELECT 'void_review_rows', cast(void_review_rows AS DECIMAL(20,2)), measured_at
FROM silver_transaction_reconciliation
UNION ALL
SELECT 'transaction_reconciliation_difference', cast(reconciliation_difference AS DECIMAL(20,2)), measured_at
FROM silver_transaction_reconciliation
UNION ALL
SELECT 'trusted_net_sales_eur', cast(round(sum(net_sales_eur), 2) AS DECIMAL(20,2)), current_timestamp()
FROM fact_sales;
