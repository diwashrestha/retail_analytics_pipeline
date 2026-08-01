-- ============================================================================
-- Einkaufpark Retail Platform — Bronze Layer
-- Databricks Lakeflow Spark Declarative Pipeline (SQL)
--
-- Pipeline target catalog/schema:
--   catalog: configured by the pipeline resource
--   schema : retail_bronze (or target-specific equivalent)
--
-- Required pipeline parameters (full Volume paths):
--   :transactions_path
--   :returns_path
--   :stores_path
--   :customers_path
--   :products_path
--   :product_prices_path
--
-- Design rules:
--   1. Preserve source values and provenance.
--   2. Read CSV columns as STRING so malformed values are not silently lost.
--   3. Use TRY_CAST / TRY_TO_TIMESTAMP in private parsing datasets.
--   4. Route hard structural errors to quarantine.
--   5. Keep warning/info rows in accepted Bronze data.
--   6. Do not deduplicate, perform SCD joins, or calculate business KPIs here.
-- ============================================================================

USE CATALOG workspace;
USE SCHEMA retail_dev_bronze;

-- ---------------------------------------------------------------------------
-- 1. POS TRANSACTION LINE ITEMS — append-only ingestion with Auto Loader
-- ---------------------------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE fact_transactions_raw (
  CONSTRAINT csv_columns_rescued
    EXPECT (_rescued_data IS NULL)
)
COMMENT 'Source-fidelity POS line items. All source columns remain strings; malformed or unexpected values are captured in _rescued_data.'
CLUSTER BY AUTO
AS
SELECT
  transaction_id,
  basket_id,
  batch_id,
  source_system,
  record_hash,
  order_date,
  order_time,
  ingestion_date,
  sales_channel,
  order_status,
  store_id,
  customer_id,
  membership_active,
  loyalty_points_earned,
  coupon_applied,
  coupon_code,
  product_id,
  is_private_label,
  brand,
  quantity,
  unit_price_eur,
  discount_pct,
  transaction_currency,
  net_revenue_eur,
  payment_type,
  pos_terminal_id,
  terminal_type,
  is_self_checkout,
  cashier_id,
  promo_week_id,
  is_promo_period,
  data_quality_flag,
  _rescued_data,
  _metadata.file_path              AS _source_file_path,
  _metadata.file_name              AS _source_file_name,
  _metadata.file_size              AS _source_file_size,
  _metadata.file_modification_time AS _source_file_modified_at,
  current_timestamp()              AS _bronze_ingested_at
FROM STREAM read_files(
  '/Volumes/workspace/retail_dev_raw/retail_input/transactions',
  format => 'csv',
  header => 'true',
  mode => 'PERMISSIVE',
  rescuedDataColumn => '_rescued_data',
  schemaEvolutionMode => 'rescue',
  schema => '
    transaction_id STRING,
    basket_id STRING,
    batch_id STRING,
    source_system STRING,
    record_hash STRING,
    order_date STRING,
    order_time STRING,
    ingestion_date STRING,
    sales_channel STRING,
    order_status STRING,
    store_id STRING,
    customer_id STRING,
    membership_active STRING,
    loyalty_points_earned STRING,
    coupon_applied STRING,
    coupon_code STRING,
    product_id STRING,
    is_private_label STRING,
    brand STRING,
    quantity STRING,
    unit_price_eur STRING,
    discount_pct STRING,
    transaction_currency STRING,
    net_revenue_eur STRING,
    payment_type STRING,
    pos_terminal_id STRING,
    terminal_type STRING,
    is_self_checkout STRING,
    cashier_id STRING,
    promo_week_id STRING,
    is_promo_period STRING,
    data_quality_flag STRING
  '
);

CREATE OR REFRESH PRIVATE STREAMING TABLE fact_transactions_parsed
COMMENT 'Internal typed and classified POS line items used to create accepted and quarantine Bronze outputs.'
AS
WITH typed AS (
  SELECT
    trim(transaction_id) AS transaction_id,
    trim(basket_id) AS basket_id,
    trim(batch_id) AS batch_id,
    trim(source_system) AS source_system,
    lower(trim(record_hash)) AS record_hash,
    try_cast(trim(order_date) AS DATE) AS order_date,
    trim(order_time) AS order_time,
    try_to_timestamp(
      concat(trim(order_date), ' ', trim(order_time)),
      'yyyy-MM-dd HH:mm:ss'
    ) AS order_timestamp,
    try_cast(trim(ingestion_date) AS DATE) AS ingestion_date,
    trim(sales_channel) AS sales_channel,
    trim(order_status) AS order_status,
    trim(store_id) AS store_id,
    nullif(trim(customer_id), '') AS customer_id,
    CASE
      WHEN lower(trim(membership_active)) IN ('true', '1') THEN TRUE
      WHEN lower(trim(membership_active)) IN ('false', '0') THEN FALSE
      ELSE NULL
    END AS membership_active,
    try_cast(trim(loyalty_points_earned) AS INT) AS loyalty_points_earned,
    CASE
      WHEN lower(trim(coupon_applied)) IN ('true', '1') THEN TRUE
      WHEN lower(trim(coupon_applied)) IN ('false', '0') THEN FALSE
      ELSE NULL
    END AS coupon_applied,
    nullif(trim(coupon_code), '') AS coupon_code,
    trim(product_id) AS product_id,
    CASE
      WHEN lower(trim(is_private_label)) IN ('true', '1') THEN TRUE
      WHEN lower(trim(is_private_label)) IN ('false', '0') THEN FALSE
      ELSE NULL
    END AS is_private_label,
    trim(brand) AS brand,
    try_cast(trim(quantity) AS INT) AS quantity,
    try_cast(trim(unit_price_eur) AS DECIMAL(10,2)) AS unit_price_eur,
    try_cast(trim(discount_pct) AS DECIMAL(5,2)) AS discount_pct,
    trim(transaction_currency) AS transaction_currency,
    try_cast(trim(net_revenue_eur) AS DECIMAL(14,2)) AS net_revenue_eur,
    trim(payment_type) AS payment_type,
    trim(pos_terminal_id) AS pos_terminal_id,
    trim(terminal_type) AS terminal_type,
    CASE
      WHEN lower(trim(is_self_checkout)) IN ('true', '1') THEN TRUE
      WHEN lower(trim(is_self_checkout)) IN ('false', '0') THEN FALSE
      ELSE NULL
    END AS is_self_checkout,
    nullif(trim(cashier_id), '') AS cashier_id,
    trim(promo_week_id) AS promo_week_id,
    CASE
      WHEN lower(trim(is_promo_period)) IN ('true', '1') THEN TRUE
      WHEN lower(trim(is_promo_period)) IN ('false', '0') THEN FALSE
      ELSE NULL
    END AS is_promo_period,
    coalesce(nullif(trim(data_quality_flag), ''), 'MISSING') AS source_data_quality_flag,
    _rescued_data,
    _source_file_path,
    _source_file_name,
    _source_file_size,
    _source_file_modified_at,
    _bronze_ingested_at,
    to_json(named_struct(
      'transaction_id', transaction_id,
      'basket_id', basket_id,
      'batch_id', batch_id,
      'source_system', source_system,
      'record_hash', record_hash,
      'order_date', order_date,
      'order_time', order_time,
      'ingestion_date', ingestion_date,
      'sales_channel', sales_channel,
      'order_status', order_status,
      'store_id', store_id,
      'customer_id', customer_id,
      'membership_active', membership_active,
      'loyalty_points_earned', loyalty_points_earned,
      'coupon_applied', coupon_applied,
      'coupon_code', coupon_code,
      'product_id', product_id,
      'is_private_label', is_private_label,
      'brand', brand,
      'quantity', quantity,
      'unit_price_eur', unit_price_eur,
      'discount_pct', discount_pct,
      'transaction_currency', transaction_currency,
      'net_revenue_eur', net_revenue_eur,
      'payment_type', payment_type,
      'pos_terminal_id', pos_terminal_id,
      'terminal_type', terminal_type,
      'is_self_checkout', is_self_checkout,
      'cashier_id', cashier_id,
      'promo_week_id', promo_week_id,
      'is_promo_period', is_promo_period,
      'data_quality_flag', data_quality_flag
    )) AS _raw_record_json
  FROM STREAM(fact_transactions_raw)
), classified AS (
  SELECT
    *,
    concat_ws('|',
      CASE WHEN _rescued_data IS NOT NULL THEN 'ERR:RESCUED_DATA' END,
      CASE WHEN transaction_id IS NULL OR transaction_id = '' THEN 'ERR:MISSING_TRANSACTION_ID' END,
      CASE WHEN basket_id IS NULL OR basket_id = '' THEN 'ERR:MISSING_BASKET_ID' END,
      CASE WHEN batch_id IS NULL OR batch_id = '' THEN 'ERR:MISSING_BATCH_ID' END,
      CASE WHEN source_system NOT IN ('SAP_POS', 'LEGACY_POS_CSV') THEN 'ERR:INVALID_SOURCE_SYSTEM' END,
      CASE WHEN record_hash IS NULL OR NOT record_hash RLIKE '^[0-9a-f]{32}$' THEN 'ERR:INVALID_RECORD_HASH' END,
      CASE WHEN order_date IS NULL THEN 'ERR:INVALID_ORDER_DATE' END,
      CASE WHEN order_timestamp IS NULL THEN 'ERR:INVALID_ORDER_TIME' END,
      CASE WHEN ingestion_date IS NULL THEN 'ERR:INVALID_INGESTION_DATE' END,
      CASE WHEN sales_channel <> 'IN_STORE' THEN 'ERR:INVALID_SALES_CHANNEL' END,
      CASE WHEN order_status NOT IN ('Completed', 'Voided') THEN 'ERR:INVALID_ORDER_STATUS' END,
      CASE WHEN store_id IS NULL OR store_id = '' THEN 'ERR:MISSING_STORE_ID' END,
      CASE WHEN membership_active IS NULL THEN 'ERR:INVALID_MEMBERSHIP_FLAG' END,
      CASE WHEN coupon_applied IS NULL THEN 'ERR:INVALID_COUPON_FLAG' END,
      CASE WHEN product_id IS NULL OR product_id = '' THEN 'ERR:MISSING_PRODUCT_ID' END,
      CASE WHEN is_private_label IS NULL THEN 'ERR:INVALID_PRIVATE_LABEL_FLAG' END,
      CASE WHEN brand IS NULL OR brand = '' THEN 'ERR:MISSING_BRAND' END,
      CASE WHEN quantity IS NULL THEN 'ERR:INVALID_QUANTITY' END,
      CASE WHEN unit_price_eur IS NULL THEN 'ERR:INVALID_UNIT_PRICE' END,
      CASE WHEN discount_pct IS NULL THEN 'ERR:INVALID_DISCOUNT' END,
      CASE WHEN transaction_currency <> 'EUR' THEN 'ERR:INVALID_CURRENCY' END,
      CASE WHEN net_revenue_eur IS NULL THEN 'ERR:INVALID_NET_REVENUE' END,
      CASE WHEN payment_type NOT IN ('Card','Cash','Apple_Pay','Google_Pay','Voucher','Gift_Card') THEN 'ERR:INVALID_PAYMENT_TYPE' END,
      CASE WHEN pos_terminal_id IS NULL OR pos_terminal_id = '' THEN 'ERR:MISSING_TERMINAL_ID' END,
      CASE WHEN terminal_type NOT IN ('CASHIER','SELF_CHECKOUT') THEN 'ERR:INVALID_TERMINAL_TYPE' END,
      CASE WHEN is_self_checkout IS NULL THEN 'ERR:INVALID_SELF_CHECKOUT_FLAG' END,
      CASE WHEN promo_week_id IS NULL OR NOT promo_week_id RLIKE '^PW[0-9]{4}-[0-9]{2}$' THEN 'ERR:INVALID_PROMO_WEEK_ID' END,
      CASE WHEN is_promo_period IS NULL THEN 'ERR:INVALID_PROMO_PERIOD_FLAG' END,
      CASE WHEN source_data_quality_flag = 'MISSING' THEN 'ERR:MISSING_SOURCE_DQ_FLAG' END,
      CASE WHEN source_data_quality_flag RLIKE '(^|\\|)ERR:' THEN 'ERR:SOURCE_FLAGGED_ROW' END
    ) AS hard_error_codes,
    concat_ws('|',
      CASE WHEN source_data_quality_flag RLIKE '(^|\\|)WARN:' THEN source_data_quality_flag END,
      CASE WHEN source_data_quality_flag RLIKE '(^|\\|)INFO:' THEN source_data_quality_flag END,
      CASE WHEN unit_price_eur < 0 THEN 'WARN:NEGATIVE_UNIT_PRICE' END,
      CASE WHEN order_status = 'Completed' AND quantity = 0 THEN 'WARN:ZERO_QUANTITY_COMPLETED' END,
      CASE WHEN quantity < 0 THEN 'WARN:NEGATIVE_QUANTITY' END,
      CASE WHEN discount_pct < 0 OR discount_pct > 100 THEN 'WARN:DISCOUNT_OUT_OF_RANGE' END,
      CASE WHEN net_revenue_eur < 0 THEN 'WARN:NEGATIVE_NET_REVENUE' END,
      CASE
        WHEN order_status = 'Completed'
         AND quantity IS NOT NULL
         AND unit_price_eur IS NOT NULL
         AND discount_pct BETWEEN 0 AND 100
         AND net_revenue_eur IS NOT NULL
         AND abs(
           net_revenue_eur - round(unit_price_eur * quantity * (1 - discount_pct / 100), 2)
         ) > 0.02
        THEN 'WARN:REVENUE_CALCULATION_MISMATCH'
      END,
      CASE
        WHEN order_status = 'Voided'
         AND (quantity <> 0 OR net_revenue_eur <> 0)
        THEN 'WARN:VOID_VALUE_MISMATCH'
      END,
      CASE WHEN ingestion_date > order_date THEN 'INFO:LATE_ARRIVAL' END,
      CASE WHEN customer_id IS NULL AND membership_active THEN 'WARN:WALK_IN_MARKED_MEMBER' END,
      CASE WHEN NOT membership_active AND loyalty_points_earned IS NOT NULL AND loyalty_points_earned <> 0 THEN 'WARN:POINTS_FOR_NON_MEMBER' END,
      CASE WHEN coupon_applied AND coupon_code IS NULL THEN 'WARN:COUPON_CODE_MISSING' END,
      CASE WHEN NOT coupon_applied AND coupon_code IS NOT NULL THEN 'WARN:UNEXPECTED_COUPON_CODE' END,
      CASE WHEN terminal_type = 'SELF_CHECKOUT' AND NOT is_self_checkout THEN 'WARN:TERMINAL_FLAG_MISMATCH' END,
      CASE WHEN terminal_type = 'CASHIER' AND is_self_checkout THEN 'WARN:TERMINAL_FLAG_MISMATCH' END,
      CASE WHEN is_self_checkout AND cashier_id IS NOT NULL THEN 'WARN:CASHIER_ON_SELF_CHECKOUT' END,
      CASE WHEN NOT is_self_checkout AND cashier_id IS NULL THEN 'WARN:MISSING_CASHIER_ID' END,
      CASE WHEN dayofweek(order_date) = 1 THEN 'WARN:SUNDAY_TRANSACTION' END
    ) AS warning_codes
  FROM typed
)
SELECT
  *,
  sha2(concat_ws('||',
    coalesce(transaction_id, ''),
    coalesce(basket_id, ''),
    coalesce(record_hash, ''),
    coalesce(_source_file_path, ''),
    coalesce(_raw_record_json, '')
  ), 256) AS _bronze_record_fingerprint,
  CASE
    WHEN hard_error_codes <> '' THEN 'QUARANTINED'
    WHEN warning_codes <> '' THEN 'ACCEPTED_WITH_WARNING'
    ELSE 'ACCEPTED'
  END AS bronze_record_status,
  current_timestamp() AS _bronze_processed_at
FROM classified;

CREATE OR REFRESH STREAMING TABLE fact_transactions
COMMENT 'Typed Bronze POS line items accepted for Silver processing. Exact retries and warning/info records are intentionally preserved.'
CLUSTER BY AUTO
AS
SELECT
  transaction_id,
  basket_id,
  batch_id,
  source_system,
  record_hash,
  order_date,
  order_time,
  order_timestamp,
  ingestion_date,
  sales_channel,
  order_status,
  store_id,
  customer_id,
  membership_active,
  loyalty_points_earned,
  coupon_applied,
  coupon_code,
  product_id,
  is_private_label,
  brand,
  quantity,
  unit_price_eur,
  discount_pct,
  transaction_currency,
  net_revenue_eur,
  payment_type,
  pos_terminal_id,
  terminal_type,
  is_self_checkout,
  cashier_id,
  promo_week_id,
  is_promo_period,
  source_data_quality_flag,
  warning_codes AS bronze_warning_codes,
  bronze_record_status,
  _bronze_record_fingerprint,
  _source_file_path,
  _source_file_name,
  _source_file_size,
  _source_file_modified_at,
  _bronze_ingested_at,
  _bronze_processed_at
FROM STREAM(fact_transactions_parsed)
WHERE hard_error_codes = '';

CREATE OR REFRESH STREAMING TABLE fact_transactions_quarantine
COMMENT 'POS line items rejected because of hard schema, parsing, enum, or source ERR conditions. Original values are retained as JSON.'
CLUSTER BY AUTO
AS
SELECT
  _bronze_record_fingerprint,
  transaction_id,
  basket_id,
  batch_id,
  record_hash,
  hard_error_codes AS quarantine_reasons,
  warning_codes AS additional_warnings,
  source_data_quality_flag,
  _rescued_data,
  _raw_record_json,
  _source_file_path,
  _source_file_name,
  _source_file_size,
  _source_file_modified_at,
  _bronze_ingested_at,
  _bronze_processed_at,
  current_timestamp() AS quarantined_at,
  'PENDING' AS repair_status
FROM STREAM(fact_transactions_parsed)
WHERE hard_error_codes <> '';

-- ---------------------------------------------------------------------------
-- 2. RETURNS — append-only ingestion with Auto Loader
-- ---------------------------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE fact_returns_raw (
  CONSTRAINT csv_columns_rescued
    EXPECT (_rescued_data IS NULL)
)
COMMENT 'Source-fidelity retail return events. Source values remain strings and malformed values are rescued.'
CLUSTER BY AUTO
AS
SELECT
  return_id,
  original_transaction_id,
  original_basket_id,
  return_date,
  return_time,
  store_id,
  customer_id,
  product_id,
  original_quantity,
  return_quantity,
  original_unit_price_eur,
  original_discount_pct,
  net_unit_price_eur,
  refund_amount_eur,
  reason_code,
  cashier_id,
  ingestion_date,
  _rescued_data,
  _metadata.file_path              AS _source_file_path,
  _metadata.file_name              AS _source_file_name,
  _metadata.file_size              AS _source_file_size,
  _metadata.file_modification_time AS _source_file_modified_at,
  current_timestamp()              AS _bronze_ingested_at
FROM STREAM read_files(
  '/Volumes/workspace/retail_dev_raw/retail_input/returns',
  format => 'csv',
  header => 'true',
  mode => 'PERMISSIVE',
  rescuedDataColumn => '_rescued_data',
  schemaEvolutionMode => 'rescue',
  schema => '
    return_id STRING,
    original_transaction_id STRING,
    original_basket_id STRING,
    return_date STRING,
    return_time STRING,
    store_id STRING,
    customer_id STRING,
    product_id STRING,
    original_quantity STRING,
    return_quantity STRING,
    original_unit_price_eur STRING,
    original_discount_pct STRING,
    net_unit_price_eur STRING,
    refund_amount_eur STRING,
    reason_code STRING,
    cashier_id STRING,
    ingestion_date STRING
  '
);

CREATE OR REFRESH PRIVATE STREAMING TABLE fact_returns_parsed
COMMENT 'Internal typed and classified return events.'
AS
WITH typed AS (
  SELECT
    trim(return_id) AS return_id,
    trim(original_transaction_id) AS original_transaction_id,
    trim(original_basket_id) AS original_basket_id,
    try_cast(trim(return_date) AS DATE) AS return_date,
    trim(return_time) AS return_time,
    try_to_timestamp(
      concat(trim(return_date), ' ', trim(return_time)),
      'yyyy-MM-dd HH:mm:ss'
    ) AS return_timestamp,
    trim(store_id) AS store_id,
    nullif(trim(customer_id), '') AS customer_id,
    trim(product_id) AS product_id,
    try_cast(trim(original_quantity) AS INT) AS original_quantity,
    try_cast(trim(return_quantity) AS INT) AS return_quantity,
    try_cast(trim(original_unit_price_eur) AS DECIMAL(10,2)) AS original_unit_price_eur,
    try_cast(trim(original_discount_pct) AS DECIMAL(5,2)) AS original_discount_pct,
    try_cast(trim(net_unit_price_eur) AS DECIMAL(10,2)) AS net_unit_price_eur,
    try_cast(trim(refund_amount_eur) AS DECIMAL(14,2)) AS refund_amount_eur,
    trim(reason_code) AS reason_code,
    nullif(trim(cashier_id), '') AS cashier_id,
    try_cast(trim(ingestion_date) AS DATE) AS ingestion_date,
    _rescued_data,
    _source_file_path,
    _source_file_name,
    _source_file_size,
    _source_file_modified_at,
    _bronze_ingested_at,
    to_json(named_struct(
      'return_id', return_id,
      'original_transaction_id', original_transaction_id,
      'original_basket_id', original_basket_id,
      'return_date', return_date,
      'return_time', return_time,
      'store_id', store_id,
      'customer_id', customer_id,
      'product_id', product_id,
      'original_quantity', original_quantity,
      'return_quantity', return_quantity,
      'original_unit_price_eur', original_unit_price_eur,
      'original_discount_pct', original_discount_pct,
      'net_unit_price_eur', net_unit_price_eur,
      'refund_amount_eur', refund_amount_eur,
      'reason_code', reason_code,
      'cashier_id', cashier_id,
      'ingestion_date', ingestion_date
    )) AS _raw_record_json
  FROM STREAM(fact_returns_raw)
), classified AS (
  SELECT
    *,
    concat_ws('|',
      CASE WHEN _rescued_data IS NOT NULL THEN 'ERR:RESCUED_DATA' END,
      CASE WHEN return_id IS NULL OR return_id = '' THEN 'ERR:MISSING_RETURN_ID' END,
      CASE WHEN original_transaction_id IS NULL OR original_transaction_id = '' THEN 'ERR:MISSING_ORIGINAL_TRANSACTION_ID' END,
      CASE WHEN original_basket_id IS NULL OR original_basket_id = '' THEN 'ERR:MISSING_ORIGINAL_BASKET_ID' END,
      CASE WHEN return_date IS NULL THEN 'ERR:INVALID_RETURN_DATE' END,
      CASE WHEN return_timestamp IS NULL THEN 'ERR:INVALID_RETURN_TIME' END,
      CASE WHEN store_id IS NULL OR store_id = '' THEN 'ERR:MISSING_STORE_ID' END,
      CASE WHEN product_id IS NULL OR product_id = '' THEN 'ERR:MISSING_PRODUCT_ID' END,
      CASE WHEN original_quantity IS NULL THEN 'ERR:INVALID_ORIGINAL_QUANTITY' END,
      CASE WHEN return_quantity IS NULL THEN 'ERR:INVALID_RETURN_QUANTITY' END,
      CASE WHEN original_unit_price_eur IS NULL THEN 'ERR:INVALID_ORIGINAL_UNIT_PRICE' END,
      CASE WHEN original_discount_pct IS NULL THEN 'ERR:INVALID_ORIGINAL_DISCOUNT' END,
      CASE WHEN net_unit_price_eur IS NULL THEN 'ERR:INVALID_NET_UNIT_PRICE' END,
      CASE WHEN refund_amount_eur IS NULL THEN 'ERR:INVALID_REFUND_AMOUNT' END,
      CASE WHEN reason_code NOT IN ('Changed_Mind','Damaged','Wrong_Item','Defective','Expired') THEN 'ERR:INVALID_RETURN_REASON' END,
      CASE WHEN ingestion_date IS NULL THEN 'ERR:INVALID_INGESTION_DATE' END
    ) AS hard_error_codes,
    concat_ws('|',
      CASE WHEN original_quantity <= 0 THEN 'WARN:NON_POSITIVE_ORIGINAL_QUANTITY' END,
      CASE WHEN return_quantity <= 0 THEN 'WARN:NON_POSITIVE_RETURN_QUANTITY' END,
      CASE WHEN return_quantity > original_quantity THEN 'WARN:RETURN_EXCEEDS_ORIGINAL_LINE' END,
      CASE WHEN original_unit_price_eur < 0 OR net_unit_price_eur < 0 THEN 'WARN:NEGATIVE_RETURN_PRICE' END,
      CASE WHEN original_discount_pct < 0 OR original_discount_pct > 100 THEN 'WARN:RETURN_DISCOUNT_OUT_OF_RANGE' END,
      CASE WHEN refund_amount_eur < 0 THEN 'WARN:NEGATIVE_REFUND' END,
      CASE
        WHEN return_quantity IS NOT NULL
         AND net_unit_price_eur IS NOT NULL
         AND refund_amount_eur IS NOT NULL
         AND abs(refund_amount_eur - round(net_unit_price_eur * return_quantity, 2)) > 0.02
        THEN 'WARN:REFUND_CALCULATION_MISMATCH'
      END,
      CASE WHEN ingestion_date > return_date THEN 'INFO:LATE_ARRIVAL' END,
      CASE WHEN dayofweek(return_date) = 1 THEN 'WARN:SUNDAY_RETURN' END
    ) AS warning_codes
  FROM typed
)
SELECT
  *,
  sha2(concat_ws('||',
    coalesce(return_id, ''),
    coalesce(original_basket_id, ''),
    coalesce(product_id, ''),
    coalesce(_source_file_path, ''),
    coalesce(_raw_record_json, '')
  ), 256) AS _bronze_record_fingerprint,
  CASE
    WHEN hard_error_codes <> '' THEN 'QUARANTINED'
    WHEN warning_codes <> '' THEN 'ACCEPTED_WITH_WARNING'
    ELSE 'ACCEPTED'
  END AS bronze_record_status,
  current_timestamp() AS _bronze_processed_at
FROM classified;

CREATE OR REFRESH STREAMING TABLE fact_returns
COMMENT 'Typed Bronze return events accepted for Silver validation against original basket-product purchases.'
CLUSTER BY AUTO
AS
SELECT
  return_id,
  original_transaction_id,
  original_basket_id,
  return_date,
  return_time,
  return_timestamp,
  store_id,
  customer_id,
  product_id,
  original_quantity,
  return_quantity,
  original_unit_price_eur,
  original_discount_pct,
  net_unit_price_eur,
  refund_amount_eur,
  reason_code,
  cashier_id,
  ingestion_date,
  warning_codes AS bronze_warning_codes,
  bronze_record_status,
  _bronze_record_fingerprint,
  _source_file_path,
  _source_file_name,
  _source_file_size,
  _source_file_modified_at,
  _bronze_ingested_at,
  _bronze_processed_at
FROM STREAM(fact_returns_parsed)
WHERE hard_error_codes = '';

CREATE OR REFRESH STREAMING TABLE fact_returns_quarantine
COMMENT 'Return events rejected because of structural, parsing, or enum errors. Original values are retained as JSON.'
CLUSTER BY AUTO
AS
SELECT
  _bronze_record_fingerprint,
  return_id,
  original_transaction_id,
  original_basket_id,
  product_id,
  hard_error_codes AS quarantine_reasons,
  warning_codes AS additional_warnings,
  _rescued_data,
  _raw_record_json,
  _source_file_path,
  _source_file_name,
  _source_file_size,
  _source_file_modified_at,
  _bronze_ingested_at,
  _bronze_processed_at,
  current_timestamp() AS quarantined_at,
  'PENDING' AS repair_status
FROM STREAM(fact_returns_parsed)
WHERE hard_error_codes <> '';

-- ---------------------------------------------------------------------------
-- 3. STORE MASTER — current snapshot
-- ---------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW dim_stores_raw
COMMENT 'Source-fidelity store master snapshot.'
AS
SELECT
  store_id,
  city,
  district,
  postal_code,
  street,
  region,
  country_code,
  country_name,
  size_class,
  terminal_count,
  source_system,
  opening_hours,
  currency,
  _rescued_data,
  _metadata.file_path AS _source_file_path,
  _metadata.file_name AS _source_file_name,
  _metadata.file_modification_time AS _source_file_modified_at,
  current_timestamp() AS _bronze_ingested_at
FROM read_files(
  '/Volumes/workspace/retail_dev_raw/retail_input/dimensions/dim_stores.csv',
  format => 'csv',
  header => 'true',
  quote => '"',
  escape => '"',
  mode => 'FAILFAST',
  rescuedDataColumn => '_rescued_data',
  schemaEvolutionMode => 'rescue',
  schema => '
    store_id STRING,
    city STRING,
    district STRING,
    postal_code STRING,
    street STRING,
    region STRING,
    country_code STRING,
    country_name STRING,
    size_class STRING,
    terminal_count STRING,
    source_system STRING,
    opening_hours STRING,
    currency STRING
  '
);

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW dim_stores_parsed AS

WITH cleaned AS (
    SELECT
        trim(store_id) AS store_id,
        trim(city) AS city,
        trim(district) AS district,
        trim(postal_code) AS postal_code,
        trim(street) AS street,
        trim(region) AS region,
        upper(trim(country_code)) AS country_code,
        trim(country_name) AS country_name,
        upper(trim(size_class)) AS size_class,

        trim(terminal_count) AS terminal_count_raw,
        try_cast(trim(terminal_count) AS INT) AS terminal_count,

        trim(source_system) AS source_system,
        trim(opening_hours) AS opening_hours,
        upper(trim(currency)) AS currency,

        _rescued_data,
        _source_file_path,
        _source_file_name,
        _source_file_modified_at,
        _bronze_ingested_at
    FROM dim_stores_raw
),

validated AS (
    SELECT
        *,

        concat_ws(
            '|',

            CASE
                WHEN store_id IS NULL OR store_id = ''
                THEN 'MISSING_STORE_ID'
            END,

            CASE
                WHEN city IS NULL OR city = ''
                THEN 'MISSING_CITY'
            END,

            CASE
                WHEN country_code IS NULL
                  OR country_code NOT RLIKE '^[A-Z]{2}$'
                THEN 'INVALID_COUNTRY_CODE'
            END,

            CASE
                WHEN terminal_count_raw IS NULL
                  OR terminal_count_raw = ''
                THEN 'MISSING_TERMINAL_COUNT'

                WHEN terminal_count IS NULL
                THEN 'INVALID_TERMINAL_COUNT'

                WHEN terminal_count <= 0
                THEN 'NON_POSITIVE_TERMINAL_COUNT'
            END,

            CASE
                WHEN opening_hours IS NULL
                  OR opening_hours = ''
                THEN 'MISSING_OPENING_HOURS'

                WHEN try_parse_json(opening_hours) IS NULL
                THEN 'INVALID_OPENING_HOURS_JSON'
            END,

            CASE
                WHEN currency IS NULL
                  OR currency NOT RLIKE '^[A-Z]{3}$'
                THEN 'INVALID_CURRENCY'
            END,

            CASE
                WHEN _rescued_data IS NOT NULL
                  AND trim(_rescued_data) <> ''
                THEN 'RESCUED_DATA_PRESENT'
            END
        ) AS hard_error_codes

    FROM cleaned
)

SELECT
    store_id,
    city,
    district,
    postal_code,
    street,
    region,
    country_code,
    country_name,
    size_class,
    terminal_count,
    source_system,
    opening_hours,
    currency,
    hard_error_codes,
    _rescued_data,
    _source_file_path,
    _source_file_name,
    _source_file_modified_at,
    _bronze_ingested_at
FROM validated;

CREATE OR REFRESH MATERIALIZED VIEW dim_stores
COMMENT 'Typed store master records accepted by Bronze.'
AS
SELECT * EXCEPT (hard_error_codes, _rescued_data)
FROM dim_stores_parsed
WHERE hard_error_codes = '';

CREATE OR REFRESH MATERIALIZED VIEW dim_stores_quarantine
COMMENT 'Invalid store master records.'
AS
SELECT
  *,
  hard_error_codes AS quarantine_reasons
FROM dim_stores_parsed
WHERE COALESCE(TRIM(hard_error_codes), '') <> '';

-- ---------------------------------------------------------------------------
-- 4. CUSTOMER MASTER — current snapshot
-- ---------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW dim_customers_raw
COMMENT 'Source-fidelity customer master snapshot.'
AS
SELECT
  customer_id,
  age,
  gender_code,
  is_member,
  loyalty_card_id,
  _rescued_data,
  _metadata.file_path AS _source_file_path,
  _metadata.file_name AS _source_file_name,
  _metadata.file_modification_time AS _source_file_modified_at,
  current_timestamp() AS _bronze_ingested_at
FROM read_files(
  '/Volumes/workspace/retail_dev_raw/retail_input/dimensions/dim_customers.csv',
  format => 'csv',
  header => 'true',
  mode => 'PERMISSIVE',
  rescuedDataColumn => '_rescued_data',
  schemaEvolutionMode => 'rescue',
  schema => '
    customer_id STRING,
    age STRING,
    gender_code STRING,
    is_member STRING,
    loyalty_card_id STRING
  '
);

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW dim_customers_parsed AS
SELECT
  trim(customer_id) AS customer_id,
  try_cast(trim(age) AS INT) AS age,
  nullif(trim(gender_code), '') AS gender_code,
  CASE
    WHEN lower(trim(is_member)) IN ('true','1') THEN TRUE
    WHEN lower(trim(is_member)) IN ('false','0') THEN FALSE
    ELSE NULL
  END AS is_member,
  nullif(trim(loyalty_card_id), '') AS loyalty_card_id,
  concat_ws('|',
    CASE WHEN _rescued_data IS NOT NULL THEN 'ERR:RESCUED_DATA' END,
    CASE WHEN customer_id IS NULL OR trim(customer_id) = '' THEN 'ERR:MISSING_CUSTOMER_ID' END,
    CASE WHEN try_cast(trim(age) AS INT) IS NULL THEN 'ERR:INVALID_AGE' END,
    CASE WHEN gender_code IS NOT NULL AND trim(gender_code) NOT IN ('M','F','D','U') THEN 'ERR:INVALID_GENDER_CODE' END,
    CASE WHEN lower(trim(is_member)) NOT IN ('true','false','1','0') THEN 'ERR:INVALID_MEMBER_FLAG' END,
    CASE WHEN lower(trim(is_member)) IN ('true','1') AND nullif(trim(loyalty_card_id), '') IS NULL THEN 'ERR:MISSING_LOYALTY_CARD' END
  ) AS hard_error_codes,
  concat_ws('|',
    CASE WHEN try_cast(trim(age) AS INT) < 0 OR try_cast(trim(age) AS INT) > 120 THEN 'WARN:AGE_OUT_OF_RANGE' END,
    CASE WHEN lower(trim(is_member)) IN ('false','0') AND nullif(trim(loyalty_card_id), '') IS NOT NULL THEN 'WARN:CARD_FOR_NON_MEMBER' END
  ) AS warning_codes,
  _rescued_data,
  _source_file_path,
  _source_file_name,
  _source_file_modified_at,
  _bronze_ingested_at
FROM dim_customers_raw;

CREATE OR REFRESH MATERIALIZED VIEW dim_customers
COMMENT 'Typed customer master records accepted by Bronze; out-of-range ages remain flagged for Silver review.'
AS
SELECT * EXCEPT (hard_error_codes, _rescued_data)
FROM dim_customers_parsed
WHERE hard_error_codes = '';

CREATE OR REFRESH MATERIALIZED VIEW dim_customers_quarantine
COMMENT 'Invalid customer master records.'
AS
SELECT *, hard_error_codes AS quarantine_reasons
FROM dim_customers_parsed
WHERE hard_error_codes <> '';

-- ---------------------------------------------------------------------------
-- 5. PRODUCT CATALOGUE — current snapshot
-- ---------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW dim_products_raw
COMMENT 'Source-fidelity product catalogue snapshot.'
AS
SELECT
  product_id,
  product_name,
  category,
  subcategory,
  default_brand,
  is_private_label_eligible,
  price_min_eur,
  price_max_eur,
  unit,
  seasonal_months,
  vat_rate,
  _rescued_data,
  _metadata.file_path AS _source_file_path,
  _metadata.file_name AS _source_file_name,
  _metadata.file_modification_time AS _source_file_modified_at,
  current_timestamp() AS _bronze_ingested_at
FROM read_files(
  '/Volumes/workspace/retail_dev_raw/retail_input/dimensions/dim_products.csv',
  format => 'csv',
  header => 'true',
  mode => 'PERMISSIVE',
  rescuedDataColumn => '_rescued_data',
  schemaEvolutionMode => 'rescue',
  schema => '
    product_id STRING,
    product_name STRING,
    category STRING,
    subcategory STRING,
    default_brand STRING,
    is_private_label_eligible STRING,
    price_min_eur STRING,
    price_max_eur STRING,
    unit STRING,
    seasonal_months STRING,
    vat_rate STRING
  '
);

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW dim_products_parsed AS
SELECT
  trim(product_id) AS product_id,
  trim(product_name) AS product_name,
  trim(category) AS category,
  trim(subcategory) AS subcategory,
  trim(default_brand) AS default_brand,
  CASE
    WHEN lower(trim(is_private_label_eligible)) IN ('true','1') THEN TRUE
    WHEN lower(trim(is_private_label_eligible)) IN ('false','0') THEN FALSE
    ELSE NULL
  END AS is_private_label_eligible,
  try_cast(trim(price_min_eur) AS DECIMAL(10,2)) AS price_min_eur,
  try_cast(trim(price_max_eur) AS DECIMAL(10,2)) AS price_max_eur,
  trim(unit) AS unit,
  nullif(trim(seasonal_months), '') AS seasonal_months,
  try_cast(trim(vat_rate) AS DECIMAL(5,4)) AS vat_rate,
  concat_ws('|',
    CASE WHEN _rescued_data IS NOT NULL THEN 'ERR:RESCUED_DATA' END,
    CASE WHEN product_id IS NULL OR trim(product_id) = '' THEN 'ERR:MISSING_PRODUCT_ID' END,
    CASE WHEN product_name IS NULL OR trim(product_name) = '' THEN 'ERR:MISSING_PRODUCT_NAME' END,
    CASE WHEN category IS NULL OR trim(category) = '' THEN 'ERR:MISSING_CATEGORY' END,
    CASE WHEN subcategory IS NULL OR trim(subcategory) = '' THEN 'ERR:MISSING_SUBCATEGORY' END,
    CASE WHEN default_brand IS NULL OR trim(default_brand) = '' THEN 'ERR:MISSING_DEFAULT_BRAND' END,
    CASE WHEN lower(trim(is_private_label_eligible)) NOT IN ('true','false','1','0') THEN 'ERR:INVALID_PRIVATE_LABEL_ELIGIBILITY' END,
    CASE WHEN try_cast(trim(price_min_eur) AS DECIMAL(10,2)) IS NULL THEN 'ERR:INVALID_MIN_PRICE' END,
    CASE WHEN try_cast(trim(price_max_eur) AS DECIMAL(10,2)) IS NULL THEN 'ERR:INVALID_MAX_PRICE' END,
    CASE WHEN try_cast(trim(price_min_eur) AS DECIMAL(10,2)) < 0 THEN 'ERR:NEGATIVE_MIN_PRICE' END,
    CASE WHEN try_cast(trim(price_max_eur) AS DECIMAL(10,2)) < try_cast(trim(price_min_eur) AS DECIMAL(10,2)) THEN 'ERR:PRICE_RANGE_REVERSED' END,
    CASE WHEN unit IS NULL OR trim(unit) = '' THEN 'ERR:MISSING_UNIT' END,
    CASE WHEN try_cast(trim(vat_rate) AS DECIMAL(5,4)) NOT IN (0.0700, 0.1900) THEN 'ERR:INVALID_VAT_RATE' END
  ) AS hard_error_codes,
  _rescued_data,
  _source_file_path,
  _source_file_name,
  _source_file_modified_at,
  _bronze_ingested_at
FROM dim_products_raw;

CREATE OR REFRESH MATERIALIZED VIEW dim_products
COMMENT 'Typed product catalogue records accepted by Bronze.'
AS
SELECT * EXCEPT (hard_error_codes, _rescued_data)
FROM dim_products_parsed
WHERE hard_error_codes = '';

CREATE OR REFRESH MATERIALIZED VIEW dim_products_quarantine
COMMENT 'Invalid product catalogue records.'
AS
SELECT *, hard_error_codes AS quarantine_reasons
FROM dim_products_parsed
WHERE hard_error_codes <> '';

-- ---------------------------------------------------------------------------
-- 6. PRODUCT PRICE HISTORY — SCD2 source snapshot
-- ---------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW dim_products_scd2_raw
COMMENT 'Source-fidelity product SCD2 price history.'
AS
SELECT
  product_id,
  product_name,
  category,
  subcategory,
  default_brand,
  effective_price_eur,
  effective_from,
  effective_to,
  is_promo_price,
  unit,
  vat_rate,
  _rescued_data,
  _metadata.file_path AS _source_file_path,
  _metadata.file_name AS _source_file_name,
  _metadata.file_modification_time AS _source_file_modified_at,
  current_timestamp() AS _bronze_ingested_at
FROM read_files(
  '/Volumes/workspace/retail_dev_raw/retail_input/dimensions/dim_products_scd2.csv',
  format => 'csv',
  header => 'true',
  mode => 'PERMISSIVE',
  rescuedDataColumn => '_rescued_data',
  schemaEvolutionMode => 'rescue',
  schema => '
    product_id STRING,
    product_name STRING,
    category STRING,
    subcategory STRING,
    default_brand STRING,
    effective_price_eur STRING,
    effective_from STRING,
    effective_to STRING,
    is_promo_price STRING,
    unit STRING,
    vat_rate STRING
  '
);

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW dim_products_scd2_parsed AS
SELECT
  trim(product_id) AS product_id,
  trim(product_name) AS product_name,
  trim(category) AS category,
  trim(subcategory) AS subcategory,
  trim(default_brand) AS default_brand,
  try_cast(trim(effective_price_eur) AS DECIMAL(10,2)) AS effective_price_eur,
  try_cast(trim(effective_from) AS DATE) AS effective_from,
  try_cast(trim(effective_to) AS DATE) AS effective_to,
  CASE
    WHEN lower(trim(is_promo_price)) IN ('true','1') THEN TRUE
    WHEN lower(trim(is_promo_price)) IN ('false','0') THEN FALSE
    ELSE NULL
  END AS is_promo_price,
  trim(unit) AS unit,
  try_cast(trim(vat_rate) AS DECIMAL(5,4)) AS vat_rate,
  concat_ws('|',
    CASE WHEN _rescued_data IS NOT NULL THEN 'ERR:RESCUED_DATA' END,
    CASE WHEN product_id IS NULL OR trim(product_id) = '' THEN 'ERR:MISSING_PRODUCT_ID' END,
    CASE WHEN product_name IS NULL OR trim(product_name) = '' THEN 'ERR:MISSING_PRODUCT_NAME' END,
    CASE WHEN category IS NULL OR trim(category) = '' THEN 'ERR:MISSING_CATEGORY' END,
    CASE WHEN subcategory IS NULL OR trim(subcategory) = '' THEN 'ERR:MISSING_SUBCATEGORY' END,
    CASE WHEN default_brand IS NULL OR trim(default_brand) = '' THEN 'ERR:MISSING_DEFAULT_BRAND' END,
    CASE WHEN try_cast(trim(effective_price_eur) AS DECIMAL(10,2)) IS NULL OR try_cast(trim(effective_price_eur) AS DECIMAL(10,2)) <= 0 THEN 'ERR:INVALID_EFFECTIVE_PRICE' END,
    CASE WHEN try_cast(trim(effective_from) AS DATE) IS NULL THEN 'ERR:INVALID_EFFECTIVE_FROM' END,
    CASE WHEN try_cast(trim(effective_to) AS DATE) IS NULL THEN 'ERR:INVALID_EFFECTIVE_TO' END,
    CASE WHEN try_cast(trim(effective_to) AS DATE) < try_cast(trim(effective_from) AS DATE) THEN 'ERR:INVALID_EFFECTIVE_RANGE' END,
    CASE WHEN lower(trim(is_promo_price)) NOT IN ('true','false','1','0') THEN 'ERR:INVALID_PROMO_PRICE_FLAG' END,
    CASE WHEN unit IS NULL OR trim(unit) = '' THEN 'ERR:MISSING_UNIT' END,
    CASE WHEN try_cast(trim(vat_rate) AS DECIMAL(5,4)) NOT IN (0.0700, 0.1900) THEN 'ERR:INVALID_VAT_RATE' END
  ) AS hard_error_codes,
  _rescued_data,
  _source_file_path,
  _source_file_name,
  _source_file_modified_at,
  _bronze_ingested_at
FROM dim_products_scd2_raw;

CREATE OR REFRESH MATERIALIZED VIEW dim_products_scd2
COMMENT 'Typed product price-history records accepted by Bronze. SCD continuity and overlap checks belong to Silver validation.'
AS
SELECT * EXCEPT (hard_error_codes, _rescued_data)
FROM dim_products_scd2_parsed
WHERE hard_error_codes = '';

CREATE OR REFRESH MATERIALIZED VIEW dim_products_scd2_quarantine
COMMENT 'Invalid product SCD2 price-history records.'
AS
SELECT *, hard_error_codes AS quarantine_reasons
FROM dim_products_scd2_parsed
WHERE hard_error_codes <> '';

-- ---------------------------------------------------------------------------
-- 7. BRONZE OPERATIONAL QUALITY SUMMARY
-- ---------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW bronze_quality_summary
COMMENT 'Current Bronze row counts and quarantine rates for operational monitoring.'
AS
SELECT
  'fact_transactions' AS dataset_name,
  (SELECT count(*) FROM fact_transactions_raw) AS raw_rows,
  (SELECT count(*) FROM fact_transactions) AS accepted_rows,
  (SELECT count(*) FROM fact_transactions_quarantine) AS quarantined_rows,
  (SELECT count(*) FROM fact_transactions WHERE bronze_record_status = 'ACCEPTED_WITH_WARNING') AS warning_rows,
  round(
    100.0 * (SELECT count(*) FROM fact_transactions_quarantine)
    / nullif((SELECT count(*) FROM fact_transactions_raw), 0),
    4
  ) AS quarantine_rate_pct,
  current_timestamp() AS measured_at
UNION ALL
SELECT
  'fact_returns',
  (SELECT count(*) FROM fact_returns_raw),
  (SELECT count(*) FROM fact_returns),
  (SELECT count(*) FROM fact_returns_quarantine),
  (SELECT count(*) FROM fact_returns WHERE bronze_record_status = 'ACCEPTED_WITH_WARNING'),
  round(
    100.0 * (SELECT count(*) FROM fact_returns_quarantine)
    / nullif((SELECT count(*) FROM fact_returns_raw), 0),
    4
  ),
  current_timestamp()
UNION ALL
SELECT
  'dim_stores',
  (SELECT count(*) FROM dim_stores_raw),
  (SELECT count(*) FROM dim_stores),
  (SELECT count(*) FROM dim_stores_quarantine),
  CAST(0 AS BIGINT),
  round(100.0 * (SELECT count(*) FROM dim_stores_quarantine) / nullif((SELECT count(*) FROM dim_stores_raw), 0), 4),
  current_timestamp()
UNION ALL
SELECT
  'dim_customers',
  (SELECT count(*) FROM dim_customers_raw),
  (SELECT count(*) FROM dim_customers),
  (SELECT count(*) FROM dim_customers_quarantine),
  (SELECT count(*) FROM dim_customers WHERE warning_codes <> ''),
  round(100.0 * (SELECT count(*) FROM dim_customers_quarantine) / nullif((SELECT count(*) FROM dim_customers_raw), 0), 4),
  current_timestamp()
UNION ALL
SELECT
  'dim_products',
  (SELECT count(*) FROM dim_products_raw),
  (SELECT count(*) FROM dim_products),
  (SELECT count(*) FROM dim_products_quarantine),
  CAST(0 AS BIGINT),
  round(100.0 * (SELECT count(*) FROM dim_products_quarantine) / nullif((SELECT count(*) FROM dim_products_raw), 0), 4),
  current_timestamp()
UNION ALL
SELECT
  'dim_products_scd2',
  (SELECT count(*) FROM dim_products_scd2_raw),
  (SELECT count(*) FROM dim_products_scd2),
  (SELECT count(*) FROM dim_products_scd2_quarantine),
  CAST(0 AS BIGINT),
  round(100.0 * (SELECT count(*) FROM dim_products_scd2_quarantine) / nullif((SELECT count(*) FROM dim_products_scd2_raw), 0), 4),
  current_timestamp();