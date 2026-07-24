-- ============================================================================
-- Einkaufpark Retail Platform — Silver Dimensions
-- Databricks Lakeflow Spark Declarative Pipeline (SQL)
--
-- Required pipeline parameters:
--   :bronze_catalog
--   :bronze_schema
--   :silver_catalog
--   :silver_schema
--
-- This file creates conformed, one-row-per-key Silver dimensions and review
-- datasets. Exact duplicate snapshot rows are collapsed. Conflicting master
-- records are preserved in review tables instead of being selected silently.
-- ============================================================================

-- Publish all unqualified datasets in the parameterized Silver target.
-- The USE statements are scoped to this source file.
USE CATALOG workspace;
USE SCHEMA retail_dev_silver;

-- ---------------------------------------------------------------------------
-- 1. STORE DIMENSION
-- ---------------------------------------------------------------------------

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW store_source_profile
AS
WITH source_rows AS (
  SELECT
    b.*,
    sha2(concat_ws('||',
      coalesce(city, ''),
      coalesce(district, ''),
      coalesce(postal_code, ''),
      coalesce(street, ''),
      coalesce(region, ''),
      coalesce(country_code, ''),
      coalesce(country_name, ''),
      coalesce(size_class, ''),
      coalesce(cast(terminal_count AS STRING), ''),
      coalesce(source_system, ''),
      coalesce(opening_hours, ''),
      coalesce(currency, '')
    ), 256) AS profile_hash
  FROM workspace.retail_dev_bronze.dim_stores b
), stats AS (
  SELECT
    store_id,
    count(*) AS source_row_count,
    count(DISTINCT profile_hash) AS distinct_profile_count
  FROM source_rows
  GROUP BY store_id
), ranked AS (
  SELECT
    r.*,
    s.source_row_count,
    s.distinct_profile_count,
    row_number() OVER (
      PARTITION BY r.store_id
      ORDER BY r._source_file_modified_at DESC, r._bronze_ingested_at DESC, r.profile_hash
    ) AS profile_rank
  FROM source_rows r
  JOIN stats s USING (store_id)
)
SELECT *
FROM ranked;

CREATE OR REFRESH MATERIALIZED VIEW dim_store_review
COMMENT 'Store master keys with conflicting source profiles. These stores are excluded from the trusted store dimension.'
AS
SELECT
  store_id,
  source_row_count,
  distinct_profile_count,
  collect_set(profile_hash) AS conflicting_profile_hashes,
  'CONFLICTING_STORE_PROFILE' AS review_reason,
  max(_source_file_modified_at) AS latest_source_file_modified_at
FROM store_source_profile
WHERE distinct_profile_count > 1
GROUP BY store_id, source_row_count, distinct_profile_count;

CREATE OR REFRESH MATERIALIZED VIEW dim_store
COMMENT 'Conformed store dimension with exactly one row per trusted store_id.'
AS
SELECT
  sha2(store_id, 256) AS store_sk,
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
  source_row_count,
  _source_file_modified_at,
  _bronze_ingested_at
FROM store_source_profile
WHERE profile_rank = 1
  AND distinct_profile_count = 1;

-- ---------------------------------------------------------------------------
-- 2. CUSTOMER DIMENSION
-- Missing age is valid and becomes Unknown. Out-of-range age is sanitized to
-- NULL but remains visible in dim_customer_review. Conflicting source profiles
-- are excluded because no deterministic profile can be trusted.
-- ---------------------------------------------------------------------------

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW customer_source_profile
AS
WITH source_rows AS (
  SELECT
    b.*,
    CASE
      WHEN lower(trim(gender_code)) = 'm' THEN 'M'
      WHEN lower(trim(gender_code)) = 'f' THEN 'F'
      WHEN lower(trim(gender_code)) IN ('d', 'divers') THEN 'D'
      WHEN lower(trim(gender_code)) = 'u' OR gender_code IS NULL THEN 'U'
      ELSE 'U'
    END AS normalized_gender_code,
    sha2(concat_ws('||',
      coalesce(cast(age AS STRING), ''),
      coalesce(lower(trim(gender_code)), ''),
      coalesce(cast(is_member AS STRING), ''),
      coalesce(loyalty_card_id, '')
    ), 256) AS profile_hash
  FROM workspace.retail_dev_bronze.dim_customers b
), stats AS (
  SELECT
    customer_id,
    count(*) AS source_row_count,
    count(DISTINCT profile_hash) AS distinct_profile_count
  FROM source_rows
  GROUP BY customer_id
), loyalty_counts AS (
  SELECT
    loyalty_card_id,
    count(DISTINCT customer_id) AS loyalty_card_customer_count
  FROM source_rows
  WHERE loyalty_card_id IS NOT NULL
  GROUP BY loyalty_card_id
), ranked AS (
  SELECT
    r.*,
    s.source_row_count,
    s.distinct_profile_count,
    coalesce(l.loyalty_card_customer_count, 0) AS loyalty_card_customer_count,
    row_number() OVER (
      PARTITION BY r.customer_id
      ORDER BY r._source_file_modified_at DESC, r._bronze_ingested_at DESC, r.profile_hash
    ) AS profile_rank
  FROM source_rows r
  JOIN stats s USING (customer_id)
  LEFT JOIN loyalty_counts l USING (loyalty_card_id)
)
SELECT *
FROM ranked;

CREATE OR REFRESH MATERIALIZED VIEW dim_customer_review
COMMENT 'Customer demographic and identity issues retained for audit without discarding otherwise usable identities.'
AS
SELECT
  customer_id,
  age AS source_age,
  normalized_gender_code,
  is_member,
  loyalty_card_id,
  source_row_count,
  distinct_profile_count,
  loyalty_card_customer_count,
  concat_ws('|',
    CASE WHEN distinct_profile_count > 1 THEN 'CONFLICTING_CUSTOMER_PROFILE' END,
    CASE WHEN age IS NULL THEN 'AGE_MISSING' END,
    CASE WHEN age IS NOT NULL AND (age < 18 OR age > 100) THEN 'AGE_OUT_OF_ANALYTICAL_RANGE' END,
    CASE WHEN is_member AND loyalty_card_id IS NULL THEN 'MEMBER_WITHOUT_LOYALTY_CARD' END,
    CASE WHEN NOT is_member AND loyalty_card_id IS NOT NULL THEN 'NON_MEMBER_WITH_LOYALTY_CARD' END,
    CASE WHEN loyalty_card_id IS NOT NULL AND loyalty_card_customer_count > 1 THEN 'DUPLICATE_LOYALTY_CARD' END,
    CASE WHEN warning_codes <> '' THEN warning_codes END
  ) AS review_reasons,
  _source_file_modified_at,
  _bronze_ingested_at
FROM customer_source_profile
WHERE profile_rank = 1
  AND (
    distinct_profile_count > 1
    OR age IS NULL
    OR age < 18
    OR age > 100
    OR (is_member AND loyalty_card_id IS NULL)
    OR (NOT is_member AND loyalty_card_id IS NOT NULL)
    OR (loyalty_card_id IS NOT NULL AND loyalty_card_customer_count > 1)
    OR warning_codes <> ''
  );

CREATE OR REFRESH MATERIALIZED VIEW dim_customer
COMMENT 'Conformed customer dimension. Missing or invalid age is represented as Unknown rather than causing customer loss.'
AS
SELECT
  sha2(customer_id, 256) AS customer_sk,
  customer_id,
  CASE WHEN age BETWEEN 18 AND 100 THEN age END AS age,
  CASE
    WHEN age IS NULL OR age < 18 OR age > 100 THEN 'Unknown'
    WHEN age BETWEEN 18 AND 24 THEN '18-24'
    WHEN age BETWEEN 25 AND 34 THEN '25-34'
    WHEN age BETWEEN 35 AND 49 THEN '35-49'
    WHEN age BETWEEN 50 AND 64 THEN '50-64'
    ELSE '65+'
  END AS age_group,
  normalized_gender_code AS gender_code,
  is_member,
  loyalty_card_id,
  CASE
    WHEN age IS NULL THEN 'MISSING'
    WHEN age < 18 OR age > 100 THEN 'INVALID'
    ELSE 'VALID'
  END AS age_quality_status,
  CASE
    WHEN loyalty_card_id IS NULL THEN TRUE
    WHEN loyalty_card_customer_count = 1 THEN TRUE
    ELSE FALSE
  END AS is_loyalty_card_unique,
  source_row_count,
  _source_file_modified_at,
  _bronze_ingested_at
FROM customer_source_profile
WHERE profile_rank = 1
  AND distinct_profile_count = 1;

-- ---------------------------------------------------------------------------
-- 3. PRODUCT DIMENSION
-- The generator historically represented unpackaged products with brand
-- 'bulk' in the catalogue and 'EKP-Classic' in transaction events. Silver
-- normalizes that representation once in the conformed product dimension.
-- ---------------------------------------------------------------------------

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW product_source_profile
AS
WITH source_rows AS (
  SELECT
    b.*,
    CASE WHEN lower(default_brand) = 'bulk' THEN 'EKP-Classic' ELSE default_brand END AS normalized_default_brand,
    sha2(concat_ws('||',
      coalesce(product_name, ''),
      coalesce(category, ''),
      coalesce(subcategory, ''),
      coalesce(CASE WHEN lower(default_brand) = 'bulk' THEN 'EKP-Classic' ELSE default_brand END, ''),
      coalesce(cast(is_private_label_eligible AS STRING), ''),
      coalesce(cast(price_min_eur AS STRING), ''),
      coalesce(cast(price_max_eur AS STRING), ''),
      coalesce(unit, ''),
      coalesce(seasonal_months, ''),
      coalesce(cast(vat_rate AS STRING), '')
    ), 256) AS profile_hash
  FROM workspace.retail_dev_bronze.dim_products b
), stats AS (
  SELECT
    product_id,
    count(*) AS source_row_count,
    count(DISTINCT profile_hash) AS distinct_profile_count
  FROM source_rows
  GROUP BY product_id
), ranked AS (
  SELECT
    r.*,
    s.source_row_count,
    s.distinct_profile_count,
    row_number() OVER (
      PARTITION BY r.product_id
      ORDER BY r._source_file_modified_at DESC, r._bronze_ingested_at DESC, r.profile_hash
    ) AS profile_rank
  FROM source_rows r
  JOIN stats s USING (product_id)
)
SELECT *
FROM ranked;

CREATE OR REFRESH MATERIALIZED VIEW dim_product_review
COMMENT 'Product master keys with conflicting current profiles.'
AS
SELECT
  product_id,
  source_row_count,
  distinct_profile_count,
  collect_set(profile_hash) AS conflicting_profile_hashes,
  'CONFLICTING_PRODUCT_PROFILE' AS review_reason,
  max(_source_file_modified_at) AS latest_source_file_modified_at
FROM product_source_profile
WHERE distinct_profile_count > 1
GROUP BY product_id, source_row_count, distinct_profile_count;

CREATE OR REFRESH MATERIALIZED VIEW dim_product
COMMENT 'Conformed current product dimension with exactly one row per trusted product_id.'
AS
SELECT
  sha2(product_id, 256) AS product_sk,
  product_id,
  product_name,
  category,
  subcategory,
  normalized_default_brand AS default_brand,
  is_private_label_eligible,
  price_min_eur,
  price_max_eur,
  round((price_min_eur + price_max_eur) / 2, 2) AS catalogue_mid_price_eur,
  CASE
    WHEN (price_min_eur + price_max_eur) / 2 < 5 THEN 'Budget'
    WHEN (price_min_eur + price_max_eur) / 2 < 20 THEN 'Standard'
    ELSE 'Premium'
  END AS price_band,
  unit,
  seasonal_months,
  vat_rate,
  source_row_count,
  _source_file_modified_at,
  _bronze_ingested_at
FROM product_source_profile
WHERE profile_rank = 1
  AND distinct_profile_count = 1;

-- ---------------------------------------------------------------------------
-- 4. PRODUCT SCD2 PRICE DIMENSION
-- Source intervals are inclusive at both ends. Exact duplicates are collapsed.
-- Overlapping intervals are excluded from the trusted dimension. Gaps are
-- retained and flagged because a sale in a gap must be routed to review.
-- ---------------------------------------------------------------------------

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW product_scd2_source_ranked
AS
WITH normalized AS (
  SELECT
    b.*,
    CASE WHEN lower(default_brand) = 'bulk' THEN 'EKP-Classic' ELSE default_brand END AS normalized_default_brand,
    sha2(concat_ws('||',
      product_id,
      cast(effective_from AS STRING),
      cast(effective_to AS STRING),
      cast(effective_price_eur AS STRING),
      cast(is_promo_price AS STRING),
      coalesce(product_name, ''),
      coalesce(category, ''),
      coalesce(subcategory, ''),
      coalesce(CASE WHEN lower(default_brand) = 'bulk' THEN 'EKP-Classic' ELSE default_brand END, ''),
      coalesce(unit, ''),
      cast(vat_rate AS STRING)
    ), 256) AS price_version_hash
  FROM workspace.retail_dev_bronze.dim_products_scd2 b
), ranked AS (
  SELECT
    *,
    count(*) OVER (PARTITION BY price_version_hash) AS exact_duplicate_count,
    row_number() OVER (
      PARTITION BY price_version_hash
      ORDER BY _source_file_modified_at DESC, _bronze_ingested_at DESC
    ) AS exact_duplicate_rank
  FROM normalized
)
SELECT *
FROM ranked;

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW product_scd2_deduplicated
AS
SELECT *
FROM product_scd2_source_ranked
WHERE exact_duplicate_rank = 1;

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW product_scd2_overlap_versions
AS
SELECT DISTINCT price_version_hash
FROM (
  SELECT a.price_version_hash
  FROM product_scd2_deduplicated a
  JOIN product_scd2_deduplicated b
    ON a.product_id = b.product_id
   AND a.price_version_hash <> b.price_version_hash
   AND a.effective_from <= b.effective_to
   AND b.effective_from <= a.effective_to
) overlapping_versions;

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW product_scd2_sequenced
AS
SELECT
  p.*,

  lag(p.effective_to) OVER (
    PARTITION BY p.product_id
    ORDER BY
      p.effective_from,
      p.effective_to,
      p.price_version_hash
  ) AS previous_effective_to,

  CASE
    WHEN o.price_version_hash IS NOT NULL THEN TRUE
    ELSE FALSE
  END AS has_overlap,

  CASE
    WHEN lag(p.effective_to) OVER (
      PARTITION BY p.product_id
      ORDER BY
        p.effective_from,
        p.effective_to,
        p.price_version_hash
    ) IS NOT NULL

    AND p.effective_from > date_add(
      lag(p.effective_to) OVER (
        PARTITION BY p.product_id
        ORDER BY
          p.effective_from,
          p.effective_to,
          p.price_version_hash
      ),
      1
    )

    THEN TRUE
    ELSE FALSE
  END AS has_gap_before

FROM product_scd2_deduplicated p

LEFT JOIN product_scd2_overlap_versions o
  ON p.price_version_hash = o.price_version_hash;

CREATE OR REFRESH MATERIALIZED VIEW dim_product_scd2_review
COMMENT 'SCD2 duplicate, overlap, continuity, and product-metadata issues.'
AS
SELECT
  s.product_id,
  s.price_version_hash AS price_version_sk,
  s.effective_from,
  s.effective_to,
  s.effective_price_eur,
  s.is_promo_price,
  concat_ws('|',
    CASE WHEN s.exact_duplicate_count > 1 THEN 'EXACT_DUPLICATE_SCD2_ROW' END,
    CASE WHEN s.has_overlap THEN 'OVERLAPPING_SCD2_INTERVAL' END,
    CASE WHEN s.has_gap_before THEN 'SCD2_GAP_BEFORE_INTERVAL' END,
    CASE WHEN p.product_id IS NULL THEN 'PRODUCT_NOT_IN_CURRENT_MASTER' END,
    CASE WHEN p.product_id IS NOT NULL AND s.product_name <> p.product_name THEN 'PRODUCT_NAME_MISMATCH' END,
    CASE WHEN p.product_id IS NOT NULL AND s.category <> p.category THEN 'CATEGORY_MISMATCH' END,
    CASE WHEN p.product_id IS NOT NULL AND s.subcategory <> p.subcategory THEN 'SUBCATEGORY_MISMATCH' END,
    CASE WHEN p.product_id IS NOT NULL AND s.normalized_default_brand <> p.default_brand THEN 'BRAND_MISMATCH' END,
    CASE WHEN p.product_id IS NOT NULL AND s.unit <> p.unit THEN 'UNIT_MISMATCH' END,
    CASE WHEN p.product_id IS NOT NULL AND s.vat_rate <> p.vat_rate THEN 'VAT_RATE_MISMATCH' END
  ) AS review_reasons,
  s._source_file_modified_at,
  s._bronze_ingested_at
FROM product_scd2_sequenced s
LEFT JOIN dim_product p
  ON s.product_id = p.product_id
WHERE s.exact_duplicate_count > 1
   OR s.has_overlap
   OR s.has_gap_before
   OR p.product_id IS NULL
   OR s.product_name <> p.product_name
   OR s.category <> p.category
   OR s.subcategory <> p.subcategory
   OR s.normalized_default_brand <> p.default_brand
   OR s.unit <> p.unit
   OR s.vat_rate <> p.vat_rate;

CREATE OR REFRESH MATERIALIZED VIEW dim_product_scd2
COMMENT 'Trusted SCD2 product price dimension. Intervals are inclusive and overlapping versions are excluded.'
AS
SELECT
  s.price_version_hash AS price_version_sk,
  p.product_sk,
  s.product_id,
  s.product_name,
  s.category,
  s.subcategory,
  s.normalized_default_brand AS default_brand,
  s.effective_price_eur,
  s.effective_from,
  s.effective_to,
  s.is_promo_price,
  s.unit,
  s.vat_rate,
  s.has_gap_before,
  s.exact_duplicate_count,
  s._source_file_modified_at,
  s._bronze_ingested_at
FROM product_scd2_sequenced s
JOIN dim_product p
  ON s.product_id = p.product_id
WHERE NOT s.has_overlap
  AND s.product_name = p.product_name
  AND s.category = p.category
  AND s.subcategory = p.subcategory
  AND s.normalized_default_brand = p.default_brand
  AND s.unit = p.unit
  AND s.vat_rate = p.vat_rate;

-- ---------------------------------------------------------------------------
-- 5. TERMINAL DIMENSION DERIVED FROM OBSERVED TRANSACTIONS
-- A terminal is trusted only when it consistently belongs to one store and
-- has one terminal type/self-checkout configuration.
-- ---------------------------------------------------------------------------

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW terminal_source_profile
AS
WITH observed AS (
  SELECT DISTINCT
    pos_terminal_id AS terminal_id,
    store_id,
    terminal_type,
    is_self_checkout,
    source_system,
    _bronze_ingested_at
  FROM workspace.retail_dev_bronze.fact_transactions
), stats AS (
  SELECT
    terminal_id,
    count(DISTINCT store_id) AS store_count,
    count(DISTINCT terminal_type) AS terminal_type_count,
    count(DISTINCT is_self_checkout) AS checkout_flag_count
  FROM observed
  GROUP BY terminal_id
), profiled AS (
  SELECT
    o.*,
    s.store_count,
    s.terminal_type_count,
    s.checkout_flag_count,
    row_number() OVER (
      PARTITION BY o.terminal_id
      ORDER BY o._bronze_ingested_at DESC, o.store_id, o.terminal_type
    ) AS terminal_rank
  FROM observed o
  JOIN stats s USING (terminal_id)
)
SELECT *
FROM profiled;

CREATE OR REFRESH MATERIALIZED VIEW dim_terminal_review
COMMENT 'Terminals observed with conflicting store or terminal-type assignments.'
AS
SELECT
  terminal_id,
  collect_set(store_id) AS observed_store_ids,
  collect_set(terminal_type) AS observed_terminal_types,
  collect_set(is_self_checkout) AS observed_self_checkout_flags,
  concat_ws('|',
    CASE WHEN max(store_count) > 1 THEN 'TERMINAL_USED_BY_MULTIPLE_STORES' END,
    CASE WHEN max(terminal_type_count) > 1 THEN 'TERMINAL_TYPE_CONFLICT' END,
    CASE WHEN max(checkout_flag_count) > 1 THEN 'SELF_CHECKOUT_FLAG_CONFLICT' END
  ) AS review_reasons
FROM terminal_source_profile
WHERE store_count > 1
   OR terminal_type_count > 1
   OR checkout_flag_count > 1
GROUP BY terminal_id;

CREATE OR REFRESH MATERIALIZED VIEW dim_terminal
COMMENT 'Conformed terminal dimension derived from consistent observed POS metadata.'
AS
SELECT
  sha2(terminal_id, 256) AS terminal_sk,
  terminal_id,
  s.store_sk,
  t.store_id,
  t.terminal_type,
  t.is_self_checkout,
  t.source_system,
  t._bronze_ingested_at
FROM terminal_source_profile t
JOIN dim_store s
  ON t.store_id = s.store_id
WHERE t.terminal_rank = 1
  AND t.store_count = 1
  AND t.terminal_type_count = 1
  AND t.checkout_flag_count = 1;

-- ---------------------------------------------------------------------------
-- 6. DIMENSION QUALITY SUMMARY
-- ---------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW silver_dimension_quality_summary
COMMENT 'Current conformed-dimension counts and review counts.'
AS
SELECT
  'dim_store' AS dataset_name,
  (SELECT count(*) FROM workspace.retail_dev_bronze.dim_stores) AS bronze_rows,
  (SELECT count(*) FROM dim_store) AS silver_rows,
  (SELECT count(*) FROM dim_store_review) AS review_keys,
  current_timestamp() AS measured_at
UNION ALL
SELECT
  'dim_customer',
  (SELECT count(*) FROM workspace.retail_dev_bronze.dim_customers),
  (SELECT count(*) FROM dim_customer),
  (SELECT count(*) FROM dim_customer_review),
  current_timestamp()
UNION ALL
SELECT
  'dim_product',
  (SELECT count(*) FROM workspace.retail_dev_bronze.dim_products),
  (SELECT count(*) FROM dim_product),
  (SELECT count(*) FROM dim_product_review),
  current_timestamp()
UNION ALL
SELECT
  'dim_product_scd2',
  (SELECT count(*) FROM workspace.retail_dev_bronze.dim_products_scd2),
  (SELECT count(*) FROM dim_product_scd2),
  (SELECT count(*) FROM dim_product_scd2_review),
  current_timestamp()
UNION ALL
SELECT
  'dim_terminal',
  (SELECT count(DISTINCT pos_terminal_id) FROM workspace.retail_dev_bronze.fact_transactions),
  (SELECT count(*) FROM dim_terminal),
  (SELECT count(*) FROM dim_terminal_review),
  current_timestamp();
