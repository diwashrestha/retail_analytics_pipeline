-- ============================================================================
-- Einkaufpark Retail Platform — Gold Basket Analysis
-- Databricks Lakeflow Spark Declarative Pipeline (SQL)
--
-- Grain of basket_analysis: exactly one row per trusted basket_id.
-- This file also defines a private enriched sales dataset reused by the other
-- Gold source files.
-- ============================================================================

USE CATALOG IDENTIFIER(:gold_catalog);
USE SCHEMA IDENTIFIER(:gold_schema);

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW gold_sales_enriched
AS
SELECT
  s.*,
  st.city AS store_city,
  st.district AS store_district,
  st.region AS store_region,
  st.country_code,
  st.size_class AS store_size_class,
  st.currency,
  p.product_name,
  p.category,
  p.subcategory,
  p.default_brand,
  p.price_band,
  p.unit AS product_unit,
  p.is_private_label_eligible,
  c.age_group AS customer_age_group,
  c.gender_code AS customer_gender_code,
  c.is_member AS customer_master_is_member
FROM IDENTIFIER(:silver_catalog || '.' || :silver_schema || '.fact_sales') s
JOIN IDENTIFIER(:silver_catalog || '.' || :silver_schema || '.dim_store') st
  ON s.store_sk = st.store_sk
JOIN IDENTIFIER(:silver_catalog || '.' || :silver_schema || '.dim_product') p
  ON s.product_sk = p.product_sk
LEFT JOIN IDENTIFIER(:silver_catalog || '.' || :silver_schema || '.dim_customer') c
  ON s.customer_sk = c.customer_sk;

CREATE OR REFRESH MATERIALIZED VIEW basket_analysis
COMMENT 'One row per trusted basket with additive basket-level revenue, units, discount, customer, store, channel, and promotion attributes.'
CLUSTER BY AUTO
AS
WITH basket_metrics AS (
  SELECT
    basket_id,
    max(transaction_id) AS transaction_id,
    min(order_date) AS order_date,
    min(order_timestamp) AS order_timestamp,
    hour(min(order_timestamp)) AS order_hour,
    dayofweek(min(order_date)) AS weekday_number,
    date_format(min(order_date), 'EEEE') AS weekday_name,
    year(min(order_date)) AS calendar_year,
    quarter(min(order_date)) AS calendar_quarter,
    month(min(order_date)) AS calendar_month,
    weekofyear(min(order_date)) AS calendar_week,

    max(store_sk) AS store_sk,
    max(store_id) AS store_id,
    max(store_city) AS store_city,
    max(store_district) AS store_district,
    max(store_region) AS store_region,
    max(store_size_class) AS store_size_class,
    max(currency) AS currency,

    max(terminal_sk) AS terminal_sk,
    max(pos_terminal_id) AS pos_terminal_id,
    max(terminal_type) AS terminal_type,
    max(CASE WHEN is_self_checkout THEN 1 ELSE 0 END) = 1 AS is_self_checkout,

    max(customer_sk) AS customer_sk,
    max(customer_id) AS customer_id,
    max(customer_type) AS customer_type,
    max(customer_age_group) AS customer_age_group,
    max(customer_gender_code) AS customer_gender_code,
    max(CASE WHEN customer_master_is_member THEN 1 ELSE 0 END) = 1 AS customer_master_is_member,

    max(payment_type) AS payment_type,
    max(source_system) AS source_system,
    max(sales_channel) AS sales_channel,
    max(CASE WHEN coupon_applied THEN 1 ELSE 0 END) = 1 AS coupon_applied,
    max(coupon_code) AS coupon_code,
    max(CASE WHEN is_promo_period THEN 1 ELSE 0 END) = 1 AS is_promo_period,
    max(promo_week_id) AS promo_week_id,

    count(*) AS line_item_count,
    count(DISTINCT product_id) AS distinct_product_count,
    count(DISTINCT category) AS distinct_category_count,
    sum(quantity) AS total_units,
    round(sum(pre_discount_sales_eur), 2) AS pre_discount_sales_eur,
    round(sum(discount_amount_eur), 2) AS discount_amount_eur,
    round(sum(net_sales_eur), 2) AS net_sales_eur,
    round(sum(net_sales_ex_vat_eur), 2) AS net_sales_ex_vat_eur,
    round(sum(vat_amount_eur), 2) AS vat_amount_eur,
    round(sum(CASE WHEN is_private_label THEN net_sales_eur ELSE 0 END), 2) AS private_label_sales_eur,
    sum(CASE WHEN is_private_label THEN quantity ELSE 0 END) AS private_label_units,
    max(arrival_delay_days) AS max_arrival_delay_days,
    max(CASE WHEN is_late_arrival THEN 1 ELSE 0 END) = 1 AS contains_late_arrival
  FROM gold_sales_enriched
  GROUP BY basket_id
)
SELECT
  *,
  round(try_divide(discount_amount_eur, pre_discount_sales_eur) * 100, 2) AS discount_rate_pct,
  round(try_divide(private_label_sales_eur, net_sales_eur) * 100, 2) AS private_label_sales_share_pct,
  round(try_divide(net_sales_eur, total_units), 2) AS revenue_per_unit_eur,
  CASE
    WHEN distinct_product_count = 1 THEN 'Single-item'
    WHEN distinct_product_count BETWEEN 2 AND 5 THEN 'Small'
    WHEN distinct_product_count BETWEEN 6 AND 12 THEN 'Medium'
    ELSE 'Large'
  END AS basket_size_segment,
  CASE
    WHEN net_sales_eur < 10 THEN 'Under EUR 10'
    WHEN net_sales_eur < 30 THEN 'EUR 10-29.99'
    WHEN net_sales_eur < 60 THEN 'EUR 30-59.99'
    WHEN net_sales_eur < 100 THEN 'EUR 60-99.99'
    ELSE 'EUR 100+'
  END AS basket_value_segment,
  current_timestamp() AS gold_refreshed_at
FROM basket_metrics;
