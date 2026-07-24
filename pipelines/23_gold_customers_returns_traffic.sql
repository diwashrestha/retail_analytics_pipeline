-- ============================================================================
-- Einkaufpark Retail Platform — Gold Customer, Return, and Traffic Analytics
--
-- Grains:
--   customer_ltv    = exactly one row per trusted customer_id
--   return_analysis = product_id x reason_code
--   hourly_traffic  = order_hour x weekday_number x store_size_class
-- ============================================================================

-- USE CATALOG IDENTIFIER(:gold_catalog);
-- USE SCHEMA IDENTIFIER(:gold_schema);

USE CATALOG workspace;
USE SCHEMA retail_dev_gold;
-- ---------------------------------------------------------------------------
-- 1. CUSTOMER LIFETIME VALUE
-- ---------------------------------------------------------------------------

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW gold_customer_basket_metrics
AS
SELECT
  customer_sk,
  customer_id,
  min(order_date) AS first_purchase_date,
  max(order_date) AS last_purchase_date,
  count(*) AS lifetime_baskets,
  count(DISTINCT order_date) AS active_purchase_days,
  sum(total_units) AS lifetime_units,
  round(sum(pre_discount_sales_eur), 2) AS lifetime_pre_discount_sales_eur,
  round(sum(discount_amount_eur), 2) AS lifetime_discount_amount_eur,
  round(sum(net_sales_eur), 2) AS lifetime_sales_eur,
  round(sum(private_label_sales_eur), 2) AS lifetime_private_label_sales_eur,
  count_if(is_promo_period) AS promo_baskets,
  count_if(coupon_applied) AS coupon_baskets,
  count_if(is_self_checkout) AS self_checkout_baskets,
  round(avg(net_sales_eur), 2) AS average_basket_value_eur,
  round(percentile_approx(net_sales_eur, 0.5), 2) AS median_basket_value_eur,
  round(avg(total_units), 2) AS average_units_per_basket
FROM basket_analysis
WHERE customer_id IS NOT NULL
GROUP BY customer_sk, customer_id;

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW gold_customer_return_metrics
AS
SELECT
  customer_sk,
  customer_id,
  count(*) AS return_event_count,
  count(DISTINCT original_basket_id) AS returned_basket_count,
  sum(return_quantity) AS returned_units,
  round(sum(refund_amount_eur), 2) AS refund_amount_eur,
  round(avg(days_to_return), 2) AS average_days_to_return
FROM workspace.retail_dev_silver.fact_returns
WHERE customer_id IS NOT NULL
GROUP BY customer_sk, customer_id;

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW gold_dataset_reference_date
AS
SELECT max(order_date) AS dataset_max_order_date
FROM basket_analysis;

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW gold_customer_ltv_base
AS
SELECT
  c.customer_sk,
  c.customer_id,
  c.age,
  c.age_group,
  c.gender_code,
  c.is_member,
  c.loyalty_card_id,
  c.age_quality_status,
  b.first_purchase_date,
  b.last_purchase_date,
  coalesce(b.lifetime_baskets, 0) AS lifetime_baskets,
  coalesce(b.active_purchase_days, 0) AS active_purchase_days,
  coalesce(b.lifetime_units, 0) AS lifetime_units,
  coalesce(b.lifetime_pre_discount_sales_eur, 0) AS lifetime_pre_discount_sales_eur,
  coalesce(b.lifetime_discount_amount_eur, 0) AS lifetime_discount_amount_eur,
  coalesce(b.lifetime_sales_eur, 0) AS lifetime_sales_eur,
  coalesce(b.lifetime_private_label_sales_eur, 0) AS lifetime_private_label_sales_eur,
  coalesce(b.promo_baskets, 0) AS promo_baskets,
  coalesce(b.coupon_baskets, 0) AS coupon_baskets,
  coalesce(b.self_checkout_baskets, 0) AS self_checkout_baskets,
  coalesce(b.average_basket_value_eur, 0) AS average_basket_value_eur,
  coalesce(b.median_basket_value_eur, 0) AS median_basket_value_eur,
  coalesce(b.average_units_per_basket, 0) AS average_units_per_basket,
  coalesce(r.return_event_count, 0) AS return_event_count,
  coalesce(r.returned_basket_count, 0) AS returned_basket_count,
  coalesce(r.returned_units, 0) AS returned_units,
  coalesce(r.refund_amount_eur, 0) AS refund_amount_eur,
  r.average_days_to_return,
  d.dataset_max_order_date,
  CASE
    WHEN b.first_purchase_date IS NULL THEN NULL
    ELSE datediff(b.last_purchase_date, b.first_purchase_date)
  END AS observed_customer_lifetime_days,
  CASE
    WHEN b.last_purchase_date IS NULL THEN NULL
    ELSE datediff(d.dataset_max_order_date, b.last_purchase_date)
  END AS recency_days
FROM workspace.retail_dev_silver.dim_customer c
LEFT JOIN gold_customer_basket_metrics b
  ON c.customer_sk = b.customer_sk
LEFT JOIN gold_customer_return_metrics r
  ON c.customer_sk = r.customer_sk
CROSS JOIN gold_dataset_reference_date d;

CREATE OR REFRESH MATERIALIZED VIEW customer_ltv
COMMENT 'One row per trusted customer with sales, refund-adjusted value, frequency, recency, ranking, and percentile segmentation.'
CLUSTER BY AUTO
AS
WITH calculated AS (
  SELECT
    *,
    round(lifetime_sales_eur - refund_amount_eur, 2) AS retained_lifetime_value_eur,
    round(try_divide(lifetime_discount_amount_eur, lifetime_pre_discount_sales_eur) * 100, 2) AS lifetime_discount_rate_pct,
    round(try_divide(lifetime_private_label_sales_eur, lifetime_sales_eur) * 100, 2) AS private_label_sales_share_pct,
    round(try_divide(promo_baskets, lifetime_baskets) * 100, 2) AS promo_basket_share_pct,
    round(try_divide(coupon_baskets, lifetime_baskets) * 100, 2) AS coupon_basket_share_pct,
    round(try_divide(self_checkout_baskets, lifetime_baskets) * 100, 2) AS self_checkout_basket_share_pct,
    round(try_divide(refund_amount_eur, lifetime_sales_eur) * 100, 2) AS refund_rate_pct,
    round(
      try_divide(
        lifetime_baskets * 30,
        greatest(coalesce(observed_customer_lifetime_days, 0), 30)
      ),
      3
    ) AS baskets_per_30_days,
    row_number() OVER (
      ORDER BY (lifetime_sales_eur - refund_amount_eur) DESC, lifetime_sales_eur DESC, customer_id
    ) AS overall_ltv_rank,
    percent_rank() OVER (
      ORDER BY (lifetime_sales_eur - refund_amount_eur) DESC, lifetime_sales_eur DESC, customer_id
    ) AS ltv_percent_rank
  FROM gold_customer_ltv_base
)
SELECT
  *,
  CASE
    WHEN overall_ltv_rank <= 1000 THEN overall_ltv_rank
  END AS ltv_rank_top_1000,
  round(ltv_percent_rank * 100, 4) AS ltv_percentile,
  CASE
    WHEN lifetime_baskets = 0 THEN 'No Purchase'
    WHEN ltv_percent_rank <= 0.01 THEN 'Champion'
    WHEN ltv_percent_rank <= 0.10 THEN 'High Value'
    WHEN ltv_percent_rank <= 0.30 THEN 'Loyal'
    WHEN ltv_percent_rank <= 0.60 THEN 'Developing'
    ELSE 'Low Activity'
  END AS customer_segment,
  CASE
    WHEN lifetime_baskets = 0 THEN 'Never Purchased'
    WHEN recency_days <= 30 THEN 'Active - 30 days'
    WHEN recency_days <= 90 THEN 'Warm - 31 to 90 days'
    WHEN recency_days <= 180 THEN 'Cooling - 91 to 180 days'
    ELSE 'At Risk - 181+ days'
  END AS recency_segment,
  current_timestamp() AS gold_refreshed_at
FROM calculated;

-- ---------------------------------------------------------------------------
-- 2. RETURN REASON ANALYSIS
-- No sales denominators are repeated at the product-reason grain. Product-level
-- return rates are available in product_performance.
-- ---------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW return_analysis
COMMENT 'Return reason distribution at product-reason grain. Contains return metrics only; product sales denominators and rates live in product_performance.'
CLUSTER BY AUTO
AS
WITH reason_metrics AS (
  SELECT
    r.product_sk,
    r.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    p.default_brand,
    p.price_band,
    r.reason_code,
    count(*) AS return_event_count,
    count(DISTINCT r.original_basket_id) AS returned_basket_count,
    count(DISTINCT r.customer_id) AS returning_customer_count,
    sum(r.return_quantity) AS returned_units,
    round(sum(r.refund_amount_eur), 2) AS refund_amount_eur,
    round(avg(r.refund_amount_eur), 2) AS average_refund_per_event_eur,
    round(avg(r.days_to_return), 2) AS average_days_to_return,
    min(r.return_date) AS first_return_date,
    max(r.return_date) AS last_return_date
  FROM workspace.retail_dev_silver.fact_returns r
  JOIN workspace.retail_dev_silver.dim_product p
    ON r.product_sk = p.product_sk
  GROUP BY
    r.product_sk,
    r.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    p.default_brand,
    p.price_band,
    r.reason_code
)
SELECT
  *,
  round(try_divide(return_event_count, sum(return_event_count) OVER (PARTITION BY product_id)) * 100, 2) AS product_return_event_share_pct,
  round(try_divide(returned_units, sum(returned_units) OVER (PARTITION BY product_id)) * 100, 2) AS product_returned_unit_share_pct,
  round(try_divide(refund_amount_eur, sum(refund_amount_eur) OVER (PARTITION BY product_id)) * 100, 2) AS product_refund_share_pct,
  round(try_divide(return_event_count, sum(return_event_count) OVER ()) * 100, 4) AS all_return_event_share_pct,
  round(try_divide(refund_amount_eur, sum(refund_amount_eur) OVER ()) * 100, 4) AS all_refund_share_pct,
  dense_rank() OVER (PARTITION BY product_id ORDER BY return_event_count DESC, reason_code) AS reason_rank_within_product,
  current_timestamp() AS gold_refreshed_at
FROM reason_metrics;

-- ---------------------------------------------------------------------------
-- 3. HOURLY TRAFFIC
-- ---------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW hourly_traffic
COMMENT 'Basket-level traffic at hour-weekday-store-size grain with additive revenue, units, basket counts, and channel shares.'
CLUSTER BY AUTO
AS
SELECT
  order_hour,
  weekday_number,
  weekday_name,
  store_size_class,
  count(*) AS basket_count,
  sum(total_units) AS units_sold,
  round(sum(net_sales_eur), 2) AS net_sales_eur,
  round(sum(discount_amount_eur), 2) AS discount_amount_eur,
  round(avg(net_sales_eur), 2) AS average_basket_value_eur,
  round(percentile_approx(net_sales_eur, 0.5), 2) AS median_basket_value_eur,
  round(avg(total_units), 2) AS average_units_per_basket,
  count_if(customer_type = 'Walk-in') AS walk_in_baskets,
  count_if(customer_type = 'Loyalty Member') AS member_baskets,
  count_if(customer_type = 'Registered Non-member') AS registered_nonmember_baskets,
  count_if(is_self_checkout) AS self_checkout_baskets,
  count_if(is_promo_period) AS promo_period_baskets,
  count_if(coupon_applied) AS coupon_baskets,
  round(try_divide(count_if(customer_type = 'Walk-in'), count(*)) * 100, 2) AS walk_in_basket_share_pct,
  round(try_divide(count_if(customer_type = 'Loyalty Member'), count(*)) * 100, 2) AS member_basket_share_pct,
  round(try_divide(count_if(is_self_checkout), count(*)) * 100, 2) AS self_checkout_basket_share_pct,
  round(try_divide(count_if(is_promo_period), count(*)) * 100, 2) AS promo_basket_share_pct,
  current_timestamp() AS gold_refreshed_at
FROM basket_analysis
GROUP BY
  order_hour,
  weekday_number,
  weekday_name,
  store_size_class;
