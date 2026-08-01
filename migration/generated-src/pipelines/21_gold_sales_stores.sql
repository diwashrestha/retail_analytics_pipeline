-- ============================================================================
-- Einkaufpark Retail Platform — Gold Daily Sales and Store Performance
--
-- Grains:
--   daily_sales       = order_date x store_id x category x subcategory
--   store_performance = exactly one row per trusted store_id
--
-- Basket and customer counts in daily_sales are scoped to the category grain
-- and are intentionally named accordingly because they are non-additive across
-- product categories.
-- ============================================================================

USE CATALOG workspace;
USE SCHEMA retail_dev_gold;

CREATE OR REFRESH MATERIALIZED VIEW daily_sales
COMMENT 'Daily category sales at date-store-category-subcategory grain. Category basket and customer counts are non-additive across categories.'
CLUSTER BY AUTO
AS
SELECT
  order_date,
  year(order_date) AS calendar_year,
  quarter(order_date) AS calendar_quarter,
  month(order_date) AS calendar_month,
  weekofyear(order_date) AS calendar_week,
  dayofweek(order_date) AS weekday_number,
  date_format(order_date, 'EEEE') AS weekday_name,

  store_sk,
  store_id,
  store_city,
  store_district,
  store_region,
  store_size_class,
  currency,

  category,
  subcategory,

  count(*) AS sales_line_count,
  sum(quantity) AS units_sold,
  round(sum(pre_discount_sales_eur), 2) AS pre_discount_sales_eur,
  round(sum(discount_amount_eur), 2) AS discount_amount_eur,
  round(sum(net_sales_eur), 2) AS net_sales_eur,
  round(sum(net_sales_ex_vat_eur), 2) AS net_sales_ex_vat_eur,
  round(sum(vat_amount_eur), 2) AS vat_amount_eur,

  count(DISTINCT basket_id) AS category_basket_count,
  count(DISTINCT CASE WHEN customer_type = 'Walk-in' THEN basket_id END) AS category_walk_in_baskets,
  count(DISTINCT CASE WHEN customer_type = 'Loyalty Member' THEN basket_id END) AS category_member_baskets,
  count(DISTINCT CASE WHEN customer_type = 'Registered Non-member' THEN basket_id END) AS category_registered_nonmember_baskets,
  count(DISTINCT customer_id) AS category_identified_customers,

  round(try_divide(sum(net_sales_eur), count(DISTINCT basket_id)), 2) AS category_revenue_per_basket_eur,
  round(try_divide(sum(net_sales_eur), sum(quantity)), 2) AS weighted_average_selling_price_eur,
  round(try_divide(sum(discount_amount_eur), sum(pre_discount_sales_eur)) * 100, 2) AS weighted_discount_rate_pct,
  round(sum(CASE WHEN is_private_label THEN net_sales_eur ELSE 0 END), 2) AS private_label_sales_eur,
  round(try_divide(
    sum(CASE WHEN is_private_label THEN net_sales_eur ELSE 0 END),
    sum(net_sales_eur)
  ) * 100, 2) AS private_label_sales_share_pct,
  current_timestamp() AS gold_refreshed_at
FROM gold_sales_enriched
GROUP BY
  order_date,
  store_sk,
  store_id,
  store_city,
  store_district,
  store_region,
  store_size_class,
  currency,
  category,
  subcategory;

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW gold_store_basket_metrics
AS
SELECT
  store_sk,
  store_id,
  max(store_city) AS store_city,
  max(store_district) AS store_district,
  max(store_region) AS store_region,
  max(store_size_class) AS store_size_class,
  max(currency) AS currency,
  min(order_date) AS first_sales_date,
  max(order_date) AS last_sales_date,
  count(DISTINCT order_date) AS active_sales_days,
  count(*) AS basket_count,
  sum(total_units) AS units_sold,
  round(sum(pre_discount_sales_eur), 2) AS pre_discount_sales_eur,
  round(sum(discount_amount_eur), 2) AS discount_amount_eur,
  round(sum(net_sales_eur), 2) AS net_sales_eur,
  round(sum(net_sales_ex_vat_eur), 2) AS net_sales_ex_vat_eur,
  round(sum(vat_amount_eur), 2) AS vat_amount_eur,
  count(DISTINCT customer_id) AS identified_customer_count,
  count_if(customer_type = 'Walk-in') AS walk_in_baskets,
  count_if(customer_type = 'Loyalty Member') AS member_baskets,
  count_if(customer_type = 'Registered Non-member') AS registered_nonmember_baskets,
  round(sum(CASE WHEN customer_type = 'Walk-in' THEN net_sales_eur ELSE 0 END), 2) AS walk_in_sales_eur,
  round(sum(CASE WHEN customer_type = 'Loyalty Member' THEN net_sales_eur ELSE 0 END), 2) AS member_sales_eur,
  round(sum(CASE WHEN customer_type = 'Registered Non-member' THEN net_sales_eur ELSE 0 END), 2) AS registered_nonmember_sales_eur,
  count_if(is_self_checkout) AS self_checkout_baskets,
  count_if(is_promo_period) AS promo_period_baskets,
  count_if(coupon_applied) AS coupon_baskets,
  round(avg(net_sales_eur), 2) AS average_basket_value_eur,
  round(percentile_approx(net_sales_eur, 0.5), 2) AS median_basket_value_eur,
  round(avg(total_units), 2) AS average_units_per_basket,
  round(sum(private_label_sales_eur), 2) AS private_label_sales_eur
FROM basket_analysis
GROUP BY store_sk, store_id;

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW gold_store_return_metrics
AS
SELECT
  store_sk,
  store_id,
  count(*) AS return_event_count,
  sum(return_quantity) AS returned_units,
  round(sum(refund_amount_eur), 2) AS refund_amount_eur,
  count(DISTINCT original_basket_id) AS returned_basket_count,
  round(avg(days_to_return), 2) AS average_days_to_return
FROM workspace.retail_dev_silver.fact_returns
GROUP BY store_sk, store_id;

CREATE OR REFRESH MATERIALIZED VIEW store_performance
COMMENT 'One row per trusted store with additive sales, basket, customer, channel, promotion, and return KPIs.'
CLUSTER BY AUTO
AS
WITH store_base AS (
  SELECT
    st.store_sk,
    st.store_id,
    st.city AS store_city,
    st.district AS store_district,
    st.region AS store_region,
    st.country_code,
    st.country_name,
    st.size_class AS store_size_class,
    st.terminal_count,
    st.source_system,
    st.currency,
    b.first_sales_date,
    b.last_sales_date,
    coalesce(b.active_sales_days, 0) AS active_sales_days,
    coalesce(b.basket_count, 0) AS basket_count,
    coalesce(b.units_sold, 0) AS units_sold,
    coalesce(b.pre_discount_sales_eur, 0) AS pre_discount_sales_eur,
    coalesce(b.discount_amount_eur, 0) AS discount_amount_eur,
    coalesce(b.net_sales_eur, 0) AS net_sales_eur,
    coalesce(b.net_sales_ex_vat_eur, 0) AS net_sales_ex_vat_eur,
    coalesce(b.vat_amount_eur, 0) AS vat_amount_eur,
    coalesce(b.identified_customer_count, 0) AS identified_customer_count,
    coalesce(b.walk_in_baskets, 0) AS walk_in_baskets,
    coalesce(b.member_baskets, 0) AS member_baskets,
    coalesce(b.registered_nonmember_baskets, 0) AS registered_nonmember_baskets,
    coalesce(b.walk_in_sales_eur, 0) AS walk_in_sales_eur,
    coalesce(b.member_sales_eur, 0) AS member_sales_eur,
    coalesce(b.registered_nonmember_sales_eur, 0) AS registered_nonmember_sales_eur,
    coalesce(b.self_checkout_baskets, 0) AS self_checkout_baskets,
    coalesce(b.promo_period_baskets, 0) AS promo_period_baskets,
    coalesce(b.coupon_baskets, 0) AS coupon_baskets,
    coalesce(b.average_basket_value_eur, 0) AS average_basket_value_eur,
    coalesce(b.median_basket_value_eur, 0) AS median_basket_value_eur,
    coalesce(b.average_units_per_basket, 0) AS average_units_per_basket,
    coalesce(b.private_label_sales_eur, 0) AS private_label_sales_eur,
    coalesce(r.return_event_count, 0) AS return_event_count,
    coalesce(r.returned_units, 0) AS returned_units,
    coalesce(r.refund_amount_eur, 0) AS refund_amount_eur,
    coalesce(r.returned_basket_count, 0) AS returned_basket_count,
    r.average_days_to_return
  FROM workspace.retail_dev_silver.dim_store st
  LEFT JOIN gold_store_basket_metrics b
    ON st.store_sk = b.store_sk
  LEFT JOIN gold_store_return_metrics r
    ON st.store_sk = r.store_sk
)
SELECT
  *,
  round(net_sales_eur - refund_amount_eur, 2) AS retained_sales_after_refunds_eur,
  round(try_divide(discount_amount_eur, pre_discount_sales_eur) * 100, 2) AS discount_rate_pct,
  round(try_divide(walk_in_baskets, basket_count) * 100, 2) AS walk_in_basket_share_pct,
  round(try_divide(member_baskets, basket_count) * 100, 2) AS member_basket_share_pct,
  round(try_divide(self_checkout_baskets, basket_count) * 100, 2) AS self_checkout_basket_share_pct,
  round(try_divide(promo_period_baskets, basket_count) * 100, 2) AS promo_basket_share_pct,
  round(try_divide(private_label_sales_eur, net_sales_eur) * 100, 2) AS private_label_sales_share_pct,
  round(try_divide(refund_amount_eur, net_sales_eur) * 100, 2) AS refund_rate_pct,
  round(try_divide(returned_units, units_sold) * 100, 2) AS returned_unit_rate_pct,
  round(try_divide(net_sales_eur, active_sales_days), 2) AS revenue_per_active_day_eur,
  round(try_divide(net_sales_eur, terminal_count), 2) AS revenue_per_terminal_eur,
  dense_rank() OVER (ORDER BY net_sales_eur DESC, store_id) AS revenue_rank,
  dense_rank() OVER (ORDER BY basket_count DESC, store_id) AS basket_rank,
  current_timestamp() AS gold_refreshed_at
FROM store_base;
