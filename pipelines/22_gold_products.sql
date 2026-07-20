-- ============================================================================
-- Einkaufpark Retail Platform — Gold Product Performance
--
-- Grain: exactly one row per trusted product_id.
-- Product sales and return denominators are joined only after independent
-- aggregation, preventing the return multiplication defect found previously.
-- ============================================================================

USE CATALOG IDENTIFIER(:gold_catalog);
USE SCHEMA IDENTIFIER(:gold_schema);

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW gold_product_sales_metrics
AS
SELECT
  product_sk,
  product_id,
  count(*) AS sales_line_count,
  count(DISTINCT basket_id) AS sales_basket_count,
  count(DISTINCT order_date) AS active_sales_days,
  min(order_date) AS first_sales_date,
  max(order_date) AS last_sales_date,
  sum(quantity) AS units_sold,
  round(sum(pre_discount_sales_eur), 2) AS pre_discount_sales_eur,
  round(sum(discount_amount_eur), 2) AS discount_amount_eur,
  round(sum(net_sales_eur), 2) AS net_sales_eur,
  round(sum(net_sales_ex_vat_eur), 2) AS net_sales_ex_vat_eur,
  round(sum(vat_amount_eur), 2) AS vat_amount_eur,
  round(sum(CASE WHEN is_promo_period THEN net_sales_eur ELSE 0 END), 2) AS promo_period_sales_eur,
  round(sum(CASE WHEN is_self_checkout THEN net_sales_eur ELSE 0 END), 2) AS self_checkout_sales_eur,
  round(sum(CASE WHEN customer_type = 'Loyalty Member' THEN net_sales_eur ELSE 0 END), 2) AS member_sales_eur,
  round(sum(CASE WHEN customer_type = 'Walk-in' THEN net_sales_eur ELSE 0 END), 2) AS walk_in_sales_eur
FROM gold_sales_enriched
GROUP BY product_sk, product_id;

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW gold_product_return_metrics
AS
SELECT
  product_sk,
  product_id,
  count(*) AS return_event_count,
  count(DISTINCT original_basket_id) AS returned_basket_count,
  sum(return_quantity) AS returned_units,
  round(sum(refund_amount_eur), 2) AS refund_amount_eur,
  round(avg(days_to_return), 2) AS average_days_to_return
FROM IDENTIFIER(:silver_catalog || '.' || :silver_schema || '.fact_returns')
GROUP BY product_sk, product_id;

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW gold_total_basket_count
AS
SELECT count(*) AS total_basket_count
FROM basket_analysis;

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW gold_product_metrics_base
AS
SELECT
  p.product_sk,
  p.product_id,
  p.product_name,
  p.category,
  p.subcategory,
  p.default_brand,
  p.is_private_label_eligible,
  p.price_min_eur,
  p.price_max_eur,
  p.catalogue_mid_price_eur,
  p.price_band,
  p.unit,
  p.vat_rate,

  s.first_sales_date,
  s.last_sales_date,
  coalesce(s.active_sales_days, 0) AS active_sales_days,
  coalesce(s.sales_line_count, 0) AS sales_line_count,
  coalesce(s.sales_basket_count, 0) AS sales_basket_count,
  coalesce(s.units_sold, 0) AS units_sold,
  coalesce(s.pre_discount_sales_eur, 0) AS pre_discount_sales_eur,
  coalesce(s.discount_amount_eur, 0) AS discount_amount_eur,
  coalesce(s.net_sales_eur, 0) AS net_sales_eur,
  coalesce(s.net_sales_ex_vat_eur, 0) AS net_sales_ex_vat_eur,
  coalesce(s.vat_amount_eur, 0) AS vat_amount_eur,
  coalesce(s.promo_period_sales_eur, 0) AS promo_period_sales_eur,
  coalesce(s.self_checkout_sales_eur, 0) AS self_checkout_sales_eur,
  coalesce(s.member_sales_eur, 0) AS member_sales_eur,
  coalesce(s.walk_in_sales_eur, 0) AS walk_in_sales_eur,

  coalesce(r.return_event_count, 0) AS return_event_count,
  coalesce(r.returned_basket_count, 0) AS returned_basket_count,
  coalesce(r.returned_units, 0) AS returned_units,
  coalesce(r.refund_amount_eur, 0) AS refund_amount_eur,
  r.average_days_to_return,
  t.total_basket_count
FROM IDENTIFIER(:silver_catalog || '.' || :silver_schema || '.dim_product') p
LEFT JOIN gold_product_sales_metrics s
  ON p.product_sk = s.product_sk
LEFT JOIN gold_product_return_metrics r
  ON p.product_sk = r.product_sk
CROSS JOIN gold_total_basket_count t;

CREATE OR REFRESH MATERIALIZED VIEW product_performance
COMMENT 'One row per trusted product with independently aggregated sales and returns, weighted pricing KPIs, ranks, and Pareto contribution.'
CLUSTER BY AUTO
AS
WITH calculated AS (
  SELECT
    *,
    round(net_sales_eur - refund_amount_eur, 2) AS retained_sales_after_refunds_eur,
    round(try_divide(net_sales_eur, units_sold), 2) AS weighted_average_selling_price_eur,
    round(try_divide(discount_amount_eur, pre_discount_sales_eur) * 100, 2) AS weighted_discount_rate_pct,
    round(try_divide(sales_basket_count, total_basket_count) * 100, 4) AS basket_penetration_pct,
    round(try_divide(units_sold, sales_basket_count), 2) AS average_units_per_product_basket,
    round(try_divide(promo_period_sales_eur, net_sales_eur) * 100, 2) AS promo_sales_share_pct,
    round(try_divide(member_sales_eur, net_sales_eur) * 100, 2) AS member_sales_share_pct,
    round(try_divide(refund_amount_eur, net_sales_eur) * 100, 2) AS refund_rate_pct,
    round(try_divide(returned_units, units_sold) * 100, 2) AS returned_unit_rate_pct,
    dense_rank() OVER (ORDER BY net_sales_eur DESC, product_id) AS revenue_rank,
    dense_rank() OVER (ORDER BY units_sold DESC, product_id) AS units_rank,
    round(try_divide(net_sales_eur, sum(net_sales_eur) OVER ()) * 100, 4) AS revenue_share_pct
  FROM gold_product_metrics_base
), pareto AS (
  SELECT
    *,
    round(
      try_divide(
        sum(net_sales_eur) OVER (
          ORDER BY net_sales_eur DESC, product_id
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ),
        sum(net_sales_eur) OVER ()
      ) * 100,
      4
    ) AS cumulative_revenue_share_pct
  FROM calculated
)
SELECT
  *,
  CASE
    WHEN cumulative_revenue_share_pct <= 80 THEN 'A - Core revenue'
    WHEN cumulative_revenue_share_pct <= 95 THEN 'B - Supporting revenue'
    ELSE 'C - Long tail'
  END AS pareto_class,
  current_timestamp() AS gold_refreshed_at
FROM pareto;
