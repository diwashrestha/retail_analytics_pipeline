
-- 1. FACT SALES
-- Grain: one trusted completed POS sales line


CREATE OR REPLACE VIEW
{{catalog}}.{{reporting_schema}}.v_fact_sales
AS 
SELECT sales_line_sk AS sales_line_key,
transaction_id,
basket_id,
CAST(order_date AS DATE) AS order_date,
order_time,
order_timestamp,
HOUR(order_timestamp) AS order_hour,
store_sk AS store_key,
store_id,
customer_sk AS customer_key,
customer_id,
customer_type,

product_sk AS product_key,
product_id,

quantity,

CAST(
effective_list_price_eur AS DECIMAL(18,2)
) AS list_price_eur,

CAST(
unit_price_eur AS DECIMAL(18,2)
) AS unit_price_eur,

CAST(
discount_pct AS DECIMAL(9,4)
) AS discount_pct,

CAST(
pre_discount_sales_eur AS DECIMAL(18,2)
) AS gross_sales_eur,

CAST(
discount_amount_eur AS DECIMAL(18,2)
) AS discount_amount_eur,

CAST(
net_sales_eur AS DECIMAL(18,2)
) AS net_sales_eur,

CAST(
net_sales_ex_vat_eur AS DECIMAL(18,2)
) AS net_sales_ex_vat_eur,

CAST(
vat_amount_eur AS DECIMAL(18,2)
) AS vat_amount_eur,

CAST(
vat_rate AS DECIMAL(9,4)
) AS vat_rate,

payment_type,
sales_channel,

membership_active AS is_loyalty_member,
coupon_applied AS is_coupon_applied,
is_private_label,
is_self_checkout,
is_promo_period,
is_promo_price,

brand,
loyalty_points_earned

FROM {{catalog}}.{{silver_schema}}.fact_sales;


-- ============================================================================
-- 2. FACT RETURNS
-- Grain: one trusted return event
-- ============================================================================

CREATE OR REPLACE VIEW
{{catalog}}.{{reporting_schema}}.v_fact_returns
AS
SELECT
return_sk AS return_key,

return_id,
original_transaction_id,
original_basket_id,

product_sk AS product_key,
product_id,

store_sk AS store_key,
store_id,

customer_sk AS customer_key,
customer_id,

CAST(
original_order_date AS DATE
) AS original_order_date,

CAST(
return_date AS DATE
) AS return_date,

days_to_return,

sold_quantity,
original_quantity,
return_quantity,

CAST(
original_unit_price_eur AS DECIMAL(18,2)
) AS original_unit_price_eur,

CAST(
original_discount_pct AS DECIMAL(9,4)
) AS original_discount_pct,

CAST(
net_unit_price_eur AS DECIMAL(18,2)
) AS net_unit_price_eur,

CAST(
refund_amount_eur AS DECIMAL(18,2)
) AS refund_amount_eur,

reason_code

FROM {{catalog}}.{{silver_schema}}.fact_returns;


-- ============================================================================
-- 3. DATE DIMENSION
-- Grain: one calendar date
--
-- Covers both sales and return dates so the same calendar can relate to
-- FactSales[order_date] and FactReturns[return_date].
-- ============================================================================

CREATE OR REPLACE VIEW
{{catalog}}.{{reporting_schema}}.v_dim_date
AS

WITH source_bounds AS (

SELECT
MIN(order_date) AS min_date,
MAX(order_date) AS max_date
FROM {{catalog}}.{{silver_schema}}.fact_sales

UNION ALL

SELECT
MIN(return_date) AS min_date,
MAX(return_date) AS max_date
FROM {{catalog}}.{{silver_schema}}.fact_returns
),

bounds AS (
SELECT
MIN(min_date) AS min_date,
MAX(max_date) AS max_date
FROM source_bounds
),

dates AS (
SELECT
EXPLODE(
SEQUENCE(
min_date,
max_date,
INTERVAL 1 DAY
)
) AS calendar_date
FROM bounds
)

SELECT
CAST(calendar_date AS DATE) AS date_key,
CAST(calendar_date AS DATE) AS calendar_date,

YEAR(calendar_date) AS year,

QUARTER(calendar_date) AS quarter_number,

CONCAT(
'Q',
QUARTER(calendar_date)
) AS quarter,

MONTH(calendar_date) AS month_number,

DATE_FORMAT(
calendar_date,
'MMMM'
) AS month_name,

DATE_FORMAT(
calendar_date,
'yyyy-MM'
) AS year_month,

WEEKOFYEAR(
calendar_date
) AS week_of_year,

DAY(
calendar_date
) AS day_of_month,

-- Monday = 1 ... Sunday = 7
WEEKDAY(calendar_date) + 1
AS day_of_week_number,

DATE_FORMAT(
calendar_date,
'EEEE'
) AS day_of_week,

CASE
WHEN WEEKDAY(calendar_date) IN (5, 6)
THEN TRUE
ELSE FALSE
END AS is_weekend,

CASE
WHEN WEEKDAY(calendar_date) = 6
THEN TRUE
ELSE FALSE
END AS is_sunday

FROM dates;



-- ============================================================================
-- 4. STORE DIMENSION
-- Grain: one trusted store
-- ============================================================================

CREATE OR REPLACE VIEW
{{catalog}}.{{reporting_schema}}.v_dim_store
AS
SELECT
store_sk AS store_key,
store_id,

city,
district,
postal_code,
street,
region,

country_code,
country_name,

size_class AS store_size_class,
terminal_count,
opening_hours,
currency

FROM {{catalog}}.{{silver_schema}}.dim_store;


-- ============================================================================
-- 5. PRODUCT DIMENSION
-- Grain: one trusted current product
-- ============================================================================

CREATE OR REPLACE VIEW
{{catalog}}.{{reporting_schema}}.v_dim_product
AS
SELECT
product_sk AS product_key,
product_id,

product_name,
category,
subcategory,

default_brand AS brand,

is_private_label_eligible,

CAST(
price_min_eur AS DECIMAL(18,2)
) AS minimum_price_eur,

CAST(
price_max_eur AS DECIMAL(18,2)
) AS maximum_price_eur,

CAST(
catalogue_mid_price_eur AS DECIMAL(18,2)
) AS catalogue_mid_price_eur,

price_band,
unit,
seasonal_months,

CAST(
vat_rate AS DECIMAL(9,4)
) AS vat_rate

FROM {{catalog}}.{{silver_schema}}.dim_product;


-- ============================================================================
-- 6. CUSTOMER DIMENSION
-- Grain: one trusted identified customer
-- ============================================================================

CREATE OR REPLACE VIEW
{{catalog}}.{{reporting_schema}}.v_dim_customer
AS
SELECT
customer_sk AS customer_key,
customer_id,

age,
age_group,

CASE gender_code
WHEN 'M' THEN 'Male'
WHEN 'F' THEN 'Female'
WHEN 'D' THEN 'Diverse'
ELSE 'Unknown'
END AS gender,

is_member AS is_loyalty_member,

loyalty_card_id,
age_quality_status

FROM {{catalog}}.{{silver_schema}}.dim_customer;


-- ============================================================================
-- 7. EXECUTIVE KPIs
-- Grain: exactly one row for the complete trusted dataset
-- ============================================================================

CREATE OR REPLACE VIEW
{{catalog}}.{{reporting_schema}}.v_executive_kpis
AS

WITH sales AS (

SELECT
COUNT(*) AS sales_line_count,

COUNT(
DISTINCT transaction_id
) AS transaction_count,

COUNT(
DISTINCT basket_id
) AS basket_count,

COUNT(
DISTINCT customer_id
) AS identified_customer_count,

SUM(quantity) AS units_sold,

CAST(
ROUND(
COALESCE(
SUM(pre_discount_sales_eur),
0
),
2
)
AS DECIMAL(18,2)
) AS gross_sales_eur,

CAST(
ROUND(
COALESCE(
SUM(discount_amount_eur),
0
),
2
)
AS DECIMAL(18,2)
) AS discount_amount_eur,

CAST(
ROUND(
COALESCE(
SUM(net_sales_eur),
0
),
2
)
AS DECIMAL(18,2)
) AS net_sales_eur,

CAST(
ROUND(
COALESCE(
SUM(
CASE
WHEN customer_type = 'Walk-in'
THEN net_sales_eur
ELSE 0
END
),
0
),
2
)
AS DECIMAL(18,2)
) AS walk_in_sales_eur,

CAST(
ROUND(
COALESCE(
SUM(
CASE
WHEN customer_type = 'Loyalty Member'
THEN net_sales_eur
ELSE 0
END
),
0
),
2
)
AS DECIMAL(18,2)
) AS member_sales_eur,

CAST(
ROUND(
COALESCE(
SUM(
CASE
WHEN customer_type = 'Registered Non-member'
THEN net_sales_eur
ELSE 0
END
),
0
),
2
)
AS DECIMAL(18,2)
) AS registered_nonmember_sales_eur,

CAST(
ROUND(
COALESCE(
SUM(
CASE
WHEN customer_type <> 'Walk-in'
THEN net_sales_eur
ELSE 0
END
),
0
),
2
)
AS DECIMAL(18,2)
) AS registered_customer_sales_eur,

CAST(
ROUND(
COALESCE(
SUM(
CASE
WHEN is_promo_period
THEN net_sales_eur
ELSE 0
END
),
0
),
2
)
AS DECIMAL(18,2)
) AS promotion_sales_eur,

CAST(
ROUND(
COALESCE(
SUM(
CASE
WHEN is_self_checkout
THEN net_sales_eur
ELSE 0
END
),
0
),
2
)
AS DECIMAL(18,2)
) AS self_checkout_sales_eur,

CAST(
MIN(order_date) AS DATE
) AS first_sales_date,

CAST(
MAX(order_date) AS DATE
) AS latest_sales_date

FROM {{catalog}}.{{silver_schema}}.fact_sales
),

returns AS (

SELECT
COUNT(*) AS return_event_count,

COALESCE(
SUM(return_quantity),
0
) AS returned_units,

CAST(
ROUND(
COALESCE(
SUM(refund_amount_eur),
0
),
2
)
AS DECIMAL(18,2)
) AS refund_amount_eur

FROM {{catalog}}.{{silver_schema}}.fact_returns
)

SELECT
s.sales_line_count,
s.transaction_count,
s.basket_count,
s.identified_customer_count,
s.units_sold,

s.gross_sales_eur,
s.discount_amount_eur,
s.net_sales_eur,

s.walk_in_sales_eur,
s.member_sales_eur,
s.registered_nonmember_sales_eur,
s.registered_customer_sales_eur,

s.promotion_sales_eur,
s.self_checkout_sales_eur,

CAST(
ROUND(
TRY_DIVIDE(
s.net_sales_eur,
s.basket_count
),
2
)
AS DECIMAL(18,2)
) AS average_basket_value_eur,

r.return_event_count,
r.returned_units,
r.refund_amount_eur,

CAST(
ROUND(
TRY_DIVIDE(
r.returned_units * 100.0,
s.units_sold
),
2
)
AS DECIMAL(9,2)
) AS returned_unit_rate_pct,

CAST(
ROUND(
s.net_sales_eur
- r.refund_amount_eur,
2
)
AS DECIMAL(18,2)
) AS retained_sales_after_refunds_eur,

s.first_sales_date,
s.latest_sales_date

FROM sales s
CROSS JOIN returns r;


-- ============================================================================
-- 8. DAILY SALES
-- Grain: date x store x category x subcategory
-- ============================================================================

CREATE OR REPLACE VIEW
{{catalog}}.{{reporting_schema}}.v_daily_sales
AS
SELECT
CAST(
order_date AS DATE
) AS order_date,

store_sk AS store_key,
store_id,

store_city,
store_district,
store_region,
store_size_class,

category,
subcategory,
currency,

sales_line_count,
units_sold,

CAST(
pre_discount_sales_eur AS DECIMAL(18,2)
) AS gross_sales_eur,

CAST(
discount_amount_eur AS DECIMAL(18,2)
) AS discount_amount_eur,

CAST(
net_sales_eur AS DECIMAL(18,2)
) AS net_sales_eur,

CAST(
net_sales_ex_vat_eur AS DECIMAL(18,2)
) AS net_sales_ex_vat_eur,

CAST(
vat_amount_eur AS DECIMAL(18,2)
) AS vat_amount_eur,

category_basket_count,
category_walk_in_baskets,
category_member_baskets,
category_registered_nonmember_baskets,
category_identified_customers,

CAST(
category_revenue_per_basket_eur
AS DECIMAL(18,2)
) AS category_revenue_per_basket_eur,

CAST(
weighted_average_selling_price_eur
AS DECIMAL(18,2)
) AS weighted_average_selling_price_eur,

CAST(
weighted_discount_rate_pct
AS DECIMAL(9,2)
) AS weighted_discount_rate_pct,

CAST(
private_label_sales_eur
AS DECIMAL(18,2)
) AS private_label_sales_eur,

CAST(
private_label_sales_share_pct
AS DECIMAL(9,2)
) AS private_label_sales_share_pct

FROM {{catalog}}.{{gold_schema}}.daily_sales;


-- ============================================================================
-- 9. STORE PERFORMANCE
-- Grain: one trusted store
-- ============================================================================

CREATE OR REPLACE VIEW
{{catalog}}.{{reporting_schema}}.v_store_performance
AS
SELECT
store_sk AS store_key,
store_id,

store_city,
store_district,
store_region,

country_code,
country_name,

store_size_class,
terminal_count,
currency,

CAST(
first_sales_date AS DATE
) AS first_sales_date,

CAST(
last_sales_date AS DATE
) AS last_sales_date,

active_sales_days,
basket_count,
units_sold,

CAST(
pre_discount_sales_eur AS DECIMAL(18,2)
) AS gross_sales_eur,

CAST(
discount_amount_eur AS DECIMAL(18,2)
) AS discount_amount_eur,

CAST(
net_sales_eur AS DECIMAL(18,2)
) AS net_sales_eur,

CAST(
net_sales_ex_vat_eur AS DECIMAL(18,2)
) AS net_sales_ex_vat_eur,

CAST(
vat_amount_eur AS DECIMAL(18,2)
) AS vat_amount_eur,

identified_customer_count,

walk_in_baskets,
member_baskets,
registered_nonmember_baskets,

CAST(
walk_in_sales_eur AS DECIMAL(18,2)
) AS walk_in_sales_eur,

CAST(
member_sales_eur AS DECIMAL(18,2)
) AS member_sales_eur,

CAST(
registered_nonmember_sales_eur
AS DECIMAL(18,2)
) AS registered_nonmember_sales_eur,

self_checkout_baskets,
promo_period_baskets,
coupon_baskets,

CAST(
average_basket_value_eur
AS DECIMAL(18,2)
) AS average_basket_value_eur,

CAST(
median_basket_value_eur
AS DECIMAL(18,2)
) AS median_basket_value_eur,

CAST(
average_units_per_basket
AS DECIMAL(18,2)
) AS average_units_per_basket,

CAST(
private_label_sales_eur
AS DECIMAL(18,2)
) AS private_label_sales_eur,

return_event_count,
returned_units,
returned_basket_count,

CAST(
refund_amount_eur
AS DECIMAL(18,2)
) AS refund_amount_eur,

CAST(
average_days_to_return
AS DECIMAL(18,2)
) AS average_days_to_return,

CAST(
retained_sales_after_refunds_eur
AS DECIMAL(18,2)
) AS retained_sales_after_refunds_eur,

CAST(
discount_rate_pct
AS DECIMAL(9,2)
) AS discount_rate_pct,

CAST(
walk_in_basket_share_pct
AS DECIMAL(9,2)
) AS walk_in_basket_share_pct,

CAST(
member_basket_share_pct
AS DECIMAL(9,2)
) AS member_basket_share_pct,

CAST(
self_checkout_basket_share_pct
AS DECIMAL(9,2)
) AS self_checkout_basket_share_pct,

CAST(
promo_basket_share_pct
AS DECIMAL(9,2)
) AS promo_basket_share_pct,

CAST(
private_label_sales_share_pct
AS DECIMAL(9,2)
) AS private_label_sales_share_pct,

CAST(
refund_rate_pct
AS DECIMAL(9,2)
) AS refund_rate_pct,

CAST(
returned_unit_rate_pct
AS DECIMAL(9,2)
) AS returned_unit_rate_pct,

CAST(
revenue_per_active_day_eur
AS DECIMAL(18,2)
) AS revenue_per_active_day_eur,

CAST(
revenue_per_terminal_eur
AS DECIMAL(18,2)
) AS revenue_per_terminal_eur,

revenue_rank,
basket_rank

FROM {{catalog}}.{{gold_schema}}.store_performance;



-- ============================================================================
-- 10. PRODUCT PERFORMANCE
-- Grain: one trusted product
-- ============================================================================

CREATE OR REPLACE VIEW
{{catalog}}.{{reporting_schema}}.v_product_performance
AS
SELECT
product_sk AS product_key,
product_id,

product_name,
category,
subcategory,

default_brand AS brand,
is_private_label_eligible,

CAST(
price_min_eur AS DECIMAL(18,2)
) AS minimum_price_eur,

CAST(
price_max_eur AS DECIMAL(18,2)
) AS maximum_price_eur,

CAST(
catalogue_mid_price_eur AS DECIMAL(18,2)
) AS catalogue_mid_price_eur,

price_band,
unit,

CAST(
vat_rate AS DECIMAL(9,4)
) AS vat_rate,

CAST(
first_sales_date AS DATE
) AS first_sales_date,

CAST(
last_sales_date AS DATE
) AS last_sales_date,

active_sales_days,
sales_line_count,
sales_basket_count,
units_sold,

CAST(
pre_discount_sales_eur AS DECIMAL(18,2)
) AS gross_sales_eur,

CAST(
discount_amount_eur AS DECIMAL(18,2)
) AS discount_amount_eur,

CAST(
net_sales_eur AS DECIMAL(18,2)
) AS net_sales_eur,

CAST(
net_sales_ex_vat_eur AS DECIMAL(18,2)
) AS net_sales_ex_vat_eur,

CAST(
vat_amount_eur AS DECIMAL(18,2)
) AS vat_amount_eur,

CAST(
promo_period_sales_eur AS DECIMAL(18,2)
) AS promotion_sales_eur,

CAST(
self_checkout_sales_eur AS DECIMAL(18,2)
) AS self_checkout_sales_eur,

CAST(
member_sales_eur AS DECIMAL(18,2)
) AS member_sales_eur,

CAST(
walk_in_sales_eur AS DECIMAL(18,2)
) AS walk_in_sales_eur,

return_event_count,
returned_basket_count,
returned_units,

CAST(
refund_amount_eur AS DECIMAL(18,2)
) AS refund_amount_eur,

CAST(
average_days_to_return AS DECIMAL(18,2)
) AS average_days_to_return,

CAST(
retained_sales_after_refunds_eur
AS DECIMAL(18,2)
) AS retained_sales_after_refunds_eur,

CAST(
weighted_average_selling_price_eur
AS DECIMAL(18,2)
) AS weighted_average_selling_price_eur,

CAST(
weighted_discount_rate_pct
AS DECIMAL(9,2)
) AS weighted_discount_rate_pct,

CAST(
basket_penetration_pct
AS DECIMAL(9,4)
) AS basket_penetration_pct,

CAST(
average_units_per_product_basket
AS DECIMAL(18,2)
) AS average_units_per_product_basket,

CAST(
promo_sales_share_pct
AS DECIMAL(9,2)
) AS promotion_sales_share_pct,

CAST(
member_sales_share_pct
AS DECIMAL(9,2)
) AS member_sales_share_pct,

CAST(
refund_rate_pct
AS DECIMAL(9,2)
) AS refund_rate_pct,

CAST(
returned_unit_rate_pct
AS DECIMAL(9,2)
) AS returned_unit_rate_pct,

revenue_rank,
units_rank,

CAST(
revenue_share_pct
AS DECIMAL(9,4)
) AS revenue_share_pct,

CAST(
cumulative_revenue_share_pct
AS DECIMAL(9,4)
) AS cumulative_revenue_share_pct,

pareto_class

FROM {{catalog}}.{{gold_schema}}.product_performance;


-- ============================================================================
-- 11. CUSTOMER LIFETIME VALUE
-- Grain: one trusted identified customer
-- ============================================================================

CREATE OR REPLACE VIEW
{{catalog}}.{{reporting_schema}}.v_customer_ltv
AS
SELECT
customer_sk AS customer_key,
customer_id,

age,
age_group,

CASE gender_code
WHEN 'M' THEN 'Male'
WHEN 'F' THEN 'Female'
WHEN 'D' THEN 'Diverse'
ELSE 'Unknown'
END AS gender,

is_member AS is_loyalty_member,

loyalty_card_id,
age_quality_status,

CAST(
first_purchase_date AS DATE
) AS first_purchase_date,

CAST(
last_purchase_date AS DATE
) AS last_purchase_date,

lifetime_baskets,
active_purchase_days,
lifetime_units,

CAST(
lifetime_pre_discount_sales_eur
AS DECIMAL(18,2)
) AS lifetime_gross_sales_eur,

CAST(
lifetime_discount_amount_eur
AS DECIMAL(18,2)
) AS lifetime_discount_amount_eur,

CAST(
lifetime_sales_eur
AS DECIMAL(18,2)
) AS lifetime_sales_eur,

CAST(
lifetime_private_label_sales_eur
AS DECIMAL(18,2)
) AS lifetime_private_label_sales_eur,

promo_baskets,
coupon_baskets,
self_checkout_baskets,

CAST(
average_basket_value_eur
AS DECIMAL(18,2)
) AS average_basket_value_eur,

CAST(
median_basket_value_eur
AS DECIMAL(18,2)
) AS median_basket_value_eur,

CAST(
average_units_per_basket
AS DECIMAL(18,2)
) AS average_units_per_basket,

return_event_count,
returned_basket_count,
returned_units,

CAST(
refund_amount_eur
AS DECIMAL(18,2)
) AS refund_amount_eur,

CAST(
average_days_to_return
AS DECIMAL(18,2)
) AS average_days_to_return,

observed_customer_lifetime_days,
recency_days,

CAST(
retained_lifetime_value_eur
AS DECIMAL(18,2)
) AS retained_lifetime_value_eur,

CAST(
lifetime_discount_rate_pct
AS DECIMAL(9,2)
) AS lifetime_discount_rate_pct,

CAST(
private_label_sales_share_pct
AS DECIMAL(9,2)
) AS private_label_sales_share_pct,

CAST(
promo_basket_share_pct
AS DECIMAL(9,2)
) AS promotion_basket_share_pct,

CAST(
coupon_basket_share_pct
AS DECIMAL(9,2)
) AS coupon_basket_share_pct,

CAST(
self_checkout_basket_share_pct
AS DECIMAL(9,2)
) AS self_checkout_basket_share_pct,

CAST(
refund_rate_pct
AS DECIMAL(9,2)
) AS refund_rate_pct,

CAST(
baskets_per_30_days
AS DECIMAL(18,3)
) AS baskets_per_30_days,

overall_ltv_rank,
ltv_rank_top_1000,

CAST(
ltv_percentile
AS DECIMAL(9,4)
) AS ltv_percentile,

customer_segment,
recency_segment

FROM {{catalog}}.{{gold_schema}}.customer_ltv;


-- ============================================================================
-- 12. RETURNS ANALYSIS
-- Grain: product x return reason
-- ============================================================================

CREATE OR REPLACE VIEW
{{catalog}}.{{reporting_schema}}.v_returns_analysis
AS
SELECT
product_sk AS product_key,
product_id,

product_name,
category,
subcategory,

default_brand AS brand,
price_band,

reason_code,

return_event_count,
returned_basket_count,
returning_customer_count,
returned_units,

CAST(
refund_amount_eur
AS DECIMAL(18,2)
) AS refund_amount_eur,

CAST(
average_refund_per_event_eur
AS DECIMAL(18,2)
) AS average_refund_per_event_eur,

CAST(
average_days_to_return
AS DECIMAL(18,2)
) AS average_days_to_return,

CAST(
first_return_date AS DATE
) AS first_return_date,

CAST(
last_return_date AS DATE
) AS last_return_date,

CAST(
product_return_event_share_pct
AS DECIMAL(9,2)
) AS product_return_event_share_pct,

CAST(
product_returned_unit_share_pct
AS DECIMAL(9,2)
) AS product_returned_unit_share_pct,

CAST(
product_refund_share_pct
AS DECIMAL(9,2)
) AS product_refund_share_pct,

CAST(
all_return_event_share_pct
AS DECIMAL(9,4)
) AS all_return_event_share_pct,

CAST(
all_refund_share_pct
AS DECIMAL(9,4)
) AS all_refund_share_pct,

reason_rank_within_product

FROM {{catalog}}.{{gold_schema}}.return_analysis;


-- ============================================================================
-- 13. HOURLY TRAFFIC
-- Grain: hour x weekday x store size
-- ============================================================================

CREATE OR REPLACE VIEW
{{catalog}}.{{reporting_schema}}.v_hourly_traffic
AS
SELECT
order_hour,
weekday_number,
weekday_name,
store_size_class,

basket_count,
units_sold,

CAST(
net_sales_eur
AS DECIMAL(18,2)
) AS net_sales_eur,

CAST(
discount_amount_eur
AS DECIMAL(18,2)
) AS discount_amount_eur,

CAST(
average_basket_value_eur
AS DECIMAL(18,2)
) AS average_basket_value_eur,

CAST(
median_basket_value_eur
AS DECIMAL(18,2)
) AS median_basket_value_eur,

CAST(
average_units_per_basket
AS DECIMAL(18,2)
) AS average_units_per_basket,

walk_in_baskets,
member_baskets,
registered_nonmember_baskets,
self_checkout_baskets,
promo_period_baskets,
coupon_baskets,

CAST(
walk_in_basket_share_pct
AS DECIMAL(9,2)
) AS walk_in_basket_share_pct,

CAST(
member_basket_share_pct
AS DECIMAL(9,2)
) AS member_basket_share_pct,

CAST(
self_checkout_basket_share_pct
AS DECIMAL(9,2)
) AS self_checkout_basket_share_pct,

CAST(
promo_basket_share_pct
AS DECIMAL(9,2)
) AS promo_basket_share_pct

FROM {{catalog}}.{{gold_schema}}.hourly_traffic;


-- ============================================================================
-- 14. DATA QUALITY SUMMARY
-- Grain: one Silver or Gold quality check
-- ============================================================================

CREATE OR REPLACE VIEW
{{catalog}}.{{reporting_schema}}.v_data_quality_summary
AS

SELECT
'Silver' AS layer,

check_name,
severity,

expected_value,
actual_value,

status,
description,
checked_at

FROM {{catalog}}.{{silver_schema}}.silver_quality_checks

UNION ALL

SELECT
'Gold' AS layer,

check_name,
severity,

expected_value,
actual_value,

status,
description,
checked_at

FROM {{catalog}}.{{gold_schema}}.gold_quality_checks;
