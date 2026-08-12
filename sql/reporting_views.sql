CREATE OR REPLACE VIEW {{catalog}}.{{reporting_schema}}.v_fact_sales AS
SELECT
    sales_line_sk AS sales_line_key,
    transaction_id,
    basket_id,

    CAST(order_date AS DATE) AS order_date,
    order_time,
    order_timestamp,

    store_sk AS store_key,
    store_id,

    customer_sk AS customer_key,
    customer_id,
    customer_type,

    product_sk AS product_key,
    product_id,

    quantity,

    CAST(effective_list_price_eur AS DECIMAL(18,2))
        AS list_price_eur,

    CAST(unit_price_eur AS DECIMAL(18,2))
        AS unit_price_eur,

    CAST(discount_pct AS DECIMAL(9,4))
        AS discount_pct,

    CAST(pre_discount_sales_eur AS DECIMAL(18,2))
        AS gross_sales_eur,

    CAST(discount_amount_eur AS DECIMAL(18,2))
        AS discount_amount_eur,

    CAST(net_sales_eur AS DECIMAL(18,2))
        AS net_sales_eur,

    CAST(net_sales_ex_vat_eur AS DECIMAL(18,2))
        AS net_sales_ex_vat_eur,

    CAST(vat_amount_eur AS DECIMAL(18,2))
        AS vat_amount_eur,

    CAST(vat_rate AS DECIMAL(9,4))
        AS vat_rate,

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



CREATE OR REPLACE VIEW {{catalog}}.{{reporting_schema}}.v_dim_store AS
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


CREATE OR REPLACE VIEW {{catalog}}.{{reporting_schema}}.v_dim_product AS
SELECT
    product_sk AS product_key,
    product_id,
    product_name,
    category,
    subcategory,
    default_brand AS brand,
    is_private_label_eligible,
    CAST(price_min_eur AS DECIMAL(18,2)) AS minimum_price_eur,
    CAST(price_max_eur AS DECIMAL(18,2)) AS maximum_price_eur,
    CAST(catalogue_mid_price_eur AS DECIMAL(18,2))
        AS catalogue_mid_price_eur,
    price_band,
    unit,
    seasonal_months,
    CAST(vat_rate AS DECIMAL(9,4)) AS vat_rate
FROM {{catalog}}.{{silver_schema}}.dim_product;



CREATE OR REPLACE VIEW {{catalog}}.{{reporting_schema}}.v_dim_product AS
SELECT
    product_sk AS product_key,
    product_id,
    product_name,
    category,
    subcategory,
    default_brand AS brand,
    is_private_label_eligible,
    CAST(price_min_eur AS DECIMAL(18,2)) AS minimum_price_eur,
    CAST(price_max_eur AS DECIMAL(18,2)) AS maximum_price_eur,
    CAST(catalogue_mid_price_eur AS DECIMAL(18,2))
        AS catalogue_mid_price_eur,
    price_band,
    unit,
    seasonal_months,
    CAST(vat_rate AS DECIMAL(9,4)) AS vat_rate
FROM {{catalog}}.{{silver_schema}}.dim_product;


CREATE OR REPLACE VIEW {{catalog}}.{{reporting_schema}}.v_dim_date AS

WITH bounds AS (
    SELECT
        MIN(order_date) AS min_date,
        MAX(order_date) AS max_date
    FROM {{catalog}}.{{silver_schema}}.fact_sales
),

dates AS (
    SELECT EXPLODE(
        SEQUENCE(min_date, max_date, INTERVAL 1 DAY)
    ) AS calendar_date
    FROM bounds
)

SELECT
    CAST(calendar_date AS DATE) AS date_key,
    calendar_date,

    YEAR(calendar_date) AS year,
    QUARTER(calendar_date) AS quarter_number,
    CONCAT('Q', QUARTER(calendar_date)) AS quarter,

    MONTH(calendar_date) AS month_number,
    DATE_FORMAT(calendar_date, 'MMMM') AS month_name,
    DATE_FORMAT(calendar_date, 'yyyy-MM') AS year_month,

    WEEKOFYEAR(calendar_date) AS week_of_year,

    DAY(calendar_date) AS day_of_month,
    DAYOFWEEK(calendar_date) AS day_of_week_number,
    DATE_FORMAT(calendar_date, 'EEEE') AS day_of_week,

    CASE
        WHEN DAYOFWEEK(calendar_date) IN (1, 7)
        THEN TRUE
        ELSE FALSE
    END AS is_weekend,

    CASE
        WHEN DAYOFWEEK(calendar_date) = 1
        THEN TRUE
        ELSE FALSE
    END AS is_sunday

FROM dates;


CREATE OR REPLACE VIEW {{catalog}}.{{reporting_schema}}.v_daily_sales AS
SELECT
    CAST(order_date AS DATE) AS order_date,

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

    CAST(pre_discount_sales_eur AS DECIMAL(18,2))
        AS gross_sales_eur,

    CAST(discount_amount_eur AS DECIMAL(18,2))
        AS discount_amount_eur,

    CAST(net_sales_eur AS DECIMAL(18,2))
        AS net_sales_eur,

    category_basket_count,
    category_walk_in_baskets,
    category_member_baskets,
    category_identified_customers,

    category_revenue_per_basket_eur,
    weighted_average_selling_price_eur,
    weighted_discount_rate_pct,
    private_label_sales_eur,
    private_label_sales_share_pct

FROM {{catalog}}.{{gold_schema}}.daily_sales;



CREATE OR REPLACE VIEW {{catalog}}.{{reporting_schema}}.v_fact_returns AS
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

    CAST(original_order_date AS DATE) AS original_order_date,
    CAST(return_date AS DATE) AS return_date,

    days_to_return,

    sold_quantity,
    original_quantity,
    return_quantity,

    CAST(original_unit_price_eur AS DECIMAL(18,2))
        AS original_unit_price_eur,

    CAST(original_discount_pct AS DECIMAL(9,4))
        AS original_discount_pct,

    CAST(net_unit_price_eur AS DECIMAL(18,2))
        AS net_unit_price_eur,

    CAST(refund_amount_eur AS DECIMAL(18,2))
        AS refund_amount_eur,

    reason_code

FROM {{catalog}}.{{silver_schema}}.fact_returns;



CREATE OR REPLACE VIEW {{catalog}}.{{reporting_schema}}.v_executive_kpis AS

WITH sales AS (
    SELECT
        COUNT(*) AS sales_line_count,
        COUNT(DISTINCT transaction_id) AS transaction_count,
        COUNT(DISTINCT basket_id) AS basket_count,
        COUNT(DISTINCT customer_id) AS identified_customer_count,

        SUM(quantity) AS units_sold,

        ROUND(SUM(pre_discount_sales_eur), 2)
            AS gross_sales_eur,

        ROUND(SUM(discount_amount_eur), 2)
            AS discount_amount_eur,

        ROUND(SUM(net_sales_eur), 2)
            AS net_sales_eur,

        ROUND(
            SUM(CASE WHEN customer_type = 'Walk-in'
                THEN net_sales_eur ELSE 0 END),
            2
        ) AS walk_in_sales_eur,

        ROUND(
            SUM(CASE WHEN customer_type = 'Loyalty Member'
                THEN net_sales_eur ELSE 0 END),
            2
        ) AS member_sales_eur,

        ROUND(
            SUM(CASE WHEN is_promo_period
                THEN net_sales_eur ELSE 0 END),
            2
        ) AS promotion_sales_eur,

        ROUND(
            SUM(CASE WHEN is_self_checkout
                THEN net_sales_eur ELSE 0 END),
            2
        ) AS self_checkout_sales_eur,

        MIN(order_date) AS first_sales_date,
        MAX(order_date) AS latest_sales_date

    FROM {{catalog}}.{{silver_schema}}.fact_sales
),

returns AS (
    SELECT
        COUNT(*) AS return_event_count,
        SUM(return_quantity) AS returned_units,
        ROUND(SUM(refund_amount_eur), 2) AS refund_amount_eur
    FROM {{catalog}}.{{silver_schema}}.fact_returns
)

SELECT
    s.*,

    ROUND(
        s.net_sales_eur / NULLIF(s.basket_count, 0),
        2
    ) AS average_basket_value_eur,

    r.return_event_count,
    r.returned_units,
    r.refund_amount_eur,

    ROUND(
        100.0 * r.returned_units
        / NULLIF(s.units_sold, 0),
        2
    ) AS returned_unit_rate_pct,

    ROUND(
        s.net_sales_eur - r.refund_amount_eur,
        2
    ) AS retained_sales_after_refunds_eur

FROM sales s
CROSS JOIN returns r;


CREATE OR REPLACE VIEW
{{catalog}}.{{reporting_schema}}.v_data_quality_summary AS

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