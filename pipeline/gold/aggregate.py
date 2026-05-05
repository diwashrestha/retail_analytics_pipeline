"""
Gold Layer — Business Aggregations (Delta + Parquet)
=====================================================
Reads clean silver Delta tables and produces pre-aggregated tables
optimized for Power BI dashboards and business reporting.

Each gold table answers specific business questions:

  gld_daily_sales         "How are we performing day-to-day?"
  gld_store_performance   "Which stores are over/underperforming?"
  gld_product_performance "What are our top sellers and slow movers?"
  gld_customer_ltv        "Who are our most valuable customers?"
  gld_basket_analysis     "What does a typical shopping trip look like?"
  gld_return_analysis     "Which products have return problems?"
  gld_hourly_traffic      "When should we staff more registers?"

Output format:
  - Delta tables in data/gold/<name>_delta/  (for SQL queries, time travel)
  - Parquet files in data/gold/<name>/       (for Power BI Desktop import)

Power BI connection:
  Get Data → Folder → data/gold/gld_daily_sales/ → select *.parquet

Run:
    python pipeline/gold/aggregate.py
    python pipeline/run_pipeline.py --layer gold
"""

import sys
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql import Window

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipeline.common.spark_session import get_spark, SILVER_DIR, GOLD_DIR, ensure_dirs


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_silver(spark, table_name: str):
    """Read a silver Delta table with existence check."""
    path = SILVER_DIR / table_name
    if not path.exists():
        raise FileNotFoundError(
            f"Silver table '{table_name}' not found at {path}. "
            f"Run the silver layer first."
        )
    return spark.read.format("delta").load(str(path))


def _write_gold(df, table_name: str) -> int:
    """Write gold table as Delta + Parquet.

    Delta: for versioned queries and time travel.
    Parquet: for Power BI Desktop import (can't read Delta natively).
    coalesce(1): single Parquet file per table — easier for Power BI.
    """
    # Delta (full fidelity, partitioned if large)
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(GOLD_DIR / f"{table_name}_delta"))
    )

    # Parquet (single file for Power BI import)
    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .parquet(str(GOLD_DIR / table_name))
    )

    row_count = df.count()
    print(f"    OK    {table_name:<28} → {row_count:>8,} rows")
    return row_count


# ═══════════════════════════════════════════════════════════════════════════
# Table 1: Daily Sales
# ═══════════════════════════════════════════════════════════════════════════

def agg_daily_sales(spark) -> int:
    """Daily revenue by store, region, and product category.

    Powers the main Power BI dashboard:
      - Revenue trend line (daily / weekly / monthly)
      - Regional comparison bar chart
      - Category mix stacked area chart
      - Year-over-year comparison (if multi-year data)

    Grain: one row per (date × store × category).
    """
    sales = _read_silver(spark, "fact_sales")

    daily = (
        sales
        .filter(F.col("dq_status") == "CLEAN")
        .groupBy(
            "order_date", "year_month", "year_quarter",
            "day_name", "day_of_week",
            "store_id", "city", "region", "size_class",
            "category", "subcategory",
        )
        .agg(
            F.countDistinct("basket_id").alias("n_baskets"),
            F.count("*").alias("n_line_items"),
            F.sum("quantity").alias("total_units_sold"),
            F.round(F.sum("net_revenue_eur"), 2).alias("net_revenue_eur"),
            F.round(F.sum("gross_revenue_eur"), 2).alias("gross_revenue_eur"),
            F.round(F.sum("vat_amount_eur"), 2).alias("vat_amount_eur"),
            F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
            F.countDistinct("customer_id").alias("unique_customers"),
            # Walk-in vs loyalty split
            F.sum(F.when(F.col("is_walk_in") == True, 1).otherwise(0))
             .alias("walk_in_baskets"),
        )
        .withColumn("revenue_per_basket", F.round(
            F.col("net_revenue_eur") / F.col("n_baskets"), 2
        ))
        .orderBy("order_date", "store_id", "category")
    )

    return _write_gold(daily, "gld_daily_sales")


# ═══════════════════════════════════════════════════════════════════════════
# Table 2: Store Performance Scorecard
# ═══════════════════════════════════════════════════════════════════════════

def agg_store_performance(spark) -> int:
    """Store-level KPIs with ranking and benchmarking.

    Powers the Store Performance dashboard page:
      - Store ranking by revenue
      - Revenue per basket comparison
      - Loyalty penetration rate
      - Return rate per store
      - Comparison to chain average

    Grain: one row per store.
    """
    sales   = _read_silver(spark, "fact_sales")
    clean   = sales.filter(F.col("dq_status") == "CLEAN")

    # Core metrics per store
    store_metrics = (
        clean.groupBy("store_id", "city", "region", "size_class")
        .agg(
            F.countDistinct("basket_id").alias("total_baskets"),
            F.count("*").alias("total_line_items"),
            F.sum("quantity").alias("total_units_sold"),
            F.round(F.sum("net_revenue_eur"), 2).alias("total_net_revenue"),
            F.round(F.sum("gross_revenue_eur"), 2).alias("total_gross_revenue"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.countDistinct("order_date").alias("active_trading_days"),
            F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
            # Loyalty penetration
            F.sum(F.when(F.col("is_walk_in") == False, 1).otherwise(0))
             .alias("loyalty_transactions"),
            F.sum(F.when(F.col("is_walk_in") == True, 1).otherwise(0))
             .alias("walk_in_transactions"),
        )
    )

    # Derived KPIs
    store_metrics = (
        store_metrics
        .withColumn("revenue_per_basket", F.round(
            F.col("total_net_revenue") / F.col("total_baskets"), 2
        ))
        .withColumn("revenue_per_day", F.round(
            F.col("total_net_revenue") / F.col("active_trading_days"), 2
        ))
        .withColumn("items_per_basket", F.round(
            F.col("total_line_items") / F.col("total_baskets"), 2
        ))
        .withColumn("loyalty_penetration_pct", F.round(
            F.col("loyalty_transactions") /
            (F.col("loyalty_transactions") + F.col("walk_in_transactions")) * 100, 1
        ))
    )

    # Return rate per store (if returns exist)
    returns_path = SILVER_DIR / "fact_returns"
    if returns_path.exists():
        returns = _read_silver(spark, "fact_returns")
        ret_by_store = (
            returns.groupBy("store_id")
            .agg(
                F.count("*").alias("return_count"),
                F.round(F.sum("refund_amount_eur"), 2).alias("total_refunds_eur"),
            )
        )
        store_metrics = (
            store_metrics
            .join(ret_by_store, on="store_id", how="left")
            .fillna(0, subset=["return_count", "total_refunds_eur"])
            .withColumn("return_rate_pct", F.round(
                F.col("return_count") / F.col("total_baskets") * 100, 2
            ))
            .withColumn("net_revenue_after_returns", F.round(
                F.col("total_net_revenue") - F.col("total_refunds_eur"), 2
            ))
        )

    # Rank stores by revenue
    w = Window.orderBy(F.col("total_net_revenue").desc())
    store_metrics = (
        store_metrics
        .withColumn("revenue_rank", F.row_number().over(w))
    )

    # Chain-wide averages for benchmarking
    chain_avg_rev   = store_metrics.agg(F.avg("total_net_revenue")).collect()[0][0]
    chain_avg_basket = store_metrics.agg(F.avg("revenue_per_basket")).collect()[0][0]

    store_metrics = (
        store_metrics
        .withColumn("vs_chain_avg_revenue_pct", F.round(
            (F.col("total_net_revenue") - F.lit(chain_avg_rev)) / F.lit(chain_avg_rev) * 100, 1
        ))
        .withColumn("vs_chain_avg_basket_pct", F.round(
            (F.col("revenue_per_basket") - F.lit(chain_avg_basket))
            / F.lit(chain_avg_basket) * 100, 1
        ))
    )

    return _write_gold(store_metrics, "gld_store_performance")


# ═══════════════════════════════════════════════════════════════════════════
# Table 3: Product Performance
# ═══════════════════════════════════════════════════════════════════════════

def agg_product_performance(spark) -> int:
    """Product-level performance with Pareto analysis.

    Powers the Product Insights dashboard page:
      - Top 20 sellers by revenue and units
      - Slow movers (bottom 20%)
      - Revenue concentration (what % of SKUs drive 80% of revenue)
      - Category performance comparison

    Grain: one row per product.
    """
    sales    = _read_silver(spark, "fact_sales")
    products = _read_silver(spark, "dim_product")
    clean    = sales.filter(F.col("dq_status") == "CLEAN")

    prod_metrics = (
        clean.groupBy("product_id")
        .agg(
            F.countDistinct("basket_id").alias("baskets_containing"),
            F.count("*").alias("times_purchased"),
            F.sum("quantity").alias("total_units_sold"),
            F.round(F.sum("net_revenue_eur"), 2).alias("total_net_revenue"),
            F.round(F.avg("unit_price_eur"), 2).alias("avg_selling_price"),
            F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
            F.countDistinct("store_id").alias("stores_selling"),
            F.countDistinct("order_date").alias("days_with_sales"),
        )
    )

    # Join product attributes
    prod_metrics = (
        prod_metrics
        .join(products.select(
            "product_id", "product_name", "category", "subcategory",
            "price_band", "price_min_eur", "price_max_eur"
        ), on="product_id", how="left")
    )

    # Revenue rank + cumulative % for Pareto analysis
    w_rev = Window.orderBy(F.col("total_net_revenue").desc())
    total_revenue = prod_metrics.agg(F.sum("total_net_revenue")).collect()[0][0]

    prod_metrics = (
        prod_metrics
        .withColumn("revenue_rank", F.row_number().over(w_rev))
        .withColumn("cumulative_revenue", F.sum("total_net_revenue").over(
            w_rev.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        ))
        .withColumn("cumulative_revenue_pct", F.round(
            F.col("cumulative_revenue") / F.lit(total_revenue) * 100, 2
        ))
        # Pareto classification
        .withColumn("pareto_class", F.when(
            F.col("cumulative_revenue_pct") <= 80, "A (top 80%)"
        ).when(
            F.col("cumulative_revenue_pct") <= 95, "B (next 15%)"
        ).otherwise("C (tail 5%)"))
        # Revenue per unit
        .withColumn("revenue_per_unit", F.round(
            F.col("total_net_revenue") / F.col("total_units_sold"), 2
        ))
    )

    # Log Pareto stats
    a_count = prod_metrics.filter(F.col("pareto_class") == "A (top 80%)").count()
    total_products = prod_metrics.count()
    print(f"          Pareto: {a_count} products ({a_count/max(total_products,1)*100:.0f}%) "
          f"drive 80% of revenue")

    return _write_gold(prod_metrics, "gld_product_performance")


# ═══════════════════════════════════════════════════════════════════════════
# Table 4: Customer Lifetime Value
# ═══════════════════════════════════════════════════════════════════════════

def agg_customer_ltv(spark) -> int:
    """Customer lifetime value with segmentation and demographics.

    Powers the Customer Insights dashboard page:
      - LTV distribution histogram
      - Segment breakdown (Champion / Loyal / Regular / Occasional)
      - Loyalty tier impact on basket value
      - Age group analysis
      - Repeat purchase frequency

    Grain: one row per customer (excludes walk-ins).
    """
    sales = _read_silver(spark, "fact_sales")

    ltv = (
        sales
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("dq_status") == "CLEAN")
        .groupBy("customer_id", "customer_loyalty_tier", "customer_age_group")
        .agg(
            F.min("order_date").alias("first_purchase_date"),
            F.max("order_date").alias("last_purchase_date"),
            F.countDistinct("basket_id").alias("total_baskets"),
            F.countDistinct("order_date").alias("active_days"),
            F.count("*").alias("total_items_bought"),
            F.round(F.sum("net_revenue_eur"), 2).alias("lifetime_net_revenue"),
            F.round(F.sum("gross_revenue_eur"), 2).alias("lifetime_gross_revenue"),
            F.round(F.avg("discount_pct"), 2).alias("avg_discount_used"),
            F.countDistinct("category").alias("categories_shopped"),
            F.countDistinct("store_id").alias("stores_visited"),
            # Most used payment method
            F.first("payment_type").alias("primary_payment_method"),
        )
    )

    ltv = (
        ltv
        .withColumn("avg_basket_value", F.round(
            F.col("lifetime_net_revenue") / F.col("total_baskets"), 2
        ))
        .withColumn("avg_items_per_basket", F.round(
            F.col("total_items_bought") / F.col("total_baskets"), 2
        ))
        .withColumn("tenure_days", F.datediff(
            F.col("last_purchase_date"), F.col("first_purchase_date")
        ))
        # Visit frequency: baskets per active month
        .withColumn("visits_per_month", F.round(F.when(
            F.col("tenure_days") > 0,
            F.col("total_baskets") / (F.col("tenure_days") / 30.0)
        ).otherwise(F.col("total_baskets")), 2))
        # Segmentation
        .withColumn("customer_segment", F.when(
            F.col("total_baskets") >= 50, "Champion"
        ).when(F.col("total_baskets") >= 20, "Loyal"
        ).when(F.col("total_baskets") >= 5, "Regular"
        ).otherwise("Occasional"))
    )

    # LTV rank
    w = Window.orderBy(F.col("lifetime_net_revenue").desc())
    ltv = ltv.withColumn("ltv_rank", F.row_number().over(w))

    # Log segment distribution
    for seg in ["Champion", "Loyal", "Regular", "Occasional"]:
        ct = ltv.filter(F.col("customer_segment") == seg).count()
        total = ltv.count()
        print(f"          {seg:<12}: {ct:>7,} ({ct/max(total,1)*100:.1f}%)")

    return _write_gold(ltv, "gld_customer_ltv")


# ═══════════════════════════════════════════════════════════════════════════
# Table 5: Basket Analysis
# ═══════════════════════════════════════════════════════════════════════════

def agg_basket_analysis(spark) -> int:
    """Basket-level metrics for shopping behavior analysis.

    Powers the Basket Analysis dashboard page:
      - Basket size distribution by store type
      - Payment method vs basket value
      - Coupon usage and discount impact
      - Walk-in vs loyalty basket comparison

    Grain: one row per basket (transaction).
    """
    sales = _read_silver(spark, "fact_sales")

    baskets = (
        sales
        .filter(F.col("dq_status") == "CLEAN")
        .groupBy(
            "basket_id", "transaction_id", "order_date", "year_month",
            "day_name", "hour_of_day",
            "store_id", "city", "size_class",
            "customer_id", "payment_type", "is_walk_in",
        )
        .agg(
            F.count("*").alias("items_in_basket"),
            F.sum("quantity").alias("total_units"),
            F.round(F.sum("net_revenue_eur"), 2).alias("basket_net_value"),
            F.round(F.sum("gross_revenue_eur"), 2).alias("basket_gross_value"),
            F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
            F.max("coupon_applied").alias("coupon_used"),
            F.countDistinct("category").alias("categories_in_basket"),
        )
    )

    # Basket value bucket for Power BI slicer
    baskets = baskets.withColumn("basket_value_bucket", F.when(
        F.col("basket_net_value") < 10, "< €10"
    ).when(F.col("basket_net_value") < 30, "€10-30"
    ).when(F.col("basket_net_value") < 75, "€30-75"
    ).otherwise("€75+"))

    # Customer type for loyalty vs walk-in comparison
    baskets = baskets.withColumn("customer_type", F.when(
        F.col("is_walk_in") == True, "Walk-in"
    ).otherwise("Loyalty"))

    return _write_gold(baskets, "gld_basket_analysis")


# ═══════════════════════════════════════════════════════════════════════════
# Table 6: Return Analysis
# ═══════════════════════════════════════════════════════════════════════════

def agg_return_analysis(spark) -> int:
    """Return rates by product, category, and reason.

    Powers the Returns & DQ dashboard page:
      - Return rate by product (top offenders)
      - Return reason breakdown
      - Revenue impact of returns
      - Return delay distribution
      - Net revenue after returns

    Grain: one row per (product × reason_code).
    """
    if not (SILVER_DIR / "fact_returns").exists():
        print(f"    SKIP  gld_return_analysis — no return data")
        return 0

    returns  = _read_silver(spark, "fact_returns")
    sales    = _read_silver(spark, "fact_sales")
    products = _read_silver(spark, "dim_product")

    # Return metrics per product × reason
    ret_agg = (
        returns.groupBy("product_id", "reason_code")
        .agg(
            F.count("*").alias("return_count"),
            F.sum("return_quantity").alias("returned_units"),
            F.round(F.sum("refund_amount_eur"), 2).alias("total_refund_eur"),
            F.round(F.avg("return_delay_days"), 1).alias("avg_delay_days"),
            F.min("return_date").alias("first_return_date"),
            F.max("return_date").alias("last_return_date"),
        )
    )

    # Sales denominator per product
    sales_agg = (
        sales
        .filter(F.col("dq_status") == "CLEAN")
        .groupBy("product_id")
        .agg(
            F.sum("quantity").alias("total_units_sold"),
            F.round(F.sum("net_revenue_eur"), 2).alias("total_net_revenue"),
            F.countDistinct("basket_id").alias("total_baskets_sold"),
        )
    )

    # Join and compute rates
    analysis = (
        ret_agg
        .join(sales_agg, on="product_id", how="left")
        .join(products.select(
            "product_id", "product_name", "category",
            "subcategory", "price_band"
        ), on="product_id", how="left")
        .withColumn("return_rate_pct", F.round(
            F.col("returned_units") / F.col("total_units_sold") * 100, 2
        ))
        .withColumn("revenue_impact_eur", F.round(
            F.col("total_refund_eur") * -1, 2
        ))
        .withColumn("net_revenue_after_returns", F.round(
            F.col("total_net_revenue") + F.col("revenue_impact_eur"), 2
        ))
        .orderBy(F.col("return_count").desc())
    )

    # Log top return reasons
    by_reason = (
        returns.groupBy("reason_code")
        .agg(F.count("*").alias("n"))
        .orderBy(F.col("n").desc())
        .collect()
    )
    for row in by_reason[:5]:
        total_returns = returns.count()
        print(f"          {row['reason_code']:<16}: {row['n']:>5,} "
              f"({row['n']/max(total_returns,1)*100:.0f}%)")

    return _write_gold(analysis, "gld_return_analysis")


# ═══════════════════════════════════════════════════════════════════════════
# Table 7: Hourly Traffic Heatmap
# ═══════════════════════════════════════════════════════════════════════════

def agg_hourly_traffic(spark) -> int:
    """Intraday traffic patterns by store type and day of week.

    Powers the Traffic Heatmap in Power BI:
      - Hour × Day matrix (color = basket count)
      - Overlay by store size class
      - Helps determine optimal staffing levels

    Validates that the generator's peak hours (10-12, 16-19) show
    up in the actual data — a good sanity check.

    Grain: one row per (hour × day × store_size_class).
    """
    sales = _read_silver(spark, "fact_sales")

    hourly = (
        sales
        .filter(F.col("dq_status") == "CLEAN")
        .groupBy("hour_of_day", "day_name", "day_of_week", "size_class")
        .agg(
            F.countDistinct("basket_id").alias("n_baskets"),
            F.count("*").alias("n_line_items"),
            F.round(F.sum("net_revenue_eur"), 2).alias("net_revenue_eur"),
            F.round(F.avg("net_revenue_eur"), 2).alias("avg_item_value"),
            F.countDistinct("order_date").alias("n_trading_days"),
        )
        # Average baskets per day (for staffing decisions)
        .withColumn("avg_baskets_per_day", F.round(
            F.col("n_baskets") / F.col("n_trading_days"), 1
        ))
        .orderBy("day_of_week", "hour_of_day", "size_class")
    )

    # Log peak hours
    peak = (
        hourly
        .groupBy("hour_of_day")
        .agg(F.sum("n_baskets").alias("total"))
        .orderBy(F.col("total").desc())
        .limit(3)
        .collect()
    )
    peak_str = ", ".join(f"{r['hour_of_day']:02d}:00 ({r['total']:,})" for r in peak)
    print(f"          Peak hours: {peak_str}")

    return _write_gold(hourly, "gld_hourly_traffic")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_gold(spark=None):
    """Execute the full gold layer.

    All 7 tables are built from silver fact_sales + dimensions.
    Each table maps to a Power BI dashboard page.
    """
    print("\n  ┌─ GOLD LAYER ──────────────────────────────────────┐")
    print("  │  Business aggregations (Delta + Parquet)           │")
    print("  └───────────────────────────────────────────────────┘")

    ensure_dirs()
    own_spark = spark is None
    if own_spark:
        spark = get_spark("einkaufpark_gold")

    total = 0

    print(f"\n  Revenue & Operations:")
    total += agg_daily_sales(spark)
    total += agg_store_performance(spark)
    total += agg_hourly_traffic(spark)

    print(f"\n  Product & Basket:")
    total += agg_product_performance(spark)
    total += agg_basket_analysis(spark)

    print(f"\n  Customer & Returns:")
    total += agg_customer_ltv(spark)
    total += agg_return_analysis(spark)

    # Summary
    print(f"\n  {'─' * 50}")
    print(f"  Gold complete: {total:,} total rows across 7 tables")
    print(f"  {'─' * 50}")
    print(f"  Power BI: Get Data → Folder → data/gold/gld_*/")

    # Print gold table sizes
    gold_path = Path(GOLD_DIR)
    if gold_path.exists():
        print(f"\n  Output files:")
        for item in sorted(gold_path.iterdir()):
            if item.is_dir() and not item.name.endswith("_delta"):
                total_bytes = sum(f.stat().st_size for f in item.rglob("*.parquet"))
                size_kb = total_bytes / 1024
                u = "KB" if size_kb < 1024 else "MB"
                s = size_kb if size_kb < 1024 else size_kb / 1024
                print(f"    {item.name:<34} {s:>8.1f} {u}")

    if own_spark:
        spark.stop()


if __name__ == "__main__":
    run_gold()