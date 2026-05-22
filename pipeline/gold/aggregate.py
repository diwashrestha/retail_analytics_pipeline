"""
Gold Layer — Business Aggregations (Delta Lake)
================================================
Reads silver Delta tables once, caches the join, and produces seven
pre-aggregated tables that map directly to Power BI dashboard pages.

Each gold table answers a specific business question:

  gld_daily_sales         "How are we performing day-to-day?"
  gld_store_performance   "Which stores are over/underperforming?"
  gld_product_performance "What are our top sellers and slow movers?"
  gld_customer_ltv        "Who are our most valuable customers?"
  gld_basket_analysis     "What does a typical shopping trip look like?"
  gld_return_analysis     "Which products have return problems?"
  gld_hourly_traffic      "When should we staff more registers?"

Success criteria — verified after every run:

  G1. Revenue reconciliation — sum(net_revenue) across silver and each gold
                                table that aggregates revenue must agree
                                to within 1 cent.
  G2. Row count sanity       — every gold table has >0 rows. Empty gold
                                tables break Power BI dashboards silently.
  G3. Walk-in segregation    — customer LTV excludes walk-ins; basket
                                analysis includes them with explicit flag.
  G4. Return rate range      — aggregate return rate within 2-6% (generator
                                default 4% ± reasonable tolerance).
  G5. Pareto sanity          — top 20% of products drive 60-90% of revenue
                                (Zipf-weighted catalogue should produce
                                something in this range).

What this gold does NOT do:
  - Re-read fact_sales for each aggregation (cached at entry).
  - Filter to dq_status == 'CLEAN' only (silver's INFO rows are valid sales).
  - Write Parquet alongside Delta (Power BI Delta connector handles it).
  - Rank 500K customers in a single shuffle (uses ntile bucketing instead).
"""

from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipeline.common.spark_session import get_spark, SILVER_DIR, GOLD_DIR, ensure_dirs


# ═══════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════

def read_silver(spark: SparkSession, name: str) -> DataFrame:
    """Read a silver Delta table or fail loudly."""
    path = SILVER_DIR / name
    if not path.exists():
        raise FileNotFoundError(
            f"Silver table '{name}' not found at {path}. "
            f"Run silver first: python pipeline/silver/transform.py"
        )
    return spark.read.format("delta").load(str(path))


def write_gold(df: DataFrame, name: str, *, partition_by: list[str] | None = None) -> int:
    """Single canonical Delta write. Power BI reads Delta natively via the
    Synapse / Databricks / Fabric connectors — no Parquet duplicate needed.
    If a non-Delta consumer needs CSV, export from the Delta table once.
    """
    w = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        w = w.partitionBy(*partition_by)
    w.save(str(GOLD_DIR / name))
    return df.count()


# ═══════════════════════════════════════════════════════════════════════════
# Aggregation 1: Daily Sales
# ═══════════════════════════════════════════════════════════════════════════

def agg_daily_sales(sales: DataFrame) -> int:
    """Daily revenue by store × category. Grain: (date × store × category)."""
    daily = (
        sales
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
            F.sum(F.when(F.col("is_walk_in"), 1).otherwise(0)).alias("walk_in_baskets"),
        )
        .withColumn("revenue_per_basket", F.round(
            F.col("net_revenue_eur") / F.col("n_baskets"), 2
        ))
    )
    return write_gold(daily, "gld_daily_sales", partition_by=["year_month"])


# ═══════════════════════════════════════════════════════════════════════════
# Aggregation 2: Store Performance
# ═══════════════════════════════════════════════════════════════════════════

def agg_store_performance(sales: DataFrame, returns: DataFrame) -> int:
    """Store-level KPIs with ranking and benchmarking. Grain: one row per store."""

    # Single pass over sales — all metrics computed in one groupBy.
    # Three customer kinds: loyalty members, identified non-members, walk-ins.
    # "membership_active" is the generator's per-row loyalty flag; is_walk_in
    # marks anonymous shoppers (NULL customer_id).
    store_metrics = (
        sales.groupBy("store_id", "city", "region", "size_class")
        .agg(
            F.countDistinct("basket_id").alias("total_baskets"),
            F.count("*").alias("total_line_items"),
            F.sum("quantity").alias("total_units_sold"),
            F.round(F.sum("net_revenue_eur"), 2).alias("total_net_revenue"),
            F.round(F.sum("gross_revenue_eur"), 2).alias("total_gross_revenue"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.countDistinct("order_date").alias("active_trading_days"),
            F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
            F.sum(F.when(F.col("membership_active"), 1).otherwise(0)).alias("member_transactions"),
            F.sum(F.when(F.col("is_walk_in"), 1).otherwise(0)).alias("walk_in_transactions"),
        )
        .withColumn("revenue_per_basket", F.round(
            F.col("total_net_revenue") / F.col("total_baskets"), 2
        ))
        .withColumn("revenue_per_day", F.round(
            F.col("total_net_revenue") / F.col("active_trading_days"), 2
        ))
        .withColumn("items_per_basket", F.round(
            F.col("total_line_items") / F.col("total_baskets"), 2
        ))
        # Member penetration: share of all line items attributed to a loyalty
        # member. (The remainder is identified non-members + walk-ins.)
        .withColumn("member_penetration_pct", F.round(
            F.col("member_transactions") / F.col("total_line_items") * 100, 1
        ))
    )

    # Returns aggregation — INNER if returns exist, otherwise zeros.
    if returns is not None:
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
    else:
        # Schema stability for downstream: always have these columns.
        store_metrics = (
            store_metrics
            .withColumn("return_count",              F.lit(0))
            .withColumn("total_refunds_eur",         F.lit(0.0))
            .withColumn("return_rate_pct",           F.lit(0.0))
            .withColumn("net_revenue_after_returns", F.col("total_net_revenue"))
        )

    # Store ranking — 50 stores fits in one partition safely.
    w = Window.orderBy(F.col("total_net_revenue").desc())
    store_metrics = store_metrics.withColumn("revenue_rank", F.row_number().over(w))

    # Chain averages — single agg call instead of two.
    avgs = store_metrics.agg(
        F.avg("total_net_revenue").alias("chain_avg_rev"),
        F.avg("revenue_per_basket").alias("chain_avg_basket"),
    ).collect()[0]

    store_metrics = (
        store_metrics
        .withColumn("vs_chain_avg_revenue_pct", F.round(
            (F.col("total_net_revenue") - F.lit(avgs["chain_avg_rev"]))
            / F.lit(avgs["chain_avg_rev"]) * 100, 1
        ))
        .withColumn("vs_chain_avg_basket_pct", F.round(
            (F.col("revenue_per_basket") - F.lit(avgs["chain_avg_basket"]))
            / F.lit(avgs["chain_avg_basket"]) * 100, 1
        ))
    )
    return write_gold(store_metrics, "gld_store_performance")


# ═══════════════════════════════════════════════════════════════════════════
# Aggregation 3: Product Performance with Pareto Analysis
# ═══════════════════════════════════════════════════════════════════════════

def agg_product_performance(sales: DataFrame) -> int:
    """Product-level performance with Pareto classification."""

    prod_metrics = (
        sales.groupBy("product_id", "product_name", "category",
                      "subcategory", "price_band")
        .agg(
            F.countDistinct("basket_id").alias("baskets_containing"),
            F.count("*").alias("times_purchased"),
            F.sum("quantity").alias("total_units_sold"),
            F.round(F.sum("net_revenue_eur"), 2).alias("total_net_revenue"),
            F.round(F.avg("unit_price_eur"), 2).alias("avg_selling_price"),
            F.round(F.avg("list_price_eur"), 2).alias("avg_list_price"),
            F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
            F.countDistinct("store_id").alias("stores_selling"),
            F.countDistinct("order_date").alias("days_with_sales"),
            # Honest gap between charged price and SCD2 list price.
            F.round(F.avg("price_delta_pct"), 2).alias("avg_price_delta_pct"),
        )
    )

    # Pareto: revenue rank + cumulative % within products.
    # Product count is in the thousands — safe for single-partition window.
    w_rev = Window.orderBy(F.col("total_net_revenue").desc())
    total_rev = prod_metrics.agg(F.sum("total_net_revenue")).collect()[0][0]

    prod_metrics = (
        prod_metrics
        .withColumn("revenue_rank", F.row_number().over(w_rev))
        .withColumn("cumulative_revenue", F.sum("total_net_revenue").over(w_rev))
        .withColumn("cumulative_revenue_pct", F.round(
            F.col("cumulative_revenue") / F.lit(total_rev) * 100, 2
        ))
        .withColumn("pareto_class", F.when(
            F.col("cumulative_revenue_pct") <= 80, "A (top 80%)"
        ).when(F.col("cumulative_revenue_pct") <= 95, "B (next 15%)"
        ).otherwise("C (tail 5%)"))
        .withColumn("revenue_per_unit", F.round(
            F.col("total_net_revenue") / F.col("total_units_sold"), 2
        ))
    )
    return write_gold(prod_metrics, "gld_product_performance")


# ═══════════════════════════════════════════════════════════════════════════
# Aggregation 4: Customer Lifetime Value
# ═══════════════════════════════════════════════════════════════════════════

def agg_customer_ltv(sales: DataFrame) -> int:
    """Per-customer LTV with segmentation. Excludes walk-ins (no FK to attribute)."""

    # Exclude walk-ins explicitly — they have no customer_id to attribute to.
    customer_sales = sales.filter(F.col("customer_id").isNotNull())

    ltv = (
        customer_sales
        .groupBy("customer_id", "customer_is_member", "customer_age_group")
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
            # Honest "most-used payment method" — actual mode, not F.first().
            # Computed below via window aggregation.
        )
        .withColumn("avg_basket_value", F.round(
            F.col("lifetime_net_revenue") / F.col("total_baskets"), 2
        ))
        .withColumn("avg_items_per_basket", F.round(
            F.col("total_items_bought") / F.col("total_baskets"), 2
        ))
        .withColumn("tenure_days", F.datediff(
            F.col("last_purchase_date"), F.col("first_purchase_date")
        ))
        .withColumn("visits_per_month", F.round(F.when(
            F.col("tenure_days") > 0,
            F.col("total_baskets") / (F.col("tenure_days") / 30.0)
        ).otherwise(F.col("total_baskets")), 2))
        .withColumn("customer_segment", F.when(
            F.col("total_baskets") >= 50, "Champion"
        ).when(F.col("total_baskets") >= 20, "Loyal"
        ).when(F.col("total_baskets") >= 5,  "Regular"
        ).otherwise(                          "Occasional"))
    )

    # Compute actual most-used payment method per customer (real mode, not F.first()).
    payment_mode = (
        customer_sales
        .groupBy("customer_id", "payment_type")
        .count()
        # Pick the payment_type with the highest count per customer.
        .withColumn("rn", F.row_number().over(
            Window.partitionBy("customer_id").orderBy(F.col("count").desc())
        ))
        .filter(F.col("rn") == 1)
        .select(
            "customer_id",
            F.col("payment_type").alias("most_used_payment_method"),
        )
    )
    ltv = ltv.join(payment_mode, on="customer_id", how="left")

    # LTV percentile bucketing — avoids single-partition window over 500K rows.
    # ntile(100) is partition-aware; gives each customer a 1-100 percentile.
    w_pct = Window.orderBy(F.col("lifetime_net_revenue").desc())
    ltv = ltv.withColumn("ltv_percentile", F.ntile(100).over(w_pct))
    # Top-K rank still useful for Power BI; only compute for top 1000 customers
    # to avoid shuffling 500K rows through a single executor.
    ltv = ltv.withColumn("ltv_rank_top_1000", F.when(
        F.col("ltv_percentile") <= (1000 * 100 / 500000),   # top ~1000
        F.row_number().over(w_pct)
    ).otherwise(F.lit(None)))

    return write_gold(ltv, "gld_customer_ltv", partition_by=["customer_segment"])


# ═══════════════════════════════════════════════════════════════════════════
# Aggregation 5: Basket Analysis
# ═══════════════════════════════════════════════════════════════════════════

def agg_basket_analysis(sales: DataFrame) -> int:
    """Per-basket shopping behavior. Grain: one row per transaction."""

    baskets = (
        sales.groupBy(
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
        .withColumn("basket_value_bucket", F.when(
            F.col("basket_net_value") < 10,  "< €10"
        ).when(F.col("basket_net_value") < 30,  "€10-30"
        ).when(F.col("basket_net_value") < 75,  "€30-75"
        ).otherwise(                            "€75+"))
        .withColumn("customer_type", F.when(
            F.col("is_walk_in"), "Walk-in"
        ).otherwise(             "Loyalty"))
    )
    return write_gold(baskets, "gld_basket_analysis", partition_by=["year_month"])


# ═══════════════════════════════════════════════════════════════════════════
# Aggregation 6: Return Analysis
# ═══════════════════════════════════════════════════════════════════════════

def agg_return_analysis(sales: DataFrame, returns: DataFrame | None) -> int:
    """Return rates per product × reason."""
    if returns is None:
        print(f"    SKIP  gld_return_analysis — no silver return data")
        return 0

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

    sales_agg = (
        sales.groupBy("product_id", "product_name", "category",
                      "subcategory", "price_band")
        .agg(
            F.sum("quantity").alias("total_units_sold"),
            F.round(F.sum("net_revenue_eur"), 2).alias("total_net_revenue"),
            F.countDistinct("basket_id").alias("total_baskets_sold"),
        )
    )

    analysis = (
        ret_agg
        .join(sales_agg, on="product_id", how="left")
        .withColumn("return_rate_pct", F.round(
            F.col("returned_units") / F.col("total_units_sold") * 100, 2
        ))
        .withColumn("revenue_impact_eur", F.round(
            F.col("total_refund_eur") * -1, 2
        ))
        .withColumn("net_revenue_after_returns", F.round(
            F.col("total_net_revenue") + F.col("revenue_impact_eur"), 2
        ))
    )
    return write_gold(analysis, "gld_return_analysis")


# ═══════════════════════════════════════════════════════════════════════════
# Aggregation 7: Hourly Traffic
# ═══════════════════════════════════════════════════════════════════════════

def agg_hourly_traffic(sales: DataFrame) -> int:
    """Intraday traffic by hour × day × store size."""
    hourly = (
        sales.groupBy("hour_of_day", "day_name", "day_of_week", "size_class")
        .agg(
            F.countDistinct("basket_id").alias("n_baskets"),
            F.count("*").alias("n_line_items"),
            F.round(F.sum("net_revenue_eur"), 2).alias("net_revenue_eur"),
            F.round(F.avg("net_revenue_eur"), 2).alias("avg_item_value"),
            F.countDistinct("order_date").alias("n_trading_days"),
        )
        .withColumn("avg_baskets_per_day", F.round(
            F.col("n_baskets") / F.col("n_trading_days"), 1
        ))
    )
    return write_gold(hourly, "gld_hourly_traffic")


# ═══════════════════════════════════════════════════════════════════════════
# Validation — success criteria from the docstring, enforced
# ═══════════════════════════════════════════════════════════════════════════

def check_revenue_reconciliation(spark: SparkSession,
                                   sales: DataFrame) -> tuple[bool, str]:
    """G1: revenue totals in silver and gold agree to within 1 cent."""
    silver_rev = sales.agg(F.sum("net_revenue_eur")).collect()[0][0] or 0.0

    daily = spark.read.format("delta").load(str(GOLD_DIR / "gld_daily_sales"))
    daily_rev = daily.agg(F.sum("net_revenue_eur")).collect()[0][0] or 0.0

    stores = spark.read.format("delta").load(str(GOLD_DIR / "gld_store_performance"))
    stores_rev = stores.agg(F.sum("total_net_revenue")).collect()[0][0] or 0.0

    products = spark.read.format("delta").load(str(GOLD_DIR / "gld_product_performance"))
    prods_rev = products.agg(F.sum("total_net_revenue")).collect()[0][0] or 0.0

    deltas = {
        "daily":    abs(silver_rev - daily_rev),
        "stores":   abs(silver_rev - stores_rev),
        "products": abs(silver_rev - prods_rev),
    }
    bad = {k: v for k, v in deltas.items() if v > 0.01}
    if bad:
        return False, f"FAIL: silver=€{silver_rev:,.2f}, deltas={bad}"
    return True, f"silver=€{silver_rev:,.2f}, all tables reconcile"


def check_nonempty(spark: SparkSession) -> tuple[bool, str]:
    """G2: every gold table has rows."""
    tables = ["gld_daily_sales", "gld_store_performance", "gld_product_performance",
              "gld_customer_ltv", "gld_basket_analysis", "gld_return_analysis",
              "gld_hourly_traffic"]
    empty = []
    for t in tables:
        path = GOLD_DIR / t
        if not path.exists():
            empty.append(f"{t}(missing)")
            continue
        n = spark.read.format("delta").load(str(path)).count()
        if n == 0:
            empty.append(t)
    if empty:
        return False, f"FAIL: empty tables: {empty}"
    return True, f"all 7 tables non-empty"


def check_walkin_segregation(spark: SparkSession) -> tuple[bool, str]:
    """G3: customer LTV excludes walk-ins; basket analysis includes them."""
    ltv = spark.read.format("delta").load(str(GOLD_DIR / "gld_customer_ltv"))
    ltv_walkins = ltv.filter(F.col("customer_id").isNull()).count()
    if ltv_walkins > 0:
        return False, f"FAIL: {ltv_walkins} walk-ins in LTV (should be 0)"

    baskets = spark.read.format("delta").load(str(GOLD_DIR / "gld_basket_analysis"))
    basket_walkins = baskets.filter(F.col("customer_type") == "Walk-in").count()
    basket_total   = baskets.count()
    walkin_pct     = basket_walkins / max(basket_total, 1) * 100
    if basket_walkins == 0:
        return False, f"FAIL: no walk-ins in basket analysis (generator produces ~10%)"
    return True, f"LTV: 0 walk-ins (correct), baskets: {walkin_pct:.1f}% walk-in"


def check_return_rate(spark: SparkSession, sales: DataFrame) -> tuple[bool, str]:
    """G4: aggregate return rate within reasonable range."""
    returns_path = SILVER_DIR / "fact_returns"
    if not returns_path.exists():
        return True, "skipped (no returns data)"

    returns = spark.read.format("delta").load(str(returns_path))
    n_returns = returns.count()
    n_baskets = sales.select("basket_id").distinct().count()
    rate = n_returns / max(n_baskets, 1) * 100
    ok = 2.0 <= rate <= 6.0
    return ok, f"return rate={rate:.2f}% ({n_returns:,} returns / {n_baskets:,} baskets)"


def check_pareto_sanity(spark: SparkSession) -> tuple[bool, str]:
    """G5: top 20% of products drive 60-90% of revenue (Zipf-weighted catalogue)."""
    products = spark.read.format("delta").load(str(GOLD_DIR / "gld_product_performance"))
    n_total = products.count()
    top_20 = max(1, int(n_total * 0.20))

    top_rev = (products
        .orderBy(F.col("total_net_revenue").desc())
        .limit(top_20)
        .agg(F.sum("total_net_revenue")).collect()[0][0] or 0.0
    )
    total_rev = products.agg(F.sum("total_net_revenue")).collect()[0][0] or 0.0
    pct = top_rev / max(total_rev, 1) * 100
    ok = 60.0 <= pct <= 90.0
    return ok, f"top 20% products drive {pct:.1f}% of revenue (expected 60-90%)"


def validate(spark: SparkSession, sales: DataFrame) -> bool:
    print(f"\n  Validation {chr(9472)*52}")
    checks = [
        ("G1 revenue reconciliation", lambda: check_revenue_reconciliation(spark, sales)),
        ("G2 non-empty tables",       lambda: check_nonempty(spark)),
        ("G3 walk-in segregation",    lambda: check_walkin_segregation(spark)),
        ("G4 return rate",            lambda: check_return_rate(spark, sales)),
        ("G5 Pareto sanity",          lambda: check_pareto_sanity(spark)),
    ]
    all_pass = True
    for name, fn in checks:
        ok, msg = fn()
        print(f"    [{'PASS' if ok else 'FAIL'}] {name:<28} {msg}")
        if not ok:
            all_pass = False
    print(f"  {chr(9472)*60}")
    return all_pass

def export_gold_to_parquet(spark, gold_dir, export_dir):
    import shutil
    from pathlib import Path
    
    tables = [
        "gld_daily_sales",
        "gld_store_performance",
        "gld_product_performance",
        "gld_customer_ltv",
        "gld_basket_analysis",
        "gld_return_analysis",
        "gld_hourly_traffic",
    ]
    
    for table in tables:
        delta_path = gold_dir / table
        if not delta_path.exists():
            continue
        df = spark.read.format("delta").load(str(delta_path))
        
        # For small tables (< 1M rows) you can safely write a single file.
        # Large tables may need more partitions, but Power BI reads multiple Parquet files fine.
        export_path = export_dir / table
        df.coalesce(1).write.mode("overwrite").parquet(str(export_path))
        print(f"   Exported {table} -> {export_path}")


# ═══════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════

def run_gold(spark: SparkSession | None = None) -> bool:
    """Execute all seven aggregations against a single cached fact_sales.

    The cache is the single highest-impact optimization. Previously each of
    the 7 functions independently re-read silver/fact_sales from disk — Spark
    cannot share intermediate state between independent reads. One cache call
    eliminates ~6 redundant Delta scans.
    """
    print("\n  ┌─ GOLD LAYER ──────────────────────────────────────┐")
    print("  │  Business aggregations (Delta)                     │")
    print("  └────────────────────────────────────────────────────┘")

    ensure_dirs()
    own_spark = spark is None
    if own_spark:
        spark = get_spark("einkaufpark_gold")

    # ── Read once, cache, pass to every aggregator ─────────────────────
    sales = read_silver(spark, "fact_sales").cache()

    # Returns may not exist (no returns flag in generator); handle gracefully.
    returns_path = SILVER_DIR / "fact_returns"
    returns = (
        spark.read.format("delta").load(str(returns_path)).cache()
        if returns_path.exists() else None
    )

    n_sales = sales.count()       # triggers cache materialization
    n_returns = returns.count() if returns is not None else 0
    print(f"\n  Inputs: {n_sales:,} sales rows, {n_returns:,} returns rows (cached)")

    # ── Revenue & Operations ────────────────────────────────────────────
    print(f"\n  Revenue & Operations:")
    n_daily   = agg_daily_sales(sales)
    n_stores  = agg_store_performance(sales, returns)
    n_hourly  = agg_hourly_traffic(sales)
    print(f"    daily={n_daily:,}  stores={n_stores:,}  hourly={n_hourly:,}")

    # ── Product & Basket ────────────────────────────────────────────────
    print(f"\n  Product & Basket:")
    n_prod    = agg_product_performance(sales)
    n_basket  = agg_basket_analysis(sales)
    print(f"    products={n_prod:,}  baskets={n_basket:,}")

    # ── Customer & Returns ──────────────────────────────────────────────
    print(f"\n  Customer & Returns:")
    n_ltv     = agg_customer_ltv(sales)
    n_returns_table = agg_return_analysis(sales, returns)
    print(f"    customers={n_ltv:,}  returns={n_returns_table:,}")

    # ── Validation ──────────────────────────────────────────────────────
    ok = validate(spark, sales)

    # Release caches.
    sales.unpersist()
    if returns is not None:
        returns.unpersist()

    print(f"\n  {chr(9472)*60}")
    print(f"  Gold {'complete' if ok else 'FAILED'}")
    print(f"  Output : {GOLD_DIR}/")
    print(f"  {chr(9472)*60}")
    
    # Export gold tables to Parquet for Power BI
    export_gold_to_parquet(spark, GOLD_DIR, Path("data/powerbi"))

    if own_spark:
        spark.stop()
    return ok


if __name__ == "__main__":
    sys.exit(0 if run_gold() else 1)