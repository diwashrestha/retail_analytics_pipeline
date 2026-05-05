"""
Silver Layer — Clean & Conform (Delta Lake)
=============================================
Reads bronze Delta tables, joins facts to dimensions, resolves DQ
issues, validates business rules, and produces clean star-schema
Delta tables ready for gold-layer aggregation.

Pipeline:
  bronze/dim_stores       ──►  silver/dim_store        (trimmed, standardized)
  bronze/dim_products     ──►  silver/dim_product       (typed, price bands)
  bronze/dim_customers    ──►  silver/dim_customer      (age validated, grouped)
  bronze/transactions     ──►  silver/fact_sales        (3-way JOIN, DQ parsed)
  bronze/transactions     ──►  silver/fact_sales_all    (includes REVIEW rows)
  bronze/returns          ──►  silver/fact_returns      (linked, delay computed)
  bronze/quarantine       ──►  (stays in bronze — not promoted to silver)

Engineering work in this layer:
  1.  3-way JOIN:       fact → dim_store, dim_product, dim_customer
  2.  Price validation: flag when unit_price outside catalogue range
  3.  VAT computation:  gross_revenue = net × (1 + vat_rate)
  4.  Walk-in handling: NULL customer_id preserved via LEFT JOIN
  5.  DQ status:        parse flag strings → CLEAN / INFO / REVIEW
  6.  Return linkage:   compute delay_days between purchase and return
  7.  Temporal columns: day_of_week, hour_of_day, year_month for analytics

Run:
    python pipeline/silver/transform.py
    python pipeline/run_pipeline.py --layer silver
"""

import sys
from pathlib import Path

from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipeline.common.spark_session import get_spark, BRONZE_DIR, SILVER_DIR, ensure_dirs


# ---------------------------------------------------------------------------
# Bronze readers
# ---------------------------------------------------------------------------

def _read_bronze(spark, table_name: str):
    """Read a bronze Delta table with existence check."""
    path = BRONZE_DIR / table_name
    if not path.exists():
        raise FileNotFoundError(
            f"Bronze table '{table_name}' not found at {path}. "
            f"Run the bronze layer first: python pipeline/bronze/ingest_raw.py"
        )
    return spark.read.format("delta").load(str(path))


def _write_silver(df, table_name: str):
    """Write a silver Delta table"""
    output_path = str(SILVER_DIR / table_name)
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(output_path)
    )
    return df.count()


# ---------------------------------------------------------------------------
# Dimension transforms
# ---------------------------------------------------------------------------

def transform_dim_store(spark) -> int:
    """Clean and standardize store dimension.

    Transformations:
      - Trim whitespace from store_id (defensive — CSV parsing artifact)
      - Drop bronze metadata columns (_ingested_at, _source_file)
      - Rename 'street' → 'store_address' for clarity
    """
    stores = _read_bronze(spark, "dim_stores")

    stores = (
        stores
        .withColumn("store_id", F.trim(F.col("store_id")))
        .withColumn("city", F.trim(F.col("city")))
        .withColumn("region", F.trim(F.col("region")))
        .drop("_ingested_at", "_source_file")
    )

    # Rename 'street' if present (some generator versions use 'area')
    if "street" in stores.columns:
        stores = stores.withColumnRenamed("street", "store_address")

    count = _write_silver(stores, "dim_store")
    print(f"    OK    dim_store              → {count:>8,} rows")
    return count


def transform_dim_product(spark) -> int:
    """Clean and enrich product dimension.

    Transformations:
      - Cast price and vat_rate to proper numeric types
      - Add price_band column for Power BI slicers
      - Validate price_min < price_max
      - Drop bronze metadata columns
    """
    products = _read_bronze(spark, "dim_products")

    products = (
        products
        .withColumn("product_id", F.trim(F.col("product_id")))
        .withColumn("vat_rate",      F.col("vat_rate").cast("double"))
        .withColumn("price_min_eur", F.col("price_min_eur").cast("double"))
        .withColumn("price_max_eur", F.col("price_max_eur").cast("double"))
        .drop("_ingested_at", "_source_file")
    )

    # Price band for dashboard slicers
    products = products.withColumn("price_band", F.when(
        F.col("price_max_eur") <= 2.0, "Budget (≤€2)"
    ).when(
        F.col("price_max_eur") <= 5.0, "Mid (€2-5)"
    ).when(
        F.col("price_max_eur") <= 10.0, "Premium (€5-10)"
    ).otherwise("High (€10+)"))

    # Validate: price_min should be <= price_max
    invalid_prices = products.filter(
        F.col("price_min_eur") > F.col("price_max_eur")
    ).count()
    if invalid_prices > 0:
        print(f"    WARN  {invalid_prices} products have price_min > price_max")

    count = _write_silver(products, "dim_product")
    print(f"    OK    dim_product            → {count:>8,} rows")
    return count


def transform_dim_customer(spark) -> int:
    """Clean and enrich customer dimension.

    Transformations:
      - Cast age to integer
      - Validate age range (0-120), flag invalids
      - Add age_group column for demographic analysis
      - Handle NULL ages (walk-in registrations with no DOB)
      - Drop bronze metadata columns
    """
    customers = _read_bronze(spark, "dim_customers")

    customers = (
        customers
        .withColumn("customer_id", F.trim(F.col("customer_id")))
        .withColumn("age", F.col("age").cast("int"))
        .drop("_ingested_at", "_source_file")
    )

    # Age validation flag
    customers = customers.withColumn("age_valid", F.when(
        F.col("age").isNull(), True            # NULL age is OK (no DOB on file)
    ).when(
        F.col("age").between(0, 120), True     # valid range
    ).otherwise(False))                         # e.g. -5 or 999

    # Age group for demographic analysis in Power BI
    customers = customers.withColumn("age_group", F.when(
        F.col("age").isNull(), "Unknown"
    ).when(F.col("age") < 25, "18-24"
    ).when(F.col("age") < 35, "25-34"
    ).when(F.col("age") < 50, "35-49"
    ).when(F.col("age") < 65, "50-64"
    ).otherwise("65+"))

    # Log profiling stats
    total     = customers.count()
    null_age  = customers.filter(F.col("age").isNull()).count()
    bad_age   = customers.filter(F.col("age_valid") == False).count()

    count = _write_silver(customers, "dim_customer")
    print(f"    OK    dim_customer           → {count:>8,} rows")
    print(f"          null age: {null_age:,}  "
          f"invalid age: {bad_age:,}  "
          f"({null_age/max(total,1)*100:.1f}% null, "
          f"{bad_age/max(total,1)*100:.2f}% invalid)")
    return count


# ---------------------------------------------------------------------------
# Fact sales — the core 3-way JOIN
# ---------------------------------------------------------------------------

def transform_fact_sales(spark) -> int:
    """Join transactions to all three dimensions.

    This is the most important transformation in the pipeline.

    Join strategy:
      - INNER JOIN on store_id:   every transaction must have a valid store
      - INNER JOIN on product_id: every transaction must have a valid product
      - LEFT JOIN on customer_id: ~30% are walk-ins with NULL customer_id

    Derived columns:
      - gross_revenue_eur:  net_revenue × (1 + vat_rate)
      - vat_amount_eur:     net_revenue × vat_rate
      - day_of_week:        1=Sunday … 7=Saturday (Spark convention)
      - day_name:           Monday, Tuesday, etc.
      - year_month:         yyyy-MM for time series grouping
      - hour_of_day:        extracted from order_time string
      - price_vs_catalogue: flags transactions where unit_price is outside
                            the product's [price_min, price_max] range

    DQ status:
      - CLEAN:       data_quality_flag = 'OK'
      - INFO:        flag contains INFO (e.g., LATE_ARRIVAL)
      - REVIEW:      flag contains WARN (e.g., UNKNOWN_TERMINAL)
      - QUARANTINED: flag contains ERR (shouldn't exist — caught in bronze)

    Output:
      - fact_sales_all:  all rows (CLEAN + INFO + REVIEW) for DQ analysis
      - fact_sales:      CLEAN + INFO only — for gold layer consumption
    """
    txn_path = BRONZE_DIR / "transactions"
    if not txn_path.exists():
        print(f"    SKIP  fact_sales — bronze/transactions not found")
        return 0

    # ── Read bronze facts + silver dimensions ─────────────────────────
    txn       = spark.read.format("delta").load(str(txn_path))
    stores    = spark.read.format("delta").load(str(SILVER_DIR / "dim_store"))
    products  = spark.read.format("delta").load(str(SILVER_DIR / "dim_product"))
    customers = spark.read.format("delta").load(str(SILVER_DIR / "dim_customer"))

    # ── Type casting ──────────────────────────────────────────────────
    # CSV inference may have gotten these wrong (string instead of numeric)
    txn = (
        txn
        .withColumn("quantity",        F.col("quantity").cast("int"))
        .withColumn("unit_price_eur",  F.col("unit_price_eur").cast("double"))
        .withColumn("discount_pct",    F.col("discount_pct").cast("double"))
        .withColumn("net_revenue_eur", F.col("net_revenue_eur").cast("double"))
        .withColumn("order_date",      F.to_date(F.col("order_date")))
    )

    # Count before join for orphan detection
    txn_count_before = txn.count()

    # ── 3-WAY JOIN ────────────────────────────────────────────────────
    joined = (
        txn
        # Store dimension — INNER: every transaction must have a store
        .join(
            F.broadcast(stores.select(
                "store_id", "city", "region", "size_class"
            )),
            on="store_id",
            how="inner"
        )
        # Product dimension — INNER: every transaction must have a product
        .join(
            products.select(
                "product_id", "product_name", "category", "subcategory",
                "vat_rate", "price_min_eur", "price_max_eur", "price_band"
            ),
            on="product_id",
            how="inner"
        )
        # Customer dimension — LEFT: walk-ins have NULL customer_id
        .join(
            customers.select(
                "customer_id", "age", "gender_code",
                "loyalty_tier", "age_group"
            )
            .withColumnRenamed("age", "customer_age")
            .withColumnRenamed("loyalty_tier", "customer_loyalty_tier")
            .withColumnRenamed("age_group", "customer_age_group"),
            on="customer_id",
            how="left"
        )
    )

    # Detect rows lost in INNER JOIN (orphan store/product IDs)
    txn_count_after = joined.count()
    orphans = txn_count_before - txn_count_after
    if orphans > 0:
        print(f"    WARN  {orphans:,} rows dropped by INNER JOIN "
              f"(orphan store_id or product_id)")

    # ── Derived columns ───────────────────────────────────────────────

    joined = (
        joined
        # Revenue: gross = net + VAT
        .withColumn("gross_revenue_eur", F.round(
            F.col("net_revenue_eur") * (1 + F.col("vat_rate")), 2
        ))
        .withColumn("vat_amount_eur", F.round(
            F.col("net_revenue_eur") * F.col("vat_rate"), 2
        ))

        # Temporal columns for analytics
        .withColumn("day_of_week",  F.dayofweek(F.col("order_date")))
        .withColumn("day_name",     F.date_format(F.col("order_date"), "EEEE"))
        .withColumn("year_month",   F.date_format(F.col("order_date"), "yyyy-MM"))
        .withColumn("year_quarter", F.concat(
            F.year(F.col("order_date")).cast("string"),
            F.lit("-Q"),
            F.quarter(F.col("order_date")).cast("string")
        ))
        .withColumn("hour_of_day",  F.substring(F.col("order_time"), 1, 2).cast("int"))

        # Price validation: is unit_price within catalogue range?
        .withColumn("price_vs_catalogue", F.when(
            F.col("unit_price_eur") < F.col("price_min_eur"), "BELOW_RANGE"
        ).when(
            F.col("unit_price_eur") > F.col("price_max_eur"), "ABOVE_RANGE"
        ).otherwise("IN_RANGE"))

        # Walk-in flag: customer_id is NULL → no loyalty data
        .withColumn("is_walk_in", F.col("customer_id").isNull())
    )

    # ── DQ status ─────────────────────────────────────────────────────
    # Bronze already quarantined ERR rows — silver only sees clean + WARN.
    # But we still check for ERR defensively.

    joined = joined.withColumn("dq_status", F.when(
        F.col("data_quality_flag").contains("ERR"), "QUARANTINED"
    ).when(
        F.col("data_quality_flag").contains("WARN"), "REVIEW"
    ).when(
        F.col("data_quality_flag").contains("INFO"), "INFO"
    ).otherwise("CLEAN"))

    # ── Drop columns not needed downstream ────────────────────────────
    # Keep price_min/max for analysis but drop bronze metadata
    drop_cols = [c for c in ["_ingested_at", "_source_file",
                             "ingestion_ts", "_batch_date"]
                 if c in joined.columns]
    if drop_cols:
        joined = joined.drop(*drop_cols)

    # ── Write: full table and clean-only table ────────────────────────

    total = _write_silver(joined, "fact_sales_all")

    clean = joined.filter(F.col("dq_status").isin("CLEAN", "INFO"))
    clean_ct = _write_silver(clean, "fact_sales")

    # ── Profiling output ──────────────────────────────────────────────
    review_ct   = total - clean_ct
    walk_in_ct  = joined.filter(F.col("is_walk_in") == True).count()
    price_below = joined.filter(F.col("price_vs_catalogue") == "BELOW_RANGE").count()
    price_above = joined.filter(F.col("price_vs_catalogue") == "ABOVE_RANGE").count()

    print(f"    OK    fact_sales_all         → {total:>8,} rows")
    print(f"    OK    fact_sales             → {clean_ct:>8,} rows (clean + info)")
    print(f"    WARN  review rows            → {review_ct:>8,} rows "
          f"({review_ct/max(total,1)*100:.1f}%)")
    print(f"          walk-ins (null cust)   : {walk_in_ct:,} "
          f"({walk_in_ct/max(total,1)*100:.1f}%)")
    print(f"          price below catalogue  : {price_below:,}")
    print(f"          price above catalogue  : {price_above:,}")

    return clean_ct


# ---------------------------------------------------------------------------
# Returns — linked to original transactions
# ---------------------------------------------------------------------------

def transform_returns(spark) -> int:
    """Clean returns and compute delay metrics.

    Transformations:
      - Cast types (quantity, price, refund amounts)
      - Parse return_date as proper date type
      - Join to fact_sales to get original order_date
      - Compute return_delay_days (days between purchase and return)
      - Drop bronze metadata
    """
    returns_path = BRONZE_DIR / "returns"
    if not returns_path.exists():
        print(f"    SKIP  fact_returns — no bronze return data")
        return 0

    returns = (
        spark.read.format("delta").load(str(returns_path))
        .withColumn("return_quantity",   F.col("return_quantity").cast("int"))
        .withColumn("unit_price_eur",    F.col("unit_price_eur").cast("double"))
        .withColumn("refund_amount_eur", F.col("refund_amount_eur").cast("double"))
        .withColumn("return_date",       F.to_date(F.col("return_date")))
        .drop("_ingested_at", "_source_file")
    )

    # Join to fact_sales to get original order_date for delay computation
    fact_sales_path = SILVER_DIR / "fact_sales_all"
    if fact_sales_path.exists():
        # Get distinct transaction → order_date mapping
        txn_dates = (
            spark.read.format("delta").load(str(fact_sales_path))
            .select("transaction_id", "order_date")
            .distinct()
        )

        returns = (
            returns
            .join(
                txn_dates.withColumnRenamed("transaction_id", "original_transaction_id")
                         .withColumnRenamed("order_date", "original_order_date"),
                on="original_transaction_id",
                how="left"
            )
            .withColumn("return_delay_days", F.when(
                F.col("original_order_date").isNotNull(),
                F.datediff(F.col("return_date"), F.col("original_order_date"))
            ).otherwise(F.lit(None)))
        )

        # Profile: average return delay
        avg_delay = returns.filter(
            F.col("return_delay_days").isNotNull()
        ).agg(
            F.round(F.avg("return_delay_days"), 1).alias("avg"),
            F.min("return_delay_days").alias("min"),
            F.max("return_delay_days").alias("max"),
        ).collect()[0]

        delay_str = (f"delay: avg={avg_delay['avg']}d, "
                     f"min={avg_delay['min']}d, max={avg_delay['max']}d")
    else:
        delay_str = "delay: not computed (fact_sales_all missing)"

    count = _write_silver(returns, "fact_returns")
    print(f"    OK    fact_returns           → {count:>8,} rows  ({delay_str})")
    return count


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_silver(spark=None):
    """Execute the full silver layer.

    Order matters:
      1. Dimensions first (needed for fact JOIN)
      2. Fact sales (3-way JOIN to dims)
      3. Returns (needs fact_sales for delay computation)
    """
    print("\n  ┌─ SILVER LAYER ────────────────────────────────────┐")
    print("  │  Joining, cleaning, conforming (Delta)             │")
    print("  └───────────────────────────────────────────────────┘")

    ensure_dirs()
    own_spark = spark is None
    if own_spark:
        spark = get_spark("einkaufpark_silver")

    total = 0

    # Step 1: Dimensions (must run before fact joins)
    print(f"\n  Dimensions:")
    total += transform_dim_store(spark)
    total += transform_dim_product(spark)
    total += transform_dim_customer(spark)

    # Step 2: Fact sales (3-way JOIN — the core transformation)
    print(f"\n  Fact sales (3-way JOIN):")
    total += transform_fact_sales(spark)

    # Step 3: Returns (linked to fact_sales for delay computation)
    print(f"\n  Returns:")
    total += transform_returns(spark)

    print(f"\n  {'─' * 50}")
    print(f"  Silver complete: {total:,} total rows across all tables")

    if own_spark:
        spark.stop()


if __name__ == "__main__":
    run_silver()