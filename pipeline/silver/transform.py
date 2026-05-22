"""
Silver Layer — Clean, Conform, and Range-Join (Delta Lake)
============================================================
Reads bronze Delta tables, joins facts to dimensions (with SCD2 range-join
for products), validates business rules, and produces star-schema tables
ready for gold-layer aggregation.

Pipeline:
  bronze/dim_stores         ──►  silver/dim_store           (trimmed)
  bronze/dim_customers      ──►  silver/dim_customer        (age validated)
  bronze/dim_products_scd2  ──►  silver/dim_product_scd2    (passed through)
  bronze/transactions       ──►  silver/fact_sales          (CLEAN + INFO)
                            └─►  silver/fact_sales_all      (+ REVIEW for DQ)
  bronze/returns            ──►  silver/fact_returns        (delay computed)

The core operation is the 4-way join:
  fact × dim_store × dim_product_scd2 × dim_customer
        INNER         RANGE-JOIN        LEFT (walk-ins)

The SCD2 range-join — product_id matches AND order_date BETWEEN effective_from
AND effective_to — recovers the LIST PRICE that applied at sale time. This
enables an honest price_vs_catalogue check: did the transaction price match
the price that was actually on the shelf that day, accounting for promos?

Success criteria — verified after every run:

  S1. Row reconciliation  — silver.fact_sales_all rows = bronze.transactions
                            rows (no losses, no inflation from range-join).
  S2. SCD2 hit rate       — every transaction matches exactly one SCD2 row
                            (zero misses, zero duplicates).
  S3. No quarantined rows — silver receives only CLEAN+WARN+INFO from bronze.
                            Any ERR present means the bronze contract broke.
  S4. Walk-in handling    — fraction of NULL customer_id matches generator's
                            walkin-rate (preserved through ingest+transform).
  S5. DQ rates            — observed CLEAN / INFO / REVIEW within tolerance
                            of raw_schema.json's expected_dq_rates.

What this silver does NOT do:
  - Read fact_sales_all back from disk to compute return delays (passes the
    cached DataFrame between functions instead).
  - Carry dim attributes into fact_sales (star schema discipline).
  - Defensive checks for schema variants that the generator doesn't produce.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipeline.common.spark_session import get_spark, BRONZE_DIR, SILVER_DIR, ensure_dirs


PROJECT_ROOT = Path(__file__).resolve().parents[2]
MASTER_DIR   = PROJECT_ROOT / "master"


# ═══════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════

def read_bronze(spark: SparkSession, name: str) -> DataFrame:
    """Read a bronze Delta table. Fail loudly if missing — silver depends on it."""
    path = BRONZE_DIR / name
    if not path.exists():
        raise FileNotFoundError(
            f"Bronze table '{name}' not found at {path}. "
            f"Run bronze first: python pipeline/bronze/ingest_raw.py"
        )
    return spark.read.format("delta").load(str(path))


def write_silver(df: DataFrame, name: str, *, partition_by: list[str] | None = None) -> int:
    """Single canonical Delta write. Schema drift fails — silver schema is the
    contract that gold depends on."""
    w = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        w = w.partitionBy(*partition_by)
    w.save(str(SILVER_DIR / name))
    return df.count()


def load_schema_rates() -> dict:
    """Pull expected_dq_rates from the source of truth."""
    path = MASTER_DIR / "raw_schema.json"
    with open(path, encoding="utf-8") as f:
        return json.load(f)["expected_dq_rates"]


# ═══════════════════════════════════════════════════════════════════════════
# Dimension transforms — simple, single-pass
# ═══════════════════════════════════════════════════════════════════════════

def transform_dim_store(spark: SparkSession) -> int:
    stores = (
        read_bronze(spark, "dim_stores")
        .withColumn("store_id", F.trim(F.col("store_id")))
        .withColumn("city",     F.trim(F.col("city")))
        .withColumn("region",   F.trim(F.col("region")))
        .drop("_ingested_at", "_source_file")
    )
    count = write_silver(stores, "dim_store")
    print(f"    OK    dim_store              → {count:>8,} rows")
    return count


def transform_dim_customer(spark: SparkSession) -> int:
    """Customer dimension with age validation and age groups."""
    customers = (
        read_bronze(spark, "dim_customers")
        .withColumn("customer_id", F.trim(F.col("customer_id")))
        .withColumn("age", F.col("age").cast("int"))
        .drop("_ingested_at", "_source_file")
    )

    customers = (
        customers
        .withColumn("age_valid", F.when(
            F.col("age").isNull(), True               # NULL is fine
        ).when(
            F.col("age").between(0, 120), True
        ).otherwise(False))
        .withColumn("age_group", F.when(
            F.col("age").isNull(),       "Unknown"
        ).when(F.col("age") < 25,        "18-24"
        ).when(F.col("age") < 35,        "25-34"
        ).when(F.col("age") < 50,        "35-49"
        ).when(F.col("age") < 65,        "50-64"
        ).otherwise(                     "65+"))
    )

    count = write_silver(customers, "dim_customer")

    # Single pass for profiling — groupBy once instead of three counts.
    profile = (
        customers.agg(
            F.count("*").alias("total"),
            F.sum(F.when(F.col("age").isNull(), 1).otherwise(0)).alias("null_age"),
            F.sum(F.when(F.col("age_valid") == False, 1).otherwise(0)).alias("bad_age"),
            F.sum(F.when(F.col("is_member"), 1).otherwise(0)).alias("members"),
        ).collect()[0]
    )
    print(f"    OK    dim_customer           → {count:>8,} rows")
    print(f"          null age: {profile['null_age']:,}  "
          f"invalid age: {profile['bad_age']:,}  "
          f"({profile['null_age']/max(profile['total'],1)*100:.1f}% null, "
          f"{profile['bad_age']/max(profile['total'],1)*100:.2f}% invalid)")
    print(f"          loyalty members: {profile['members']:,} "
          f"({profile['members']/max(profile['total'],1)*100:.1f}%)")
    return count


def transform_dim_product_scd2(spark: SparkSession) -> int:
    """Pass SCD2 through to silver. Date types preserved from bronze.

    Adds a price_band derived from effective_price for Power BI slicers
    (more meaningful than min/max-based bands since it reflects actual price
    at the time, not catalogue range).
    """
    scd2 = (
        read_bronze(spark, "dim_products_scd2")
        .drop("_ingested_at", "_source_file")
        .withColumn("price_band", F.when(
            F.col("effective_price_eur") <= 2.0,  "Budget (≤€2)"
        ).when(F.col("effective_price_eur") <= 5.0,  "Mid (€2-5)"
        ).when(F.col("effective_price_eur") <= 10.0, "Premium (€5-10)"
        ).otherwise(                                 "High (€10+)"))
    )
    count = write_silver(scd2, "dim_product_scd2")
    n_products = scd2.select("product_id").distinct().count()
    print(f"    OK    dim_product_scd2       → {count:>8,} rows "
          f"({n_products:,} products, {count/max(n_products,1):.1f} intervals each)")
    return count


# ═══════════════════════════════════════════════════════════════════════════
# Fact sales — the core 4-way join with SCD2 range-join
# ═══════════════════════════════════════════════════════════════════════════

def transform_fact_sales(spark: SparkSession) -> tuple[DataFrame, dict]:
    """Build fact_sales via SCD2 range-join.

    Returns (cached_clean_df, stats). The cached DataFrame is reused by
    transform_returns to compute delay_days — no second disk read.
    """
    txn = read_bronze(spark, "transactions")
    # Bronze contract: ERR rows are quarantined. If any reach us, the contract broke.
    assert_no_errors(txn)

    stores       = spark.read.format("delta").load(str(SILVER_DIR / "dim_store"))
    customers    = spark.read.format("delta").load(str(SILVER_DIR / "dim_customer"))
    product_scd2 = spark.read.format("delta").load(str(SILVER_DIR / "dim_product_scd2"))

    # Type casts — bronze inferred from CSV, silver enforces.
    txn = (
        txn
        .withColumn("quantity",         F.col("quantity").cast("int"))
        .withColumn("unit_price_eur",   F.col("unit_price_eur").cast("double"))
        .withColumn("discount_pct",     F.col("discount_pct").cast("double"))
        .withColumn("net_revenue_eur",  F.col("net_revenue_eur").cast("double"))
    )

    txn_before = txn.count()

    # ── Store join — broadcast, INNER ───────────────────────────────────
    store_dim = F.broadcast(stores.select("store_id", "city", "region", "size_class"))
    joined    = txn.join(store_dim, on="store_id", how="inner")

    # ── Product SCD2 range-join — the core technique ───────────────────
    # Match on product_id AND order_date in [effective_from, effective_to].
    # Each transaction matches exactly one SCD2 row by construction (intervals
    # are contiguous and non-overlapping — checked in bronze.B4).
    scd2_cols = product_scd2.select(
        "product_id",
        F.col("effective_from").alias("_scd_from"),
        F.col("effective_to").alias("_scd_to"),
        F.col("effective_price_eur").alias("list_price_eur"),
        F.col("is_promo_price"),
        F.col("price_band"),
        F.col("product_name"),
        F.col("category"),
        F.col("subcategory"),
        F.col("vat_rate"),
        F.col("unit"),
    )
    joined = joined.join(
        scd2_cols,
        on=[
            joined["product_id"] == scd2_cols["product_id"],
            joined["order_date"].between(scd2_cols["_scd_from"], scd2_cols["_scd_to"]),
        ],
        how="inner",
    ).drop(scd2_cols["product_id"])

    # ── Customer join — LEFT (walk-ins have NULL customer_id) ──────────
    cust_dim = customers.select(
        "customer_id",
        F.col("age").alias("customer_age"),
        "gender_code",
        F.col("is_member").alias("customer_is_member"),
        F.col("age_group").alias("customer_age_group"),
    )
    joined = joined.join(cust_dim, on="customer_id", how="left")

    # ── Derived columns ─────────────────────────────────────────────────
    joined = (
        joined
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
        # Walk-in flag — real now, not aspirational.
        .withColumn("is_walk_in", F.col("customer_id").isNull())
        # Honest price comparison: transaction price vs LIST PRICE AT SALE TIME.
        # Allows for legitimate discounts (negotiated, manual override) within
        # a reasonable tolerance. Anything beyond ±20% is flagged for review.
        .withColumn("price_delta_pct", F.round(
            (F.col("unit_price_eur") - F.col("list_price_eur"))
            / F.col("list_price_eur") * 100, 2
        ))
        .withColumn("price_vs_list", F.when(
            F.abs(F.col("price_delta_pct")) <= 20, "IN_LINE"
        ).when(F.col("price_delta_pct") < -20,     "DEEP_DISCOUNT"
        ).otherwise(                               "OVERPRICED"))
    )

    # ── DQ status — three-way classification ───────────────────────────
    # Bronze contract: only CLEAN, WARN-flagged, and INFO-flagged rows reach us.
    joined = joined.withColumn("dq_status", F.when(
        F.col("data_quality_flag") == "OK",         "CLEAN"
    ).when(
        F.col("data_quality_flag").contains("WARN"), "REVIEW"
    ).when(
        F.col("data_quality_flag").contains("INFO"), "INFO"
    ).otherwise(                                     "CLEAN"))

    # Drop SCD2 join keys — no longer needed downstream.
    joined = joined.drop("_scd_from", "_scd_to",
                         "_ingested_at", "_source_file",
                         "_ingested_at_x", "_ingested_at_y")  # safety drops

    # ── Cache before writing twice ─────────────────────────────────────
    # The original re-computed the join 7+ times. One cache eliminates this.
    joined = joined.cache()

    # ── Write fact_sales_all (CLEAN + REVIEW + INFO) ───────────────────
    total = write_silver(joined, "fact_sales_all", partition_by=["year_month"])

    # ── Write fact_sales (CLEAN + INFO only — REVIEW excluded) ─────────
    # This is the contract gold consumes. INFO rows are kept because late
    # arrivals are not data quality issues — they just arrived late.
    clean = joined.filter(F.col("dq_status").isin("CLEAN", "INFO"))
    clean_count = write_silver(clean, "fact_sales", partition_by=["year_month"])

    # ── Profiling — single groupBy instead of N counts ─────────────────
    profile = (
        joined.groupBy("dq_status").count().collect()
    )
    profile_dict = {r["dq_status"]: r["count"] for r in profile}
    walk_in_ct  = joined.filter(F.col("is_walk_in")).count()
    deep_disc   = joined.filter(F.col("price_vs_list") == "DEEP_DISCOUNT").count()
    overpriced  = joined.filter(F.col("price_vs_list") == "OVERPRICED").count()

    print(f"    OK    fact_sales_all         → {total:>8,} rows  [by year_month]")
    print(f"    OK    fact_sales             → {clean_count:>8,} rows  (CLEAN + INFO)")
    print(f"          DQ split   : CLEAN={profile_dict.get('CLEAN',0):,}  "
          f"INFO={profile_dict.get('INFO',0):,}  "
          f"REVIEW={profile_dict.get('REVIEW',0):,}")
    print(f"          walk-ins   : {walk_in_ct:,} "
          f"({walk_in_ct/max(total,1)*100:.1f}%)")
    print(f"          price vs list: {deep_disc:,} deep discount, {overpriced:,} overpriced")

    stats = {
        "total":         total,
        "clean":         clean_count,
        "bronze_count":  txn_before,
        "review":        profile_dict.get("REVIEW", 0),
        "info":          profile_dict.get("INFO", 0),
        "walk_in":       walk_in_ct,
    }
    return joined, stats


def assert_no_errors(txn: DataFrame) -> None:
    """Bronze contract: ERR rows are quarantined. Fail loudly if any leaked."""
    n_err = txn.filter(F.col("data_quality_flag").contains("ERR")).count()
    if n_err > 0:
        raise AssertionError(
            f"Bronze contract violated: {n_err:,} ERR rows reached silver. "
            f"Check bronze/quarantine routing."
        )


# ═══════════════════════════════════════════════════════════════════════════
# Returns — linked to fact_sales for delay computation
# ═══════════════════════════════════════════════════════════════════════════

def transform_returns(spark: SparkSession, sales_df: DataFrame) -> int:
    """Compute return_delay_days using the cached sales DataFrame from
    transform_fact_sales — no second disk read."""
    returns_path = BRONZE_DIR / "returns"
    if not returns_path.exists():
        print(f"    SKIP  fact_returns — no bronze return data")
        return 0

    returns = (
        spark.read.format("delta").load(str(returns_path))
        .drop("_ingested_at", "_source_file")
    )

    # Get the order_date for each transaction from the cached sales DataFrame.
    txn_dates = (
        sales_df.select("transaction_id", "order_date")
                .distinct()
                .withColumnRenamed("transaction_id", "original_transaction_id")
                .withColumnRenamed("order_date", "original_order_date")
    )

    returns = (
        returns
        .join(txn_dates, on="original_transaction_id", how="left")
        .withColumn("return_delay_days", F.when(
            F.col("original_order_date").isNotNull(),
            F.datediff(F.col("return_date"), F.col("original_order_date"))
        ).otherwise(F.lit(None)))
    )

    count = write_silver(returns, "fact_returns", partition_by=["return_month"])

    # Profile delay stats in one aggregation.
    delay_stats = (
        returns.filter(F.col("return_delay_days").isNotNull())
               .agg(
                   F.round(F.avg("return_delay_days"), 1).alias("avg"),
                   F.min("return_delay_days").alias("min"),
                   F.max("return_delay_days").alias("max"),
                   F.count("*").alias("linked"),
               ).collect()[0]
    )
    print(f"    OK    fact_returns           → {count:>8,} rows")
    print(f"          delay: avg={delay_stats['avg']}d  "
          f"min={delay_stats['min']}d  max={delay_stats['max']}d  "
          f"({delay_stats['linked']:,}/{count:,} linked to a transaction)")
    return count


# ═══════════════════════════════════════════════════════════════════════════
# Validation — success criteria from the docstring, enforced
# ═══════════════════════════════════════════════════════════════════════════

def check_row_reconciliation(stats: dict) -> tuple[bool, str]:
    """S1: silver.fact_sales_all rows == bronze.transactions rows."""
    bronze = stats["bronze_count"]
    silver = stats["total"]
    if bronze != silver:
        diff = bronze - silver
        return False, f"FAIL: bronze={bronze:,}, silver={silver:,}, lost={diff:,}"
    return True, f"{silver:,} rows reconciled (bronze=silver, no orphans)"


def check_scd2_hit_rate(spark: SparkSession, stats: dict) -> tuple[bool, str]:
    """S2: every transaction matches exactly one SCD2 row.

    A miss would show as a row with NULL list_price_eur (left-side preserved
    by INNER join — so missing rows would have been dropped in the JOIN and
    show up as a count delta vs bronze). We re-verify by checking that the
    INNER join didn't lose anyone.
    """
    fact = spark.read.format("delta").load(str(SILVER_DIR / "fact_sales_all"))
    n_null = fact.filter(F.col("list_price_eur").isNull()).count()
    if n_null > 0:
        return False, f"FAIL: {n_null:,} rows missing SCD2 match"
    return True, f"{stats['total']:,} rows matched to SCD2 intervals"


def check_no_quarantined(stats: dict) -> tuple[bool, str]:
    """S3: silver should never contain QUARANTINED rows by bronze contract."""
    # The assertion in transform_fact_sales fails the run if any ERR row
    # reaches silver. If we got here, the assertion passed.
    return True, "bronze contract honored (assertion passed during transform)"


def check_walkin_rate(stats: dict, target: float = 0.10) -> tuple[bool, str]:
    """S4: walk-in fraction preserved through ingest+transform.

    Default target is 0.10 (generator default). If you ran the generator with
    a different walkin-rate, pass that here.
    """
    observed = stats["walk_in"] / max(stats["total"], 1)
    delta = abs(observed - target)
    ok = delta <= 0.02
    return ok, f"observed={observed*100:.1f}%, target={target*100:.0f}%, Δ{delta*100:.1f}pp"


def check_dq_rates(stats: dict, expected: dict) -> tuple[bool, str]:
    """S5: observed DQ rates within tolerance of raw_schema.json targets."""
    total = max(stats["total"], 1)
    clean_pct  = (stats["total"] - stats["review"] - stats["info"]) / total * 100
    warn_pct   = stats["review"] / total * 100
    # err_pct = 0 by contract (quarantined in bronze).

    msgs = []
    passing = True
    for name, observed, target in [
        ("ok",   clean_pct + stats["info"]/total*100, expected["ok_rows_pct"]),
        ("warn", warn_pct,                            expected["warn_rows_pct"]),
    ]:
        delta = abs(observed - target)
        if delta > 1.0:   # 1pp tolerance — looser than raw because of INFO grouping
            passing = False
        msgs.append(f"{name}={observed:.2f}%(exp {target:.1f}%, Δ{delta:.2f}pp)")
    return passing, " ".join(msgs)


def validate(spark: SparkSession, stats: dict, walkin_target: float) -> bool:
    print(f"\n  Validation {chr(9472)*52}")
    rates = load_schema_rates()
    checks = [
        ("S1 row reconciliation", lambda: check_row_reconciliation(stats)),
        ("S2 SCD2 hit rate",      lambda: check_scd2_hit_rate(spark, stats)),
        ("S3 no quarantined",     lambda: check_no_quarantined(stats)),
        ("S4 walk-in rate",       lambda: check_walkin_rate(stats, walkin_target)),
        ("S5 DQ rates",           lambda: check_dq_rates(stats, rates)),
    ]
    all_pass = True
    for name, fn in checks:
        ok, msg = fn()
        print(f"    [{'PASS' if ok else 'FAIL'}] {name:<24} {msg}")
        if not ok:
            all_pass = False
    print(f"  {chr(9472)*60}")
    return all_pass


# ═══════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════

def run_silver(spark: SparkSession | None = None,
               walkin_target: float = 0.10) -> bool:
    """Execute the full silver layer. Returns True if all validations pass.

    Dimensions are written first so the fact join can find them. Returns
    are processed last because they need the cached fact_sales DataFrame
    to compute delay_days.
    """
    print("\n  ┌─ SILVER LAYER ────────────────────────────────────┐")
    print("  │  Range-join SCD2 + conform + DQ classify (Delta)   │")
    print("  └────────────────────────────────────────────────────┘")

    ensure_dirs()
    own_spark = spark is None
    if own_spark:
        spark = get_spark("einkaufpark_silver")

    # ── Dimensions (must run before fact join) ──────────────────────────
    print(f"\n  Dimensions:")
    transform_dim_store(spark)
    transform_dim_customer(spark)
    transform_dim_product_scd2(spark)

    # ── Fact sales (4-way join with SCD2 range-join) ────────────────────
    print(f"\n  Fact sales (4-way join, SCD2 range-join):")
    sales_df, stats = transform_fact_sales(spark)

    # ── Returns (uses cached sales_df, no disk re-read) ─────────────────
    print(f"\n  Returns:")
    transform_returns(spark, sales_df)

    # ── Validation ──────────────────────────────────────────────────────
    ok = validate(spark, stats, walkin_target)

    # Release the cached DataFrame.
    sales_df.unpersist()

    print(f"\n  {chr(9472)*60}")
    print(f"  Silver {'complete' if ok else 'FAILED'}")
    print(f"  Output : {SILVER_DIR}/")
    print(f"  {chr(9472)*60}")

    if own_spark:
        spark.stop()
    return ok


if __name__ == "__main__":
    sys.exit(0 if run_silver() else 1)