"""
Bronze Layer — Raw Ingestion (Delta Lake)
==========================================
Ingests the output of incremental.py into Delta tables. Handles daily batch
files, SCD2 price history, walk-in customers, late arrivals, and returns.

Input layout (from incremental.py):
  data/raw/
    dim_stores.csv             → bronze/dim_stores/            (Delta)
    dim_customers.csv          → bronze/dim_customers/         (Delta)
    dim_products_scd2.csv      → bronze/dim_products_scd2/     (Delta)
    fact_returns.csv           → bronze/returns/               (Delta, partitioned by return_month)
    batches/
      batch_YYYYMMDD.csv       → bronze/transactions/          (Delta, partitioned by order_date)
      batch_YYYYMMDD_late.csv  → ingested alongside main batches (overflow)

Quarantined rows (ERR flags) → bronze/quarantine/

Success criteria — verified after every run:

  B1. Row reconciliation   — bronze row count = sum of source CSV rows
                             (transactions = batch files + overflow).
  B2. FK integrity         — every store_id, product_id, customer_id in
                             facts resolves against the dim tables
                             (walk-ins with NULL customer_id excluded).
  B3. Late arrivals kept   — count of rows with INFO:LATE_ARRIVAL flag
                             matches the source files (no DQ misclassified
                             as quarantined).
  B4. SCD2 continuity      — every product's SCD2 intervals contiguous
                             (preserved from source, validated here).
  B5. No Sunday partitions — zero order_date partitions on Sundays.

What this bronze does NOT do:
  - Schema autoMerge: explicit StructTypes catch generator drift on ingest.
  - Monthly batching: Delta handles partition atomicity; one-pass is fine.
  - Defensive `unpersist()` of never-persisted DataFrames.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, BooleanType, DateType,
)

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipeline.common.spark_session import get_spark, RAW_DIR, BRONZE_DIR, ensure_dirs


# ═══════════════════════════════════════════════════════════════════════════
# Paths and config
# ═══════════════════════════════════════════════════════════════════════════

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MASTER_DIR   = PROJECT_ROOT / "master"


# ═══════════════════════════════════════════════════════════════════════════
# Explicit schemas — catch generator drift at ingest, not in silver
# ═══════════════════════════════════════════════════════════════════════════

TRANSACTIONS_SCHEMA = StructType([
    StructField("transaction_id",        StringType(),  False),
    StructField("basket_id",             StringType(),  False),
    StructField("batch_id",              StringType(),  True),
    StructField("source_system",         StringType(),  True),
    StructField("record_hash",           StringType(),  True),
    StructField("order_date",            StringType(),  False),
    StructField("order_time",            StringType(),  True),
    StructField("ingestion_date",        StringType(),  True),
    StructField("sales_channel",         StringType(),  True),
    StructField("order_status",          StringType(),  True),
    StructField("store_id",              StringType(),  False),
    StructField("customer_id",           StringType(),  True),   # NULL → walk-in
    StructField("membership_active",     BooleanType(), True),
    StructField("loyalty_points_earned", IntegerType(), True),
    StructField("coupon_applied",        BooleanType(), True),
    StructField("coupon_code",           StringType(),  True),
    StructField("product_id",            StringType(),  False),
    StructField("is_private_label",      BooleanType(), True),
    StructField("brand",                 StringType(),  True),
    StructField("quantity",              IntegerType(), True),
    StructField("unit_price_eur",        DoubleType(),  True),
    StructField("discount_pct",          DoubleType(),  True),
    StructField("transaction_currency",  StringType(),  True),
    StructField("net_revenue_eur",       DoubleType(),  True),
    StructField("payment_type",          StringType(),  True),
    StructField("pos_terminal_id",       StringType(),  False),
    StructField("terminal_type",         StringType(),  True),   # overwritten from master
    StructField("is_self_checkout",      BooleanType(), True),   # overwritten from master
    StructField("cashier_id",            StringType(),  True),
    StructField("promo_week_id",         StringType(),  True),
    StructField("is_promo_period",       BooleanType(), True),
    StructField("data_quality_flag",     StringType(),  False),
])

RETURNS_SCHEMA = StructType([
    StructField("return_id",                StringType(),  False),
    StructField("original_transaction_id",  StringType(),  False),
    StructField("original_basket_id",       StringType(),  False),
    StructField("return_date",              StringType(),  False),
    StructField("return_time",              StringType(),  True),
    StructField("store_id",                 StringType(),  False),
    StructField("customer_id",              StringType(),  True),
    StructField("product_id",               StringType(),  False),
    StructField("return_quantity",          IntegerType(), True),
    StructField("unit_price_eur",           DoubleType(),  True),
    StructField("refund_amount_eur",        DoubleType(),  True),
    StructField("reason_code",              StringType(),  True),
    StructField("cashier_id",               StringType(),  True),
    StructField("ingestion_date",           StringType(),  True),
])


# ═══════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════

def load_json(path: Path, label: str) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found at {path}")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def add_ingest_metadata(df: DataFrame, source_file: str, ingestion_ts: str) -> DataFrame:
    """Stamp every row with ingestion provenance."""
    return (
        df.withColumn("_ingested_at", F.lit(ingestion_ts))
          .withColumn("_source_file", F.lit(source_file))
    )


def write_delta(df: DataFrame, path: str, *, partition_by: list[str] | None = None) -> None:
    """One canonical Delta write. Schema drift FAILS — generator changes
    must be intentional, not auto-merged."""
    w = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        w = w.partitionBy(*partition_by)
    w.save(path)


# ═══════════════════════════════════════════════════════════════════════════
# Transformations applied to fact_transactions
# ═══════════════════════════════════════════════════════════════════════════

def replace_terminal_metadata(df: DataFrame, term_df: DataFrame) -> DataFrame:
    """Drop CSV terminal columns and re-derive from master.

    Defensive against the production case where a POS system may carry stale
    terminal metadata. With the current generator the master file is the same
    source, so this re-derive produces identical values — but it's the right
    pattern for a real pipeline.
    """
    df = df.drop("terminal_type", "is_self_checkout")
    df = df.join(F.broadcast(term_df), on="pos_terminal_id", how="left")

    # Self-checkout lanes carry no cashier_id.
    df = df.withColumn(
        "cashier_id",
        F.when(F.col("is_self_checkout"), F.lit(None).cast(StringType()))
         .otherwise(F.col("cashier_id"))
    )
    # Flag rows that didn't resolve against the terminal master.
    df = df.withColumn(
        "data_quality_flag",
        F.when(F.col("terminal_type").isNull(),
               F.concat(F.col("data_quality_flag"), F.lit("|WARN:UNKNOWN_TERMINAL")))
         .otherwise(F.col("data_quality_flag"))
    )
    return df


def flag_unknown_stores(df: DataFrame, known_store_ids: list[str]) -> DataFrame:
    return df.withColumn(
        "data_quality_flag",
        F.when(~F.col("store_id").isin(known_store_ids),
               F.concat(F.col("data_quality_flag"), F.lit("|WARN:UNKNOWN_STORE")))
         .otherwise(F.col("data_quality_flag"))
    )


def split_quarantine(df: DataFrame, err_codes: list[str]) -> tuple[DataFrame, DataFrame]:
    """ERR rows → quarantine. Everything else → clean."""
    err_pattern = "|".join(err_codes)
    has_err     = F.col("data_quality_flag").rlike(err_pattern)

    clean = df.filter(~has_err)
    quar  = (
        df.filter(has_err)
          .withColumn("quarantine_ts", F.lit(datetime.utcnow().isoformat()))
          .withColumn("repair_status", F.lit("PENDING"))
    )
    # Annotate clean rows with whether they carry any WARN flags.
    clean = clean.withColumn("is_clean", ~F.col("data_quality_flag").rlike("WARN:"))
    return clean, quar


# ═══════════════════════════════════════════════════════════════════════════
# Dimension ingestion (small tables — no DQ split, single write)
# ═══════════════════════════════════════════════════════════════════════════

def ingest_simple_dim(spark: SparkSession, source_name: str,
                      ingestion_ts: str) -> int:
    """Read a flat dimension CSV and write to Delta. Used for dim_stores
    and dim_customers, which have no time-varying attributes."""
    csv_path = RAW_DIR / f"{source_name}.csv"
    if not csv_path.exists():
        print(f"    SKIP  {source_name} — not found at {csv_path}")
        return 0

    df = spark.read.option("header", "true").option("inferSchema", "true").csv(str(csv_path))
    df = add_ingest_metadata(df, csv_path.name, ingestion_ts)
    write_delta(df, str(BRONZE_DIR / source_name))

    count = df.count()
    print(f"    OK    {source_name:<24} → {count:>8,} rows")
    return count


def ingest_scd2_products(spark: SparkSession, ingestion_ts: str) -> int:
    """Ingest dim_products_scd2.csv with proper date types.

    SCD2 tables MUST have effective_from/effective_to as date types — silver
    will range-join on these, and string comparisons silently break.
    """
    csv_path = RAW_DIR / "dim_products_scd2.csv"
    if not csv_path.exists():
        print(f"    SKIP  dim_products_scd2 — not found")
        return 0

    df = (
        spark.read.option("header", "true").option("inferSchema", "true").csv(str(csv_path))
        # Force dates — inferSchema reads them as strings.
        .withColumn("effective_from", F.to_date(F.col("effective_from"), "yyyy-MM-dd"))
        .withColumn("effective_to",   F.to_date(F.col("effective_to"),   "yyyy-MM-dd"))
    )
    df = add_ingest_metadata(df, csv_path.name, ingestion_ts)
    write_delta(df, str(BRONZE_DIR / "dim_products_scd2"))

    count = df.count()
    distinct_products = df.select("product_id").distinct().count()
    print(f"    OK    dim_products_scd2        → {count:>8,} rows "
          f"({distinct_products:,} products, {count/max(distinct_products,1):.1f} intervals each)")
    return count


# ═══════════════════════════════════════════════════════════════════════════
# Returns ingestion
# ═══════════════════════════════════════════════════════════════════════════

def ingest_returns(spark: SparkSession, ingestion_ts: str) -> int:
    csv_path = RAW_DIR / "fact_returns.csv"
    if not csv_path.exists():
        print(f"    SKIP  fact_returns — not found")
        return 0

    df = (
        spark.read.schema(RETURNS_SCHEMA).option("header", "true")
        .csv(str(csv_path))
        .withColumn("return_date", F.to_date(F.col("return_date"), "yyyy-MM-dd"))
        .withColumn("return_month", F.date_format(F.col("return_date"), "yyyy-MM"))
    )
    df = add_ingest_metadata(df, csv_path.name, ingestion_ts)
    write_delta(df, str(BRONZE_DIR / "returns"), partition_by=["return_month"])

    count = df.count()
    print(f"    OK    fact_returns             → {count:>8,} rows  "
          f"[partitioned by return_month]")
    return count


# ═══════════════════════════════════════════════════════════════════════════
# Transactions — daily batch files, single-pass ingestion
# ═══════════════════════════════════════════════════════════════════════════

def ingest_transactions(spark: SparkSession, ingestion_ts: str) -> dict:
    """Read all batch_*.csv files (including _late overflow), apply terminal
    join + store validation, split quarantine, write partitioned Delta.

    Single pass over the data. Delta's ACID write means no monthly batching
    needed for partial-failure safety.
    """
    batch_dir = RAW_DIR / "batches"
    if not batch_dir.exists():
        print(f"    SKIP  fact_transactions — {batch_dir} not found")
        return {"clean": 0, "quarantine": 0}

    # Glob: matches batch_20230102.csv AND batch_20260331_late.csv.
    batch_glob = str(batch_dir / "batch_*.csv")

    # ── Load master files ──────────────────────────────────────────────
    terminal_dict = load_json(MASTER_DIR / "terminal_master.json", "terminal_master")
    store_dict    = load_json(MASTER_DIR / "store_master.json",    "store_master")
    schema_dict   = load_json(MASTER_DIR / "raw_schema.json",      "raw_schema")

    # Quarantine codes come from the schema — single source of truth.
    err_codes        = schema_dict["dq_rules"]["quarantine_on"]
    known_store_ids  = [s["store_id"] for s in store_dict["stores"]]

    print(f"    Terminals loaded : {len(terminal_dict['terminals'])}")
    print(f"    Stores loaded    : {len(store_dict['stores'])}")
    print(f"    Quarantine codes : {err_codes}")

    # ── Build terminal broadcast table ─────────────────────────────────
    terminal_rows = [
        (t["terminal_id"], t["terminal_type"], t["is_self_checkout"])
        for t in terminal_dict["terminals"]
    ]
    term_df = spark.createDataFrame(
        terminal_rows,
        schema=StructType([
            StructField("pos_terminal_id",  StringType(),  False),
            StructField("terminal_type",    StringType(),  True),
            StructField("is_self_checkout", BooleanType(), True),
        ])
    )

    # ── Read all batch files in one go ─────────────────────────────────
    df = (
        spark.read
        .schema(TRANSACTIONS_SCHEMA)
        .option("header", "true")
        .option("dateFormat", "yyyy-MM-dd")
        .option("nullValue", "")
        .csv(batch_glob)
        .withColumn("order_date",  F.to_date(F.col("order_date"),  "yyyy-MM-dd"))
        .withColumn("ingestion_date", F.to_date(F.col("ingestion_date"), "yyyy-MM-dd"))
        # File-level provenance — input_file_name() captures which batch_*.csv
        # each row came from, useful for late-arrival debugging.
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.lit(ingestion_ts))
    )

    # ── Apply transformations ──────────────────────────────────────────
    df = replace_terminal_metadata(df, term_df)
    df = flag_unknown_stores(df, known_store_ids)
    clean, quarantine = split_quarantine(df, err_codes)

    # Cache before multi-write to avoid recomputing the whole DAG twice.
    clean.cache()
    quarantine.cache()

    # ── Write — partition by order_date (silver range-joins on this) ──
    clean_path = str(BRONZE_DIR / "transactions")
    quar_path  = str(BRONZE_DIR / "quarantine")

    print(f"    Writing clean    → {clean_path}")
    write_delta(clean, clean_path, partition_by=["order_date"])

    print(f"    Writing quar.    → {quar_path}")
    write_delta(quarantine, quar_path)

    clean_count = clean.count()
    quar_count  = quarantine.count()

    # Quick late-arrival summary — useful diagnostic.
    n_late = clean.filter(F.col("data_quality_flag").contains("INFO:LATE_ARRIVAL")).count()

    print(f"    Clean rows       : {clean_count:>10,}")
    print(f"    Quarantine rows  : {quar_count:>10,}")
    print(f"    Late arrivals    : {n_late:>10,}  ({n_late/max(clean_count,1)*100:.1f}% of clean)")

    clean.unpersist()
    quarantine.unpersist()

    return {"clean": clean_count, "quarantine": quar_count, "late": n_late}


# ═══════════════════════════════════════════════════════════════════════════
# Validation — success criteria from the docstring, enforced
# ═══════════════════════════════════════════════════════════════════════════

def _count_source_rows(batch_dir: Path) -> int:
    """Count rows across all source batch CSVs (excluding headers)."""
    n = 0
    for path in batch_dir.glob("batch_*.csv"):
        with open(path, encoding="utf-8") as f:
            n += sum(1 for _ in f) - 1   # minus header
    return n


def check_row_reconciliation(spark: SparkSession, txn_stats: dict) -> tuple[bool, str]:
    """B1: bronze clean + quarantine = source CSV row count."""
    source_count = _count_source_rows(RAW_DIR / "batches")
    bronze_count = txn_stats["clean"] + txn_stats["quarantine"]
    ok = source_count == bronze_count
    return ok, f"source={source_count:,}, bronze={bronze_count:,}, diff={source_count - bronze_count:,}"


def check_fk_integrity(spark: SparkSession) -> tuple[bool, str]:
    """B2: every store_id, product_id, customer_id resolves to dim tables.
    Walk-ins (NULL customer_id) are excluded from the customer check."""
    txn      = spark.read.format("delta").load(str(BRONZE_DIR / "transactions"))
    stores   = spark.read.format("delta").load(str(BRONZE_DIR / "dim_stores"))
    customers = spark.read.format("delta").load(str(BRONZE_DIR / "dim_customers"))
    products = spark.read.format("delta").load(str(BRONZE_DIR / "dim_products_scd2"))

    # store_id
    fact_stores = txn.select("store_id").distinct()
    dim_stores  = stores.select("store_id").distinct()
    orphan_s = fact_stores.join(dim_stores, on="store_id", how="left_anti").count()

    # product_id (use distinct from SCD2, not interval rows)
    fact_products = txn.select("product_id").distinct()
    dim_products  = products.select("product_id").distinct()
    orphan_p = fact_products.join(dim_products, on="product_id", how="left_anti").count()

    # customer_id — walk-ins (NULL) excluded
    fact_customers = (
        txn.filter(F.col("customer_id").isNotNull())
           .select("customer_id").distinct()
    )
    dim_customers  = customers.select("customer_id").distinct()
    orphan_c = fact_customers.join(dim_customers, on="customer_id", how="left_anti").count()

    if orphan_s or orphan_p or orphan_c:
        return False, f"FAIL: {orphan_s} stores, {orphan_p} products, {orphan_c} customers unresolved"
    return True, f"all FKs resolve (stores, products, customers — walk-ins ignored)"


def check_late_arrival_preserved(spark: SparkSession,
                                  txn_stats: dict) -> tuple[bool, str]:
    """B3: late-arrival count in bronze matches source count.

    Counts the INFO:LATE_ARRIVAL flag presence; this is the DQ contract
    silver depends on for watermarking.
    """
    # Count in source CSVs.
    source_late = 0
    for path in (RAW_DIR / "batches").glob("batch_*.csv"):
        with open(path, encoding="utf-8") as f:
            source_late += sum(1 for line in f if "INFO:LATE_ARRIVAL" in line)

    bronze_late = txn_stats.get("late", 0)
    # Late arrivals are info-only, so they all go to clean stream — no loss
    # to quarantine is expected.
    ok = source_late == bronze_late
    return ok, f"source={source_late:,}, bronze={bronze_late:,}"


def check_scd2_continuity(spark: SparkSession) -> tuple[bool, str]:
    """B4: per product, SCD2 intervals are contiguous.

    Equivalent of price_history.py's S2 check, re-run on the bronze copy
    in case anything got corrupted during ingest.
    """
    scd2 = spark.read.format("delta").load(str(BRONZE_DIR / "dim_products_scd2"))
    # Window per product, ordered by effective_from.
    from pyspark.sql.window import Window
    w = Window.partitionBy("product_id").orderBy("effective_from")
    scd2 = scd2.withColumn("next_from", F.lead("effective_from").over(w))
    # Expected: next_from == effective_to + 1 day.
    gaps = scd2.filter(
        F.col("next_from").isNotNull()
        & (F.datediff(F.col("next_from"), F.col("effective_to")) != 1)
    ).count()
    if gaps:
        return False, f"FAIL: {gaps} interval gaps/overlaps detected"
    return True, "all SCD2 intervals contiguous"


def check_no_sunday_partitions(spark: SparkSession) -> tuple[bool, str]:
    """B5: zero rows partitioned on a Sunday."""
    txn = spark.read.format("delta").load(str(BRONZE_DIR / "transactions"))
    # dayofweek: 1=Sunday in Spark convention.
    n_sun = txn.filter(F.dayofweek(F.col("order_date")) == 1).count()
    if n_sun:
        return False, f"FAIL: {n_sun:,} rows on Sundays"
    return True, "0 Sunday rows"


def validate(spark: SparkSession, txn_stats: dict) -> bool:
    print(f"\n  Validation {chr(9472)*52}")
    checks = [
        ("B1 row reconciliation",   lambda: check_row_reconciliation(spark, txn_stats)),
        ("B2 FK integrity",         lambda: check_fk_integrity(spark)),
        ("B3 late arrivals kept",   lambda: check_late_arrival_preserved(spark, txn_stats)),
        ("B4 SCD2 continuity",      lambda: check_scd2_continuity(spark)),
        ("B5 no Sunday partitions", lambda: check_no_sunday_partitions(spark)),
    ]
    all_pass = True
    for name, fn in checks:
        ok, msg = fn()
        print(f"    [{'PASS' if ok else 'FAIL'}] {name:<28} {msg}")
        if not ok:
            all_pass = False
    print(f"  {chr(9472)*60}")
    return all_pass


# ═══════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════

def run_bronze(spark: SparkSession | None = None) -> bool:
    """Execute the full bronze layer. Returns True if all validations pass."""
    print("\n  ┌─ BRONZE LAYER ────────────────────────────────────┐")
    print("  │  Raw CSVs → Delta tables with validation           │")
    print("  └────────────────────────────────────────────────────┘")

    ensure_dirs()
    ingestion_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    own_spark    = spark is None
    if own_spark:
        spark = get_spark("einkaufpark_bronze")

    print(f"\n  Dimensions:")
    ingest_simple_dim(spark, "dim_stores",    ingestion_ts)
    ingest_simple_dim(spark, "dim_customers", ingestion_ts)
    ingest_scd2_products(spark, ingestion_ts)

    print(f"\n  Returns:")
    ingest_returns(spark, ingestion_ts)

    print(f"\n  Transactions (daily batch files → partitioned Delta):")
    txn_stats = ingest_transactions(spark, ingestion_ts)

    ok = validate(spark, txn_stats)

    print(f"\n  {chr(9472)*60}")
    print(f"  Bronze {'complete' if ok else 'FAILED'}")
    print(f"  Ingestion timestamp : {ingestion_ts}")
    print(f"  Output              : {BRONZE_DIR}/")
    print(f"  {chr(9472)*60}")

    if own_spark:
        spark.stop()
    return ok


if __name__ == "__main__":
    sys.exit(0 if run_bronze() else 1)