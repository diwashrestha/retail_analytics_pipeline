"""
Bronze Layer — Raw Ingestion (Delta Lake)
==========================================
Ingests 5 separate CSVs from the Einkaufpark generator into Delta
tables with metadata, validation, and quarantine logic.

Input files (from --mode normalized):
  dim_stores.csv          → bronze/dim_stores/          (Delta, full snapshot)
  dim_products.csv        → bronze/dim_products/        (Delta, full snapshot)
  dim_customers.csv       → bronze/dim_customers/       (Delta, full snapshot)
  fact_transactions.csv   → bronze/transactions/        (Delta, monthly batched)
  fact_returns.csv        → bronze/returns/             (Delta, full load)

Quarantined rows (ERR flags) → bronze/quarantine/ (Delta, separate table)

Key features preserved from v1 flat-file bronze:
  - Terminal master join (broadcast, replace CSV values with master truth)
  - Store ID validation against store_master.json
  - DQ split: clean rows → transactions/, ERR rows → quarantine/
  - Monthly batch processing to cap memory at ~1 month of data
  - Broadcast terminal lookup for efficient join

Why Delta over Parquet:
  - ACID transactions (no corrupt partial writes)
  - Schema enforcement on write
  - Time travel (query previous versions)
  - MERGE support (for future SCD2 / incremental loads)
  - Direct path to Databricks — same format, zero migration

Run:
    python pipeline/bronze/ingest_raw.py
    python pipeline/run_pipeline.py --layer bronze
"""

import json
import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, BooleanType,
)

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipeline.common.spark_session import get_spark, RAW_DIR, BRONZE_DIR, ensure_dirs


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MASTER_DIR   = PROJECT_ROOT / "master"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_json(path: Path, label: str) -> dict:
    """Load a JSON master file with clear error message if missing."""
    if not path.exists():
        raise FileNotFoundError(f"{label} not found at {path}")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Transformations (applied to fact_transactions only)
# ---------------------------------------------------------------------------

def apply_terminal_join(df: DataFrame, term_df: DataFrame) -> DataFrame:
    """Replace CSV terminal columns with authoritative master-file values.

    - Drops terminal_type and is_self_checkout from the CSV
    - Joins master terminal data on pos_terminal_id
    - Nulls out cashier_id on self-checkout lanes
    - Flags rows with no terminal match as WARN:UNKNOWN_TERMINAL
    """
    cols_to_drop = [c for c in ["terminal_type", "is_self_checkout"]
                    if c in df.columns]
    if cols_to_drop:
        df = df.drop(*cols_to_drop)

    df = df.join(F.broadcast(term_df), on="pos_terminal_id", how="left")

    # Self-checkout lanes have no cashier
    df = df.withColumn(
        "cashier_id",
        F.when(
            F.col("is_self_checkout") == True,
            F.lit(None).cast(StringType())
        ).otherwise(F.col("cashier_id"))
    )

    # Flag unmatched terminals
    df = df.withColumn(
        "data_quality_flag",
        F.when(
            F.col("terminal_type").isNull(),
            F.concat(F.col("data_quality_flag"), F.lit("|WARN:UNKNOWN_TERMINAL"))
        ).otherwise(F.col("data_quality_flag"))
    )
    return df


def apply_store_check(df: DataFrame, known_store_ids: list) -> DataFrame:
    """Flag rows whose store_id is not in store_master.json."""
    return df.withColumn(
        "data_quality_flag",
        F.when(
            ~F.col("store_id").isin(known_store_ids),
            F.concat(F.col("data_quality_flag"), F.lit("|WARN:UNKNOWN_STORE"))
        ).otherwise(F.col("data_quality_flag"))
    )


def split_dq(df: DataFrame, quarantine_codes: list):
    """Split into (df_clean, df_quarantine).

    ERR rows → quarantine (with timestamp and repair status).
    WARN-only rows → clean stream with is_clean = False.
    OK rows → clean stream with is_clean = True.
    """
    err_pattern = "|".join(quarantine_codes)
    df = df.withColumn("_has_error", F.col("data_quality_flag").rlike(err_pattern))

    df_clean = (df
        .filter(~F.col("_has_error"))
        .drop("_has_error")
        .withColumn("is_clean", ~F.col("data_quality_flag").rlike("WARN:")))

    df_quarantine = (df
        .filter(F.col("_has_error"))
        .drop("_has_error")
        .withColumn("quarantine_ts", F.lit(datetime.utcnow().isoformat()))
        .withColumn("repair_status", F.lit("PENDING")))

    return df_clean, df_quarantine


# ---------------------------------------------------------------------------
# Dimension ingestion (simple — no DQ split, no batching)
# ---------------------------------------------------------------------------

def ingest_dimension(spark, source_name: str, ingestion_ts: str) -> int:
    """Read a dimension CSV and write as a Delta table with metadata.

    Dimensions are small (50 stores, 2k products, 300k customers)
    so we read them in one shot — no monthly batching needed.
    Delta overwrite replaces the entire table each run.
    """
    csv_path = RAW_DIR / f"{source_name}.csv"
    if not csv_path.exists():
        print(f"    SKIP  {source_name} — file not found at {csv_path}")
        return 0

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("encoding", "UTF-8")
        .csv(str(csv_path))
        .withColumn("_ingested_at", F.lit(ingestion_ts))
        .withColumn("_source_file", F.lit(csv_path.name))
    )

    output_path = str(BRONZE_DIR / source_name)
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(output_path)
    )

    row_count = df.count()
    print(f"    OK    {source_name:<24} → {row_count:>8,} rows  [Delta]")
    return row_count


# ---------------------------------------------------------------------------
# Returns ingestion
# ---------------------------------------------------------------------------

def ingest_returns(spark, ingestion_ts: str) -> int:
    """Read fact_returns.csv and write as a Delta table with metadata.

    Returns are typically ~2-4% of transaction volume.
    Partitioned by return_month for efficient downstream querying.
    """
    csv_path = RAW_DIR / "fact_returns.csv"
    if not csv_path.exists():
        print(f"    SKIP  fact_returns — file not found")
        return 0

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("encoding", "UTF-8")
        .csv(str(csv_path))
        .withColumn("_ingested_at", F.lit(ingestion_ts))
        .withColumn("_source_file", F.lit("fact_returns.csv"))
        .withColumn("return_month", F.date_format(
            F.to_date(F.col("return_date")), "yyyy-MM"
        ))
    )

    output_path = str(BRONZE_DIR / "returns")
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("return_month")
        .save(output_path)
    )

    row_count = df.count()
    print(f"    OK    fact_returns            → {row_count:>8,} rows  "
          f"[Delta, partitioned by return_month]")
    return row_count


# ---------------------------------------------------------------------------
# Fact transactions — monthly batched with terminal join + DQ split
# ---------------------------------------------------------------------------

def ingest_transactions(spark, ingestion_ts: str) -> dict:
    """Read fact_transactions.csv and process in monthly batches.

    Same monthly batch strategy as the v1 flat-file bronze layer:
      1. Build lazy query plan over the full CSV
      2. Discover distinct months
      3. For each month:
         a. Filter to that month's rows
         b. Join terminal master (broadcast)
         c. Validate store_ids against store master
         d. Split into clean + quarantine
         e. Write to Delta table (partitioned by order_month)
         f. Release memory

    Peak heap = one month of data at a time.

    Delta advantages over Parquet here:
      - ACID: if the job crashes mid-month, no corrupt partial folders
      - Schema enforcement: catches type drift between generator versions
      - Time travel: can query previous pipeline runs
    """
    csv_path = RAW_DIR / "fact_transactions.csv"
    if not csv_path.exists():
        print(f"    SKIP  fact_transactions — file not found")
        return {"clean": 0, "quarantine": 0}

    # ── Load master files ─────────────────────────────────────────────
    terminal_dict = load_json(MASTER_DIR / "terminal_master.json", "terminal_master")
    store_dict    = load_json(MASTER_DIR / "store_master.json", "store_master")

    quarantine_codes = ["ERR:PRICE_NULL", "ERR:QTY_NULL", "ERR:REVENUE_NULL"]
    known_store_ids  = [s["store_id"] for s in store_dict["stores"]]

    print(f"    Terminals loaded : {len(terminal_dict['terminals'])}")
    print(f"    Stores loaded    : {len(store_dict['stores'])}")

    # ── Build terminal broadcast table ────────────────────────────────
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

    # ── Build lazy query plan ─────────────────────────────────────────
    df_full = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("dateFormat", "yyyy-MM-dd")
        .option("nullValue", "")
        .option("encoding", "UTF-8")
        .csv(str(csv_path))
        .withColumn("ingestion_ts", F.lit(ingestion_ts))
        .withColumn("order_month", F.date_format(
            F.to_date(F.col("order_date")), "yyyy-MM"
        ))
    )

    # ── Discover distinct months ──────────────────────────────────────
    months = (
        df_full
        .select("order_month")
        .distinct()
        .orderBy("order_month")
        .collect()
    )

    print(f"    Months found     : {len(months)}")
    print(f"    Partition key    : order_month")
    print(f"    Storage format   : Delta Lake\n")

    # ── Output paths ──────────────────────────────────────────────────
    clean_path = str(BRONZE_DIR / "transactions")
    quar_path  = str(BRONZE_DIR / "quarantine")

    # ── Monthly batch loop ────────────────────────────────────────────
    total_clean = 0
    total_quar  = 0

    for i, row in enumerate(months):
        ym = row["order_month"]
        is_first = (i == 0)

        # Filter to this month — Spark reads only matching rows from CSV
        batch = df_full.filter(F.col("order_month") == ym)

        # Apply transformations
        batch = apply_terminal_join(batch, term_df)
        batch = apply_store_check(batch, known_store_ids)
        df_clean, df_quarantine = split_dq(batch, quarantine_codes)

        clean_count = df_clean.count()
        quar_count  = df_quarantine.count()
        total_clean += clean_count
        total_quar  += quar_count

        # First batch: overwrite (clears previous run)
        # Subsequent batches: append (adds new month partition)
        write_mode = "overwrite" if is_first else "append"

        (df_clean
         .write
         .format("delta")
         .mode(write_mode)
         .partitionBy("order_month")
         .save(clean_path))

        if quar_count > 0:
            (df_quarantine
             .write
             .format("delta")
             .mode(write_mode)
             .save(quar_path))

        print(f"    [{i+1:>3}/{len(months)}] {ym}  "
              f"clean={clean_count:>7,}  quarantine={quar_count:>5,}  "
              f"({write_mode})")

        # Release memory before next iteration
        batch.unpersist()
        df_clean.unpersist()
        df_quarantine.unpersist()

    return {"clean": total_clean, "quarantine": total_quar}


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_bronze(spark=None):
    """Execute the full bronze layer.

    Ingestion order:
      1. Dimensions (small, Delta, no DQ split)
      2. Returns (medium, Delta, no DQ split)
      3. Transactions (large, Delta, monthly batched, terminal join, DQ split)
    """
    print("\n  ┌─ BRONZE LAYER ─────────────────────────────────┐")
    print("  │  Raw CSVs → Delta tables with validation        │")
    print("  └─────────────────────────────────────────────────┘")

    ensure_dirs()
    ingestion_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    own_spark    = spark is None

    if own_spark:
        spark = get_spark("einkaufpark_bronze")

    total_rows = 0

    # Step 1: Dimensions (simple Delta ingestion)
    print(f"\n  Dimensions:")
    for dim in ["dim_stores", "dim_products", "dim_customers"]:
        total_rows += ingest_dimension(spark, dim, ingestion_ts)

    # Step 2: Returns (Delta with month partitioning)
    print(f"\n  Returns:")
    total_rows += ingest_returns(spark, ingestion_ts)

    # Step 3: Transactions (monthly batched, terminal join, DQ split)
    print(f"\n  Transactions (monthly batch mode):")
    txn_counts = ingest_transactions(spark, ingestion_ts)
    total_rows += txn_counts["clean"] + txn_counts["quarantine"]

    # Summary
    print(f"\n  {'─' * 50}")
    print(f"  Bronze complete")
    print(f"  {'─' * 50}")
    print(f"  Total rows ingested  : {total_rows:>10,}")
    print(f"  Transactions (clean) : {txn_counts['clean']:>10,}")
    print(f"  Transactions (quar.) : {txn_counts['quarantine']:>10,}")
    print(f"  Storage format       : Delta Lake")
    print(f"  Ingestion timestamp  : {ingestion_ts}")
    print(f"  Output               : {BRONZE_DIR}/")

    if own_spark:
        spark.stop()

    return total_rows


if __name__ == "__main__":
    run_bronze()