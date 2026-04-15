"""
spark_bronze.py  —  Einkaufpark DE Bronze Layer
================================================
Monthly batch mode — processes one calendar month at a time so peak
heap stays at ~275K rows instead of 10M.

Partition strategy:
  - Filters by month in the loop           (controls memory)
  - Writes partitioned by order_month      (controls folder count)
  Each batch writes to exactly ONE partition folder — no OOM risk.

Run from project root:
    python spark/spark_bronze.py
"""

import json
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType,
    BooleanType, DateType, DecimalType
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MASTER_DIR   = PROJECT_ROOT / "master"
RAW_CSV      = PROJECT_ROOT / "data" / "raw" / "einkaufpark_de_sales_raw.csv"
BRONZE_DIR   = PROJECT_ROOT / "data" / "bronze"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_json(path: Path, label: str) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found at {path}")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def build_schema(schema_dict: dict) -> StructType:
    """Build a PySpark StructType from raw_schema.json column definitions."""
    type_map = {
        "string":  StringType(),
        "integer": IntegerType(),
        "boolean": BooleanType(),
        "date":    DateType(),
    }
    fields = []
    for col in schema_dict["columns"]:
        name     = col["name"]
        nullable = col.get("nullable", True)
        if col["type"] == "decimal":
            t = DecimalType(col.get("precision", 10), col.get("scale", 2))
        elif col["type"] in type_map:
            t = type_map[col["type"]]
        else:
            print(f"  [WARN] Unknown type '{col['type']}' for '{name}' -> StringType")
            t = StringType()
        fields.append(StructField(name, t, nullable))
    return StructType(fields)


def create_spark() -> SparkSession:
    return (SparkSession.builder
            .appName("EinkaufPark Bronze Layer")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.autoBroadcastJoinThreshold", "10485760")
            .getOrCreate())


# ---------------------------------------------------------------------------
# Transformations
# ---------------------------------------------------------------------------

def apply_terminal_join(df: DataFrame, term_df: DataFrame) -> DataFrame:
    """
    Replace CSV terminal columns with authoritative master-file values.
    Nulls out cashier_id on SCO lanes.
    Flags rows with no terminal match.
    """
    df = (df
          .drop("terminal_type", "is_self_checkout")
          .join(F.broadcast(term_df), on="pos_terminal_id", how="left"))

    df = df.withColumn(
        "cashier_id",
        F.when(
            F.col("is_self_checkout") == True,
            F.lit(None).cast(StringType())
        ).otherwise(F.col("cashier_id"))
    )

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
    """
    Split into (df_clean, df_quarantine).
    ERR: rows -> quarantine.
    WARN-only rows -> clean stream with is_clean = False.
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
# Main
# ---------------------------------------------------------------------------

def run():
    print("\n" + "-" * 55)
    print("  Einkaufpark DE — Bronze Layer (monthly batch mode)")
    print("-" * 55)

    # ── Load master files ──────────────────────────────────────────────────
    raw_schema_dict  = load_json(MASTER_DIR / "raw_schema.json",      "raw_schema.json")
    terminal_dict    = load_json(MASTER_DIR / "terminal_master.json", "terminal_master.json")
    store_dict       = load_json(MASTER_DIR / "store_master.json",    "store_master.json")

    schema_version   = raw_schema_dict.get("schema_version", "?")
    quarantine_codes = raw_schema_dict.get("dq_rules", {}).get("quarantine_on", [
        "ERR:PRICE_NULL", "ERR:QTY_NULL", "ERR:REVENUE_NULL"
    ])
    known_store_ids  = [s["store_id"] for s in store_dict["stores"]]
    ingestion_ts     = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    print(f"  Schema   : v{schema_version} · {len(raw_schema_dict['columns'])} columns")
    print(f"  Terminals: {len(terminal_dict['terminals'])}")
    print(f"  Stores   : {len(store_dict['stores'])}")

    # ── Build Spark objects ────────────────────────────────────────────────
    bronze_schema = build_schema(raw_schema_dict)
    spark         = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Terminal lookup table — broadcast once, reused in every batch
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

    # ── Build lazy query plan over the full CSV ────────────────────────────
    # No data is read here. Spark records the plan and executes it
    # inside each monthly filter below.
    print(f"\n  Building query plan from: {RAW_CSV.name}")
    df_full = (spark.read
               .option("header",     True)
               .option("dateFormat", "yyyy-MM-dd")
               .option("nullValue",  "")
               .option("mode",       "PERMISSIVE")
               .schema(bronze_schema)
               .csv(str(RAW_CSV))
               .withColumn("ingestion_ts", F.lit(ingestion_ts))
               # Derive order_month here so every batch already has the column.
               # This is the partition key — one folder per month on disk.
               .withColumn("order_month", F.date_format("order_date", "yyyy-MM")))

    # ── Discover distinct months (reads order_date column only) ───────────
    print("  Scanning for distinct months...")
    months = (df_full
              .select("order_month")
              .distinct()
              .orderBy("order_month")
              .collect())

    print(f"  Found {len(months)} months\n")
    print(f"  Partition strategy : order_month  "
          f"({len(months)} folders, one per month)")
    print(f"  Each batch writes  : exactly 1 folder  (no OOM risk)\n")

    # ── Output paths ───────────────────────────────────────────────────────
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    clean_path = str(BRONZE_DIR / "transactions")
    quar_path  = str(BRONZE_DIR / "quarantine")

    # ── Monthly batch loop ─────────────────────────────────────────────────
    #
    # Memory profile per iteration:
    #   - Filter  : Spark scans CSV, materialises only this month's rows
    #   - Transform: all operations run on ~275K rows in memory
    #   - Write   : flushes to ONE partition folder, releases memory
    #   - Unpersist: explicitly frees the batch before the next iteration
    #
    # Peak heap at any moment = memory for ONE month (~275K rows)
    # regardless of the total dataset size.

    total_clean = 0
    total_quar  = 0

    for i, row in enumerate(months):
        ym       = row["order_month"]
        is_first = (i == 0)

        # Filter to this month — Spark reads only the matching rows from CSV
        batch = df_full.filter(F.col("order_month") == ym)

        # Apply transformations
        batch = apply_terminal_join(batch, term_df)
        batch = apply_store_check(batch, known_store_ids)
        df_clean, df_quarantine = split_dq(batch, quarantine_codes)

        clean_count = df_clean.count()
        quar_count  = df_quarantine.count()
        total_clean += clean_count
        total_quar  += quar_count

        # overwrite on first batch — clears previous run's output
        # append on subsequent batches — adds without touching earlier months
        write_mode = "overwrite" if is_first else "append"

        # Each batch writes to exactly one folder:
        #   bronze/transactions/order_month=2023-01/
        #   bronze/transactions/order_month=2023-02/  ...etc
        (df_clean
         .write
         .mode(write_mode)
         .partitionBy("order_month")
         .parquet(clean_path))

        (df_quarantine
         .write
         .mode(write_mode)
         .parquet(quar_path))

        print(f"  [{i+1:>3}/{len(months)}] {ym}  "
              f"clean={clean_count:>7,}  quarantine={quar_count:>5,}  "
              f"-> order_month={ym}/  ({write_mode})")

        # Release memory before the next iteration
        batch.unpersist()
        df_clean.unpersist()
        df_quarantine.unpersist()

    # ── Summary ────────────────────────────────────────────────────────────
    total_rows = total_clean + total_quar

    print(f"\n  Bronze complete")
    print(f"  {'-' * 45}")
    print(f"  Months processed : {len(months)}")
    print(f"  Total rows       : {total_rows:,}")
    print(f"  Clean written    : {total_clean:,}  ({total_clean/total_rows*100:.1f}%)")
    print(f"  Quarantined      : {total_quar:,}  ({total_quar/total_rows*100:.1f}%)")
    print(f"  Clean path       : {clean_path}/order_month=YYYY-MM/")
    print(f"  Quarantine path  : {quar_path}/")
    print(f"  Schema version   : v{schema_version}\n")

    spark.stop()


if __name__ == "__main__":
    run()