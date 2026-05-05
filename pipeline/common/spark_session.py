"""
Shared SparkSession factory for the Einkaufpark pipeline.

Creates a consistently configured SparkSession with Delta Lake support.
Every pipeline script imports this instead of creating its own session.

In production Databricks, this entire file is unnecessary — the runtime
provides a pre-configured SparkSession with Delta built in.
"""

from pathlib import Path
from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Paths — all relative to project root (/app inside Docker)
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR      = PROJECT_ROOT / "data" / "raw"
BRONZE_DIR   = PROJECT_ROOT / "data" / "bronze"
SILVER_DIR   = PROJECT_ROOT / "data" / "silver"
GOLD_DIR     = PROJECT_ROOT / "data" / "gold"


def get_spark(app_name: str = "einkaufpark") -> SparkSession:
    """Create or return a SparkSession with Delta Lake enabled.

    Configuration choices:
      - local[*]                  : use all available cores
      - shuffle.partitions = 4    : sensible for laptop-scale data
      - driver.memory = 2g        : enough for 1M rows, safe on 8GB host
      - autoBroadcastJoinThreshold: broadcast dims up to 10MB
      - Delta extensions          : enables Delta format read/write
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        # Delta Lake
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.3.0")
        # Performance tuning for local mode
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.autoBroadcastJoinThreshold", "10485760")
        .config("spark.default.parallelism", "4")
        # Delta defaults
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        # Suppress noisy INFO logs
        .config("spark.driver.extraJavaOptions", "-Dlog4j.logLevel=WARN")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def ensure_dirs():
    """Create output directories if they don't exist."""
    for d in [RAW_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR]:
        d.mkdir(parents=True, exist_ok=True)