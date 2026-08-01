"""Run Bronze → Silver → Gold with one shared Spark session."""

from pipeline.bronze.ingest_raw import run_bronze
from pipeline.common.spark_session import get_spark
from pipeline.gold.aggregate import run_gold
from pipeline.silver.transform import run_silver


def main() -> None:
    spark = get_spark("einkaufpark_end_to_end")
    try:
        if not run_bronze(spark):
            raise RuntimeError("Bronze validation failed")
        if not run_silver(spark, walkin_target=0.10):
            raise RuntimeError("Silver validation failed")
        if not run_gold(spark):
            raise RuntimeError("Gold validation failed")
    finally:
        # Databricks owns its active Spark session; stopping is harmless locally
        # but should be avoided in notebooks. Only local mode reaches this call.
        from pipeline.common.spark_session import is_databricks

        if not is_databricks():
            spark.stop()


if __name__ == "__main__":
    main()
