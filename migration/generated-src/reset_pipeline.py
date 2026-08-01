"""Drop existing medallion outputs before the first corrected pipeline run."""

from pipeline.common.spark_session import drop_table, get_spark, is_databricks

TABLES = {
    "gold": [
        "gld_daily_sales",
        "gld_store_performance",
        "gld_product_performance",
        "gld_customer_ltv",
        "gld_basket_analysis",
        "gld_return_analysis",
        "gld_hourly_traffic",
    ],
    "silver": [
        "dim_store",
        "dim_customer",
        "dim_product_scd2",
        "dim_product",
        "duplicate_transactions",
        "fact_sales_all",
        "fact_sales",
        "fact_voids",
        "fact_sales_review",
        "duplicate_returns",
        "fact_returns",
        "fact_returns_review",
    ],
    "bronze": [
        "dim_stores",
        "dim_customers",
        "dim_products_scd2",
        "returns",
        "transactions",
        "quarantine",
    ],
}


def main() -> None:
    spark = get_spark("einkaufpark_reset")
    try:
        for layer, names in TABLES.items():
            for name in names:
                drop_table(spark, layer, name)
                print(f"dropped {layer}.{name}")
    finally:
        if not is_databricks():
            spark.stop()


if __name__ == "__main__":
    main()
