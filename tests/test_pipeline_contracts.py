from __future__ import annotations

from datetime import date, datetime

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pipeline.gold import aggregate
from pipeline.silver.transform import (
    deduplicate_retries,
    detect_transaction_conflicts,
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("einkaufpark-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def _transaction_schema() -> StructType:
    return StructType([
        StructField("transaction_id", StringType(), False),
        StructField("basket_id", StringType(), False),
        StructField("store_id", StringType(), False),
        StructField("order_date", DateType(), False),
        StructField("customer_id", StringType(), True),
        StructField("record_hash", StringType(), False),
        StructField("data_quality_flag", StringType(), False),
        StructField("ingestion_date", DateType(), False),
        StructField("_ingested_at", TimestampType(), False),
        StructField("_source_file", StringType(), False),
    ])


def test_exact_retry_is_removed_but_original_is_kept(spark):
    rows = [
        ("TX1", "B1", "S1", date(2025, 1, 1), "C1", "HASH1", "OK",
         date(2025, 1, 1), datetime(2025, 1, 1, 1), "batch_1.csv"),
        ("TX1", "B1", "S1", date(2025, 1, 1), "C1", "HASH1",
         "INFO:DUPLICATE_TXN", date(2025, 1, 1),
         datetime(2025, 1, 1, 2), "batch_1.csv"),
    ]
    frame = spark.createDataFrame(rows, _transaction_schema())
    kept, duplicates = deduplicate_retries(frame)

    assert kept.count() == 1
    assert duplicates.count() == 1
    assert kept.first().data_quality_flag == "OK"


def test_transaction_id_cannot_change_business_context(spark):
    rows = [
        ("TX1", "B1", "S1", date(2025, 1, 1), "C1", "H1", "OK",
         date(2025, 1, 1), datetime(2025, 1, 1, 1), "a.csv"),
        ("TX1", "B2", "S2", date(2025, 1, 2), "C2", "H2", "OK",
         date(2025, 1, 2), datetime(2025, 1, 2, 1), "b.csv"),
    ]
    frame = spark.createDataFrame(rows, _transaction_schema())
    assert detect_transaction_conflicts(frame) == 1


def test_product_performance_has_one_row_per_product(spark, monkeypatch):
    sales = spark.createDataFrame([
        ("P1", "Product", "Category", "Sub", "Brand", "unit", "B1", "S1",
         date(2025, 1, 1), 1, 10.0, 1.0, 9.0, 10.0, 10.0),
        ("P1", "Product", "Category", "Sub", "Brand", "unit", "B2", "S1",
         date(2025, 2, 1), 2, 24.0, 4.0, 20.0, 12.0, 12.0),
    ], [
        "product_id", "product_name", "category", "subcategory",
        "default_brand", "unit", "basket_id", "store_id", "order_date",
        "quantity", "sales_before_discount_eur", "discount_amount_eur",
        "revenue_ex_vat_eur", "unit_price_eur", "list_price_eur",
    ])
    captured = {}

    def fake_write(df, name, partition_by=None):
        captured["df"] = df.cache()
        return df.count()

    monkeypatch.setattr(aggregate, "write_gold", fake_write)
    assert aggregate.agg_product_performance(sales) == 1
    output = captured["df"]
    assert output.select("product_id").distinct().count() == output.count() == 1
    captured["df"].unpersist()


def test_return_analysis_does_not_multiply_refunds(spark, monkeypatch):
    sales = spark.createDataFrame([
        ("P1", "Product", "Category", "Sub", 2, 18.0, "B1"),
        ("P1", "Product", "Category", "Sub", 3, 27.0, "B2"),
    ], [
        "product_id", "product_name", "category", "subcategory",
        "quantity", "revenue_ex_vat_eur", "basket_id",
    ])
    returns = spark.createDataFrame([
        ("P1", "Damaged", "R1", "B1", 1, 1.0, 2),
        ("P1", "Changed_Mind", "R2", "B2", 1, 2.0, 3),
    ], [
        "product_id", "reason_code", "return_id", "original_basket_id",
        "return_quantity", "refund_amount_eur", "return_delay_days",
    ])
    captured = {}

    def fake_write(df, name, partition_by=None):
        captured["df"] = df.cache()
        return df.count()

    monkeypatch.setattr(aggregate, "write_gold", fake_write)
    assert aggregate.agg_return_analysis(sales, returns) == 2
    output = captured["df"]
    assert output.agg(F.sum("total_refund_eur")).first()[0] == pytest.approx(3.0)
    assert output.groupBy("product_id", "reason_code").count().filter(
        F.col("count") > 1
    ).count() == 0
    captured["df"].unpersist()


def test_store_customer_types_are_distinct_baskets(spark, monkeypatch):
    sales = spark.createDataFrame([
        ("S1", "Berlin", "Berlin", "L", "B1", "C1", True, False, True,
         date(2025, 1, 1), 1, 10.0, 1.0, 9.0, 0.63, 9.63),
        ("S1", "Berlin", "Berlin", "L", "B1", "C1", True, False, True,
         date(2025, 1, 1), 2, 20.0, 2.0, 18.0, 1.26, 19.26),
        ("S1", "Berlin", "Berlin", "L", "B2", None, False, True, False,
         date(2025, 1, 1), 1, 5.0, 0.0, 5.0, 0.35, 5.35),
    ], [
        "store_id", "city", "region", "size_class", "basket_id",
        "customer_id", "customer_is_member", "is_walk_in",
        "membership_active", "order_date", "quantity",
        "sales_before_discount_eur", "discount_amount_eur",
        "revenue_ex_vat_eur", "vat_amount_eur", "revenue_inc_vat_eur",
    ])
    return_schema = StructType([
        StructField("store_id", StringType(), False),
        StructField("return_id", StringType(), False),
        StructField("original_basket_id", StringType(), False),
        StructField("return_quantity", IntegerType(), False),
        StructField("refund_amount_eur", DoubleType(), False),
    ])
    returns = spark.createDataFrame([], return_schema)
    captured = {}

    def fake_write(df, name, partition_by=None):
        captured["df"] = df.cache()
        return df.count()

    monkeypatch.setattr(aggregate, "write_gold", fake_write)
    assert aggregate.agg_store_performance(sales, returns) == 1
    row = captured["df"].first()
    assert row.total_baskets == 2
    assert row.member_baskets == 1
    assert row.walk_in_baskets == 1
    assert row.identified_non_member_baskets == 0
    captured["df"].unpersist()
