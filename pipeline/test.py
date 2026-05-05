"""
Data Quality Tests (Delta + Parquet)
=====================================
Validates silver and gold layer integrity after pipeline run.

Usage:
  python -m pytest tests/ -v
  make test
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline.common.spark_session import get_spark, SILVER_DIR, GOLD_DIR


@pytest.fixture(scope="session")
def spark():
    s = get_spark("einkaufpark_tests")
    yield s
    s.stop()


# ═══════════════════════════════════════════════════════════════════════════
# Silver Layer Tests
# ═══════════════════════════════════════════════════════════════════════════

class TestSilverDimensions:

    def test_stores_no_nulls_in_store_id(self, spark):
        stores = spark.read.format("delta").load(str(SILVER_DIR / "dim_store"))
        nulls = stores.filter(stores.store_id.isNull()).count()
        assert nulls == 0, f"{nulls} NULL store_ids"

    def test_products_have_price_band(self, spark):
        products = spark.read.format("delta").load(str(SILVER_DIR / "dim_product"))
        valid = {"Budget (≤€2)", "Mid (€2-5)", "Premium (€5-10)", "High (€10+)"}
        actual = {r.price_band for r in
                  products.select("price_band").distinct().collect()}
        assert actual.issubset(valid), f"Invalid price bands: {actual - valid}"

    def test_customers_have_age_group(self, spark):
        customers = spark.read.format("delta").load(str(SILVER_DIR / "dim_customer"))
        valid = {"18-24", "25-34", "35-49", "50-64", "65+", "Unknown"}
        actual = {r.age_group for r in
                  customers.select("age_group").distinct().collect()}
        assert actual.issubset(valid), f"Invalid age groups: {actual - valid}"


class TestSilverFacts:

    def test_no_orphan_store_ids(self, spark):
        sales  = spark.read.format("delta").load(str(SILVER_DIR / "fact_sales"))
        stores = spark.read.format("delta").load(str(SILVER_DIR / "dim_store"))
        fact_ids = {r.store_id for r in sales.select("store_id").distinct().collect()}
        dim_ids  = {r.store_id for r in stores.select("store_id").distinct().collect()}
        assert fact_ids.issubset(dim_ids), f"Orphan store_ids: {fact_ids - dim_ids}"

    def test_no_orphan_product_ids(self, spark):
        sales    = spark.read.format("delta").load(str(SILVER_DIR / "fact_sales"))
        products = spark.read.format("delta").load(str(SILVER_DIR / "dim_product"))
        fact_ids = {r.product_id for r in sales.select("product_id").distinct().collect()}
        dim_ids  = {r.product_id for r in products.select("product_id").distinct().collect()}
        assert fact_ids.issubset(dim_ids), f"Orphan product_ids: {fact_ids - dim_ids}"

    def test_clean_rows_positive_revenue(self, spark):
        sales = spark.read.format("delta").load(str(SILVER_DIR / "fact_sales"))
        clean = sales.filter(sales.dq_status == "CLEAN")
        bad   = clean.filter(
            (clean.net_revenue_eur.isNull()) | (clean.net_revenue_eur <= 0)
        ).count()
        total = clean.count()
        assert bad / max(total, 1) < 0.01, f"{bad}/{total} non-positive revenue"

    def test_gross_revenue_gte_net(self, spark):
        sales = spark.read.format("delta").load(str(SILVER_DIR / "fact_sales"))
        clean = sales.filter(sales.dq_status == "CLEAN")
        bad = clean.filter(clean.gross_revenue_eur < clean.net_revenue_eur).count()
        assert bad == 0, f"{bad} rows where gross < net (VAT error)"

    def test_no_sunday_transactions(self, spark):
        sales = spark.read.format("delta").load(str(SILVER_DIR / "fact_sales"))
        # Spark: 1=Sunday
        sundays = sales.filter(sales.day_of_week == 1).count()
        assert sundays == 0, f"{sundays} Sunday transactions"

    def test_price_vs_catalogue_valid_values(self, spark):
        sales = spark.read.format("delta").load(str(SILVER_DIR / "fact_sales"))
        valid = {"IN_RANGE", "BELOW_RANGE", "ABOVE_RANGE"}
        actual = {r.price_vs_catalogue for r in
                  sales.select("price_vs_catalogue").distinct().collect()
                  if r.price_vs_catalogue is not None}
        assert actual.issubset(valid), f"Invalid price_vs_catalogue: {actual - valid}"

    def test_walk_in_flag_consistent(self, spark):
        """is_walk_in=True iff customer_id is NULL."""
        sales = spark.read.format("delta").load(str(SILVER_DIR / "fact_sales"))
        mismatched = sales.filter(
            (sales.is_walk_in == True) & (sales.customer_id.isNotNull()) |
            (sales.is_walk_in == False) & (sales.customer_id.isNull())
        ).count()
        assert mismatched == 0, f"{mismatched} rows where is_walk_in doesn't match customer_id"


# ═══════════════════════════════════════════════════════════════════════════
# Gold Layer Tests
# ═══════════════════════════════════════════════════════════════════════════

class TestGoldDailySales:

    def test_no_null_keys(self, spark):
        daily = spark.read.parquet(str(GOLD_DIR / "gld_daily_sales"))
        nulls = daily.filter(
            daily.order_date.isNull() |
            daily.store_id.isNull() |
            daily.category.isNull()
        ).count()
        assert nulls == 0, f"{nulls} NULL keys"

    def test_revenue_positive(self, spark):
        daily = spark.read.parquet(str(GOLD_DIR / "gld_daily_sales"))
        bad = daily.filter(daily.net_revenue_eur <= 0).count()
        assert bad == 0, f"{bad} rows with non-positive revenue"


class TestGoldStorePerformance:

    def test_all_stores_present(self, spark):
        stores = spark.read.format("delta").load(str(SILVER_DIR / "dim_store"))
        perf   = spark.read.parquet(str(GOLD_DIR / "gld_store_performance"))
        expected = stores.count()
        actual   = perf.count()
        assert actual == expected, f"Expected {expected} stores, got {actual}"

    def test_revenue_rank_unique(self, spark):
        perf = spark.read.parquet(str(GOLD_DIR / "gld_store_performance"))
        ranks  = perf.select("revenue_rank").distinct().count()
        stores = perf.count()
        assert ranks == stores, "Duplicate revenue ranks"


class TestGoldProductPerformance:

    def test_pareto_classes_valid(self, spark):
        prod = spark.read.parquet(str(GOLD_DIR / "gld_product_performance"))
        valid = {"A (top 80%)", "B (next 15%)", "C (tail 5%)"}
        actual = {r.pareto_class for r in
                  prod.select("pareto_class").distinct().collect()}
        assert actual.issubset(valid), f"Invalid pareto classes: {actual - valid}"

    def test_cumulative_revenue_reaches_100(self, spark):
        prod = spark.read.parquet(str(GOLD_DIR / "gld_product_performance"))
        max_cum = prod.agg({"cumulative_revenue_pct": "max"}).collect()[0][0]
        assert max_cum >= 99.9, f"Cumulative revenue only reaches {max_cum}%"


class TestGoldCustomerLTV:

    def test_segments_valid(self, spark):
        ltv = spark.read.parquet(str(GOLD_DIR / "gld_customer_ltv"))
        valid = {"Champion", "Loyal", "Regular", "Occasional"}
        actual = {r.customer_segment for r in
                  ltv.select("customer_segment").distinct().collect()}
        assert actual.issubset(valid), f"Invalid segments: {actual - valid}"

    def test_no_walk_ins_in_ltv(self, spark):
        """LTV table should only contain identified customers."""
        ltv = spark.read.parquet(str(GOLD_DIR / "gld_customer_ltv"))
        nulls = ltv.filter(ltv.customer_id.isNull()).count()
        assert nulls == 0, "Walk-ins (NULL customer_id) in LTV table"


class TestGoldBasketAnalysis:

    def test_basket_value_buckets_valid(self, spark):
        baskets = spark.read.parquet(str(GOLD_DIR / "gld_basket_analysis"))
        valid = {"< €10", "€10-30", "€30-75", "€75+"}
        actual = {r.basket_value_bucket for r in
                  baskets.select("basket_value_bucket").distinct().collect()}
        assert actual.issubset(valid), f"Invalid buckets: {actual - valid}"

    def test_customer_type_valid(self, spark):
        baskets = spark.read.parquet(str(GOLD_DIR / "gld_basket_analysis"))
        valid = {"Walk-in", "Loyalty"}
        actual = {r.customer_type for r in
                  baskets.select("customer_type").distinct().collect()}
        assert actual.issubset(valid), f"Invalid customer type: {actual - valid}"