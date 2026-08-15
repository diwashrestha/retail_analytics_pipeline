from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
PIPELINES = ROOT / "pipelines"


def read(name: str) -> str:
    return (PIPELINES / name).read_text(encoding="utf-8")


def test_expected_gold_files_exist() -> None:
    expected = {
        "20_gold_baskets.sql",
        "21_gold_sales_stores.sql",
        "22_gold_products.sql",
        "23_gold_customers_returns_traffic.sql",
        "24_gold_quality.sql",
    }
    assert expected.issubset({p.name for p in PIPELINES.glob("*.sql")})


def test_expected_gold_datasets_are_defined_once() -> None:
    sql = "\n".join(
        read(name)
        for name in [
            "20_gold_baskets.sql",
            "21_gold_sales_stores.sql",
            "22_gold_products.sql",
            "23_gold_customers_returns_traffic.sql",
            "24_gold_quality.sql",
        ]
    )
    for dataset in [
        "basket_analysis",
        "daily_sales",
        "store_performance",
        "product_performance",
        "customer_ltv",
        "return_analysis",
        "hourly_traffic",
        "gold_quality_checks",
        "gold_quality_gate",
    ]:
        assert sql.count(f"MATERIALIZED VIEW {dataset}") == 1, dataset


def test_product_performance_has_product_grain_and_independent_return_join() -> None:
    sql = read("22_gold_products.sql")
    assert "GROUP BY product_sk, product_id" in sql
    assert "LEFT JOIN gold_product_return_metrics" in sql
    assert (
        "price_band"
        not in sql.split(
            "CREATE OR REFRESH PRIVATE MATERIALIZED VIEW gold_product_sales_metrics", 1
        )[1].split(
            "CREATE OR REFRESH PRIVATE MATERIALIZED VIEW gold_product_return_metrics", 1
        )[0]
    )


def test_return_analysis_does_not_repeat_sales_denominators() -> None:
    sql = read("23_gold_customers_returns_traffic.sql")
    block = sql.split("CREATE OR REFRESH MATERIALIZED VIEW return_analysis", 1)[1]
    block = block.split("CREATE OR REFRESH MATERIALIZED VIEW hourly_traffic", 1)[0]
    assert "sold_quantity" not in block
    assert "original_net_sales_eur" not in block
    assert "product_return_event_share_pct" in block


def test_customer_top_1000_uses_row_number() -> None:
    sql = read("23_gold_customers_returns_traffic.sql")
    assert "row_number() OVER" in sql
    assert "overall_ltv_rank <= 1000" in sql
    assert "ltv_rank_top_1000" in sql


def test_gold_quality_reconciles_all_major_outputs() -> None:
    sql = read("24_gold_quality.sql")
    for check in [
        "daily_sales_revenue_reconciliation",
        "basket_revenue_reconciliation",
        "store_revenue_reconciliation",
        "product_revenue_reconciliation",
        "hourly_revenue_reconciliation",
        "identified_customer_revenue_reconciliation",
        "product_refund_reconciliation",
        "return_reason_refund_reconciliation",
    ]:
        assert check in sql
    assert "ON VIOLATION FAIL UPDATE" in sql


def test_bundle_contains_gold_schema_and_sources() -> None:
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text(encoding="utf-8"))
    assert "gold_schema" in bundle["variables"]

    pipeline = yaml.safe_load(
        (ROOT / "resources" / "medallion_pipeline.yml").read_text(encoding="utf-8")
    )
    resource = pipeline["resources"]["pipelines"]["retail_medallion_pipeline"]
    source_paths = {item["file"]["path"] for item in resource["libraries"]}
    for name in [
        "20_gold_baskets.sql",
        "21_gold_sales_stores.sql",
        "22_gold_products.sql",
        "23_gold_customers_returns_traffic.sql",
        "24_gold_quality.sql",
    ]:
        assert f"../pipelines/{name}" in source_paths


def test_sql_delimiters_are_balanced() -> None:
    for path in PIPELINES.glob("*.sql"):
        text = path.read_text(encoding="utf-8")
        assert text.count("(") == text.count(")"), path.name
        assert text.count("'") % 2 == 0, path.name
