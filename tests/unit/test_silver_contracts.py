"""Static contracts for the Databricks SQL Silver package.

These tests do not replace a Databricks pipeline update. They catch accidental
renames, missing source files, and deployment drift before bundle deployment.
"""
from __future__ import annotations

from pathlib import Path
import re

import yaml

ROOT = Path(__file__).resolve().parents[2]
PIPELINES = ROOT / "pipelines"
RESOURCES = ROOT / "resources"

SILVER_FILES = [
    PIPELINES / "10_silver_dimensions.sql",
    PIPELINES / "11_silver_sales.sql",
    PIPELINES / "12_silver_returns.sql",
    PIPELINES / "13_silver_quality.sql",
]

EXPECTED_TABLES = {
    "dim_store",
    "dim_customer",
    "dim_product",
    "dim_product_scd2",
    "dim_terminal",
    "fact_sales",
    "fact_sales_review",
    "fact_voids",
    "fact_voids_review",
    "fact_returns",
    "fact_returns_review",
    "duplicate_transactions",
    "duplicate_returns",
    "silver_transaction_reconciliation",
    "silver_return_reconciliation",
    "silver_quality_checks",
    "silver_quality_gate",
}


def _created_dataset_names(sql: str) -> set[str]:
    pattern = re.compile(
        r"CREATE\s+OR\s+REFRESH\s+(?:PRIVATE\s+)?(?:MATERIALIZED\s+VIEW|STREAMING\s+TABLE)\s+([A-Za-z_][A-Za-z0-9_]*)",
        re.IGNORECASE,
    )
    return {match.group(1).lower() for match in pattern.finditer(sql)}


def test_all_expected_source_files_exist() -> None:
    assert (PIPELINES / "00_bronze.sql").is_file()
    for path in SILVER_FILES:
        assert path.is_file(), path


# def test_silver_files_publish_to_parameterized_schema() -> None:
#     for path in SILVER_FILES:
#         sql = path.read_text(encoding="utf-8")
#         assert "USE CATALOG IDENTIFIER(:silver_catalog);" in sql
#         assert "USE SCHEMA IDENTIFIER(:silver_schema);" in sql

def test_silver_files_publish_to_dev_schema() -> None:
    for path in SILVER_FILES:
        sql = path.read_text(encoding="utf-8")

        assert "USE CATALOG workspace;" in sql
        assert "USE SCHEMA retail_dev_silver;" in sql


def test_expected_silver_datasets_are_declared() -> None:
    names: set[str] = set()
    for path in SILVER_FILES:
        names |= _created_dataset_names(path.read_text(encoding="utf-8"))
    assert EXPECTED_TABLES <= names


def test_exact_retries_are_preserved_for_audit() -> None:
    sql = (PIPELINES / "11_silver_sales.sql").read_text(encoding="utf-8")
    assert "transaction_payload_hash" in sql
    assert "canonical_bronze_record_fingerprint" in sql
    assert "EXACT_POS_RETRY" in sql


def test_invalid_returns_do_not_consume_cumulative_capacity() -> None:
    sql = (PIPELINES / "12_silver_returns.sql").read_text(encoding="utf-8")
    assert "returns_base_classified" in sql
    assert "return_cumulative_candidates" in sql
    assert "WHERE base_review_reasons = ''" in sql


def test_quality_gate_fails_critical_contract_violations() -> None:
    sql = (PIPELINES / "13_silver_quality.sql").read_text(encoding="utf-8")
    assert "EXPECT (failed_critical_checks = 0)" in sql
    assert "ON VIOLATION FAIL UPDATE" in sql


def test_bundle_defines_one_combined_pipeline() -> None:
    resource_files = sorted(RESOURCES.glob("*.yml"))
    documents = [yaml.safe_load(path.read_text(encoding="utf-8")) for path in resource_files]
    pipelines: dict = {}
    for document in documents:
        pipelines.update((document or {}).get("resources", {}).get("pipelines", {}))
    assert set(pipelines) == {"retail_medallion_pipeline"}
    libraries = pipelines["retail_medallion_pipeline"]["libraries"]
    paths = {item["file"]["path"] for item in libraries}
    assert paths == {
        "../pipelines/00_bronze.sql",
        "../pipelines/10_silver_dimensions.sql",
        "../pipelines/11_silver_sales.sql",
        "../pipelines/12_silver_returns.sql",
        "../pipelines/13_silver_quality.sql",
        "../pipelines/20_gold_baskets.sql",
        "../pipelines/21_gold_sales_stores.sql",
        "../pipelines/22_gold_products.sql",
        "../pipelines/23_gold_customers_returns_traffic.sql",
        "../pipelines/24_gold_quality.sql",
        }


def test_bundle_uses_advanced_serverless_triggered_pipeline() -> None:
    document = yaml.safe_load(
        (RESOURCES / "medallion_pipeline.yml").read_text(encoding="utf-8")
    )
    pipeline = document["resources"]["pipelines"]["retail_medallion_pipeline"]
    assert pipeline["serverless"] is True
    assert pipeline["continuous"] is False
    assert pipeline["edition"] == "ADVANCED"
