from __future__ import annotations

import argparse
import re
import sys
from datetime import date, timedelta
from typing import Any

from pyspark.sql import Row, SparkSession


IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate the Einkaufpark Silver and Gold pipeline outputs."
    )

    parser.add_argument("--catalog", required=True)
    parser.add_argument("--silver-schema", required=True)
    parser.add_argument("--gold-schema", required=True)
    parser.add_argument("--expected-end-date", required=True)

    return parser.parse_args()


def validate_identifier(value: str, parameter_name: str) -> str:
    if not IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(
            f"Invalid {parameter_name}: {value!r}. "
            "Only letters, digits, and underscores are allowed."
        )

    return value


def table_name(catalog: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"


def read_single_row(spark: SparkSession, table: str) -> Row:
    rows = spark.table(table).limit(2).collect()

    if len(rows) != 1:
        raise RuntimeError(
            f"Expected exactly one row in {table}, found {len(rows)}."
        )

    return rows[0]


def previous_trading_date(value: date) -> date:
    result = value

    # The generator does not generate Sunday sales.
    while result.weekday() == 6:
        result -= timedelta(days=1)

    return result


def add_check(
    results: list[tuple[str, bool, str]],
    name: str,
    passed: bool,
    details: str,
) -> None:
    results.append((name, passed, details))


def print_quality_failures(
    spark: SparkSession,
    quality_table: str,
) -> None:
    failures = (
        spark.table(quality_table)
        .where("status = 'FAILED'")
        .select(
            "check_name",
            "severity",
            "expected_value",
            "actual_value",
            "description",
        )
        .orderBy("severity", "check_name")
        .collect()
    )

    if not failures:
        print(f"\nNo failed checks in {quality_table}")
        return

    print(f"\nFailed checks in {quality_table}:")

    for row in failures:
        print(
            f"  [{row['severity']}] {row['check_name']}: "
            f"expected={row['expected_value']}, "
            f"actual={row['actual_value']}"
        )
        print(f"      {row['description']}")


def main() -> int:
    args = parse_args()

    catalog = validate_identifier(args.catalog, "catalog")
    silver_schema = validate_identifier(
        args.silver_schema,
        "silver schema",
    )
    gold_schema = validate_identifier(
        args.gold_schema,
        "gold schema",
    )

    expected_end_date = date.fromisoformat(args.expected_end_date)
    expected_latest_order_date = previous_trading_date(expected_end_date)

    spark = SparkSession.getActiveSession()

    if spark is None:
        spark = SparkSession.builder.getOrCreate()

    silver_gate_table = table_name(
        catalog,
        silver_schema,
        "silver_quality_gate",
    )
    silver_checks_table = table_name(
        catalog,
        silver_schema,
        "silver_quality_checks",
    )
    silver_transaction_reconciliation_table = table_name(
        catalog,
        silver_schema,
        "silver_transaction_reconciliation",
    )
    silver_return_reconciliation_table = table_name(
        catalog,
        silver_schema,
        "silver_return_reconciliation",
    )
    fact_sales_table = table_name(
        catalog,
        silver_schema,
        "fact_sales",
    )

    gold_gate_table = table_name(
        catalog,
        gold_schema,
        "gold_quality_gate",
    )
    gold_checks_table = table_name(
        catalog,
        gold_schema,
        "gold_quality_checks",
    )

    results: list[tuple[str, bool, str]] = []

    # Silver quality gate.
    silver_gate = read_single_row(spark, silver_gate_table)

    silver_failed_critical = int(
        silver_gate["failed_critical_checks"]
    )
    silver_failed_warnings = int(
        silver_gate["failed_warning_checks"]
    )

    add_check(
        results,
        "Silver critical quality gate",
        silver_failed_critical == 0,
        (
            f"failed_critical_checks={silver_failed_critical}, "
            f"failed_warning_checks={silver_failed_warnings}"
        ),
    )

    # Gold quality gate.
    gold_gate = read_single_row(spark, gold_gate_table)

    gold_failed_critical = int(
        gold_gate["failed_critical_checks"]
    )
    gold_failed_warnings = int(
        gold_gate["failed_warning_checks"]
    )

    add_check(
        results,
        "Gold critical quality gate",
        gold_failed_critical == 0,
        (
            f"failed_critical_checks={gold_failed_critical}, "
            f"failed_warning_checks={gold_failed_warnings}"
        ),
    )

    # Silver transaction reconciliation.
    transaction_reconciliation = read_single_row(
        spark,
        silver_transaction_reconciliation_table,
    )

    transaction_difference = int(
        transaction_reconciliation["reconciliation_difference"]
    )

    add_check(
        results,
        "Silver transaction reconciliation",
        transaction_difference == 0,
        f"reconciliation_difference={transaction_difference}",
    )

    # Silver return reconciliation.
    return_reconciliation = read_single_row(
        spark,
        silver_return_reconciliation_table,
    )

    return_difference = int(
        return_reconciliation["reconciliation_difference"]
    )

    add_check(
        results,
        "Silver return reconciliation",
        return_difference == 0,
        f"reconciliation_difference={return_difference}",
    )

    # Basic trusted-sales checks.
    sales_metrics = spark.sql(
        f"""
        SELECT
          count(*) AS sales_rows,
          count(DISTINCT basket_id) AS basket_count,
          round(coalesce(sum(net_sales_eur), 0), 2) AS net_sales_eur,
          min(order_date) AS earliest_order_date,
          max(order_date) AS latest_order_date
        FROM {fact_sales_table}
        """
    ).first()

    if sales_metrics is None:
        raise RuntimeError(
            f"Unable to query trusted sales table {fact_sales_table}."
        )

    sales_rows = int(sales_metrics["sales_rows"])
    basket_count = int(sales_metrics["basket_count"])
    net_sales_eur = float(sales_metrics["net_sales_eur"])
    latest_order_date: Any = sales_metrics["latest_order_date"]

    add_check(
        results,
        "Trusted sales are populated",
        sales_rows > 0,
        (
            f"sales_rows={sales_rows}, "
            f"basket_count={basket_count}, "
            f"net_sales_eur={net_sales_eur:.2f}"
        ),
    )

    latest_date_passed = (
        latest_order_date is not None
        and latest_order_date >= expected_latest_order_date
    )

    add_check(
        results,
        "Expected business date loaded",
        latest_date_passed,
        (
            f"latest_order_date={latest_order_date}, "
            f"expected_at_least={expected_latest_order_date}"
        ),
    )

    print("\nEinkaufpark medallion validation")
    print("=" * 72)

    failed_checks: list[str] = []

    for name, passed, details in results:
        status = "PASS" if passed else "FAIL"
        print(f"[{status}] {name}")
        print(f"       {details}")

        if not passed:
            failed_checks.append(name)

    print_quality_failures(spark, silver_checks_table)
    print_quality_failures(spark, gold_checks_table)

    print("\n" + "=" * 72)

    if failed_checks:
        print(
            "Validation failed: "
            + ", ".join(failed_checks)
        )
        return 1

    print("All critical medallion validation checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())