from __future__ import annotations

import argparse
import re
from decimal import Decimal
from typing import Any

from pyspark.sql import SparkSession

IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


REQUIRED_VIEWS = (
    "v_fact_sales",
    "v_fact_returns",
    "v_dim_date",
    "v_dim_store",
    "v_dim_product",
    "v_dim_customer",
    "v_executive_kpis",
    "v_daily_sales",
    "v_store_performance",
    "v_product_performance",
    "v_customer_ltv",
    "v_returns_analysis",
    "v_hourly_traffic",
    "v_data_quality_summary",
)


BUSINESS_VIEWS = tuple(
    view for view in REQUIRED_VIEWS if view != "v_data_quality_summary"
)


FORBIDDEN_TECHNICAL_COLUMNS = {
    "batch_id",
    "record_hash",
    "transaction_payload_hash",
    "ingestion_date",
    "source_data_quality_flag",
    "silver_warning_codes",
    "gold_refreshed_at",
    "_source_file_path",
    "_source_file_name",
    "_source_file_modified_at",
    "_bronze_ingested_at",
    "_bronze_processed_at",
}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=("Validate the Einkaufpark Power BI reporting contract.")
    )

    parser.add_argument("--catalog", required=True)
    parser.add_argument("--silver-schema", required=True)
    parser.add_argument("--gold-schema", required=True)
    parser.add_argument("--reporting-schema", required=True)

    parser.add_argument(
        "--revenue-tolerance-eur",
        type=Decimal,
        default=Decimal("0.02"),
    )

    return parser.parse_args()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def validate_identifier(value: str, parameter_name: str) -> str:
    if not IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(
            f"{parameter_name} contains an invalid SQL identifier: {value!r}"
        )

    return value


def quote_identifier(value: str) -> str:
    return f"`{value}`"


def table_name(
    catalog: str,
    schema: str,
    table: str,
) -> str:
    return (
        f"{quote_identifier(catalog)}."
        f"{quote_identifier(schema)}."
        f"{quote_identifier(table)}"
    )


def scalar(
    spark: SparkSession,
    sql_text: str,
) -> Any:
    row = spark.sql(sql_text).first()

    if row is None:
        raise RuntimeError("Expected query to return exactly one scalar row.")

    return row[0]


def table_count(
    spark: SparkSession,
    table: str,
) -> int:
    return int(
        scalar(
            spark,
            f"SELECT COUNT(*) FROM {table}",
        )
    )


def decimal_value(value: Any) -> Decimal:
    if value is None:
        return Decimal(0)

    return Decimal(str(value))


def add_check(
    results: list[tuple[str, bool, str]],
    name: str,
    passed: bool,
    details: str,
) -> None:
    results.append(
        (
            name,
            bool(passed),
            details,
        )
    )


def check_count_match(
    spark: SparkSession,
    results: list[tuple[str, bool, str]],
    name: str,
    reporting_table: str,
    source_table: str,
) -> None:
    reporting_rows = table_count(
        spark,
        reporting_table,
    )

    source_rows = table_count(
        spark,
        source_table,
    )

    add_check(
        results,
        name,
        reporting_rows == source_rows,
        (f"reporting_rows={reporting_rows}, source_rows={source_rows}"),
    )


def check_unique_key(
    spark: SparkSession,
    results: list[tuple[str, bool, str]],
    name: str,
    table: str,
    key: str,
) -> None:
    row = spark.sql(
        f"""
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT {quote_identifier(key)})
                AS distinct_key_count,
            COUNT_IF({quote_identifier(key)} IS NULL)
                AS null_key_count
        FROM {table}
        """
    ).first()

    if row is None:
        raise RuntimeError(f"Could not validate unique key for {table}")

    row_count = int(row["row_count"])
    distinct_count = int(row["distinct_key_count"])
    null_count = int(row["null_key_count"])

    passed = row_count == distinct_count and null_count == 0

    add_check(
        results,
        name,
        passed,
        (f"rows={row_count}, distinct_keys={distinct_count}, null_keys={null_count}"),
    )


# ---------------------------------------------------------------------------
# Main validation
# ---------------------------------------------------------------------------


def main() -> None:
    args = parse_args()

    catalog = validate_identifier(
        args.catalog,
        "--catalog",
    )

    silver_schema = validate_identifier(
        args.silver_schema,
        "--silver-schema",
    )

    gold_schema = validate_identifier(
        args.gold_schema,
        "--gold-schema",
    )

    reporting_schema = validate_identifier(
        args.reporting_schema,
        "--reporting-schema",
    )

    tolerance = args.revenue_tolerance_eur

    spark = SparkSession.builder.getOrCreate()

    results: list[tuple[str, bool, str]] = []

    # -----------------------------------------------------------------------
    # Table references
    # -----------------------------------------------------------------------

    reporting = {
        view: table_name(
            catalog,
            reporting_schema,
            view,
        )
        for view in REQUIRED_VIEWS
    }

    silver_fact_sales = table_name(
        catalog,
        silver_schema,
        "fact_sales",
    )

    silver_fact_returns = table_name(
        catalog,
        silver_schema,
        "fact_returns",
    )

    silver_dim_store = table_name(
        catalog,
        silver_schema,
        "dim_store",
    )

    silver_dim_product = table_name(
        catalog,
        silver_schema,
        "dim_product",
    )

    silver_dim_customer = table_name(
        catalog,
        silver_schema,
        "dim_customer",
    )

    silver_quality_checks = table_name(
        catalog,
        silver_schema,
        "silver_quality_checks",
    )

    gold_daily_sales = table_name(
        catalog,
        gold_schema,
        "daily_sales",
    )

    gold_store_performance = table_name(
        catalog,
        gold_schema,
        "store_performance",
    )

    gold_product_performance = table_name(
        catalog,
        gold_schema,
        "product_performance",
    )

    gold_customer_ltv = table_name(
        catalog,
        gold_schema,
        "customer_ltv",
    )

    gold_return_analysis = table_name(
        catalog,
        gold_schema,
        "return_analysis",
    )

    gold_hourly_traffic = table_name(
        catalog,
        gold_schema,
        "hourly_traffic",
    )

    gold_quality_checks = table_name(
        catalog,
        gold_schema,
        "gold_quality_checks",
    )

    print(
        "\nEinkaufpark reporting validation",
        flush=True,
    )
    print("=" * 72, flush=True)

    # -----------------------------------------------------------------------
    # 1. Required views exist
    # -----------------------------------------------------------------------

    view_rows = spark.sql(
        f"""
        SHOW VIEWS IN
        {quote_identifier(catalog)}.
        {quote_identifier(reporting_schema)}
        """
    ).collect()

    existing_views: set[str] = set()

    for row in view_rows:
        values = row.asDict()

        name = (
            values.get("viewName")
            or values.get("view_name")
            or values.get("tableName")
            or values.get("table_name")
        )

        if name:
            existing_views.add(str(name))

    missing_views = sorted(set(REQUIRED_VIEWS) - existing_views)

    add_check(
        results,
        "All required reporting views exist",
        not missing_views,
        (
            "all 14 required views exist"
            if not missing_views
            else "missing=" + ", ".join(missing_views)
        ),
    )

    # If views are missing, later checks would produce noisy Spark errors.
    if missing_views:
        print_results_and_fail(results)

    # -----------------------------------------------------------------------
    # 2. Silver fact/dimension row reconciliation
    # -----------------------------------------------------------------------

    count_mappings = (
        (
            "Fact sales row reconciliation",
            reporting["v_fact_sales"],
            silver_fact_sales,
        ),
        (
            "Fact returns row reconciliation",
            reporting["v_fact_returns"],
            silver_fact_returns,
        ),
        (
            "Store dimension row reconciliation",
            reporting["v_dim_store"],
            silver_dim_store,
        ),
        (
            "Product dimension row reconciliation",
            reporting["v_dim_product"],
            silver_dim_product,
        ),
        (
            "Customer dimension row reconciliation",
            reporting["v_dim_customer"],
            silver_dim_customer,
        ),
        (
            "Daily sales row reconciliation",
            reporting["v_daily_sales"],
            gold_daily_sales,
        ),
        (
            "Store performance row reconciliation",
            reporting["v_store_performance"],
            gold_store_performance,
        ),
        (
            "Product performance row reconciliation",
            reporting["v_product_performance"],
            gold_product_performance,
        ),
        (
            "Customer LTV row reconciliation",
            reporting["v_customer_ltv"],
            gold_customer_ltv,
        ),
        (
            "Returns analysis row reconciliation",
            reporting["v_returns_analysis"],
            gold_return_analysis,
        ),
        (
            "Hourly traffic row reconciliation",
            reporting["v_hourly_traffic"],
            gold_hourly_traffic,
        ),
    )

    for (
        check_name,
        reporting_table,
        source_table,
    ) in count_mappings:
        check_count_match(
            spark,
            results,
            check_name,
            reporting_table,
            source_table,
        )

    # -----------------------------------------------------------------------
    # 3. Stable grains / unique keys
    # -----------------------------------------------------------------------

    unique_checks = (
        (
            "Reporting sales line grain",
            reporting["v_fact_sales"],
            "sales_line_key",
        ),
        (
            "Reporting return grain",
            reporting["v_fact_returns"],
            "return_key",
        ),
        (
            "Reporting store grain",
            reporting["v_dim_store"],
            "store_key",
        ),
        (
            "Reporting product grain",
            reporting["v_dim_product"],
            "product_key",
        ),
        (
            "Reporting customer grain",
            reporting["v_dim_customer"],
            "customer_key",
        ),
        (
            "Reporting date grain",
            reporting["v_dim_date"],
            "date_key",
        ),
    )

    for name, table, key in unique_checks:
        check_unique_key(
            spark,
            results,
            name,
            table,
            key,
        )

    # -----------------------------------------------------------------------
    # 4. Date dimension completeness
    # -----------------------------------------------------------------------

    source_date_row = spark.sql(
        f"""
        WITH source_bounds AS (

            SELECT
                MIN(order_date) AS min_date,
                MAX(order_date) AS max_date
            FROM {silver_fact_sales}

            UNION ALL

            SELECT
                MIN(return_date) AS min_date,
                MAX(return_date) AS max_date
            FROM {silver_fact_returns}
        )

        SELECT
            MIN(min_date) AS min_date,
            MAX(max_date) AS max_date,
            DATEDIFF(
                MAX(max_date),
                MIN(min_date)
            ) + 1 AS expected_days
        FROM source_bounds
        """
    ).first()

    reporting_date_row = spark.sql(
        f"""
        SELECT
            MIN(calendar_date) AS min_date,
            MAX(calendar_date) AS max_date,
            COUNT(*) AS actual_days
        FROM {reporting["v_dim_date"]}
        """
    ).first()

    if source_date_row is None or reporting_date_row is None:
        raise RuntimeError("Unable to validate reporting date dimension.")

    expected_min = source_date_row["min_date"]
    expected_max = source_date_row["max_date"]
    expected_days = int(source_date_row["expected_days"] or 0)

    actual_min = reporting_date_row["min_date"]
    actual_max = reporting_date_row["max_date"]
    actual_days = int(reporting_date_row["actual_days"] or 0)

    add_check(
        results,
        "Date dimension complete",
        (
            actual_min == expected_min
            and actual_max == expected_max
            and actual_days == expected_days
        ),
        (
            f"expected={expected_min}→{expected_max} "
            f"({expected_days} days), "
            f"actual={actual_min}→{actual_max} "
            f"({actual_days} days)"
        ),
    )
    # -----------------------------------------------------------------------
    # 5. Executive KPI row / revenue reconciliation
    # -----------------------------------------------------------------------

    executive_rows = table_count(
        spark,
        reporting["v_executive_kpis"],
    )

    add_check(
        results,
        "Executive KPI has one row",
        executive_rows == 1,
        f"rows={executive_rows}",
    )

    silver_revenue = decimal_value(
        scalar(
            spark,
            f"""
            SELECT
                ROUND(
                    COALESCE(SUM(net_sales_eur), 0),
                    2
                )
            FROM {silver_fact_sales}
            """,
        )
    )

    executive_revenue = decimal_value(
        scalar(
            spark,
            f"""
            SELECT net_sales_eur
            FROM {reporting["v_executive_kpis"]}
            """,
        )
    )

    revenue_difference = abs(silver_revenue - executive_revenue)

    add_check(
        results,
        "Executive revenue reconciliation",
        revenue_difference <= tolerance,
        (
            f"silver={silver_revenue}, "
            f"reporting={executive_revenue}, "
            f"difference={revenue_difference}, "
            f"tolerance={tolerance}"
        ),
    )

    # -----------------------------------------------------------------------
    # 6. Daily sales revenue reconciliation
    # -----------------------------------------------------------------------

    reporting_daily_revenue = decimal_value(
        scalar(
            spark,
            f"""
            SELECT
                ROUND(
                    COALESCE(SUM(net_sales_eur), 0),
                    2
                )
            FROM {reporting["v_daily_sales"]}
            """,
        )
    )

    daily_difference = abs(silver_revenue - reporting_daily_revenue)

    add_check(
        results,
        "Reporting daily sales revenue reconciliation",
        daily_difference <= tolerance,
        (
            f"silver={silver_revenue}, "
            f"reporting={reporting_daily_revenue}, "
            f"difference={daily_difference}, "
            f"tolerance={tolerance}"
        ),
    )

    # -----------------------------------------------------------------------
    # 7. Quality summary reconciliation
    # -----------------------------------------------------------------------

    expected_quality_rows = table_count(
        spark,
        silver_quality_checks,
    ) + table_count(
        spark,
        gold_quality_checks,
    )

    actual_quality_rows = table_count(
        spark,
        reporting["v_data_quality_summary"],
    )

    add_check(
        results,
        "Quality summary row reconciliation",
        actual_quality_rows == expected_quality_rows,
        (f"expected={expected_quality_rows}, actual={actual_quality_rows}"),
    )

    failed_critical_checks = int(
        scalar(
            spark,
            f"""
            SELECT COUNT(*)
            FROM {reporting["v_data_quality_summary"]}
            WHERE severity = 'CRITICAL'
              AND status = 'FAILED'
            """,
        )
    )

    add_check(
        results,
        "No failed critical reporting quality checks",
        failed_critical_checks == 0,
        f"failed_critical_checks={failed_critical_checks}",
    )

    # -----------------------------------------------------------------------
    # 8. Reporting contract hygiene
    # -----------------------------------------------------------------------

    for view in BUSINESS_VIEWS:
        table = reporting[view]

        schema = spark.table(table).schema

        column_names = {field.name for field in schema.fields}

        technical_columns = sorted(column_names & FORBIDDEN_TECHNICAL_COLUMNS)

        technical_columns.extend(
            sorted(
                name
                for name in column_names
                if name.startswith("_source_") or name.startswith("_bronze_")
            )
        )

        technical_columns = sorted(set(technical_columns))

        add_check(
            results,
            f"{view}: no ingestion metadata",
            not technical_columns,
            (
                "none"
                if not technical_columns
                else "found=" + ", ".join(technical_columns)
            ),
        )

        # ---------------------------------------------------------------
        # Currency columns should be DECIMAL
        # ---------------------------------------------------------------

        currency_type_errors: list[str] = []

        for field in schema.fields:
            if field.name.endswith("_eur"):
                data_type = field.dataType.simpleString()

                if not data_type.startswith("decimal("):
                    currency_type_errors.append(f"{field.name}:{data_type}")

        add_check(
            results,
            f"{view}: currency columns use DECIMAL",
            not currency_type_errors,
            (
                "all *_eur columns are DECIMAL"
                if not currency_type_errors
                else "invalid=" + ", ".join(currency_type_errors)
            ),
        )

        # ---------------------------------------------------------------
        # Date columns should be DATE
        # ---------------------------------------------------------------

        date_type_errors: list[str] = []

        for field in schema.fields:
            if (
                field.name.endswith("_date")
                or field.name == "date_key"
                or field.name == "calendar_date"
            ):
                data_type = field.dataType.simpleString()

                if data_type != "date":
                    date_type_errors.append(f"{field.name}:{data_type}")

        add_check(
            results,
            f"{view}: date columns use DATE",
            not date_type_errors,
            (
                "all business dates use DATE"
                if not date_type_errors
                else "invalid=" + ", ".join(date_type_errors)
            ),
        )

        # ---------------------------------------------------------------
        # Boolean naming
        # ---------------------------------------------------------------

        boolean_name_errors: list[str] = []

        for field in schema.fields:
            if field.dataType.simpleString() == "boolean":
                if not (field.name.startswith("is_") or field.name.startswith("has_")):
                    boolean_name_errors.append(field.name)

        add_check(
            results,
            f"{view}: boolean columns clearly named",
            not boolean_name_errors,
            (
                "all booleans use is_/has_ prefixes"
                if not boolean_name_errors
                else "invalid=" + ", ".join(boolean_name_errors)
            ),
        )

    # -----------------------------------------------------------------------
    # Final output
    # -----------------------------------------------------------------------

    print_results_and_fail(results)


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


def print_results_and_fail(
    results: list[tuple[str, bool, str]],
) -> None:

    failed: list[tuple[str, str]] = []

    for name, passed, details in results:
        status = "PASS" if passed else "FAIL"

        print(
            f"[{status}] {name}",
            flush=True,
        )
        print(
            f"       {details}",
            flush=True,
        )

        if not passed:
            failed.append(
                (
                    name,
                    details,
                )
            )

    print("=" * 72, flush=True)

    if failed:
        details = "\n  - ".join(f"{name}: {message}" for name, message in failed)

        raise RuntimeError("Reporting validation failed:\n  - " + details)

    print(
        "All reporting-layer validation checks passed.",
        flush=True,
    )


if __name__ == "__main__":
    main()
