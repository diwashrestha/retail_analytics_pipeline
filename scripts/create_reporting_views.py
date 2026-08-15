from __future__ import annotations

import argparse
import re
from pathlib import Path

from pyspark.sql import SparkSession


IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create Einkaufpark Power BI reporting views over trusted "
            "Silver and Gold datasets."
        )
    )

    parser.add_argument("--catalog", required=True)
    parser.add_argument("--silver-schema", required=True)
    parser.add_argument("--gold-schema", required=True)
    parser.add_argument("--reporting-schema", required=True)

    parser.add_argument(
        "--sql-file",
        default="sql/reporting_views.sql",
        help="Path to reporting view DDL file.",
    )

    return parser.parse_args()


# ---------------------------------------------------------------------------
# Validation / identifiers
# ---------------------------------------------------------------------------

def validate_identifier(value: str, argument_name: str) -> str:
    if not IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(
            f"{argument_name} contains an invalid SQL identifier: {value!r}"
        )

    return value


def quote_identifier(value: str) -> str:
    """
    Identifiers have already been validated, so backtick quoting is sufficient.
    """
    return f"`{value}`"


# ---------------------------------------------------------------------------
# SQL file discovery
# ---------------------------------------------------------------------------

def resolve_sql_file(raw_path: str) -> Path:
    requested = Path(raw_path)

    if requested.is_absolute():
        if requested.is_file():
            return requested

        raise FileNotFoundError(
            f"Reporting SQL file does not exist: {requested}"
        )

    cwd = Path.cwd()

    candidates = [
        cwd / requested,
        cwd.parent / requested,
    ]

    # Useful when the task happens to start inside scripts/
    if cwd.name == "scripts":
        candidates.append(cwd.parent / requested)

    for candidate in candidates:
        if candidate.is_file():
            return candidate.resolve()

    checked = "\n  - ".join(str(path) for path in candidates)

    raise FileNotFoundError(
        "Could not locate reporting SQL file.\n"
        f"Checked:\n  - {checked}"
    )


# ---------------------------------------------------------------------------
# Lightweight SQL splitter
# ---------------------------------------------------------------------------

def split_sql_statements(sql_text: str) -> list[str]:
    """
    Split SQL on semicolons while respecting quoted strings,
    backtick identifiers, and SQL comments.

    This avoids requiring another Python package just to execute the
    reporting DDL.
    """

    statements: list[str] = []
    buffer: list[str] = []

    in_single_quote = False
    in_double_quote = False
    in_backtick = False
    in_line_comment = False
    in_block_comment = False

    i = 0

    while i < len(sql_text):
        char = sql_text[i]
        next_char = sql_text[i + 1] if i + 1 < len(sql_text) else ""

        # ---------------------------------------------------------------
        # Existing line comment
        # ---------------------------------------------------------------
        if in_line_comment:
            buffer.append(char)

            if char == "\n":
                in_line_comment = False

            i += 1
            continue

        # ---------------------------------------------------------------
        # Existing block comment
        # ---------------------------------------------------------------
        if in_block_comment:
            buffer.append(char)

            if char == "*" and next_char == "/":
                buffer.append(next_char)
                in_block_comment = False
                i += 2
                continue

            i += 1
            continue

        # ---------------------------------------------------------------
        # Comment starts
        # ---------------------------------------------------------------
        if (
            not in_single_quote
            and not in_double_quote
            and not in_backtick
        ):
            if char == "-" and next_char == "-":
                buffer.extend([char, next_char])
                in_line_comment = True
                i += 2
                continue

            if char == "/" and next_char == "*":
                buffer.extend([char, next_char])
                in_block_comment = True
                i += 2
                continue

        # ---------------------------------------------------------------
        # Quote handling
        # ---------------------------------------------------------------
        if char == "'" and not in_double_quote and not in_backtick:
            # SQL escaped single quote: ''
            if in_single_quote and next_char == "'":
                buffer.extend([char, next_char])
                i += 2
                continue

            in_single_quote = not in_single_quote
            buffer.append(char)
            i += 1
            continue

        if char == '"' and not in_single_quote and not in_backtick:
            in_double_quote = not in_double_quote
            buffer.append(char)
            i += 1
            continue

        if char == "`" and not in_single_quote and not in_double_quote:
            in_backtick = not in_backtick
            buffer.append(char)
            i += 1
            continue

        # ---------------------------------------------------------------
        # Statement delimiter
        # ---------------------------------------------------------------
        if (
            char == ";"
            and not in_single_quote
            and not in_double_quote
            and not in_backtick
        ):
            statement = "".join(buffer).strip()

            if statement:
                statements.append(statement)

            buffer = []
            i += 1
            continue

        buffer.append(char)
        i += 1

    trailing = "".join(buffer).strip()

    if trailing:
        statements.append(trailing)

    return statements


# ---------------------------------------------------------------------------
# Template rendering
# ---------------------------------------------------------------------------

def render_sql(
    sql_text: str,
    catalog: str,
    silver_schema: str,
    gold_schema: str,
    reporting_schema: str,
) -> str:

    replacements = {
        "{{catalog}}": quote_identifier(catalog),
        "{{silver_schema}}": quote_identifier(silver_schema),
        "{{gold_schema}}": quote_identifier(gold_schema),
        "{{reporting_schema}}": quote_identifier(reporting_schema),
    }

    rendered = sql_text

    for token, value in replacements.items():
        rendered = rendered.replace(token, value)

    unresolved = re.findall(r"\{\{[^{}]+\}\}", rendered)

    if unresolved:
        raise RuntimeError(
            "Unresolved reporting SQL placeholders: "
            + ", ".join(sorted(set(unresolved)))
        )

    return rendered


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()

    catalog = validate_identifier(args.catalog, "--catalog")
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

    sql_file = resolve_sql_file(args.sql_file)

    print(
        "\nEinkaufpark reporting view deployment",
        flush=True,
    )
    print("=" * 72, flush=True)
    print(f"catalog          : {catalog}", flush=True)
    print(f"silver schema    : {silver_schema}", flush=True)
    print(f"gold schema      : {gold_schema}", flush=True)
    print(f"reporting schema : {reporting_schema}", flush=True)
    print(f"SQL file         : {sql_file}", flush=True)
    print("=" * 72, flush=True)

    spark = SparkSession.builder.getOrCreate()

    # Fail early if the bundle-managed reporting schema is missing.
    schemas = {
        row["databaseName"]
        for row in spark.sql(
            f"SHOW SCHEMAS IN {quote_identifier(catalog)}"
        ).collect()
    }

    if reporting_schema not in schemas:
        raise RuntimeError(
            f"Reporting schema does not exist: "
            f"{catalog}.{reporting_schema}. "
            "It should be created by the Databricks bundle."
        )

    source_text = sql_file.read_text(encoding="utf-8")

    rendered_sql = render_sql(
        source_text,
        catalog=catalog,
        silver_schema=silver_schema,
        gold_schema=gold_schema,
        reporting_schema=reporting_schema,
    )

    statements = split_sql_statements(rendered_sql)

    if not statements:
        raise RuntimeError(
            f"No SQL statements found in {sql_file}"
        )

    print(
        f"\nExecuting {len(statements)} reporting SQL statements...",
        flush=True,
    )

    for index, statement in enumerate(statements, start=1):
        # Give useful task logs without printing the entire SQL.
        preview = " ".join(statement.split())

        if len(preview) > 120:
            preview = preview[:117] + "..."

        print(
            f"[{index:02d}/{len(statements):02d}] {preview}",
            flush=True,
        )

        try:
            spark.sql(statement)
        except Exception as exc:
            raise RuntimeError(
                f"Reporting SQL statement {index} failed.\n"
                f"Statement preview:\n{preview}"
            ) from exc

    print(
        "\nReporting views created successfully.",
        flush=True,
    )


if __name__ == "__main__":
    main()