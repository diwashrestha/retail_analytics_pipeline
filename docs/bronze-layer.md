# Einkaufpark Bronze Layer

## Purpose

The Bronze layer preserves source fidelity while making the data safe and observable for downstream processing.

It deliberately does **not**:

- remove exact POS retries;
- resolve conflicting transaction IDs;
- join sales to SCD2 prices;
- validate returns against original purchases;
- calculate business KPIs.

Those rules belong in Silver or Gold.

## Published datasets

| Dataset | Purpose |
|---|---|
| `fact_transactions_raw` | Append-only source strings plus Auto Loader rescued data and file metadata |
| `fact_transactions` | Typed rows accepted for Silver; warnings and exact duplicates remain |
| `fact_transactions_quarantine` | Hard structural, parsing, enum, and source-ERR failures |
| `fact_returns_raw` | Source-fidelity return events |
| `fact_returns` | Typed return events accepted for Silver validation |
| `fact_returns_quarantine` | Structurally invalid return events |
| `dim_stores_raw`, `dim_stores` | Raw and typed store master snapshot |
| `dim_customers_raw`, `dim_customers` | Raw and typed customer snapshot |
| `dim_products_raw`, `dim_products` | Raw and typed product catalogue |
| `dim_products_scd2_raw`, `dim_products_scd2` | Raw and typed SCD2 price history |
| `bronze_quality_summary` | Accepted/quarantine counts and rates |

Private `*_parsed` datasets are internal to the pipeline and are not published to Unity Catalog.

## Required raw layout

```text
/Volumes/<catalog>/<raw_schema>/<raw_volume>/
├── transactions/
│   └── batch_*.csv
├── returns/
│   └── fact_returns*.csv
└── dimensions/
    ├── dim_stores.csv
    ├── dim_customers.csv
    ├── dim_products.csv
    └── dim_products_scd2.csv
```

Use append-only transaction and return file names. Do not overwrite an already ingested batch file. For a fully clean demo rerun, reset the pipeline state and raw Volume first.

## Hard error versus warning

Hard errors enter quarantine. Examples:

- malformed or missing required identifiers;
- invalid date/time/boolean/decimal parsing;
- unknown enum values;
- unexpected CSV columns captured by `_rescued_data`;
- source rows explicitly marked with an `ERR:` data-quality flag.

Warnings remain accepted. Examples:

- negative or zero business values intentionally injected for testing;
- revenue calculation mismatch;
- late arrival;
- inconsistent coupon/member/terminal metadata;
- exact duplicate information flags.

This preserves potentially useful records while preventing structurally unreadable rows from reaching Silver.

## Expected reconciliation

For every source dataset:

```text
raw_rows = accepted_rows + quarantined_rows
```

The `bronze_quality_summary` materialized view exposes this operational information. A separate validation task should fail the workflow when reconciliation is broken or quarantine rates exceed an agreed threshold.
