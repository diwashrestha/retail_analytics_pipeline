# Einkaufpark Silver Layer

## Purpose

The Silver layer converts structurally accepted Bronze records into trusted,
conformed facts and dimensions. It performs business-level validation that does
not belong in Bronze:

- exact retry deduplication;
- transaction, basket, and record-hash conflict detection;
- conformed dimension creation;
- effective-date SCD2 price joins;
- completed-sale and void separation;
- financial calculation validation;
- original-sale and cumulative return validation;
- review-table routing;
- complete row-count reconciliation.

The Silver layer does **not** create dashboard KPIs. Revenue trends, product
performance, customer LTV, basket analysis, and store ranking belong in Gold.

## Required Bronze compatibility correction

The generator deliberately emits customers with missing age and uses the raw
value `Divers` for some customers. Both are valid source states:

- missing age means demographic information is unavailable;
- `Divers` is normalized to `D` in Silver.

The previously supplied Bronze code treated missing age and `Divers` as hard
errors. The package therefore includes a corrected replacement:

```text
pipelines/00_bronze.sql
```

Replace the previous Bronze SQL file before running Bronze again. The corrected
Bronze layer:

- accepts empty age;
- records `INFO:AGE_MISSING`;
- accepts `M`, `F`, `D`, `U`, and `Divers`;
- leaves normalization to Silver.

A full Bronze refresh is recommended after applying the patch.

## Pipeline inputs

The Silver source files read these Unity Catalog tables from the configured Bronze
catalog and schema:

```text
fact_transactions
fact_returns
dim_stores
dim_customers
dim_products
dim_products_scd2
```

The source table identifiers are parameterized using:

```text
bronze_catalog
bronze_schema
```

## Published dimensions

### `dim_store`

**Grain:** one row per trusted `store_id`.

Conflicting store profiles are excluded and preserved in
`dim_store_review`. Exact duplicate snapshot rows are collapsed.

### `dim_customer`

**Grain:** one row per trusted `customer_id`.

Rules:

- missing age becomes `age = NULL`, `age_group = Unknown`;
- age outside 18–100 becomes analytically unknown and is recorded in review;
- `Divers` is normalized to `D`;
- loyalty-card uniqueness is exposed as a quality attribute;
- conflicting source profiles are excluded.

Customer demographic issues remain visible in `dim_customer_review` without
unnecessarily discarding usable customer identities.

### `dim_product`

**Grain:** one row per trusted `product_id`.

The source brand value `bulk` is normalized to `EKP-Classic` to match generated
transaction events. Conflicting current product profiles are preserved in
`dim_product_review`.

### `dim_product_scd2`

**Grain:** one row per trusted product price interval.

The source intervals are inclusive:

```text
effective_from <= order_date <= effective_to
```

The layer:

- collapses exact duplicate intervals;
- detects overlapping intervals;
- detects gaps;
- compares SCD2 attributes with the current product master;
- excludes overlapping intervals from the trusted dimension;
- retains gap information so affected sales are routed to review.

Issues appear in `dim_product_scd2_review`.

### `dim_terminal`

**Grain:** one row per consistently observed `terminal_id`.

The dimension is derived from Bronze transaction metadata. A terminal is
trusted only if it always maps to one store, one terminal type, and one
self-checkout flag. Conflicts appear in `dim_terminal_review`.

## Transaction routing

Accepted Bronze transaction rows are routed into mutually exclusive outcomes:

```text
Bronze fact_transactions
        |
        +-- transaction_hash_conflict_review
        |
        +-- duplicate_transactions
        |
        +-- fact_sales
        |
        +-- fact_sales_review
        |
        +-- fact_voids
        |
        +-- fact_voids_review
```

The reconciliation rule is:

```text
Bronze rows
= record-hash conflict rows
+ exact duplicate rows
+ trusted sales
+ sales review rows
+ trusted void rows
+ void review rows
```

This is exposed by `silver_transaction_reconciliation`.

## Exact retry deduplication

The generator creates exact POS retries by replaying an entire basket. Silver
does not trust `record_hash` alone.

For each source line it calculates a business-payload hash. Then:

- same `record_hash` + same payload = exact retry;
- same `record_hash` + different payload = record-hash conflict.

For exact retries, the earliest canonical row is kept and later copies are
written to `duplicate_transactions`.

Rows with conflicting payloads are written to
`transaction_hash_conflict_review` and never enter trusted facts.

## Transaction and basket conflicts

A transaction can have multiple product lines, but these values must remain
stable across the transaction:

- basket;
- store;
- date;
- customer or walk-in state;
- source system;
- order status;
- terminal;
- payment type.

Conflicts appear in `transaction_id_conflicts`.

A basket must also map consistently to one transaction/store/date/customer
context. Conflicts appear in `basket_id_conflicts`.

Unexpected repeated product lines inside a deduplicated basket appear in
`basket_product_conflicts`.

## Trusted sales

`fact_sales` contains completed transaction lines that pass all critical rules:

- no ID/hash/context conflict;
- no exact duplicate retry;
- trusted store, terminal, and product;
- trusted customer when a customer ID is supplied;
- exactly one effective SCD2 price;
- transaction unit price matches the effective SCD2 price;
- positive quantity and unit price;
- discount between 0% and 100%;
- non-negative revenue;
- revenue arithmetic matches the source value;
- no Sunday sale.

### Financial definitions

The generator uses customer-facing prices that include German VAT. Silver
publishes:

| Column | Meaning |
|---|---|
| `pre_discount_sales_eur` | Unit price × quantity, including VAT |
| `discount_amount_eur` | Discount amount, including VAT |
| `net_sales_eur` | Amount paid after discount, including VAT |
| `net_sales_ex_vat_eur` | Amount paid excluding VAT |
| `vat_amount_eur` | VAT component of `net_sales_eur` |
| `effective_list_price_eur` | SCD2 price effective on the order date |
| `unit_price_variance_eur` | Transaction unit price minus effective SCD2 price |

The row-level arithmetic should satisfy:

```text
pre_discount_sales_eur - discount_amount_eur = net_sales_eur
net_sales_ex_vat_eur + vat_amount_eur = net_sales_eur
```

`fact_sales_review` preserves excluded completed lines and explicit review
reasons.

## Voids

Voided events never enter `fact_sales`.

A trusted void line must have:

```text
quantity = 0
discount_pct = 0
net_revenue_eur = 0
```

Trusted void events appear in `fact_voids`. Invalid voids appear in
`fact_voids_review`.

## Return routing

Accepted Bronze returns are routed into:

```text
Bronze fact_returns
        |
        +-- return_id_conflict_review
        |
        +-- duplicate_returns
        |
        +-- fact_returns
        |
        +-- fact_returns_review
```

The reconciliation rule is exposed by `silver_return_reconciliation`.

A trusted return must:

- have a unique and consistent `return_id`;
- link to a trusted Silver sale using basket and product;
- match the original transaction, store, and customer;
- occur after the sale and within the configured return window;
- have positive return quantity;
- match the original quantity, unit price, discount, and net unit price;
- have refund equal to net unit price × return quantity;
- remain within cumulative sold quantity;
- remain within cumulative amount paid.

The default return window is 30 days. The generator currently creates returns
within seven days.

## Review versus quarantine

Bronze quarantine means the source row could not be safely parsed or violated a
hard structural contract.

Silver review means the source row was structurally readable but failed a
business, relationship, deduplication, or financial rule.

Review rows are not deleted. They remain available for audit and debugging.

## Quality contract

`silver_quality_checks` publishes machine-readable checks with:

```text
check_name
severity
expected_value
actual_value
status
description
checked_at
```

A downstream Python or SQL task should fail the parent job when any row has:

```text
severity = CRITICAL
status   = FAILED
```

## Parameters

The Silver pipeline resource uses:

| Parameter | Default | Purpose |
|---|---:|---|
| `bronze_catalog` | bundle catalog | Bronze source catalog |
| `bronze_schema` | bundle Bronze schema | Bronze source schema |
| `price_tolerance_eur` | `0.02` | Unit-price comparison tolerance |
| `revenue_tolerance_eur` | `0.02` | Revenue/refund comparison tolerance |
| `max_return_window_days` | `30` | Maximum allowed days between sale and return |

## Recommended job order

```text
Bronze pipeline
      ↓
Silver pipeline
      ↓
Silver critical-check task
      ↓
Gold pipeline
```

Do not run Silver until the Bronze update has succeeded.

## Free Edition deployment model

Bronze and Silver are deployed as one triggered serverless pipeline. The
pipeline default target is the Bronze schema. Each Silver source file begins
with parameterized `USE CATALOG` and `USE SCHEMA` commands, so its unqualified
outputs are published to the Silver schema while retaining a single dependency
graph.

```sql
USE CATALOG IDENTIFIER(:silver_catalog);
USE SCHEMA IDENTIFIER(:silver_schema);
```

This avoids maintaining separate active Bronze and Silver pipelines in the Free
Edition workspace.

## Cumulative return ordering

Return validation runs in two phases:

1. Independent linkage and arithmetic checks.
2. Cumulative quantity and refund checks.

Only returns that pass the independent checks consume cumulative return
capacity. A malformed return therefore goes to `fact_returns_review` without
causing every later valid return for the same basket and product to fail.

## Deployment resources

```text
resources/unity_catalog.yml
    Creates raw, Bronze, and Silver schemas plus the managed raw Volume.

resources/medallion_pipeline.yml
    Deploys one serverless Advanced Lakeflow pipeline containing all Bronze and
    Silver SQL source files.

resources/retail_job.yml
    Creates the Lakeflow Job used to trigger the pipeline update.
```

## First-run acceptance criteria

The first 100,000-row Databricks run is accepted only when:

- every critical row in `silver_quality_checks` is `PASSED`;
- transaction and return routing differences are zero;
- trusted dimensions have unique business keys;
- trusted sales have unique `sales_line_sk` values;
- trusted returns have unique `return_id` values;
- trusted SCD2 intervals do not overlap;
- no trusted return exceeds sold quantity or amount paid;
- review tables contain explicit reasons rather than silent row loss.
