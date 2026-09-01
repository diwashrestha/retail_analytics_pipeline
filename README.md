# Einkaufpark Retail Analytics Platform

Einkaufpark is a data engineering project for a fictional multi-store retailer called **EinkaufPark**. A Python data generator creates transactions, customers, stores, products, price changes, promotions, and returns. Those files land in a Unity Catalog Volume and move through a Databricks Medallion Architecture.

The data follows a **Bronze → Silver → Gold** flow. Each layer has a different job: Bronze keeps and classifies incoming data, Silver builds trusted business entities, and Gold prepares reusable datasets for analysis.

The project deliberately works with imperfect retail data. Records can arrive late or appear more than once. Some customers are anonymous. Product prices change over time, and returns may arrive days after the original purchase. A record can also be technically readable while still breaking a business rule.

The pipeline is built to catch those cases before the data reaches reporting.

---


# Getting started

## Prerequisites

You will need:

- Git
- Python 3.11+
- Databricks CLI with Asset Bundle support
- access to a Databricks workspace
- Unity Catalog support in that workspace
- Power BI Desktop if you want to open the dashboard

The local development dependencies are small:

```text
pytest
pyyaml
```

They are listed in `requirements-dev.txt`.

---

## 1. Clone the repository

```bash
git clone https://github.com/diwashrestha/retail_analytics_pipeline.git
cd retail_analytics_pipeline
```

## 2. Create a Python environment

On Linux or macOS:

```bash
python -m venv .venv
source .venv/bin/activate
```

On Windows PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

Install the dependencies:

```bash
python -m pip install --upgrade pip
pip install -r requirements-dev.txt
```

## 3. Authenticate Databricks

The project does not use a `.env` file. Databricks authentication goes through the Databricks CLI.

Configure a profile for your workspace and check that it works:

```bash
databricks current-user me --profile <your-profile>
```

`scripts/deploy_dev.sh` uses `einkaufpark-free` as its default profile. To use another profile:

```bash
export DATABRICKS_PROFILE=<your-profile>
```

Do not commit credentials to the repository.

## 4. Run the local tests

```bash
python -m pytest tests/unit -q
```

The current unit suite covers bundle configuration, generator behaviour, Silver contracts, and Gold contracts.

There are additional test assets under `tests/`, including SQL tests and `test_pipeline_contracts.py`. The current GitHub Actions workflow runs `tests/unit`.

## 5. Validate the Databricks bundle

```bash
databricks bundle validate \
  --target dev \
  --profile <your-profile>
```

## 6. Deploy the development environment

```bash
databricks bundle deploy \
  --target dev \
  --profile <your-profile>
```

## 7. Run the complete platform

```bash
databricks bundle run \
  retail_medallion_job \
  --target dev \
  --profile <your-profile>
```

Without extra parameters, the job uses the defaults from `databricks.yml`.

A full run follows this sequence:

```text
generate the demo data
        ↓
refresh Bronze / Silver / Gold
        ↓
validate the medallion model
        ↓
create reporting views
        ↓
validate the reporting layer
```

# Controlling generated data

The job accepts these runtime parameters:

```text
mode
records
customers
seed
start_date
end_date
price_history_end_date
walkin_rate
late_rate
return_rate
duplicate_rate
```

For example:

```bash
databricks bundle run retail_medallion_job \
  --target dev \
  --params mode=demo,records=500000,customers=25000,start_date=2023-01-01,end_date=2026-03-31,seed=42
```

The behaviour parameters use decimal rates:

```text
walkin_rate=0.10
late_rate=0.05
return_rate=0.04
duplicate_rate=0.001
```

Those values correspond approximately to:

```text
10% walk-in behaviour
5% late-arrival behaviour
4% return behaviour
0.1% duplicate injection
```

They exist so the pipeline can be tested against imperfect input rather than only clean examples.

---

# Demo, incremental, and reset modes

The generator has three modes.

## `demo`

`demo` creates the initial deterministic baseline.

It refuses to overwrite an existing published landing dataset. Rebuilding the baseline must be an explicit action.

## `incremental`

`incremental` appends a new, non-overlapping business-date range to an existing baseline.

Example:

```bash
databricks bundle run retail_medallion_job \
  --target dev \
  --params mode=incremental,records=5000,start_date=2026-04-01,end_date=2026-04-03
```

The run reuses the existing dimensions and product price history.

Published manifests track the batches, reject overlapping date ranges, and make an exact rerun safe.

## `reset`

The Python generator can remove only the landing directories it owns.

For a local landing zone:

```bash
python data_generator/incremental.py \
  --mode reset \
  --output-dir data/raw
```

Use reset carefully in Databricks. The current `retail_medallion_job` is an end-to-end workflow, so a successful generator task continues into Bronze, Silver, Gold, and validation. It is not a reset-only job.

If a dedicated Databricks reset workflow is added later, it should stay separate from the normal processing job.

---

# Running the generator locally

You can run the generator without Databricks when working on generation logic or generator tests.

```bash
python data_generator/incremental.py \
  --mode demo \
  --records 100000 \
  --customers 10000 \
  --seed 42 \
  --start-date 2023-01-01 \
  --end-date 2026-03-31 \
  --price-history-end-date 2026-12-31 \
  --output-dir data/raw \
  --master-dir master
```

The full Bronze → Silver → Gold → Reporting workflow still requires Databricks.

--- 


## Architecture

![Einkaufpark Retail Analytics Platform Architecture](pipeline_image.png)

---

## Technology

| Area | Technology | Why it is used |
|---|---|---|
| Programming | Python | Data generation, validation, and orchestration helpers |
| Transformation | PySpark / Spark SQL | Distributed transformations and analytical logic |
| Storage | Delta Lake | Managed analytical tables |
| Data platform | Databricks | Compute, jobs, SQL, pipelines, and governance |
| Pipelines | Lakeflow Declarative Pipelines | Bronze → Silver → Gold execution |
| Governance | Unity Catalog | Schemas and managed input Volume |
| Deployment | Databricks Asset Bundles | Source-controlled resources and environments |
| Analytics | Power BI | Semantic model and dashboard |
| Testing | pytest | Generator, bundle, Silver, and Gold contracts |
| CI | GitHub Actions | Syntax, configuration, and unit checks |
| Version control | Git / GitHub | Code, platform configuration, and PBIP source |

---

## What the project covers

The current implementation includes:

- deterministic synthetic retail data generation
- customers, stores, products, transactions, and returns
- promotions and product price changes
- anonymous walk-in customers
- duplicate and late-arriving records
- Bronze ingestion and quarantine
- trusted Silver facts and dimensions
- SCD Type 2 product price history
- return-to-purchase validation
- business and financial data-quality checks
- Gold analytical models
- revenue and row-count reconciliation
- incremental source batches
- manifest-based batch tracking
- reporting views for Power BI
- a source-controlled Power BI project
- unit and contract tests
- GitHub Actions CI
- Databricks Asset Bundle deployment

The generator intentionally creates cases such as duplicates, late arrivals, promotions, walk-in customers, returns, and product price changes. That gives the pipeline something realistic to deal with instead of testing only against already-clean input.

# How data moves through the platform

## 1. Synthetic source data

The pipeline starts with a Python retail data generator.

The main files are:

```text
data_generator/
├── generator.py
├── incremental.py
├── price_history.py
├── product_catalogue.py
└── progress.py
```

It generates stores, terminals, customers, products, transactions, baskets, returns, promotions, and product price history.

Reference data lives in:

```text
master/
├── raw_schema.json
├── store_master.json
└── terminal_master.json
```

A fixed random seed makes a run reproducible. The generator also introduces the imperfect cases that the rest of the pipeline is expected to handle.

### Default demo configuration

The bundle currently uses these defaults:

| Parameter | Default |
|---|---:|
| Transaction lines | 2,000,000 |
| Customers | 50,000 |
| Seed | 42 |
| Start date | 2023-01-01 |
| End date | 2026-03-31 |
| Price-history end | 2026-12-31 |
| Walk-in rate | 10% |
| Late-arrival rate | 5% |
| Return rate | 4% |
| Duplicate rate | 0.1% |

These values are defaults rather than fixed limits.

For example, a 500,000-row demo can be started with:

```bash
databricks bundle run retail_medallion_job \
  --target dev \
  --params mode=demo,records=500000,customers=25000,start_date=2023-01-01,end_date=2026-03-31,seed=42
```

---

## 2. Raw landing zone

Generated files are written to a managed Unity Catalog Volume.

For the development target, the defaults are:

```text
catalog: workspace
schema:  retail_dev_raw
volume:  retail_input
```

The generator manages these folders:

```text
dimensions/
transactions/
returns/
_manifests/
_staging/
```

Files are staged before they are published. Each published batch gets a manifest, which acts as its commit record. That makes batches traceable and allows an exact rerun to behave safely.

---

## 3. Bronze

Bronze is the ingestion boundary.

It keeps the source data close to what arrived, applies the expected types, and separates records that should not move forward.

```text
Incoming record
      │
      ├── structurally usable ─────► Bronze
      │
      └── invalid / malformed ─────► Quarantine
```

Bad records therefore have an explicit place to go instead of disappearing silently.

The Bronze implementation starts in `pipelines/00_bronze.sql`.

More detail is available in [`docs/bronze-layer.md`](docs/bronze-layer.md).

<!-- Recommended screenshot:
     docs/images/lakeflow-pipeline.png
     Place a screenshot of the Lakeflow Bronze/Silver/Gold DAG here. -->

---

## 4. Silver

Silver turns readable source records into trusted business entities.

The relevant files are:

```text
pipelines/
├── 10_silver_dimensions.sql
├── 11_silver_sales.sql
├── 12_silver_returns.sql
└── 13_silver_quality.sql
```

This is where the pipeline handles duplicate logic, referential integrity, pricing consistency, revenue checks, return-to-purchase relationships, return-window validation, trusted facts, and Silver quality gates.

Product price history uses SCD Type 2, so a transaction can be checked against the product price that was valid when the sale happened.

More detail is available in [`docs/silver-layer.md`](docs/silver-layer.md).

---

## 5. Gold

Gold contains analytical models built from trusted Silver data.

```text
pipelines/
├── 20_gold_baskets.sql
├── 21_gold_sales_stores.sql
├── 22_gold_products.sql
├── 23_gold_customers_returns_traffic.sql
└── 24_gold_quality.sql
```

The models cover basket behaviour, daily sales, store performance, product performance, customer lifetime value, returns analysis, hourly traffic, and data-quality reconciliation.

Reusable business logic stays here instead of being recreated separately in Power BI visuals.

---

## 6. Validation and reconciliation

A Spark job finishing successfully does not tell us whether the resulting numbers are correct.

The project checks the outputs with:

```text
scripts/
├── validate_medallion.py
└── validate_reporting.py
```

Silver and Gold contain explicit quality checks. Gold also reconciles analytical outputs with trusted source data.

In other words, the project checks both whether the job ran and whether the numbers still agree.

---

## 7. Reporting layer

Power BI connects through a reporting layer rather than depending directly on every internal Silver and Gold table.

```text
Silver / Gold
      │
      ▼
Reporting Views
      │
      ▼
Power BI
```

The views are defined in `sql/reporting_views.sql`:

```text
v_fact_sales
v_fact_returns

v_dim_date
v_dim_store
v_dim_product
v_dim_customer

v_executive_kpis
v_daily_sales
v_store_performance
v_product_performance
v_customer_ltv
v_returns_analysis
v_hourly_traffic
v_data_quality_summary
```

That reporting contract gives the BI model a stable interface while the internal transformation layers can continue to evolve.

`scripts/create_reporting_views.py` creates the views, and `scripts/validate_reporting.py` checks them afterwards.

---

## 8. Power BI

The Power BI project is stored as PBIP:

```text
powerbi/
├── retail_chain_dashboard.pbip
├── retail_chain_dashboard.Report/
├── retail_chain_dashboard.SemanticModel/
├── einkaufpark-fluent2.json
└── Theme.json
```

Using `.pbip` keeps the report definition and semantic model in source control instead of relying only on a binary `.pbix` file.

![Power BI Executive Dashboard](docs/images/powerbi-dashboard-executive.png)
![Power BI Return Dashboard](docs/images/powerbi-dashboard-return.png)
![Power BI Store Dashboard](docs/images/powerbi-dashboard-store.png)

To open the report:

1. Install Power BI Desktop.
2. Open `powerbi/retail_chain_dashboard.pbip`.
3. Make sure the Databricks data-source credentials are valid.
4. Use **Home → Refresh** after the Databricks pipeline has completed.

# Databricks architecture

The bundle creates five logical schemas:

```text
Raw
 ↓
Bronze
 ↓
Silver
 ↓
Gold
 ↓
Reporting
```

The development target uses:

```text
workspace.retail_dev_raw
workspace.retail_dev_bronze
workspace.retail_dev_silver
workspace.retail_dev_gold
workspace.retail_dev_reporting
```

The release target uses:

```text
workspace.retail_raw
workspace.retail_bronze
workspace.retail_silver
workspace.retail_gold
workspace.retail_reporting
```

Both targets use the same pipeline code. Environment-specific names live in `databricks.yml`, so there is no separate copy of the transformations for each environment.

---

# Orchestration

The main Databricks job is `retail_medallion_job`.

Its tasks run in this order:

```text
generate_retail_data
        │
        ▼
refresh_bronze_silver_gold
        │
        ▼
validate_medallion
        │
        ▼
create_reporting_views
        │
        ▼
validate_reporting
```

Each downstream task waits for its dependency to succeed.

The job is defined in `resources/retail_job.yml`. The Lakeflow pipeline lives in `resources/medallion_pipeline.yml`, and the Unity Catalog schemas and managed input Volume are defined in `resources/unity_catalog.yml`.

![Databricks job run](docs/images/databricks-job-run.png)

---

# Repository structure

```text
retail_analytics_pipeline/
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── data_generator/
│   ├── generator.py
│   ├── incremental.py
│   ├── price_history.py
│   ├── product_catalogue.py
│   └── progress.py
│
├── docs/
│   ├── bronze-layer.md
│   └── silver-layer.md
│
├── master/
│   ├── raw_schema.json
│   ├── store_master.json
│   └── terminal_master.json
│
├── pipelines/
│   ├── 00_bronze.sql
│   ├── 10_silver_dimensions.sql
│   ├── 11_silver_sales.sql
│   ├── 12_silver_returns.sql
│   ├── 13_silver_quality.sql
│   ├── 20_gold_baskets.sql
│   ├── 21_gold_sales_stores.sql
│   ├── 22_gold_products.sql
│   ├── 23_gold_customers_returns_traffic.sql
│   └── 24_gold_quality.sql
│
├── powerbi/
│   ├── retail_chain_dashboard.pbip
│   ├── retail_chain_dashboard.Report/
│   ├── retail_chain_dashboard.SemanticModel/
│   ├── einkaufpark-fluent2.json
│   └── Theme.json
│
├── resources/
│   ├── medallion_pipeline.yml
│   ├── retail_job.yml
│   └── unity_catalog.yml
│
├── scripts/
│   ├── create_reporting_views.py
│   ├── deploy_dev.sh
│   ├── validate_medallion.py
│   └── validate_reporting.py
│
├── sql/
│   └── reporting_views.sql
│
├── tests/
│   ├── unit/
│   ├── sql/
│   └── test_pipeline_contracts.py
│
├── databricks.yml
├── requirements-dev.txt
├── pipeline_image.png
└── README.md
```

