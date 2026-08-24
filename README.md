# Einkaufpark Retail Analytics Platform

Einkaufpark is a data engineering project built around a fictional multi-store retailer. It starts with synthetic retail data — transactions, customers, stores, products, promotions, price changes, and returns — and carries that data all the way through Databricks to a Power BI reporting layer.

The pipeline follows a **Bronze → Silver → Gold** architecture. Raw events are ingested first, then cleaned and validated, and finally shaped into datasets that are useful for analysis.

I built the project around a simple reality: retail data is rarely as clean as the CSV files used in tutorials. Records can arrive late or more than once. Some customers are anonymous. Product prices change. Returns happen after the original purchase. And a row can be technically valid while still being wrong from a business point of view.

Einkaufpark is my way of working through those problems end to end, before the data reaches a dashboard.

## Architecture

<!-- Add pipeline architecture image here -->

![Einkaufpark Retail Analytics Platform Architecture](pipeline_image.png)

## Technology

| Area | Technology | What it does here |
|---|---|---|
| Programming | Python | Generates data and runs validation and orchestration helpers |
| Transformation | PySpark / Spark SQL | Cleans, validates, and models the retail data |
| Storage | Delta Lake | Stores managed analytical tables |
| Data platform | Databricks | Runs compute, jobs, SQL, pipelines, and governance |
| Pipelines | Lakeflow Declarative Pipelines | Executes the Bronze → Silver → Gold flow |
| Governance | Unity Catalog | Manages schemas and the input Volume |
| Deployment | Databricks Asset Bundles | Keeps platform resources and environments in source control |
| Analytics | Power BI | Provides the semantic model and dashboards |
| Testing | pytest | Checks generator, bundle, Silver, and Gold contracts |
| CI | GitHub Actions | Runs syntax, configuration, and unit checks |
| Version control | Git / GitHub | Tracks code, platform configuration, and PBIP source |

## What the project covers

The project currently handles:

- deterministic synthetic retail data generation
- customers, stores, products, transactions, and returns
- promotions and product price changes
- anonymous walk-in customers
- duplicate records
- late-arriving records
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

The generator intentionally creates some of the messiness the pipeline is expected to handle: duplicates, late arrivals, promotions, walk-in purchases, returns, and changing product prices. That makes it possible to test the pipeline against realistic failure cases instead of only perfect input.

# How the data moves through the platform

## 1. Synthetic source data

Everything starts with the Python data generator.

The main files are:

```text
data_generator/
├── generator.py
├── incremental.py
├── price_history.py
├── product_catalogue.py
└── progress.py
```

Together they generate stores, terminals, customers, products, transactions, baskets, returns, promotions, and product price history.

The supporting reference data lives in:

```text
master/
├── raw_schema.json
├── store_master.json
└── terminal_master.json
```

The generator also creates late arrivals, duplicate rows, walk-in customers, returns, promotions, and price changes on purpose. A fixed random seed keeps the generated dataset reproducible, which is especially useful when debugging or comparing pipeline runs.

### Default demo configuration

The current bundle defaults are:

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

These values are defaults rather than fixed limits. You can override them when you start the job.

For example, to run a custom demo:

```bash
databricks bundle run retail_medallion_job \
  --target dev \
  --params mode=demo,records=500000,customers=25000,start_date=2023-01-01,end_date=2026-03-31,seed=42
```

---

## 2. Raw landing zone

Generated files are written to a managed Unity Catalog Volume.

For the development target, the default location is built from:

```text
catalog: workspace
schema:  retail_dev_raw
volume:  retail_input
```

The generator owns these folders inside the landing area:

```text
dimensions/
transactions/
returns/
_manifests/
_staging/
```

A batch is staged before it is published. Once publishing succeeds, its manifest becomes the record of what was committed. That gives each generated batch a traceable identity and makes exact reruns safe.

---

## 3. Bronze

Bronze is where source data first enters the medallion pipeline.

The point of this layer is not to make the data analytically perfect. It is to preserve what arrived, give it usable types, and separate records that are too malformed to move forward safely.

```text
Incoming record
      │
      ├── structurally usable ─────► Bronze
      │
      └── invalid / malformed ─────► Quarantine
```

Bad records are therefore visible and inspectable instead of simply disappearing.

The Bronze implementation starts in `pipelines/00_bronze.sql`.

More detail is available in [`docs/bronze-layer.md`](docs/bronze-layer.md).

<!-- Recommended screenshot:
     docs/images/lakeflow-pipeline.png
     Place a screenshot of the Lakeflow Bronze/Silver/Gold DAG here. -->

---

## 4. Silver

Silver is where readable source records become trusted business data.

The relevant pipeline files are:

```text
pipelines/
├── 10_silver_dimensions.sql
├── 11_silver_sales.sql
├── 12_silver_returns.sql
└── 13_silver_quality.sql
```

This layer deals with duplicate logic, referential integrity, pricing consistency, revenue checks, return-to-purchase relationships, return-window validation, trusted fact construction, and Silver quality gates.

Product prices are modeled with SCD Type 2 history. That lets a transaction be checked against the price that was actually valid when the purchase happened, rather than whatever the current product price happens to be.

More detail is available in [`docs/silver-layer.md`](docs/silver-layer.md).

---

## 5. Gold

Gold turns trusted Silver data into reusable analytical models.

The current Gold pipeline is split across:

```text
pipelines/
├── 20_gold_baskets.sql
├── 21_gold_sales_stores.sql
├── 22_gold_products.sql
├── 23_gold_customers_returns_traffic.sql
└── 24_gold_quality.sql
```

These models cover basket behavior, daily sales, store performance, product performance, customer lifetime value, returns analysis, hourly traffic, and data-quality reconciliation.

I keep this business logic in the data platform rather than rebuilding the same calculations independently in Power BI visuals. That way, the analytical definitions have one place to live and can be reused downstream.

---

## 6. Validation and reconciliation

A successful Spark run tells me the computation finished. It does not, by itself, tell me the resulting numbers are right.

That is why validation is part of the pipeline rather than something checked manually at the end.

The main validation scripts are:

```text
scripts/
├── validate_medallion.py
└── validate_reporting.py
```

Silver and Gold contain explicit quality checks, while Gold also reconciles analytical outputs back to trusted source data.

In other words:

```text
"the job succeeded"
```

and

```text
"the numbers reconcile"
```

are two different questions.

The project checks both.

---

## 7. Reporting layer

Power BI does not need to know how every internal Silver or Gold table is implemented.

Instead, a reporting layer sits between the analytical models and the semantic model:

```text
Silver / Gold
      │
      ▼
Reporting Views
      │
      ▼
Power BI
```

The views are defined in `sql/reporting_views.sql` and currently include:

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

This gives Power BI a stable contract while leaving the underlying Silver and Gold implementation free to evolve.

The views are created by `scripts/create_reporting_views.py` and checked by `scripts/validate_reporting.py`.

---

## 8. Power BI

The BI project is kept in Power BI Project (`.pbip`) format:

```text
powerbi/
├── retail_chain_dashboard.pbip
├── retail_chain_dashboard.Report/
├── retail_chain_dashboard.SemanticModel/
├── einkaufpark-fluent2.json
└── Theme.json
```

Using PBIP means the report definition and semantic model can live in Git with the rest of the project instead of being hidden inside a single binary `.pbix` file.

### Dashboard previews

![Power BI Executive Dashboard](docs/images/powerbi-dashboard-executive.png)

![Power BI Return Dashboard](docs/images/powerbi-dashboard-return.png)

![Power BI Store Dashboard](docs/images/powerbi-dashboard-store.png)

To open the report:

1. Install Power BI Desktop.
2. Open `powerbi/retail_chain_dashboard.pbip`.
3. Make sure your Databricks data-source credentials are valid.
4. After the Databricks pipeline finishes, use **Home → Refresh** to load the latest reporting data.

# Databricks architecture

The bundle separates the platform into five logical schemas:

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

For development, those schemas are:

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

Both targets use the same pipeline code. The environment-specific names live in `databricks.yml`, so there is no second copy of the transformation logic to keep in sync.

---

# Orchestration

The main Databricks workflow is `retail_medallion_job`.

It runs the platform in this order:

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

Each task depends on the one before it. If an upstream step fails, the later steps do not continue against a bad state.

The job itself is defined in `resources/retail_job.yml`. The Lakeflow pipeline lives in `resources/medallion_pipeline.yml`, while the Unity Catalog schemas and managed input Volume are defined in `resources/unity_catalog.yml`.

![Databricks job run](docs/images/databricks-job-run.png)

---

# Repository structure

The main working parts of the repository are:

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

# Getting started

## Prerequisites

You will need:

- Git
- Python 3.11+
- Databricks CLI with Asset Bundle support
- access to a Databricks workspace
- Unity Catalog support in that workspace
- Power BI Desktop if you want to open the dashboard

Local development only needs two Python packages:

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

Then install the development dependencies:

```bash
python -m pip install --upgrade pip
pip install -r requirements-dev.txt
```

## 3. Authenticate with Databricks

The project does not use a `.env` file for Databricks credentials. Authentication is handled by the Databricks CLI.

Configure a profile for your workspace, then check that it works:

```bash
databricks current-user me --profile <your-profile>
```

`scripts/deploy_dev.sh` uses `einkaufpark-free` as its default profile name. If your profile has a different name, set:

```bash
export DATABRICKS_PROFILE=<your-profile>
```

Keep credentials out of the repository.

## 4. Run the local tests

```bash
python -m pytest tests/unit -q
```

The current unit suite checks the bundle configuration, generator behavior, Silver contracts, and Gold contracts.

There are additional test assets under `tests/`, including SQL tests and `test_pipeline_contracts.py`. The current GitHub Actions workflow runs `tests/unit`.

## 5. Validate the Databricks bundle

```bash
databricks bundle validate \
  --target dev \
  --profile <your-profile>
```

This is a quick way to catch bundle configuration problems before deploying anything.

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

Without extra parameters, the job uses the defaults defined in `databricks.yml`.

A normal run goes through:

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

---

# Controlling generated data

The Databricks job exposes these runtime parameters:

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

The behavior parameters are expressed as rates:

```text
walkin_rate=0.10
late_rate=0.05
return_rate=0.04
duplicate_rate=0.001
```

That corresponds to roughly:

```text
10% walk-in behavior
5% late-arrival behavior
4% return behavior
0.1% duplicate injection
```

These knobs are there to make the input imperfect on purpose, so the pipeline can be tested against the situations it was built to handle.

---

# Demo, incremental, and reset modes

The generator supports three operating modes.

## `demo`

`demo` creates the initial deterministic baseline.

If published landing data already exists, the generator refuses to overwrite it. Replacing the baseline is meant to be an explicit action, not an accidental side effect of rerunning a command.

## `incremental`

`incremental` adds a new, non-overlapping business-date range to an existing baseline.

For example:

```bash
databricks bundle run retail_medallion_job \
  --target dev \
  --params mode=incremental,records=5000,start_date=2026-04-01,end_date=2026-04-03
```

The generator reuses the existing dimensions and product price history. Published manifests keep track of completed batches, prevent overlapping date ranges, and make an exact rerun safe.

## `reset`

The Python generator also has a reset mode. It removes only the landing directories owned by the generator.

For a local landing area:

```bash
python data_generator/incremental.py \
  --mode reset \
  --output-dir data/raw
```

One detail is worth calling out: `retail_medallion_job` is an end-to-end workflow, not a reset-only job. Once its generator task succeeds, it continues into Bronze, Silver, Gold, and validation.

For that reason, any dedicated Databricks reset workflow should stay separate from the normal processing job.

---

# Running the generator locally

You can work on the generator without running the full Databricks platform.

For example:

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

This is useful when changing generation logic or running generator-focused tests.

The full **Bronze → Silver → Gold → Reporting** workflow still runs in Databricks.
