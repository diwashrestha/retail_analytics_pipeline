# Einkaufpark Retail Analytics Platform

Einkaufpark is data engineering project for a retail company. The project simulates a multi store retailer "EinkaufPark", a Python generator creates trasnactions, customers, stores, products, price changes, promotions, and returns. These events are landed in a Unity Catlog Volume and processed through a Databricks Medallion Architecture.  

The data flows through a Bronze -> Silver -> Gold Medallion Architecture in Databricks. 

## Architecture

![Einkaufpark Pipeline Architecture](pipeline_image.png)

## Tech Stack

- Data Engineering: Databricks, Lakeflow Declarative Pipelines, Delta Lake, Unity Catalog, Spark SQL, PySpark

- Programming: Python, SQL

- Analytics: Power BI, DAX

- DevOps: Git, GitHub, Databricks Bundles, GitHub Actions

## Project Covers

The pipeline currently handles:

- synthetic retail data generation;

- transaction and return ingestion;

- schema and data-quality validation;

- invalid-record quarantine;

-duplicate detection;

- product price history using SCD Type 2;

- return-to-purchase validation;

- Bronze, Silver, and Gold transformations;

- revenue and data-quality reconciliation;

- business-ready analytical tables.

The data generator also includes scenarios such as duplicates, late-arriving data, promotions, walk-in customers, returns, and product price changes so that the pipeline can be tested against imperfect data rather than only clean examples.


## Current Status

✅ Working

- Synthetic retail data generator

- Unity Catalog data landing

- Bronze pipeline

- Silver facts and dimensions

- SCD Type 2 product history

- Sales and return validation

- Gold analytical models

- End-to-end Lakeflow pipeline execution

- Fully reproducible Databricks Bundle deployment

🚧 In Progress

- Incremental and idempotent ingestion

- Reporting layer

- Power BI dashboard

- GitHub Actions CI

- Clean-clone reproducibility testing