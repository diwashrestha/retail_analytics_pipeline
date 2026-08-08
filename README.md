# Einkaufpark Retail Analytics Platform

Einkaufpark is a data engineering project for a fictional multi-store retailer, **EinkaufPark**. A Python-based data generator creates transactions, customers, stores, products, price changes, promotions, and returns. These events are landed in a Unity Catalog Volume and processed through a Databricks Medallion Architecture.

The data flows through a **Bronze → Silver → Gold** architecture, where raw events are progressively validated, cleaned, and transformed into analytics-ready datasets.

## Architecture

<!-- Add pipeline architecture image here -->

![Einkaufpark Retail Analytics Platform Architecture](pipeline_image.png)

## Tech Stack

### Data Engineering

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge\&logo=databricks\&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge\&logo=apachespark\&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-003366?style=for-the-badge)
![Lakeflow](https://img.shields.io/badge/Lakeflow%20Declarative%20Pipelines-FF3621?style=for-the-badge\&logo=databricks\&logoColor=white)
![Unity Catalog](https://img.shields.io/badge/Unity%20Catalog-FF3621?style=for-the-badge\&logo=databricks\&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge\&logo=apachespark\&logoColor=white)
![Spark SQL](https://img.shields.io/badge/Spark%20SQL-E25A1C?style=for-the-badge\&logo=apachespark\&logoColor=white)

### Programming

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge\&logo=python\&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge\&logoColor=white)

### Analytics

![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=for-the-badge\&logo=powerbi\&logoColor=black)
![DAX](https://img.shields.io/badge/DAX-F2C811?style=for-the-badge\&logo=powerbi\&logoColor=black)

### DevOps

![Git](https://img.shields.io/badge/Git-F05032?style=for-the-badge\&logo=git\&logoColor=white)
![GitHub](https://img.shields.io/badge/GitHub-181717?style=for-the-badge\&logo=github\&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-2088FF?style=for-the-badge\&logo=githubactions\&logoColor=white)
![Databricks Bundles](https://img.shields.io/badge/Databricks%20Bundles-FF3621?style=for-the-badge\&logo=databricks\&logoColor=white)

## Project Covers

The pipeline currently handles:

* Synthetic retail data generation
* Transaction and return ingestion
* Schema and data-quality validation
* Invalid-record quarantine
* Duplicate detection
* Product price history using SCD Type 2
* Return-to-purchase validation
* Bronze, Silver, and Gold transformations
* Revenue and data-quality reconciliation
* Business-ready analytical tables

The data generator also includes scenarios such as duplicates, late-arriving data, promotions, walk-in customers, returns, and product price changes. This allows the pipeline to be tested against imperfect data rather than only clean examples.

## Current Status

### ✅ Working

* Synthetic retail data generator
* Unity Catalog data landing
* Bronze pipeline
* Silver facts and dimensions
* SCD Type 2 product history
* Sales and return validation
* Gold analytical models
* End-to-end Lakeflow pipeline execution
* Fully reproducible Databricks Bundle deployment

### 🚧 In Progress

* Incremental and idempotent ingestion
* Reporting layer
* Power BI dashboard
* GitHub Actions CI
* Clean-clone reproducibility testing

---

> **Note:** This project is under active development. More detailed documentation, validation results, Power BI dashboards, CI/CD workflows, and setup instructions will be added as the project progresses.
