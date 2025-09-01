# Ecommerce Data Engineering Project

## 📌 Overview
This repository implements an **Ecommerce Data Pipeline** in **Databricks** using the **Medallion Architecture** (Bronze → Silver → Gold).  
It is organized into modular Python scripts for ingestion, transformation, utilities, configuration, and testing.

## 📂 Repository Contents

- **`config.py`**  
  Central configuration file. Contains file paths, schema definitions, and constants used across the pipeline.  

- **`utils.py`**  
  Reusable helper functions (e.g., date parsing, Spark DataFrame read/write, validations).  

- **`test_utils.py`**  
  Unit and integration tests using `pytest` to validate the functions in `utils.py`.  

- **`bronze_notebook.py`**  
  Ingests raw data into the **Bronze layer** with minimal transformations.  

- **`silver_notebook.py`**  
  Cleanses, standardizes, and enriches Bronze data into the **Silver layer**.  

- **`gold_notebook.py`**  
  Curates Silver data into business-ready **Gold tables**.

- **`sql_aggregates.py`**  
  Contains SQL queries for aggregations and final reporting.  

## 🏗️ Medallion Architecture Flow
```text
        Source Systems
              │
              ▼
          Bronze Layer
   (Raw ingestion, schema enforcement)
              │
              ▼
          Silver Layer
   (Cleansing, standardization, enrichment)
              │
              ▼
           Gold Layer
   (Business aggregates, KPIs, facts/dimensions)
