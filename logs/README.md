# Retail Store Supply Chain & Pricing Optimization Data Platform

## 🚀 Project Overview

This project builds an end-to-end data platform for a retail business, focusing on optimizing inventory levels and pricing strategies. It demonstrates the complete data lifecycle, from raw data generation and local ingestion to multi-layered transformations, AI-driven insights (forecasting and recommendations), and outbound data integration.

### Why this project is useful (Problem Solved):

* **Inefficient Inventory:** Helps reduce stockouts and excess inventory by predicting demand.
* **Suboptimal Pricing:** Enables dynamic pricing strategies to maximize revenue and competitiveness.
* **Manual Data Handling:** Automates the entire data pipeline, replacing manual, error-prone processes with efficient, automated workflows.
* **Fueling AI Applications:** Provides clean, structured data necessary for AI-powered inventory and pricing applications.

### Alignment with UiPath Data Engineer Role:

This project directly showcases skills required for the UiPath Data Engineer role by:
* Creating **seamless data flows** that enhance operational efficiency.
* Demonstrating expertise in **data transformation and automation** using Python.
* Building the data backbone for **AI-powered inventory and pricing applications**.
* Focusing on **tangible impact** for business operations through data.

## 🏗️ Architecture

Raw Data (CSVs)
↓
Python Data Loader (duckdb_loader.py)
↓
Local DuckDB Database (retail_data.duckdb)
↓
Python Transformations (transform_staging.py, transform_intermediate.py, transform_marts.py)
↓
Cleaned/Transformed Data (in DuckDB schemas: staging, intermediate, marts)
↓
Python AI/ML Models (inventory_forecaster.py, pricing_recommender.py)
↓
AI-driven Insights (in DuckDB schemas: forecasts, recommendations)
↓
Python Outbound Integrator (outbound_integrator.py)
↓
Processed Data Exports (CSV files in data/processed/ for customer systems)

## 🛠️ Technologies Used

* **Python:** Primary scripting language for all pipeline components.
* **Pandas:** Data manipulation and transformation within Python scripts.
* **DuckDB:** High-performance, embedded analytical database for local data storage and querying.
* **SQL:** Used within Python scripts for data extraction and loading into DuckDB.
* **`Faker`:** For generating realistic synthetic data.
* **`statsmodels`:** Python library for statistical modeling (e.g., ARIMA for forecasting).
* **`Prophet`:** Meta's forecasting library for time series data.

## 🚀 Getting Started

These instructions will get a copy of the project up and running on your local machine.

### Prerequisites

* Python 3.8+ installed (with `python` command added to PATH).
* Visual Studio Code (VS Code) installed.
* Git (recommended for version control and GitHub upload).

### Installation and Setup

1.  **Clone the repository (if on GitHub) or download the project files.**
2.  **Open the project in VS Code.**
    * Open your Command Prompt (e.g., from `File` > `Terminal` > `New Terminal` in VS Code).
    * Navigate to the project root: `cd C:\Users\YourUser\UI_PATH\retail_data_platform`
3.  **Create and Activate Python Virtual Environment:**
    ```bash
    python -m venv data-platform-env
    .\data-platform-env\Scripts\activate
    ```
4.  **Install Python Dependencies:**
    ```bash
    pip install pandas numpy faker duckdb statsmodels prophet
    ```
5.  **Generate Raw Data:**
    ```bash
    python scripts/data_generator.py
    ```
6.  **Load Raw Data into DuckDB:**
    ```bash
    python scripts/duckdb_loader.py
    ```
7.  **Run Data Transformations (Staging, Intermediate, Marts):**
    ```bash
    python scripts/transform_staging.py
    python scripts/transform_intermediate.py
    python scripts/transform_marts.py
    ```
8.  **Run AI/ML Components (Forecasting, Recommendations):**
    ```bash
    python scripts/inventory_forecaster.py
    python scripts/pricing_recommender.py
    ```
9.  **Run Outbound Data Integration:**
    ```bash
    python scripts/outbound_integrator.py
    ```

## 📊 Verifying the Pipeline (Local Data Exploration)

You can directly query your `retail_data.duckdb` file using the DuckDB CLI:

1.  Open your Command Prompt in the project root (`retail_data_platform`).
2.  Activate your virtual environment.
3.  Enter DuckDB CLI: `duckdb`
4.  Attach and use your database:
    ```sql
    ATTACH 'data/retail_data.duckdb';
    USE retail_data;
    ```
5.  Run sample queries:
    ```sql
    -- Check raw data
    SELECT COUNT(*) FROM main.sales;
    SELECT * FROM main.product_catalog LIMIT 5;

    -- Check staging data
    SELECT COUNT(*) FROM staging.stg_sales;
    SELECT * FROM staging.stg_inventory LIMIT 5;

    -- Check intermediate data
    SELECT COUNT(*) FROM intermediate.int_daily_product_sales;
    SELECT * FROM intermediate.int_product_details LIMIT 5;

    -- Check mart data
    SELECT COUNT(*) FROM marts.fct_sales;
    SELECT * FROM marts.dim_products LIMIT 5;
    SELECT * FROM marts.agg_daily_inventory_summary LIMIT 5;

    -- Check AI/ML insights
    SELECT * FROM forecasts.product_demand_forecasts LIMIT 10;
    SELECT * FROM recommendations.product_pricing_recommendations LIMIT 10;
    ```
6.  Exit DuckDB CLI: `.exit`

## 📸 Proof of Work & Showcase

To effectively showcase this project for your portfolio (especially since it's local):

1.  **Screenshots:**
    * **Successful Script Runs:** Capture screenshots of the terminal output after running each `python scripts/*.py` file, showing "Successfully loaded X rows", "All staging transformations complete", "Inventory forecasting complete", "Outbound data integration complete", etc.
    * **Generated Raw Data:** Screenshot of `data/raw/` folder showing CSVs.
    * **Processed Exports:** Screenshot of `data/processed/` folder showing `inventory_snapshot_*.csv` and `pricing_recommendations_*.csv`.
    * **DuckDB CLI Queries:** Take screenshots of the DuckDB CLI showing query results from `SELECT COUNT(*)` on various schemas/tables (raw, staging, intermediate, marts, forecasts, recommendations). This visually confirms data exists at each layer.
    * **Project Structure:** A screenshot of your VS Code Explorer showing the overall `retail_data_platform` folder structure.

2.  **Video Demonstration (Highly Recommended):**
    * Record a short (2-5 minute) screen-share video.
    * Briefly show the project folder.
    * Open the terminal.
    * Activate the environment.
    * Run `python scripts/data_generator.py` and show the output.
    * Run `python scripts/duckdb_loader.py` and show the output.
    * Run `python scripts/transform_marts.py` (or all three transform scripts in sequence) and show the output.
    * Run `python scripts/inventory_forecaster.py` and `python scripts/pricing_recommender.py` and show output.
    * Run `python scripts/outbound_integrator.py` and show the output.
    * (Optional but powerful) Briefly jump into the DuckDB CLI and run one `SELECT * FROM ... LIMIT 5;` from the `marts` or `forecasts` schema to quickly show data existing.
    * Upload this video to YouTube (unlisted) or Google Drive and link it in your `README.md`.

## 📚 Future Enhancements

* **Orchestration:** Implement a workflow orchestrator (e.g., Apache Airflow, Prefect, Dagster) to automate the daily/weekly execution of these scripts.
* **Cloud Integration:**
    * Use cloud storage (AWS S3, Azure Blob Storage, Google Cloud Storage) for raw data.
    * Integrate with cloud-managed data warehouses (Snowflake, Databricks, BigQuery) for scalability if DuckDB becomes insufficient.
* **Data Visualization:** Connect a BI tool (e.g., Tableau Public, Power BI Desktop, Looker Studio) to the `retail_data.duckdb` file to create interactive dashboards.
* **More Advanced AI/ML:**
    * Explore more sophisticated forecasting models (e.g., deep learning time series models).
    * Develop A/B testing frameworks for pricing strategies.
    * Integrate Large Language Models (LLMs) like **Gemini** for natural language querying of data ("What products had the highest predicted demand last quarter?") or automated narrative generation for reports.
* **Data Quality & Monitoring:** Implement automated data quality checks and set up monitoring and alerting for pipeline failures or data anomalies.

## 🤝 Contribution

Feel free to fork this repository and extend its functionality!

## 📧 Contact

Somya Sharma
