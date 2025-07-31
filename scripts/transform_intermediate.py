# scripts/transform_intermediate.py
import duckdb
import pandas as pd
import os

# Define paths relative to the project root
PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..')
DUCKDB_DB_PATH = os.path.join(PROJECT_ROOT, 'data', 'retail_data.duckdb')

def transform_intermediate_data():
    print(f"Connecting to DuckDB database: {DUCKDB_DB_PATH}")
    con = duckdb.connect(database=DUCKDB_DB_PATH)

    # Create a schema for intermediate data if it doesn't exist
    con.execute("CREATE SCHEMA IF NOT EXISTS intermediate;")
    print("Intermediate schema ensured.")

    # --- Intermediate Transformation: Daily Product Sales ---
    print("\nCreating intermediate: Daily Product Sales...")
    # Join stg_sales with stg_products for richer context and then aggregate
    query_daily_sales = """
    SELECT
        s.sale_date,
        s.product_id,
        s.store_id,
        SUM(s.quantity_sold) AS daily_quantity_sold,
        SUM(s.net_sales_amount) AS daily_net_sales
    FROM staging.stg_sales AS s
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3;
    """
    int_daily_product_sales_df = con.execute(query_daily_sales).fetchdf()

    con.execute("DROP TABLE IF EXISTS intermediate.int_daily_product_sales;")
    con.execute("CREATE TABLE intermediate.int_daily_product_sales AS SELECT * FROM int_daily_product_sales_df;")
    print(f"Loaded {len(int_daily_product_sales_df)} rows into intermediate.int_daily_product_sales.")


    # --- Intermediate Transformation: Product Details (joining products with suppliers) ---
    print("\nCreating intermediate: Product Details...")
    query_product_details = """
    SELECT
        p.product_id,
        p.product_name,
        p.category,
        p.brand,
        p.cost_price,
        s.supplier_name,
        s.lead_time_days
    FROM staging.stg_products AS p
    LEFT JOIN staging.stg_supplier AS s ON p.supplier_id = s.supplier_id;
    """
    int_product_details_df = con.execute(query_product_details).fetchdf()

    con.execute("DROP TABLE IF EXISTS intermediate.int_product_details;")
    con.execute("CREATE TABLE intermediate.int_product_details AS SELECT * FROM int_product_details_df;")
    print(f"Loaded {len(int_product_details_df)} rows into intermediate.int_product_details.")

    con.close()
    print("\nAll intermediate transformations complete. DuckDB connection closed.")

if __name__ == "__main__":
    transform_intermediate_data()