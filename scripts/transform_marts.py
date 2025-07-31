# scripts/transform_marts.py
import duckdb
import pandas as pd
import os

# Define paths relative to the project root
PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..')
DUCKDB_DB_PATH = os.path.join(PROJECT_ROOT, 'data', 'retail_data.duckdb')

def transform_marts_data():
    print(f"Connecting to DuckDB database: {DUCKDB_DB_PATH}")
    con = duckdb.connect(database=DUCKDB_DB_PATH)

    # Create a schema for mart data if it doesn't exist
    con.execute("CREATE SCHEMA IF NOT EXISTS marts;")
    print("Marts schema ensured.")

    # --- Mart: dim_products (Dimension Table) ---
    print("\nCreating mart: dim_products...")
    query_dim_products = """
    SELECT
        product_id,
        product_name,
        category,
        brand,
        cost_price,
        supplier_name,
        lead_time_days
    FROM intermediate.int_product_details;
    """
    dim_products_df = con.execute(query_dim_products).fetchdf()

    con.execute("DROP TABLE IF EXISTS marts.dim_products;")
    con.execute("CREATE TABLE marts.dim_products AS SELECT * FROM dim_products_df;")
    print(f"Loaded {len(dim_products_df)} rows into marts.dim_products.")


    # --- Mart: fct_sales (Fact Table) ---
    print("\nCreating mart: fct_sales...")
    query_fct_sales = """
    SELECT
        s.sale_date,
        s.transaction_id,
        s.product_id,
        s.customer_id,
        s.store_id,
        s.quantity_sold,
        s.price_per_unit,
        s.discount_applied,
        s.net_sales_amount,
        p.category,
        p.brand,
        p.cost_price,
        p.supplier_name
    FROM staging.stg_sales AS s
    LEFT JOIN marts.dim_products AS p ON s.product_id = p.product_id;
    """
    fct_sales_df = con.execute(query_fct_sales).fetchdf()

    con.execute("DROP TABLE IF EXISTS marts.fct_sales;")
    con.execute("CREATE TABLE marts.fct_sales AS SELECT * FROM fct_sales_df;")
    print(f"Loaded {len(fct_sales_df)} rows into marts.fct_sales.")


    # --- Mart: agg_daily_inventory_summary (Aggregated Mart) ---
    print("\nCreating mart: agg_daily_inventory_summary...")
    query_agg_inventory = """
    SELECT
        i.inventory_date,
        i.store_id,
        i.product_id,
        i.current_stock_level,
        p.product_name,
        p.category,
        p.brand,
        p.cost_price,
        p.supplier_name,
        p.lead_time_days
    FROM staging.stg_inventory AS i
    LEFT JOIN marts.dim_products AS p ON i.product_id = p.product_id;
    """
    agg_daily_inventory_summary_df = con.execute(query_agg_inventory).fetchdf()

    con.execute("DROP TABLE IF EXISTS marts.agg_daily_inventory_summary;")
    con.execute("CREATE TABLE marts.agg_daily_inventory_summary AS SELECT * FROM agg_daily_inventory_summary_df;")
    print(f"Loaded {len(agg_daily_inventory_summary_df)} rows into marts.agg_daily_inventory_summary.")


    con.close()
    print("\nAll mart transformations complete. DuckDB connection closed.")

if __name__ == "__main__":
    transform_marts_data()