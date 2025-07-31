# scripts/transform_staging.py
import duckdb
import pandas as pd
import os

# Define paths relative to the project root
PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..')
DUCKDB_DB_PATH = os.path.join(PROJECT_ROOT, 'data', 'retail_data.duckdb') # The DuckDB database file

def transform_staging_data():
    print(f"Connecting to DuckDB database: {DUCKDB_DB_PATH}")
    con = duckdb.connect(database=DUCKDB_DB_PATH)

    # Create a schema for staging data if it doesn't exist
    con.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    print("Staging schema ensured.")

    # --- Staging Transformation for Sales ---
    print("\nTransforming sales data to staging...")
    raw_sales_df = con.execute("SELECT * FROM sales;").fetchdf()
    stg_sales_df = raw_sales_df.copy() # Work on a copy to avoid SettingWithCopyWarning

    # Apply transformations similar to stg_sales.sql
    stg_sales_df['sale_date'] = pd.to_datetime(stg_sales_df['sale_date'])
    stg_sales_df['net_sales_amount'] = (
        stg_sales_df['quantity_sold'] * stg_sales_df['price_per_unit'] * (1 - stg_sales_df['discount_applied'])
    )

    # Select and reorder columns as needed for staging
    stg_sales_df = stg_sales_df[[
        'transaction_id', 'product_id', 'customer_id', 'sale_date',
        'quantity_sold', 'price_per_unit', 'discount_applied', 'store_id', 'net_sales_amount'
    ]]

    # Load into DuckDB staging schema
    con.execute("DROP TABLE IF EXISTS staging.stg_sales;") # Drop if exists to ensure clean replace
    con.execute("CREATE TABLE staging.stg_sales AS SELECT * FROM stg_sales_df;")
    print(f"Loaded {len(stg_sales_df)} rows into staging.stg_sales.")


    # --- Staging Transformation for Products ---
    print("\nTransforming product data to staging...")
    raw_product_df = con.execute("SELECT * FROM product_catalog;").fetchdf()
    stg_product_df = raw_product_df.copy()

    # Apply transformations (mostly just select/rename for staging)
    stg_product_df = stg_product_df[[
        'product_id', 'product_name', 'category', 'brand', 'cost_price',
        'weight_kg', 'dimensions_cm', 'supplier_id'
    ]]

    con.execute("DROP TABLE IF EXISTS staging.stg_products;")
    con.execute("CREATE TABLE staging.stg_products AS SELECT * FROM stg_product_df;")
    print(f"Loaded {len(stg_product_df)} rows into staging.stg_products.")


    # --- Staging Transformation for Inventory ---
    print("\nTransforming inventory data to staging...")
    raw_inventory_df = con.execute("SELECT * FROM inventory;").fetchdf()
    stg_inventory_df = raw_inventory_df.copy()

    # Apply transformations
    stg_inventory_df['inventory_date'] = pd.to_datetime(stg_inventory_df['last_updated'])

    stg_inventory_df = stg_inventory_df[[
        'product_id', 'store_id', 'current_stock_level', 'inventory_date'
    ]]

    con.execute("DROP TABLE IF EXISTS staging.stg_inventory;")
    con.execute("CREATE TABLE staging.stg_inventory AS SELECT * FROM stg_inventory_df;")
    print(f"Loaded {len(stg_inventory_df)} rows into staging.stg_inventory.")


    # --- Staging Transformation for Suppliers ---
    print("\nTransforming supplier data to staging...")
    raw_supplier_df = con.execute("SELECT * FROM supplier;").fetchdf()
    stg_supplier_df = raw_supplier_df.copy()

    # Apply transformations (mostly just select for staging)
    stg_supplier_df = stg_supplier_df[[
        'supplier_id', 'supplier_name', 'contact_person', 'lead_time_days', 'minimum_order_quantity'
    ]]

    con.execute("DROP TABLE IF EXISTS staging.stg_supplier;")
    con.execute("CREATE TABLE staging.stg_supplier AS SELECT * FROM stg_supplier_df;")
    print(f"Loaded {len(stg_supplier_df)} rows into staging.stg_supplier.")

    con.close()
    print("\nAll staging transformations complete. DuckDB connection closed.")

if __name__ == "__main__":
    transform_staging_data()