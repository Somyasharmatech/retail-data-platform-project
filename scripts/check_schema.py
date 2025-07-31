# scripts/check_schema.py
import duckdb
import os
import pandas as pd # Import pandas to display results nicely

# Define path to your DuckDB database file
PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..')
DUCKDB_DB_PATH = os.path.join(PROJECT_ROOT, 'data', 'retail_data.duckdb')

def check_table_schema(db_path, table_name):
    print(f"Connecting to DuckDB database: {db_path}")
    con = duckdb.connect(database=db_path)

    try:
        print(f"\n--- Schema for {table_name} ---")
        # DuckDB's describe statement
        query = f"DESCRIBE {table_name};"
        schema_df = con.execute(query).fetchdf() # Fetch results into a pandas DataFrame
        print(schema_df.to_string(index=False)) # Print DataFrame without index

    except Exception as e:
        print(f"Error describing table {table_name}: {e}")
    finally:
        con.close()
        print("\nDuckDB connection closed.")

if __name__ == "__main__":
    table_to_check = "recommendations.product_pricing_recommendations"
    check_table_schema(DUCKDB_DB_PATH, table_to_check)