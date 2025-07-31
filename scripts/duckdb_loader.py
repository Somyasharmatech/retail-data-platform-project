# scripts/duckdb_loader.py
import duckdb
import pandas as pd
import os
import glob

# Define paths relative to the project root
# This script will assume it's run from retail_data_platform/dbt_project or similar,
# so we go up one level to retail_data_platform, then into data/raw
# And create the duckdb file at retail_data_platform/data/retail_data.duckdb
PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..')
RAW_DATA_DIR = os.path.join(PROJECT_ROOT, 'data', 'raw')
DUCKDB_DB_PATH = os.path.join(PROJECT_ROOT, 'data', 'retail_data.duckdb') # The DuckDB database file

def load_csv_to_duckdb():
    print(f"Connecting to DuckDB database: {DUCKDB_DB_PATH}")
    # Connect to DuckDB. If the file doesn't exist, it will be created.
    con = duckdb.connect(database=DUCKDB_DB_PATH)

    # Iterate through each CSV file and load it
    csv_files = glob.glob(os.path.join(RAW_DATA_DIR, '*.csv'))

    if not csv_files:
        print(f"No CSV files found in {RAW_DATA_DIR}. Please run data_generator.py first.")
        return

    for csv_file in csv_files:
        file_name = os.path.basename(csv_file)
        table_name = os.path.splitext(file_name)[0] # e.g., 'sales_data'

        # Clean up table name if needed (e.g., remove _data suffix for cleaner names in DB)
        if table_name.endswith('_data'):
            table_name = table_name[:-5] # remove '_data'

        print(f"\nLoading '{file_name}' into DuckDB table '{table_name}'...")

        try:
            # Use DuckDB's powerful COPY FROM command for efficient loading
            # READ_CSV_AUTO detects column types and headers automatically
            con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_file}');")
            print(f"Successfully loaded {con.execute(f'SELECT COUNT(*) FROM {table_name}').fetchone()[0]} rows into {table_name}.")
        except Exception as e:
            print(f"Error loading {file_name}: {e}")

    con.close()
    print("\nAll raw CSVs loaded to DuckDB. Database closed.")

if __name__ == "__main__":
    load_csv_to_duckdb()