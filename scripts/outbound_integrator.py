# scripts/outbound_integrator.py
import duckdb
import pandas as pd
import os
from datetime import datetime

# Define paths relative to the project root
PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..')
DUCKDB_DB_PATH = os.path.join(PROJECT_ROOT, 'data', 'retail_data.duckdb') # The DuckDB database file
OUTBOUND_DIR = os.path.join(PROJECT_ROOT, 'data', 'processed') # Directory for outbound files

# Ensure the outbound directory exists
os.makedirs(OUTBOUND_DIR, exist_ok=True)

def export_data_for_customers():
    print(f"Connecting to DuckDB database: {DUCKDB_DB_PATH}")
    con = duckdb.connect(database=DUCKDB_DB_PATH)

    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

    # --- Export Inventory Summary ---
    print("\nExporting Inventory Summary for customer ERP...")
    query_inventory_summary = """
    SELECT
        inventory_date,
        store_id,
        product_id,
        current_stock_level,
        product_name,
        category,
        brand,
        cost_price,
        supplier_name,
        lead_time_days
    FROM marts.agg_daily_inventory_summary
    WHERE inventory_date = (SELECT MAX(inventory_date) FROM marts.agg_daily_inventory_summary); -- Get the latest summary
    """
    inventory_summary_df = con.execute(query_inventory_summary).fetchdf()

    if not inventory_summary_df.empty:
        output_filepath = os.path.join(OUTBOUND_DIR, f"inventory_snapshot_{timestamp}.csv")
        inventory_summary_df.to_csv(output_filepath, index=False)
        print(f"Successfully exported {len(inventory_summary_df)} rows to {output_filepath}")
    else:
        print("No inventory summary data found to export.")

    # --- Export Pricing Recommendations ---
    print("\nExporting Pricing Recommendations for customer's e-commerce system...")
    query_pricing_recommendations = """
    SELECT
        product_id,
        product_name,
        current_price_reference,
        recommended_price,
        pricing_reason,
        recommendation_date
    FROM recommendations.product_pricing_recommendations
    WHERE recommendation_date = (SELECT MAX(recommendation_date) FROM recommendations.product_pricing_recommendations); -- Get the latest recommendations
    """
    pricing_recommendations_df = con.execute(query_pricing_recommendations).fetchdf()

    if not pricing_recommendations_df.empty:
        output_filepath = os.path.join(OUTBOUND_DIR, f"pricing_recommendations_{timestamp}.csv")
        pricing_recommendations_df.to_csv(output_filepath, index=False)
        print(f"Successfully exported {len(pricing_recommendations_df)} rows to {output_filepath}")
    else:
        print("No pricing recommendations data found to export.")

    con.close()
    print("\nOutbound data integration complete. DuckDB connection closed.")

if __name__ == "__main__":
    export_data_for_customers()