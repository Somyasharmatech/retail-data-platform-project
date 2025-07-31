# scripts/pricing_recommender.py
import duckdb
import pandas as pd
import os
from datetime import datetime, timedelta # datetime is used for CURRENT_DATE() equivalent

# Define paths relative to the project root
PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..')
DUCKDB_DB_PATH = os.path.join(PROJECT_ROOT, 'data', 'retail_data.duckdb')

def generate_pricing_recommendations():
    print(f"Connecting to DuckDB database: {DUCKDB_DB_PATH}")
    con = duckdb.connect(database=DUCKDB_DB_PATH)

    # Create a schema for recommendations if it doesn't exist
    con.execute("CREATE SCHEMA IF NOT EXISTS recommendations;")
    print("Recommendations schema ensured.")

    # Fetch necessary data from marts and forecasts
    print("\nFetching data for pricing recommendations...")
    query_pricing_data = """
    SELECT
        dp.product_id,
        dp.product_name,
        dp.category,
        dp.cost_price,
        COALESCE(inv.current_stock_level, 0) AS current_stock_level,
        COALESCE(fd.predicted_quantity, 0) AS predicted_demand_tomorrow,
        -- Get average historical price from fact sales for reference
        (SELECT AVG(price_per_unit) FROM marts.fct_sales fs WHERE fs.product_id = dp.product_id) AS historical_avg_price
    FROM marts.dim_products AS dp
    LEFT JOIN marts.agg_daily_inventory_summary AS inv
        ON dp.product_id = inv.product_id AND inv.inventory_date = CURRENT_DATE() -- Assuming we need today's stock
    LEFT JOIN forecasts.product_demand_forecasts AS fd
        ON dp.product_id = fd.product_id AND fd.forecast_date = CURRENT_DATE() + INTERVAL '1 day' -- Demand for tomorrow
    ;
    """
    pricing_data_df = con.execute(query_pricing_data).fetchdf()

    if pricing_data_df.empty:
        print("No data found for pricing recommendations. Cannot proceed.")
        con.close()
        return

    print(f"Generating pricing recommendations for {len(pricing_data_df)} products...")
    recommendations = []

    # Simple Rule-Based Pricing Logic (can be expanded with more complex ML)
    for _, row in pricing_data_df.iterrows():
        product_id = row['product_id']
        # Use historical average price as a starting point, otherwise apply a default markup on cost
        current_price_reference = row['historical_avg_price'] if pd.notnull(row['historical_avg_price']) else (row['cost_price'] * 1.5)
        recommended_price = current_price_reference
        reason = []

        # Rule 1: High Inventory & Low Predicted Demand -> Discount
        if row['current_stock_level'] > 50 and row['predicted_demand_tomorrow'] < 10:
            recommended_price *= 0.90  # 10% discount
            reason.append("High stock, low predicted demand (10% discount)")

        # Rule 2: Low Inventory & High Predicted Demand -> Premium
        elif row['current_stock_level'] < 10 and row['predicted_demand_tomorrow'] > 30:
            recommended_price *= 1.15  # 15% premium
            reason.append("Low stock, high predicted demand (15% premium)")

        # Rule 3: High Cost Product with no recent sales -> Adjust initial markup
        elif pd.isna(row['historical_avg_price']) and row['cost_price'] > 1000:
            recommended_price = row['cost_price'] * 1.8 # Higher markup for high-cost, unselling items
            reason.append("High cost product, no historical sales (higher default markup)")

        # Ensure price doesn't go below a certain margin (e.g., 10% above cost)
        recommended_price = max(recommended_price, row['cost_price'] * 1.10) # Minimum 10% margin

        recommendations.append({
            'product_id': product_id,
            'product_name': row['product_name'],
            'current_price_reference': round(current_price_reference, 2),
            'recommended_price': round(recommended_price, 2),
            'pricing_reason': "; ".join(reason) if reason else "Standard pricing based on cost/historical average",
            'recommendation_date': datetime.now().strftime('%Y-%m-%d') # <<< THIS IS THE CRUCIAL LINE ADDED
        })

    pricing_recommendations_df = pd.DataFrame(recommendations)

    # Store recommendations in DuckDB
    if not pricing_recommendations_df.empty:
        con.execute("DROP TABLE IF EXISTS recommendations.product_pricing_recommendations;")
        con.execute("CREATE TABLE recommendations.product_pricing_recommendations AS SELECT * FROM pricing_recommendations_df;")
        print(f"\nLoaded {len(pricing_recommendations_df)} pricing recommendations into recommendations.product_pricing_recommendations.")
    else:
        print("\nNo pricing recommendations generated to load into DuckDB.")

    con.close()
    print("\nDynamic pricing recommendation complete. DuckDB connection closed.")

if __name__ == "__main__":
    generate_pricing_recommendations()