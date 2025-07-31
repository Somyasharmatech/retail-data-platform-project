# app.py
import streamlit as st
import duckdb
import pandas as pd
import os
import plotly.express as px
from datetime import datetime, timedelta

# Define path to your DuckDB database file (relative to app.py)
DUCKDB_DB_PATH = os.path.join("data", "retail_data.duckdb")

# --- Streamlit App Configuration ---
st.set_page_config(layout="wide") # Use wide layout for better dashboard space
st.title("📊 Retail Data Platform Dashboard")

# --- Database Connection Function ---
@st.cache_resource # Cache the database connection
def get_duckdb_connection():
    """Establishes and returns a DuckDB connection."""
    try:
        con = duckdb.connect(database=DUCKDB_DB_PATH, read_only=True)
        return con
    except Exception as e:
        st.error(f"Error connecting to DuckDB: {e}")
        return None

con = get_duckdb_connection()

if con:
    # --- Display Raw Sales Data ---
    st.header("Raw Sales Data Sample")
    try:
        raw_sales_df = con.execute("SELECT * FROM main.sales LIMIT 10;").fetchdf()
        st.dataframe(raw_sales_df)
    except Exception as e:
        st.warning(f"Could not load raw sales data: {e}")

    # --- Display Transformed Sales Data (Fact Sales) ---
    st.header("Fact Sales Data Sample (Mart Layer)")
    try:
        fct_sales_df = con.execute("SELECT * FROM marts.fct_sales LIMIT 10;").fetchdf()
        st.dataframe(fct_sales_df) # Displays the sample table

        if not fct_sales_df.empty:
            st.subheader("Sales Trend Over Time")
            # Aggregate daily sales for the chart
            # Ensure sale_date is in datetime format for plotting
            daily_sales_trend = fct_sales_df.groupby('sale_date')['net_sales_amount'].sum().reset_index()
            daily_sales_trend['sale_date'] = pd.to_datetime(daily_sales_trend['sale_date'])

            st.dataframe(daily_sales_trend) # <<< ADDED FOR DEBUGGING/VERIFICATION (can remove later)

            fig_sales_trend = px.line(
                daily_sales_trend, 
                x='sale_date', 
                y='net_sales_amount', 
                title='Daily Net Sales Trend',
                labels={'net_sales_amount': 'Net Sales Amount', 'sale_date': 'Date'}
            )
            st.plotly_chart(fig_sales_trend, use_container_width=True)
        else:
            st.info("No data in Fact Sales to display Sales Trend Chart.")

    except Exception as e:
        st.warning(f"Could not load fact sales data or generate sales trend: {e}")

    # --- Display Pricing Recommendations Sample ---
    st.header("Pricing Recommendations Sample (AI/ML Output)")
    try:
        pricing_reco_df = con.execute("SELECT * FROM recommendations.product_pricing_recommendations LIMIT 10;").fetchdf()
        st.dataframe(pricing_reco_df)
    except Exception as e:
        st.warning(f"Could not load pricing recommendations: {e}")

    # --- Display Demand Forecasts Sample ---
    st.header("Demand Forecasts Sample (AI/ML Output)")
    try:
        forecasts_df = con.execute("SELECT * FROM forecasts.product_demand_forecasts LIMIT 10;").fetchdf()
        st.dataframe(forecasts_df) # Displays the sample table

        if not forecasts_df.empty:
            st.subheader("Inventory Levels vs. Next Day's Demand Forecast")

            # --- Fetching latest available inventory date and data ---
            latest_inventory_date_query = """
            SELECT MAX(inventory_date) FROM marts.agg_daily_inventory_summary;
            """
            latest_inventory_date_result = con.execute(latest_inventory_date_query).fetchone()
            latest_inventory_date = latest_inventory_date_result[0] if latest_inventory_date_result else None

            latest_inventory_df = pd.DataFrame() # Initialize empty DataFrame
            if latest_inventory_date:
                latest_inventory_df_query = f"""
                SELECT
                    product_id,
                    current_stock_level
                FROM marts.agg_daily_inventory_summary
                WHERE inventory_date = '{latest_inventory_date.strftime('%Y-%m-%d')}';
                """
                latest_inventory_df = con.execute(latest_inventory_df_query).fetchdf()

            # --- Fetching forecasts for the day immediately after the latest sales date ---
            latest_sales_date_query = """
            SELECT MAX(sale_date) FROM marts.fct_sales;
            """
            latest_sales_date_result = con.execute(latest_sales_date_query).fetchone()
            latest_sales_date = latest_sales_date_result[0] if latest_sales_date_result else None

            forecast_tomorrow_df = pd.DataFrame() # Initialize empty DataFrame
            if latest_sales_date:
                # Find the forecast for the day immediately following the latest sales date
                forecast_target_date = latest_sales_date + timedelta(days=1)
                forecast_tomorrow_df_query = f"""
                SELECT product_id, predicted_quantity
                FROM forecasts.product_demand_forecasts
                WHERE forecast_date = '{forecast_target_date.strftime('%Y-%m-%d')}';
                """
                forecast_tomorrow_df = con.execute(forecast_tomorrow_df_query).fetchdf()


            if not latest_inventory_df.empty and not forecast_tomorrow_df.empty:
                # Merge inventory and tomorrow's forecast
                merged_df = pd.merge(
                    latest_inventory_df, 
                    forecast_tomorrow_df[['product_id', 'predicted_quantity']], 
                    on='product_id', 
                    how='left'
                )
                merged_df['predicted_quantity'] = merged_df['predicted_quantity'].fillna(0).astype(int) # Fill NaNs

                # Sort for better visualization
                merged_df = merged_df.sort_values(by='predicted_quantity', ascending=False).head(20) # Top 20 for chart clarity

                fig_inv_forecast = px.bar(
                    merged_df, 
                    x='product_id', 
                    y=['current_stock_level', 'predicted_quantity'], 
                    barmode='group', 
                    title="Current Stock vs. Tomorrow's Predicted Demand (Top 20 Products)",
                    labels={'value': 'Quantity', 'variable': 'Metric'}
                )
                st.plotly_chart(fig_inv_forecast, use_container_width=True)
            else:
                st.info("Not enough data to display Inventory vs. Demand Forecast chart. Ensure your data covers relevant dates or adjust forecast query logic.")
    except Exception as e:
        st.warning(f"Could not load demand forecasts or generate inventory vs demand chart: {e}")

    # Note: We open read_only so we don't need to explicitly close the connection.
    # Streamlit handles resource caching for us.