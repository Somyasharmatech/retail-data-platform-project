# scripts/inventory_forecaster.py
import duckdb
import pandas as pd
import os
from datetime import datetime, timedelta

# Importing forecasting libraries
from statsmodels.tsa.statespace.sarimax import SARIMAX # More general than ARIMA
from prophet import Prophet
import warnings

warnings.filterwarnings("ignore") # Ignore some common warnings from statsmodels/prophet

# Define paths relative to the project root
PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..')
DUCKDB_DB_PATH = os.path.join(PROJECT_ROOT, 'data', 'retail_data.duckdb') # The DuckDB database file

def forecast_inventory_demand():
    print(f"Connecting to DuckDB database: {DUCKDB_DB_PATH}")
    con = duckdb.connect(database=DUCKDB_DB_PATH)

    # Create a schema for forecasts if it doesn't exist
    con.execute("CREATE SCHEMA IF NOT EXISTS forecasts;")
    print("Forecasts schema ensured.")

    # Fetch historical daily sales data from the fct_sales mart
    print("\nFetching historical daily product sales data...")
    # Note: fct_sales should exist after transform_marts.py runs successfully
    query_daily_sales = """
    SELECT
        sale_date,
        product_id,
        SUM(quantity_sold) AS total_quantity_sold
    FROM marts.fct_sales
    GROUP BY sale_date, product_id
    ORDER BY sale_date, product_id;
    """
    historical_sales_df = con.execute(query_daily_sales).fetchdf()

    if historical_sales_df.empty:
        print("No historical sales data found. Cannot perform forecasting.")
        con.close()
        return

    # Ensure sale_date is datetime and set as index for time series operations
    historical_sales_df['sale_date'] = pd.to_datetime(historical_sales_df['sale_date'])

    all_forecasts = pd.DataFrame()
    forecast_horizon_days = 30 # Forecast for the next 30 days

    print(f"Starting forecasting for {len(historical_sales_df['product_id'].unique())} unique products...")

    for product_id in historical_sales_df['product_id'].unique():
        product_df = historical_sales_df[historical_sales_df['product_id'] == product_id].set_index('sale_date')
        product_df = product_df['total_quantity_sold'].resample('D').sum().fillna(0) # Resample to daily, fill missing days with 0

        if len(product_df) < 60: # Need a reasonable amount of data for forecasting
            # print(f"  Skipping forecasting for {product_id}: Not enough historical data ({len(product_df)} days).")
            # Fallback: Simple average of last 7 days or 0 if no data
            predicted_quantity = round(product_df.tail(7).mean()) if not product_df.empty else 0

            # Create simple forecast for the horizon if not enough data
            last_date = product_df.index[-1] if not product_df.empty else datetime.strptime(historical_sales_df['sale_date'].max().strftime('%Y-%m-%d'), '%Y-%m-%d')
            for i in range(1, forecast_horizon_days + 1):
                forecast_date = last_date + timedelta(days=i)
                all_forecasts = pd.concat([all_forecasts, pd.DataFrame([{
                    'product_id': product_id,
                    'forecast_date': forecast_date.strftime('%Y-%m-%d'),
                    'predicted_quantity': max(0, predicted_quantity) # Ensure non-negative
                }])], ignore_index=True)
            continue

        try:
            # --- Use Prophet for forecasting ---
            # Prophet requires columns 'ds' (datestamp) and 'y' (value)
            prophet_df = product_df.reset_index().rename(columns={'sale_date': 'ds', 'total_quantity_sold': 'y'})

            # Fit Prophet model
            model = Prophet(
                yearly_seasonality=True,
                weekly_seasonality=True,
                daily_seasonality=False,
                interval_width=0.95 # Confidence interval
            )
            model.fit(prophet_df)

            # Make future dataframe
            future = model.make_future_dataframe(periods=forecast_horizon_days, freq='D')

            # Predict
            forecast = model.predict(future)

            # Extract relevant part of the forecast (future dates only)
            last_historical_date = prophet_df['ds'].max()
            future_forecast = forecast[forecast['ds'] > last_historical_date]

            for _, row in future_forecast.iterrows():
                all_forecasts = pd.concat([all_forecasts, pd.DataFrame([{
                    'product_id': product_id,
                    'forecast_date': row['ds'].strftime('%Y-%m-%d'),
                    'predicted_quantity': max(0, round(row['yhat'])) # yhat is the prediction
                }])], ignore_index=True)
            # print(f"  Forecasted for {product_id} using Prophet.")

        except Exception as e:
            print(f"  Error forecasting {product_id} with Prophet (using SARIMAX fallback): {e}")
            # Fallback to SARIMAX if Prophet fails or for robustness (simpler SARIMAX)
            try:
                # Choose SARIMAX order (p,d,q)(P,D,Q,s) - very basic example order
                # (1,1,1)(0,0,0,0) is a common starting point for non-seasonal
                sarimax_model = SARIMAX(product_df, order=(1,1,1), seasonal_order=(0,0,0,0))
                sarimax_fit = sarimax_model.fit(disp=False) # disp=False suppresses verbose output

                # Forecast
                sarimax_forecast = sarimax_fit.predict(start=len(product_df), end=len(product_df) + forecast_horizon_days - 1)
                forecast_dates = pd.date_range(start=product_df.index[-1] + timedelta(days=1), periods=forecast_horizon_days, freq='D')

                for date, value in zip(forecast_dates, sarimax_forecast):
                    all_forecasts = pd.concat([all_forecasts, pd.DataFrame([{
                        'product_id': product_id,
                        'forecast_date': date.strftime('%Y-%m-%d'),
                        'predicted_quantity': max(0, round(value)) # Ensure non-negative
                    }])], ignore_index=True)
                # print(f"  Forecasted for {product_id} using SARIMAX.")
            except Exception as sarimax_e:
                print(f"  Error forecasting {product_id} with SARIMAX fallback: {sarimax_e}. Skipping this product.")
                # If all else fails, add 0 prediction for the product
                last_date = product_df.index[-1]
                for i in range(1, forecast_horizon_days + 1):
                    forecast_date = last_date + timedelta(days=i)
                    all_forecasts = pd.concat([all_forecasts, pd.DataFrame([{
                        'product_id': product_id,
                        'forecast_date': forecast_date.strftime('%Y-%m-%d'),
                        'predicted_quantity': 0
                    }])], ignore_index=True)


    # Store forecasts in DuckDB
    if not all_forecasts.empty:
        con.execute("DROP TABLE IF EXISTS forecasts.product_demand_forecasts;")
        con.execute("CREATE TABLE forecasts.product_demand_forecasts AS SELECT * FROM all_forecasts;")
        print(f"\nLoaded {len(all_forecasts)} demand forecasts into forecasts.product_demand_forecasts.")
    else:
        print("\nNo forecasts generated to load into DuckDB.")

    con.close()
    print("\nInventory forecasting complete. DuckDB connection closed.")

if __name__ == "__main__":
    forecast_inventory_demand()