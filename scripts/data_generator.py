# scripts/data_generator.py
import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import os 

fake = Faker('en_IN') # Indian locale for some realism, adjust as needed

def generate_sales_data(num_records=10000, start_date='2024-01-01', end_date='2024-12-31'):
    products = [f'PROD{i:04d}' for i in range(1, 101)] # 100 products
    customers = [f'CUST{i:05d}' for i in range(1, 501)] # 500 customers
    stores = [f'STORE{i:02d}' for i in range(1, 11)] # 10 stores

    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d') # Corrected: use end_dt instead of end_date
    time_delta = (end_dt - start_dt).days

    data = []
    for i in range(num_records):
        sale_date = start_dt + timedelta(days=random.randint(0, time_delta))
        product_id = random.choice(products)
        customer_id = random.choice(customers)
        store_id = random.choice(stores)
        quantity_sold = random.randint(1, 5)
        price_per_unit = round(random.uniform(100.0, 5000.0), 2)
        discount_applied = round(random.uniform(0.0, 0.2), 2) if random.random() < 0.3 else 0.0 # 30% chance of discount

        data.append({
            'transaction_id': f'TXN{i:06d}',
            'product_id': product_id,
            'customer_id': customer_id,
            'sale_date': sale_date.strftime('%Y-%m-%d'),
            'quantity_sold': quantity_sold,
            'price_per_unit': price_per_unit,
            'discount_applied': discount_applied,
            'store_id': store_id
        })
    return pd.DataFrame(data)

def generate_product_catalog(num_products=100):
    categories = ['Electronics', 'Home Goods', 'Apparel', 'Books', 'Groceries']
    brands = [fake.company() for _ in range(20)]
    suppliers = [f'SUP{i:03d}' for i in range(1, 21)]

    data = []
    for i in range(1, num_products + 1):
        product_id = f'PROD{i:04d}'
        category = random.choice(categories)
        brand = random.choice(brands)
        cost_price = round(random.uniform(50.0, 3000.0), 2)
        supplier_id = random.choice(suppliers)

        data.append({
            'product_id': product_id,
            'product_name': fake.catch_phrase(),
            'category': category,
            'brand': brand,
            'cost_price': cost_price,
            'weight_kg': round(random.uniform(0.1, 10.0), 2),
            'dimensions_cm': f"{random.randint(5,50)}x{random.randint(5,50)}x{random.randint(5,50)}",
            'supplier_id': supplier_id
        })
    return pd.DataFrame(data)

def generate_inventory_data(num_stores=10, num_products=100, current_date='2024-12-31'):
    products = [f'PROD{i:04d}' for i in range(1, num_products + 1)]
    stores = [f'STORE{i:02d}' for i in range(1, 11)]

    data = []
    for store_id in stores:
        for product_id in products:
            current_stock_level = random.randint(0, 100) if random.random() < 0.9 else 0 # 10% chance of being out of stock
            data.append({
                'product_id': product_id,
                'store_id': store_id,
                'current_stock_level': current_stock_level,
                'last_updated': current_date
            })
    return pd.DataFrame(data)

def generate_supplier_data(num_suppliers=20):
    data = []
    for i in range(1, num_suppliers + 1):
        supplier_id = f'SUP{i:03d}'
        data.append({
            'supplier_id': supplier_id,
            'supplier_name': fake.company() + ' Suppliers',
            'contact_person': fake.name(),
            'lead_time_days': random.randint(3, 30),
            'minimum_order_quantity': random.randint(10, 200)
        })
    return pd.DataFrame(data)

# Main execution
if __name__ == "__main__":
    # Define output directory relative to the project root
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw')
    os.makedirs(output_dir, exist_ok=True)

    print("Generating sales data...")
    sales_df = generate_sales_data(num_records=50000)
    sales_df.to_csv(os.path.join(output_dir, 'sales_data.csv'), index=False)
    print(f"Generated {len(sales_df)} sales records.")

    print("Generating product catalog data...")
    product_df = generate_product_catalog(num_products=200)
    product_df.to_csv(os.path.join(output_dir, 'product_catalog.csv'), index=False)
    print(f"Generated {len(product_df)} product records.")

    print("Generating inventory data...")
    inventory_df = generate_inventory_data(num_stores=10, num_products=200)
    inventory_df.to_csv(os.path.join(output_dir, 'inventory_data.csv'), index=False)
    print(f"Generated {len(inventory_df)} inventory records.")

    print("Generating supplier data...")
    supplier_df = generate_supplier_data(num_suppliers=30)
    supplier_df.to_csv(os.path.join(output_dir, 'supplier_data.csv'), index=False)
    print(f"Generated {len(supplier_df)} supplier records.")

    print("Data generation complete!")