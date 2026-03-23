"""
Generate sample data for Spark optimization exercise
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

np.random.seed(42)

# Generate large orders dataset (100k records)
n_orders = 100000
orders = pd.DataFrame({
    'order_id': [f'order_{i:08d}' for i in range(n_orders)],
    'customer_id': [f'cust_{np.random.randint(0, 10000):06d}' for _ in range(n_orders)],
    'product_id': [f'prod_{np.random.randint(0, 1000):05d}' for _ in range(n_orders)],
    'revenue': np.random.uniform(10, 1000, n_orders),
    'status': np.random.choice(['completed', 'pending', 'cancelled'], n_orders, p=[0.8, 0.15, 0.05]),
    'order_date': [datetime(2025, 1, 1) + timedelta(days=np.random.randint(0, 365)) for _ in range(n_orders)]
})

# Generate customers dimension table (10k records)
n_customers = 10000
customers = pd.DataFrame({
    'customer_id': [f'cust_{i:06d}' for i in range(n_customers)],
    'customer_name': [f'Customer {i}' for i in range(n_customers)],
    'segment': np.random.choice(['Premium', 'Standard', 'Basic'], n_customers)
})

# Generate products dimension table (1k records)
n_products = 1000
products = pd.DataFrame({
    'product_id': [f'prod_{i:05d}' for i in range(n_products)],
    'product_name': [f'Product {i}' for i in range(n_products)],
    'category': np.random.choice(['Electronics', 'Clothing', 'Home', 'Sports'], n_products)
})

# Save as Parquet with compatible timestamp format
orders.to_parquet('data/orders_large.parquet', index=False, engine='pyarrow', use_deprecated_int96_timestamps=True)
customers.to_parquet('data/customers.parquet', index=False, engine='pyarrow')
products.to_parquet('data/products.parquet', index=False, engine='pyarrow')

print(f"Generated {len(orders)} orders")
print(f"Generated {len(customers)} customers")
print(f"Generated {len(products)} products")
