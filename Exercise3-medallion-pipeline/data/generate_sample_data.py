"""
Sample Data Generator for Exercise 3
Run this script to generate sample data files for the medallion pipeline exercise
"""

import pandas as pd
import json
import os
from datetime import datetime, timedelta
import random

# Create data directory
os.makedirs('data', exist_ok=True)

print("Generating sample data for CloudMart medallion pipeline...")

# Generate sample orders.parquet
print("Creating orders.parquet...")
orders_data = []
for i in range(1000):
    orders_data.append({
        'order_id': i + 1,
        'user_id': f'user_{random.randint(1, 100)}',
        'product_id': f'prod_{random.randint(1, 50)}',
        'order_value': round(random.uniform(10, 500), 2),
        'order_timestamp': datetime.now() - timedelta(days=random.randint(0, 30))
    })

orders_df = pd.DataFrame(orders_data)
orders_df.to_parquet('data/orders.parquet', index=False)
print(f"Generated {len(orders_data)} orders")

# Generate sample clickstream.json
print("Creating clickstream.json...")
clickstream_data = []
for i in range(500):
    clickstream_data.append({
        'user_id': f'user_{random.randint(1, 100)}',
        'timestamp': (datetime.now() - timedelta(hours=random.randint(0, 72))).isoformat(),
        'events': {
            'page': random.choice(['home', 'product', 'cart', 'checkout']),
            'actions': random.choices(['view', 'click', 'add_to_cart', 'purchase'], k=random.randint(1, 3))
        }
    })

with open('data/clickstream.json', 'w') as f:
    for record in clickstream_data:
        f.write(json.dumps(record) + '\n')

print(f"Generated {len(clickstream_data)} clickstream events")

print("\nSample data generation completed!")
print("Files created:")
print("- data/orders.parquet")
print("- data/clickstream.json")
print("\nYou can now run the medallion pipeline exercise.")
