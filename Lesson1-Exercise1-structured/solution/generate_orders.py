import pandas as pd
import numpy as np
from datetime import datetime, timedelta

np.random.seed(42)
n = 10000

df = pd.DataFrame({
    'order_id': [f'order_{i:06d}' for i in range(n)],
    'user_id': [f'user_{np.random.randint(0, 1000):05d}' for _ in range(n)],
    'product_id': [f'prod_{np.random.randint(0, 100):03d}' for _ in range(n)],
    'order_value': np.random.uniform(10, 500, n),
    'order_date': [datetime(2025, 1, 1) + timedelta(days=np.random.randint(0, 365)) for _ in range(n)],
    'status': np.random.choice(['completed', 'pending', 'cancelled'], n)
})

# Add nulls (5.2%)
null_idx = np.random.choice(n, int(n * 0.052), replace=False)
df.loc[null_idx, 'order_value'] = np.nan

# Add duplicates (2.1%)
dup_idx = np.random.choice(n, int(n * 0.021), replace=False)
df.loc[dup_idx, 'order_id'] = df.loc[dup_idx - 1, 'order_id'].values

df.to_parquet('data/orders.parquet', index=False, coerce_timestamps='ms', allow_truncated_timestamps=True)
print(f"Generated {len(df)} orders")
