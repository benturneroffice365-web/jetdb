python"""Generate 3 sample datasets for demo (1.5M total rows)"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_sales_data(quarter: int, num_rows: int = 500000):
    """Generate realistic sales data"""
    np.random.seed(42 + quarter)
    
    start_date = datetime(2024, (quarter-1)*3 + 1, 1)
    dates = pd.date_range(start_date, periods=num_rows, freq='1min')
    
    products = ['Widget Pro', 'Gadget Plus', 'Tool Master', 'Device X', 'System Y']
    regions = ['North', 'South', 'East', 'West', 'Central']
    reps = ['John Smith', 'Jane Doe', 'Bob Johnson', 'Alice Williams', 'Charlie Brown']
    
    df = pd.DataFrame({
        'date': dates,
        'product': np.random.choice(products, num_rows),
        'quantity': np.random.randint(1, 100, num_rows),
        'unit_price': np.random.uniform(10, 500, num_rows).round(2),
        'region': np.random.choice(regions, num_rows),
        'sales_rep': np.random.choice(reps, num_rows),
    })
    
    df['revenue'] = (df['quantity'] * df['unit_price']).round(2)
    
    return df

# Generate 3 quarters
for q in [1, 2, 3]:
    df = generate_sales_data(q)
    filename = f'sales_2024_q{q}.csv'
    df.to_csv(filename, index=False)
    print(f"âœ… Generated {filename}: {len(df):,} rows, {df.memory_usage(deep=True).sum() / 1024 / 1024:.1f} MB")

print("\nðŸŽ‰ Demo data ready! Upload these to JetDB.")
