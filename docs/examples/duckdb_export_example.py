"""
Example usage of dbt-magics with DuckDB export functionality

This example demonstrates how to use the Snowflake magics with DuckDB export features
using dbt naming conventions.
"""

# Step 1: Load the magics
# %load_ext dbt_magics.snowflakeMagics

# Step 2: Export a dbt model to DuckDB with schema-aware naming
# Table will be created as dbt_dev.fact_sales (schema from profiles.yml, table name from ref)
"""
%%snowflake --export_duckdb
SELECT 
    customer_id,
    order_date,
    total_amount,
    product_category
FROM {{ ref('fact_sales') }}
WHERE order_date >= '2024-01-01'
"""

# Step 3: Append additional data to the same dbt model table
"""
%%snowflake --export_duckdb --duckdb_mode append
SELECT 
    customer_id,
    order_date,
    total_amount,
    product_category
FROM {{ ref('fact_sales') }}
WHERE order_date >= '2024-06-01'
"""

# Step 4: Export a staging model (will be created as dbt_dev.stg_customers)
"""
%%snowflake --export_duckdb
SELECT * FROM {{ ref('stg_customers') }}
"""

# Step 5: Export any DataFrame to DuckDB using dbt naming conventions
"""
import pandas as pd
from dbt_magics.snowflakeMagics import export_dataframe_to_duckdb

# Create a sample DataFrame
df = pd.DataFrame({
    'customer_id': [1, 2, 3, 4, 5],
    'customer_name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
    'signup_date': pd.date_range('2024-01-01', periods=5, freq='D'),
    'total_orders': [10, 5, 8, 12, 3]
})

# Export to DuckDB using dbt naming (will create dbt_dev.dim_customers)
export_dataframe_to_duckdb(df, 'dim_customers')

# Export to DuckDB (append to existing table)
export_dataframe_to_duckdb(df, 'dim_customers', if_exists='append')
"""

# Step 6: Query the exported data using DuckDB with schema names
"""
import duckdb

# Connect to your DuckDB database
conn = duckdb.connect('/path/to/your/analytics.duckdb')

# Query the exported data using schema.table format
result = conn.execute("SELECT * FROM dbt_dev.fact_sales LIMIT 10").fetchdf()
print(result)

# Analyze the data across multiple dbt tables
analysis = conn.execute('''
    SELECT 
        fs.product_category,
        COUNT(DISTINCT fs.customer_id) as unique_customers,
        COUNT(*) as order_count,
        SUM(fs.total_amount) as total_revenue,
        AVG(fs.total_amount) as avg_order_value
    FROM dbt_dev.fact_sales fs
    JOIN dbt_dev.dim_customers dc ON fs.customer_id = dc.customer_id
    GROUP BY fs.product_category
    ORDER BY total_revenue DESC
''').fetchdf()

print(analysis)

# Show all tables in the dbt schema
tables = conn.execute("SHOW TABLES FROM dbt_dev").fetchdf()
print("Tables in dbt_dev schema:", tables)

conn.close()
"""

print("Example usage script created!")
print("Key features:")
print("1. Configure dbt profiles.yml with DuckDB settings including schema")
print("2. Load the magics: %load_ext dbt_magics.snowflakeMagics") 
print("3. Use --export_duckdb flag in your %%snowflake cells")
print("4. Table name is automatically extracted from ref() function")
print("5. Tables are created as schema.table_name following dbt conventions")
print("6. Use export_dataframe_to_duckdb() for standalone DataFrames")
print("7. Query using schema.table format in DuckDB")
