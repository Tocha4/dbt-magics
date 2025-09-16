# Source Schema Resolution Fix

## Problem

When using `source('source_name', 'table_name')` in dbt-magics, the function was incorrectly using the source name as the schema name, even when a different schema was specified in the `sources.yml` file.

## Example of the Issue

Given this `sources.yml`:
```yaml
version: 2
sources:
  - name: raw_data
    schema: landing_zone  # Actual schema in database
    tables:
      - name: customers
```

**Before Fix (WRONG):**
- `source('raw_data', 'customers')` → `database.raw_data.customers`

**After Fix (CORRECT):**
- `source('raw_data', 'customers')` → `database.landing_zone.customers`

## Root Cause

In `src/dbt_magics/dbt_helper.py`, the `_search_for_source_table()` method was using the source `name` as the schema instead of checking for a `schema` property.

## Fix Applied

Updated the `_search_for_source_table()` method to:

1. **Check for schema property**: Use `entry.get('schema', source_name)` to get the schema
2. **Fall back gracefully**: If no schema is specified, use the source name (maintains backward compatibility)
3. **Return correct schema**: Use the resolved schema name in the return value

## Code Changes

```python
# OLD CODE:
def _search_for_source_table(self, SOURCES, target_schema, target_table, default_database):
    # ... 
    database, name, tables = entry.get('database', default_database), entry.get("name"), entry['tables']
    # ...
    return dict(database=database, schema=name, table=table['name'])

# NEW CODE:  
def _search_for_source_table(self, SOURCES, target_schema, target_table, default_database):
    # ...
    database = entry.get('database', default_database)
    source_name = entry.get("name")
    schema_name = entry.get('schema', source_name)  # ← KEY FIX
    # ...
    return dict(database=database, schema=schema_name, table=table['name'])
```

## Test Cases

| Source Config | source() Call | Expected Result |
|---------------|---------------|-----------------|
| `name: raw_data, schema: landing_zone` | `source('raw_data', 'customers')` | `db.landing_zone.customers` |
| `name: staging` (no schema) | `source('staging', 'dim_table')` | `db.staging.dim_table` |
| `name: api, schema: external_data` | `source('api', 'weather')` | `db.external_data.weather` |

## Backward Compatibility

✅ **Fully backward compatible** - existing sources without explicit schema continue to work exactly as before.

## Affected Files

- `src/dbt_magics/dbt_helper.py` - Fixed `_search_for_source_table()` method
- Works for all adapters: Snowflake, Athena, BigQuery, SQLite

## Usage Examples

```yaml
# sources.yml
version: 2
sources:
  - name: raw_sales
    schema: raw_data_schema  # Different from source name
    database: data_lake     # Optional database override
    tables:
      - name: transactions
      - name: customers

  - name: staging
    # No schema specified - uses 'staging' as schema
    tables:
      - name: cleaned_data
```

```sql
-- In your dbt model or magic cell:
SELECT * FROM {{ source('raw_sales', 'transactions') }}
-- Now correctly generates: data_lake.raw_data_schema.transactions

SELECT * FROM {{ source('staging', 'cleaned_data') }}  
-- Still works: default_db.staging.cleaned_data
```

This fix ensures that dbt-magics properly respects the schema configuration in your dbt sources, making it consistent with standard dbt behavior.
