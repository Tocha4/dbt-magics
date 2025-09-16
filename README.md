# dbt-magics
### What is dbt-magics?
dbt-magics is a python package that provides python line and cell magics when developing with dbt.
The magics allow you to create and run SQL commands against AWS Athena and Google BigQuery from within a Jupyter notebook or VSCode notebook.
So, instead of using the Athena Query Editor or the BigQuery Console, you can use the magics to run SQL commands directly from within your notebook. 

## Required IDE (one of the following)
- jupyter-notebook
- jupyter-lab
- VSCode (Notebook)

## Python dbt Package Requirements  
- dbt-core
- dbt-bigquery *(for bigquery magics)*
- dbt-athena-community  *(for athena magics)*

## Installation
```bash
pip install git+https://github.com/Tocha4/dbt-magics.git 
```

## Setup dbt
For setup instructions for AWS Athena and Google BigQuery, please see the [dbt documentation](https://docs.getdbt.com/docs/running-a-dbt-project/using-the-command-line-interface#section-2-configure-your-profile).

## Athena Magics
In order to use the Athena magics, you first have to load the magics into your notebook:

```python
# load the magics for athena into your notebook
%load_ext dbt_magics.athenaMagics
```

### Cell Magic
The line magic will run the SQL command and return the results as a pandas dataframe.
```python
%%athena
SELECT * FROM my_database.my_table
```
### Line Magic
The cell magic provides a visual dropdown interface that allows to select a specific database, table and its columns. Then, a SQL-Query is generated based on the selections. The SQL-Query can then be run using the line magic.
```python
%athena
```
### Docstring
Run the following command for the full docstring including the arguments
```python
%athena?
```

## BigQuery Magics
BigQuery magics are very similar to Athena magics. Yyou first have to load the magics into your notebook:

```python
# load the magics for bigquery into your notebook
%load_ext dbt_magics.bigqueryMagics
```

### Cell Magic
```python
%%bigquery
SELECT * FROM my_project.my_dataset.my_table
```

### Line Magic
```python
%bigquery
```

The image below shows an example of the interface for the cell magic.
![BigQuery Cell Magic](img/bigquery_cell.png)

### Docstring
```python
%bigquery?
```

## Snowflake Magics
Snowflake magics work similarly to other magics. First load the magics:

```python
# load the magics for snowflake into your notebook
%load_ext dbt_magics.snowflakeMagics
```

### Cell Magic
```python
%%snowflake
SELECT * FROM my_database.my_schema.my_table
```

### DuckDB Export Feature
The Snowflake magics include a built-in feature to export query results to DuckDB. This is useful for local analytics and data storage.

#### Configuration
Add DuckDB configuration to your dbt `profiles.yml`:

```yaml
your_profile:
  outputs:
    dev:
      type: snowflake
      # ... your snowflake config
      duckdb:
        path: /path/to/your/database.duckdb
        schema: dbt_dev  # Schema name for dbt tables in DuckDB
```

Or create a separate DuckDB profile:

```yaml
duckdb_profile:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /path/to/your/database.duckdb
      schema: dbt_analytics  # Default schema for dbt tables
```

#### Usage Examples

**Export query results to DuckDB (replace table):**
```python
%%snowflake --export_duckdb
SELECT * FROM {{ ref('some_model') }}
```
This will create the table as `dbt_dev.some_model` (using the schema from your profile and table name from ref()).

**Append to existing DuckDB table:**
```python
%%snowflake --export_duckdb --duckdb_mode append
SELECT * FROM {{ ref('some_model') }}
```

**Export any DataFrame to DuckDB:**
```python
# For standalone DataFrame export
from dbt_magics.snowflakeMagics import export_dataframe_to_duckdb

# Replace table (default) - will use dbt naming conventions
export_dataframe_to_duckdb(my_df, 'my_model')

# Append to table
export_dataframe_to_duckdb(my_df, 'my_model', if_exists='append')
```

**Schema and Table Naming:**
- Tables are created using dbt naming conventions: `schema.table_name`
- Schema comes from the `duckdb.schema` setting in your profiles.yml
- Table name is automatically extracted from the `ref()` function in your SQL
- If no schema is specified, it falls back to dbt's custom schema logic or the default schema
- **Important**: Your SQL must contain a `ref('table_name')` for automatic table naming to work

### Docstring
```python
%snowflake?
```

## Contributing
In order to edit the code, please install the package in editable mode and run the command below:
```bash
pip install -e .
```

### Adding a new magic for a new database software
1. Create a new magic file in the dbt_magics folder
2. Create a new dbtHelper class that inherits from the dbtHelper class in the dbtHelper.py file
3. Create a new DataController class that inherits from the datacontroller.DataController and implement the abstract methods for the specific database software
