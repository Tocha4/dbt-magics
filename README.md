# dbt-magics
### What is dbt-magics?
dbt-magics is a python package that provides python line and cell magics when developing with dbt.
The magics allow you to create and run SQL commands against AWS Athena and Google BigQuery from within a Jupyter notebook or VSCode notebook.
So, instead of using the Athena Query Editor or the BigQuery Console, you can use the magics to run SQL commands directly from within your notebook.

## Required IDE (one of the following)
- jupyter-notebook
- jupyter-lab
- VSCode

## Python dbt Package Requirements  
- dbt-core
- dbt-bigquery
- dbt-athena-community

## Installation

```bash
pip install /path/to/dbt-magics
```

## Setup dbt
For setup instructions for AWS Athena and Google BigQuery, please see the [dbt documentation](https://docs.getdbt.com/docs/running-a-dbt-project/using-the-command-line-interface#section-2-configure-your-profile).

## Usage
```python
# load the magics for bigquery and athena, respectively, into your notebook
%load_ext dbt_magics.bigqueryMagics
%load_ext dbt_magics.athenaMagics
```

## Athena Magics
In order to use the Athena magics, run the following command s in your notebook:
### Line Magic
The line magic will run the SQL command and return the results as a pandas dataframe.
```python
%athena
SELECT * FROM my_database.my_table
```
### Cell Magic
The cell magic provides a visual dropdown interface that allows to select a specific database, table and its columns. Then, a SQL-Query is generated based on the selections. The SQL-Query can then be run using the line magic.
```python
%%athena
```
### Docstring
Run the following command for the full docstring including the arguments
```python
%athena?
```

## BigQuery Magics
BigQuery magics are very similar to Athena magics. The commands are as follows:
### Line Magic
```python
%bigquery
SELECT * FROM my_project.my_dataset.my_table
```

### Cell Magic
```python
%%bigquery
```

The image below shows an example of the interface for the cell magic.
![BigQuery Cell Magic](img/bigquery_cell.png)

### Docstring
```python
%bigquery?
```

## Contributing
In order to edit the code, please install the package in editable mode and run the command below:
```bash
pip install -e .
```