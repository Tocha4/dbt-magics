"""
dbt-magics: Jupyter magics for dbt and SQL

This package provides magic commands for running dbt and SQL queries
in Jupyter notebooks with various database backends.
"""

# Import main export functions for easy access
try:
    from .snowflakeMagics import export_dataframe_to_duckdb
    from .athenaMagics import export_dataframe_to_duckdb_athena
    __all__ = ['export_dataframe_to_duckdb', 'export_dataframe_to_duckdb_athena']
except ImportError:
    # Handle case where dependencies are not installed
    __all__ = []

__version__ = "1.3.0"