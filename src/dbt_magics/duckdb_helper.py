"""
DuckDB Helper Module for dbt-magics

This module provides shared DuckDB functionality that can be used
across different database adapters (Snowflake, Athena, etc.)
"""

import os
import re
from time import time
import duckdb
import pandas as pd


class DuckDBHelper:
    """Helper class for DuckDB operations in dbt-magics"""
    
    def __init__(self, dbt_helper):
        """
        Initialize DuckDB helper with a dbt helper instance
        
        Parameters:
        - dbt_helper: Instance of dbtHelper or its subclasses
        """
        self.dbt_helper = dbt_helper
        self.prStyle = self._get_pr_style()
    
    def _get_pr_style(self):
        """Get prStyle from datacontroller, with fallback"""
        try:
            from dbt_magics.datacontroller import prStyle
            return prStyle
        except ImportError:
            # Fallback class if prStyle is not available
            class FallbackStyle:
                RED = '\033[91m'
                GREEN = '\033[92m'
                YELLOW = '\033[93m'
                RESET = '\033[0m'
            return FallbackStyle()
    
    def get_duckdb_config(self):
        """Get DuckDB configuration from dbt profiles"""
        duckdb_config = self.dbt_helper.profile_config.get('duckdb', {})
        if not duckdb_config:
            # Try to find duckdb profile in the same profiles.yml
            profiles = self.dbt_helper._get_profiles()
            duckdb_profiles = [p for p in profiles if any([profiles[p]['outputs'][o].get('type') == 'duckdb' for o in profiles[p]['outputs']])]
            if duckdb_profiles:
                duckdb_profile = profiles[duckdb_profiles[0]]
                target = duckdb_profile.get('target', list(duckdb_profile['outputs'].keys())[0])
                duckdb_config = duckdb_profile['outputs'][target]
        
        return duckdb_config
    
    def check_duckdb_availability(self):
        """
        Check if DuckDB database is available and not locked
        
        Returns:
        - True if DuckDB is available for writing
        - False if DuckDB is locked or unavailable
        """
        duckdb_config = self.get_duckdb_config()
        
        if not duckdb_config:
            return False
            
        db_path = duckdb_config.get('path', duckdb_config.get('database', ':memory:'))
        
        # Skip check for in-memory databases
        if db_path == ':memory:':
            return True
            
        try:
            # Try to connect with a short timeout to check if database is locked
            conn = duckdb.connect(db_path)
            
            # Try a simple operation to ensure the database is writable
            conn.execute("SELECT 1")
            conn.close()
            return True
            
        except Exception as e:
            error_msg = str(e).lower()
            if 'database is locked' in error_msg or 'locked' in error_msg:
                print(f"{self.prStyle.RED}DuckDB database is locked: {db_path}{self.prStyle.RESET}")
                print(f"{self.prStyle.YELLOW}Skipping query execution. Please close other DuckDB connections and try again.{self.prStyle.RESET}")
            else:
                print(f"{self.prStyle.RED}DuckDB connection error: {e}{self.prStyle.RESET}")
            return False
    
    def get_duckdb_table_name(self, table_name):
        """
        Generate DuckDB table name using dbt naming conventions
        
        Parameters:
        - table_name: base table name
        
        Returns:
        - Fully qualified table name for DuckDB (schema.table_name)
        """
        duckdb_config = self.get_duckdb_config()
        
        # Get schema from DuckDB config, fallback to dbt project schema naming
        duckdb_schema = duckdb_config.get('schema')
        
        if not duckdb_schema:
            # Use dbt naming convention: try to get custom schema or use default
            try:
                custom_schema = self.dbt_helper._get_custom_schema(table_name)
                duckdb_schema = custom_schema
            except:
                # Fallback to target schema or default schema
                duckdb_schema = self.dbt_helper.profile_config.get('schema', 'main')
        
        # Return schema.table_name format
        return f"{duckdb_schema}.{table_name}"
    
    def extract_ref_table_name(self, sql_statement):
        """
        Extract table name from dbt ref() function in SQL statement
        
        Parameters:
        - sql_statement: SQL statement containing ref() calls
        
        Returns:
        - First table name found in ref() function, or None if not found
        """
        # Pattern to match ref('table_name') or ref("table_name")
        ref_pattern = r"ref\s*\(\s*['\"]([^'\"]+)['\"]"
        
        matches = re.findall(ref_pattern, sql_statement, re.IGNORECASE)
        
        if matches:
            # Return the first ref table name found
            # If multiple refs exist, user should be explicit about which one to export
            if len(matches) > 1:
                print(f"{self.prStyle.YELLOW}Multiple ref() functions found: {matches}. Using the first one: '{matches[0]}' for DuckDB export.{self.prStyle.RESET}")
            return matches[0]
        
        return None
    
    def export_to_duckdb(self, df, table_name, if_exists='replace'):
        """
        Export DataFrame to DuckDB using dbt naming conventions
        
        Parameters:
        - df: pandas DataFrame to export
        - table_name: base table name (will be prefixed with schema)
        - if_exists: 'replace' (default) or 'append'
        """
        if df is None or df.empty:
            print(f"{self.prStyle.RED}DataFrame is empty or None. Nothing to export.{self.prStyle.RESET}")
            return
            
        duckdb_config = self.get_duckdb_config()
        
        if not duckdb_config:
            print(f"{self.prStyle.RED}DuckDB configuration not found in dbt profiles. Please add duckdb configuration.{self.prStyle.RESET}")
            return
            
        # Get database path from config
        db_path = duckdb_config.get('path', duckdb_config.get('database', ':memory:'))
        
        # Get fully qualified table name with schema
        full_table_name = self.get_duckdb_table_name(table_name)
        schema_name, table_only = full_table_name.split('.', 1)
        
        try:
            # Connect to DuckDB
            conn = duckdb.connect(db_path)
            
            # Create schema if it doesn't exist
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            
            # Export DataFrame to DuckDB
            if if_exists == 'replace':
                conn.execute(f"DROP TABLE IF EXISTS {full_table_name}")
            
            # Register DataFrame as temporary table
            temp_table_name = f"temp_{table_only}_{int(time())}"
            conn.register(temp_table_name, df)
            
            # Create or insert into the target table
            if if_exists == 'replace':
                conn.execute(f"CREATE TABLE {full_table_name} AS SELECT * FROM {temp_table_name}")
            else:  # append
                # Check if table exists, create if not
                table_exists = conn.execute(f"""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_schema = '{schema_name}' AND table_name = '{table_only}'
                """).fetchone()[0] > 0
                
                if not table_exists:
                    conn.execute(f"CREATE TABLE {full_table_name} AS SELECT * FROM {temp_table_name}")
                else:
                    conn.execute(f"INSERT INTO {full_table_name} SELECT * FROM {temp_table_name}")
            
            conn.unregister(temp_table_name)
            conn.close()
            
            action = "replaced" if if_exists == 'replace' else "appended to"
            print(f"{self.prStyle.GREEN}DataFrame successfully {action} table '{full_table_name}' in DuckDB at: {db_path}{self.prStyle.RESET}")
            
        except Exception as e:
            print(f"{self.prStyle.RED}Error exporting to DuckDB: {str(e)}{self.prStyle.RESET}")
            # Clean up in case of error
            try:
                if 'conn' in locals():
                    if 'temp_table_name' in locals():
                        conn.unregister(temp_table_name)
                    conn.close()
            except:
                pass


def export_dataframe_to_duckdb_with_profile(df, table_name, profile_name=None, target=None, adapter_name='snowflake', if_exists='replace'):
    """
    Standalone function to export any DataFrame to DuckDB using dbt profile configuration
    
    Parameters:
    - df: pandas DataFrame to export
    - table_name: name of the table in DuckDB
    - profile_name: dbt profile name (optional)
    - target: dbt target (optional)
    - adapter_name: dbt adapter name ('snowflake', 'athena', etc.)
    - if_exists: 'replace' (default) or 'append'
    
    Usage:
    export_dataframe_to_duckdb_with_profile(my_df, 'my_table')
    export_dataframe_to_duckdb_with_profile(my_df, 'my_table', if_exists='append')
    export_dataframe_to_duckdb_with_profile(my_df, 'my_table', adapter_name='athena')
    """
    from dbt_magics.dbt_helper import dbtHelper
    
    # Create a temporary dbt helper to access configuration
    class TempDbtHelper(dbtHelper):
        def __init__(self, adapter_name, profile_name, target):
            super().__init__(adapter_name=adapter_name, profile_name=profile_name, target=target)
    
    helper = TempDbtHelper(adapter_name, profile_name, target)
    duckdb_helper = DuckDBHelper(helper)
    duckdb_helper.export_to_duckdb(df, table_name, if_exists)
