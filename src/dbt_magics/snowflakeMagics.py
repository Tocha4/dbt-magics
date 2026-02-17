import io
import os
from pathlib import Path
from time import time


#import snowflake.connector 
from snowflake.snowpark import Session
from snowflake.core import Root
from snowflake.core.database import Database
import pandas as pd
import duckdb
from IPython import get_ipython
from IPython.core import display, magic_arguments
from IPython.core.magic import Magics, line_cell_magic, magics_class
from jinja2 import Environment, Template, meta

from dbt_magics.datacontroller import DataController, prStyle
from dbt_magics.dbt_helper import dbtHelper
from dbt_magics.duckdb_helper import DuckDBHelper

"""
Implementation of the AthenaDataContoller class.
Implement abstract methods from DataController class.
"""
class SnowflakeDataController(DataController):
    def __init__(self, target=None, profile_name=None):
        self.dbt_helper = dbtHelperAdapter(adapter_name= 'snowflake', profile_name=profile_name, target=target)
        connection_parameters= dict(user = self.dbt_helper.profile_config.get("user"),
                               authenticator = self.dbt_helper.profile_config.get("authenticator"),
                              role = self.dbt_helper.profile_config.get("role"),
                              account = self.dbt_helper.profile_config.get("account"),
                              warehouse = self.dbt_helper.profile_config.get("warehouse"),
                              database = self.dbt_helper.profile_config.get("database"),
                              schema = self.dbt_helper.profile_config.get("schema")
                             )
        self.root = self.get_metadata(connection_parameters)
        super().__init__(r"%%snowflake")

    """
    Implemented Abstract methods
    """
 

    
    def get_projects(self):
        databases = self.root.databases.iter()
        return [database.name for database in databases]
    
        
    def get_datasets(self, database):
        schema_list = self.root.databases[database].schemas.iter()
        return [schema_obj.name for schema_obj in schema_list]
        
        
    def get_tables(self,schema):
        tables = self.root.databases[self.wg_project.value].schemas[schema].tables.iter()
        views = self.root.databases[self.wg_project.value].schemas[schema].views.iter()
        return [table.name+' (t)' for table in tables] + [view.name+' (v)' for view in views]
        
    def get_columns(self, table):
        table_name, table_type = table.split(' (')
        if table_type[0] == 't':
            table = self.root.databases[self.wg_project.value].schemas[self.wg_database.value].tables[table_name].fetch()
        elif table_type[0] == 'v':
            table = self.root.databases[self.wg_project.value].schemas[self.wg_database.value].views[table_name].fetch()
        return [(col.name, col.datatype) for col in table.columns]
    
        
    def get_metadata(self,connection_parameters):
        return self.dbt_helper.snowflake_connection_query_execution(connection_parameters)


class dbtHelperAdapter(dbtHelper):
    def __init__(self, adapter_name='snowflake', profile_name= None, target=None):
        super().__init__(adapter_name=adapter_name, profile_name=profile_name, target=target)
        self.duckdb_helper = DuckDBHelper(self)
    
        
    def source(self, schema, table):
        SOURCES, _ = self._sources_and_models()
        default_database = [i for i in map(self.profile_config.get, ['dbname', 'database', 'dataset']) if i][0]
        source = self._search_for_source_table(SOURCES, target_schema=schema, target_table=table, default_database=default_database)
        return '{database}.{schema}.{table}'.format(database=source['database'], schema=source['schema'], table=source['table'])


    def ref(self, table_name):
        custom_schema = self._get_custom_schema(table_name)
        #print(f'custom_schema: value {custom_schema}')
        return (f'{custom_schema}.{table_name}')
    
    # DuckDB methods - delegated to DuckDBHelper
    def get_duckdb_config(self):
        """Get DuckDB configuration from dbt profiles"""
        return self.duckdb_helper.get_duckdb_config()
    
    def check_duckdb_availability(self):
        """Check if DuckDB database is available and not locked"""
        return self.duckdb_helper.check_duckdb_availability()
    
    def get_duckdb_table_name(self, table_name):
        """Generate DuckDB table name using dbt naming conventions"""
        return self.duckdb_helper.get_duckdb_table_name(table_name)
    
    def extract_ref_table_name(self, sql_statement):
        """Extract table name from dbt ref() function in SQL statement"""
        return self.duckdb_helper.extract_ref_table_name(sql_statement)
    
    def export_to_duckdb(self, df, table_name, if_exists='replace'):
        """Export DataFrame to DuckDB using dbt naming conventions"""
        return self.duckdb_helper.export_to_duckdb(df, table_name, if_exists)
    
    
    
    def snowflake_connection_query_execution(self, connection_parameters,statement=None):
        session = Session.builder.configs(connection_parameters).create()
        root = Root(session)
        if statement==None:
            return root
        else:
            
            #------------------------------ start ----------------------------
            # 1. Run query
            try:
                start_time = time()  # Start the timer
                df = session.sql(statement).to_pandas()
                execution_time_seconds = time() - start_time  # Measure execution time
                print(f"{prStyle.GREEN}EXECUTION_TIME {execution_time_seconds:.3f} seconds" )

            except Exception as e:
                print(f"{prStyle.RED}Not a SELECT statement.\n{e}")
                df = None
            return df
            #----------------------------- end ------------------------------


            

@magics_class
class SnowflakeSQLMagics(Magics):
    pd.set_option('display.max_columns', None)

    @line_cell_magic
    @magic_arguments.magic_arguments()
    @magic_arguments.argument('--n_output', '-n', default=5, help='Number of rows to display. Set to 0 to suppress output display.')
    @magic_arguments.argument('--dataframe', '-df', default="df", help='The variable to return the results in.')
    @magic_arguments.argument('--parser', '-p', action='store_true', help='Translate Jinja.')
    @magic_arguments.argument('--params', default='', help='Add additional Jinja params.')
    @magic_arguments.argument('--profile', default='dbt_snowflake_dwh', help='')
    @magic_arguments.argument('--target', default='dev', help='')
    @magic_arguments.argument('--export_duckdb', '-ddb', action='store_true', help='Export DataFrame to DuckDB using table name from dbt ref().')
    @magic_arguments.argument('--duckdb_mode', '-mode', default='replace', choices=['replace', 'append'], help='DuckDB export mode: replace (default) or append.')
    def snowflake(self, line, cell=None):
        """
        ---------------------------------------------------------------------------
        %%snowflake

        SELECT * FROM {{ ref('table_in_dbt_project') }}
        ---------------------------------------------------------------------------
        ---------------------------------------------------------------------------

        test_func = lambda x: x+1

        %%snowflake -p 
        {{test_func(41)}}
        SELECT * FROM {{ ref('table_in_dbt_project') }}
        ---------------------------------------------------------------------------
        ---------------------------------------------------------------------------
        Export to DuckDB:
        
        %%snowflake --export_duckdb
        SELECT * FROM {{ ref('my_model') }}
        
        %%snowflake --export_duckdb --duckdb_mode append
        SELECT * FROM {{ ref('my_model') }}
        
        Output Control:
        
        %%snowflake -n 10
        SELECT * FROM {{ ref('my_model') }}  # Shows first 10 rows
        
        %%snowflake -n 0
        SELECT * FROM {{ ref('my_model') }}  # No output displayed (silent execution)
        
        Note: 
        - Table name is automatically extracted from ref() function
        - DuckDB lock status is checked before query execution
        - Query is skipped if DuckDB database is locked
        - Use -n 0 to suppress output display while still storing in dataframe variable
        ---------------------------------------------------------------------------
        """
        if cell == None:
            target = line.split('--target ')[-1].split()[0] if '--target' in line else None
            profile_name = line.split('--profile ')[-1].split()[0] if '--profile' in line else None
            dc = SnowflakeDataController(target=target, profile_name=profile_name)
            return dc()

        args = magic_arguments.parse_argstring(self.snowflake, line)
        #print(args)
        self.dbt_helper = dbtHelperAdapter('snowflake', args.profile, args.target) 

        env = Environment()
        def ipython(variable):
            try: result = get_ipython().ev(variable)
            except: result = False
            return result

        kwargs = {i:ipython(i) for i in meta.find_undeclared_variables(env.parse(cell)) if ipython(i)}

        macros_txt = self.dbt_helper.macros_txt
        jinja_statement = macros_txt+cell
        statement = Template(jinja_statement).render(source=self.dbt_helper.source, ref=self.dbt_helper.ref, var=self.dbt_helper.var, **kwargs).strip()
        #print(f'statement of query {statement}')

        
        if args.parser:
            print(statement)
        else:
            # Check DuckDB availability before executing query if export is requested
            if args.export_duckdb:
                if not self.dbt_helper.check_duckdb_availability():
                    print(f"{prStyle.RED}Aborting query execution due to DuckDB unavailability.{prStyle.RESET}")
                    return None
            
            connection_parameters= dict(user = self.dbt_helper.profile_config.get("user"),
                               authenticator = self.dbt_helper.profile_config.get("authenticator"),
                              role = self.dbt_helper.profile_config.get("role"),
                              account = self.dbt_helper.profile_config.get("account"),
                              warehouse = self.dbt_helper.profile_config.get("warehouse"),
                              database = self.dbt_helper.profile_config.get("database"),
                              schema = self.dbt_helper.profile_config.get("schema")
                             )
            
            df = self.dbt_helper.snowflake_connection_query_execution(connection_parameters,statement)

            self.shell.user_ns[args.dataframe] = df
            
            # Export to DuckDB if requested
            if args.export_duckdb and df is not None:
                # Extract table name from ref() in the original cell content
                table_name = self.dbt_helper.extract_ref_table_name(cell)
                
                if table_name:
                    self.dbt_helper.export_to_duckdb(df, table_name, args.duckdb_mode)
                else:
                    print(f"{prStyle.RED}No ref() function found in SQL. Please use ref('table_name') to specify the table for DuckDB export.{prStyle.RESET}")
            
            # Handle n_output behavior: if 0, don't display dataframe
            if int(args.n_output) == 0:
                return None
            else:
                df = df.head(int(args.n_output)) if type(df)==pd.DataFrame else None
                return df 
        
def export_dataframe_to_duckdb(df, table_name, profile_name=None, target=None, if_exists='replace'):
    """
    Standalone function to export any DataFrame to DuckDB using dbt profile configuration
    
    Parameters:
    - df: pandas DataFrame to export
    - table_name: name of the table in DuckDB
    - profile_name: dbt profile name (optional)
    - target: dbt target (optional) 
    - if_exists: 'replace' (default) or 'append'
    
    Usage:
    export_dataframe_to_duckdb(my_df, 'my_table')
    export_dataframe_to_duckdb(my_df, 'my_table', if_exists='append')
    """
    helper = dbtHelperAdapter('snowflake', profile_name, target)
    helper.export_to_duckdb(df, table_name, if_exists)


def load_ipython_extension(ipython):
    js = """IPython.CodeCell.options_default.highlight_modes['magic_sql'] = {'reg':[/^%%(snowflake)/]};
    IPython.notebook.events.one('kernel_ready.Kernel', function(){
        IPython.notebook.get_cells().map(function(cell){
            if (cell.cell_type == 'code'){ cell.auto_highlight(); } }) ;
    });
    """
    display.display_javascript(js, raw=True)
    ipython.register_magics(SnowflakeSQLMagics)