import io
import os
from pathlib import Path
from time import time


#import snowflake.connector 
from snowflake.snowpark import Session
from snowflake.core import Root
from snowflake.core.database import Database
import pandas as pd
from IPython import get_ipython
from IPython.core import display, magic_arguments
from IPython.core.magic import Magics, line_cell_magic, magics_class
from jinja2 import Environment, Template, meta

from dbt_magics.datacontroller import DataController, prStyle
from dbt_magics.dbt_helper import dbtHelper

"""
Implementation of the AthenaDataContoller class.
Implement abstract methods from DataController class.
"""
class SnowflakeDataController(DataController):
    def __init__(self, target=None):
        self.dbt_helper = dbtHelperAdapter(adapter_name= 'snowflake', target=target)
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
        return [table.name for table in tables]
        
    def get_columns(self, table):
        table = self.root.databases[self.wg_project.value].schemas[self.wg_database.value].tables[table].fetch()
        return [(col.name, col.datatype) for col in table.columns]
    

    
    
    def get_metadata(self,connection_parameters):
        return self.dbt_helper.snowflake_connection_query_execution(connection_parameters)


class dbtHelperAdapter(dbtHelper):
    def __init__(self, adapter_name='snowflake', profile_name= None, target=None):
        super().__init__(adapter_name=adapter_name, profile_name=profile_name, target=target)
    
        
    def source(self, schema, table):
        SOURCES, _ = self._sources_and_models()
        default_database = [i for i in map(self.profile_config.get, ['dbname', 'database', 'dataset']) if i][0]
        source = self._search_for_source_table(SOURCES, target_schema=schema, target_table=table, default_database=default_database)
        return '{database}.{schema}.{table}'.format(database=source['database'], schema=source['schema'], table=source['table'])


    def ref(self, table_name):
        custom_schema = self._get_custom_schema(table_name)
        #print(f'custom_schema: value {custom_schema}')
        return (f'{custom_schema}.{table_name}')
    
    def snowflake_connection_query_execution(self, connection_parameters,statement=None):
        session = Session.builder.configs(connection_parameters).create()
        root = Root(session)
        if statement==None:
            return root
        else:
            
            #------------------------------ start ----------------------------
            # 1. Run query
            try:
                df = session.sql(statement).to_pandas()

                # Get the last query ID
                query_id = session.sql("SELECT LAST_QUERY_ID()").collect()[0][0]
                #print(query_id)

                # Query the INFORMATION_SCHEMA.QUERY_HISTORY table function for perfoemance metrics
                history = session.sql(f"""
                    SELECT 
                        total_elapsed_time/1000 AS total_elapsed_time,
                        execution_time /1000 as execution_time
                        
                    FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
                    WHERE query_id = '{query_id}'
                """).collect()

                if history:
                    print(f"{prStyle.GREEN} TOTAL ELAPSED TIME : {history[0]['TOTAL_ELAPSED_TIME']:.3f} sec {prStyle.RESET}| {prStyle.RESET}| EXECUTION_TIME {prStyle.RED}{history[0]['EXECUTION_TIME']:.3f} sec ${prStyle.RESET} " )
                    
                else:
                    print("Query details not found in recent history.")

                
            except Exception as e:
                print(f"{prStyle.RED}Not a SELECT statement.\n{e}")
                df = None
            return df
            #----------------------------- end ------------------------------


            

@magics_class
class SQLMagics(Magics):
    pd.set_option('display.max_columns', None)

    @line_cell_magic
    @magic_arguments.magic_arguments()
    @magic_arguments.argument('--n_output', '-n', default=5, help='')
    @magic_arguments.argument('--dataframe', '-df', default="df", help='The variable to return the results in.')
    @magic_arguments.argument('--parser', '-p', action='store_true', help='Translate Jinja.')
    @magic_arguments.argument('--params', default='', help='Add additional Jinja params.')
    @magic_arguments.argument('--profile', default='dbt_snowflake_dwh', help='')
    @magic_arguments.argument('--target', default='dev', help='')
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
        """
        if cell == None:
            target = line.split('--target ')[-1] if '--target' in line else None
            dc = SnowflakeDataController(target=target)
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
            df = df.head(int(args.n_output)) if type(df)==pd.DataFrame else None
            return df 
def load_ipython_extension(ipython):
    js = """IPython.CodeCell.options_default.highlight_modes['magic_sql'] = {'reg':[/^%%(snowflake)/]};
    IPython.notebook.events.one('kernel_ready.Kernel', function(){
        IPython.notebook.get_cells().map(function(cell){
            if (cell.cell_type == 'code'){ cell.auto_highlight(); } }) ;
    });
    """
    display.display_javascript(js, raw=True)
    ipython.register_magics(SQLMagics)