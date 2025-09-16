import io
import os
from pathlib import Path

import boto3
import pandas as pd
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
class AthenaDataController(DataController):
    def __init__(self, target=None):
        dbth = dbtHelperAdapter(adapter_name='athena', target=target)
        session = boto3.Session(profile_name=dbth.profile_config['aws_profile_name'])
        self.client = session.client('athena')

        super().__init__(r"%%athena")

    """
    Implemented Abstract methods
    """

    def get_datasets(self, database):
        DatabaseList = self.list_databases(CatalogName=database) if database else []
        return [i['Name'] for i in DatabaseList]
    
    def get_projects(self):
        self.DataCatalogs = self.client.list_data_catalogs()['DataCatalogsSummary']
        return [i['CatalogName'] for i in self.DataCatalogs]

    
    def get_tables(self, database):
        if database:
            self.TableMetadataList = self.list_table_metadata(CatalogName=self.wg_project.value, DatabaseName=database)
            return [i['Name'] for i in self.TableMetadataList]
        else: 
            return []
    
    def get_columns(self, table):
        columns = []
        for i in self.TableMetadataList:
            if i['Name']==table:
                cols = [(i['Name'], i['Type']) for i in i['Columns']]
                # self.check_boxes = [f(f"{i['Name']} -- {i['Type']}") for i in i['Columns']]
                partition_columns = [(i['Name'], i['Type']+'(Part.)') for i in i.get('PartitionKeys', [])]
                columns = [(i['Name'], i['Type']) for i in i['Columns']] + partition_columns

        return columns

    """ 
    Additional methods 
    """

    def list_databases(self, CatalogName):
        response = self.client.list_databases(CatalogName=CatalogName, MaxResults=50)
        DatabaseList = response['DatabaseList']
        while 'NextToken' in response:
            response = self.client.list_databases(CatalogName=CatalogName, MaxResults=50, NextToken=response["NextToken"])
            DatabaseList += response['DatabaseList']        
        return DatabaseList

    def list_table_metadata(self, CatalogName, DatabaseName):
        response = self.client.list_table_metadata(
            CatalogName=CatalogName,
            DatabaseName=DatabaseName,
            MaxResults=50
        )
        TableMetadataList = response['TableMetadataList']
        while 'NextToken' in response:
            response = self.client.list_table_metadata(
                            CatalogName=CatalogName,
                            DatabaseName=DatabaseName,
                            MaxResults=50,
                            NextToken=response["NextToken"]
            )
            TableMetadataList += response['TableMetadataList']        
        return TableMetadataList


    def list_dataset_metadata(self, CatalogName, DatabaseName):
        response = self.client.list_table_metadata(
            CatalogName=CatalogName,
            DatabaseName=DatabaseName,
            MaxResults=50
        )
        TableMetadataList = response['TableMetadataList']
        while 'NextToken' in response:
            response = self.client.list_table_metadata(
                            CatalogName=CatalogName,
                            DatabaseName=DatabaseName,
                            MaxResults=50,
                            NextToken=response["NextToken"]
            )
            TableMetadataList += response['TableMetadataList']        
        return TableMetadataList    

class dbtHelperAdapter(dbtHelper):
    def __init__(self, adapter_name='athena', profile_name=None, target=None):
        super().__init__(adapter_name=adapter_name, profile_name=profile_name, target=target)
        self.duckdb_helper = DuckDBHelper(self)
        
    def source(self, schema, table):
        SOURCES, _ = self._sources_and_models()
        default_database = [i for i in map(self.profile_config.get, ['dbname', 'database', 'dataset']) if i][0]
        source = self._search_for_source_table(SOURCES, target_schema=schema, target_table=table, default_database=default_database)
        return '"{database}"."{schema}"."{table}"'.format(database=source['database'], schema=source['schema'], table=source['table'])

    def ref(self, table_name):
        custom_schema = self._get_custom_schema(table_name)
        default_schema = self.profile_config.get("schema")
        return (f'"{default_schema}_{custom_schema}"."{table_name}"', f'"{default_schema}"."{table_name}"')[self.target=='dev']

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

    def run_query(self, sql_statement, profile_name, schema, database, output_location, work_group):
        session = boto3.Session(profile_name=profile_name)
        client = session.client('athena')
        s3 = session.client('s3')

        ########### START QUERY ###########
        start_response = client.start_query_execution(
            QueryString=sql_statement,
            QueryExecutionContext={
                'Database': schema,
                'Catalog': database
            },
            ResultConfiguration={'OutputLocation': output_location},
            WorkGroup=work_group
        )

        ########### STATUS - WAIT FOR RESULTS ###########
        state = ""
        while state!='SUCCEEDED':
            status = client.get_query_execution(QueryExecutionId=start_response["QueryExecutionId"])
            state = status['QueryExecution']["Status"]["State"]
            TotalExecutionTimeInMillis = status["QueryExecution"]["Statistics"]["TotalExecutionTimeInMillis"]
            print(f"{TotalExecutionTimeInMillis/1000:3.3f} sec.", end="\r")
            if state=="FAILED":
                raise BaseException(f"SQL statement FAILED for AWS Profile '{profile_name}' & Catalog '{database}' & Database '{schema}'.\nSQL: {sql_statement}\n\n{status['QueryExecution']['Status']}")

        DataScannedInBytes = status["QueryExecution"]["Statistics"]["DataScannedInBytes"]*0.00000095367432
        PriceInDollar = (DataScannedInBytes*0.000085, 0.00085)[DataScannedInBytes<=10]
        print(f"{prStyle.GREEN}{TotalExecutionTimeInMillis/1000:.3f} sec. {prStyle.RESET}| {prStyle.MAGENTA}{DataScannedInBytes:.3f} MB scanned {prStyle.RESET}| {prStyle.RED}{PriceInDollar:3.5f} ${prStyle.RESET}")

        ########### DOWNLOAD RESULTS ###########
        try:
            s3_file_url = status["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
            file_location = s3_file_url.replace("s3://","").split("/")
            bucket, key = file_location[0], "/".join(file_location[1:])
            obj = s3.get_object(Bucket=bucket, Key=key)
            df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        except:
            print("Not a SELECT statement.")
            df = None
        return df


@magics_class
class SQLMagics(Magics):
    pd.set_option('display.max_columns', None)

    @line_cell_magic
    @magic_arguments.magic_arguments()
    @magic_arguments.argument('--n_output', '-n', default=5, help='Number of rows to display. Set to 0 to suppress output display.')
    @magic_arguments.argument('--dataframe', '-df', default="df", help='The variable to return the results in.')
    @magic_arguments.argument('--parser', '-p', action='store_true', help='Translate Jinja.')
    @magic_arguments.argument('--profile', default=None, help='')
    @magic_arguments.argument('--target', default='prod', help='')
    @magic_arguments.argument('--export_duckdb', '-ddb', action='store_true', help='Export DataFrame to DuckDB using table name from dbt ref().')
    @magic_arguments.argument('--duckdb_mode', '-mode', default='replace', choices=['replace', 'append'], help='DuckDB export mode: replace (default) or append.')
    def athena(self, line, cell=None):
        """
---------------------------------------------------------------------------
%%athena

SELECT * FROM {{ ref('table_in_dbt_project') }}
---------------------------------------------------------------------------

DuckDB Export Examples:

%%athena --export_duckdb
SELECT * FROM {{ ref('my_table') }}

%%athena --export_duckdb --duckdb_mode append  
SELECT * FROM {{ ref('my_table') }}

Output Control:

%%athena -n 10
SELECT * FROM {{ ref('my_model') }}  # Shows first 10 rows

%%athena -n 0
SELECT * FROM {{ ref('my_model') }}  # No output displayed (silent execution)

Note:
- Use -n 0 to suppress output display while still storing in dataframe variable
---------------------------------------------------------------------------
---------------------------------------------------------------------------
asdf = {'a':'value','b':'value2'}
test_func = lambda x: x+1

%%athena -p 
{{test_func(41)}}
{{asdf}}
{{asdf.b, asdf.a}}
SELECT * FROM {{ ref('table_in_dbt_project') }}
---------------------------------------------------------------------------
"""
        if cell is None:
            target = line.split('--target ')[-1] if '--target' in line else None
            dc = AthenaDataController(target=target)
            return dc()
        else:        
            args = magic_arguments.parse_argstring(self.athena, line)
            self.dbt_helper = dbtHelperAdapter(profile_name=args.profile, target=args.target)
            env = Environment()
            def ipython(variable):
                try: result = get_ipython().ev(variable)
                except: result = False
                return result

            kwargs = {i:ipython(i) for i in meta.find_undeclared_variables(env.parse(cell)) if ipython(i)}

            macros_txt = self.dbt_helper.macros_txt
            jinja_statement = macros_txt+cell
            statement = Template(jinja_statement).render(source=self.dbt_helper.source, ref=self.dbt_helper.ref, var=self.dbt_helper.var, **kwargs).strip()

            if args.parser:
                print(statement)
            else:
                # Check DuckDB availability before executing query if export is requested
                if args.export_duckdb:
                    if not self.dbt_helper.check_duckdb_availability():
                        print(f"{prStyle.RED}Aborting query execution due to DuckDB unavailability.{prStyle.RESET}")
                        return None
                
                #--------------------------------------------- Start
                df = self.dbt_helper.run_query(
                    sql_statement=statement,
                    profile_name=self.dbt_helper.profile_config.get("aws_profile_name"),
                    schema=self.dbt_helper.profile_config.get("schema"),
                    database=self.dbt_helper.profile_config.get("database"),
                    output_location=self.dbt_helper.profile_config.get("OutputLocation"),
                    work_group=[i for i in map(self.dbt_helper.profile_config.get, ['work_group', 'WorkGroup']) if i][0]
                    )  
                #--------------------------------------------- End

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

def export_dataframe_to_duckdb_athena(df, table_name, profile_name=None, target=None, if_exists='replace'):
    """
    Standalone function to export any DataFrame to DuckDB using dbt profile configuration (Athena version)
    
    Parameters:
    - df: pandas DataFrame to export
    - table_name: name of the table in DuckDB
    - profile_name: dbt profile name (optional)
    - target: dbt target (optional) 
    - if_exists: 'replace' (default) or 'append'
    
    Usage:
    export_dataframe_to_duckdb_athena(my_df, 'my_table')
    export_dataframe_to_duckdb_athena(my_df, 'my_table', if_exists='append')
    """
    helper = dbtHelperAdapter('athena', profile_name, target)
    helper.export_to_duckdb(df, table_name, if_exists)

def load_ipython_extension(ipython):
    js = """IPython.CodeCell.options_default.highlight_modes['magic_sql'] = {'reg':[/^%%(athena)/]};
    IPython.notebook.events.one('kernel_ready.Kernel', function(){
        IPython.notebook.get_cells().map(function(cell){
            if (cell.cell_type == 'code'){ cell.auto_highlight(); } }) ;
    });
    """
    display.display_javascript(js, raw=True)
    ipython.register_magics(SQLMagics)