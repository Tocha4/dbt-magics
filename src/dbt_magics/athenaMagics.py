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

"""
Implementation of the AthenaDataContoller class.
Implement abstract methods from DataController class.
"""
class AthenaDataController(DataController):
    def __init__(self):
        self.client = boto3.client('athena', region_name='eu-central-1')

        super().__init__(r"%%athena")

    """
    Implemented Abstract methods
    """

    def get_datasets(self, database):
        DatabaseList = self.list_databases(CatalogName=database)
        return [i['Name'] for i in DatabaseList]
    
    def get_projects(self):
        self.DataCatalogs = self.client.list_data_catalogs()['DataCatalogsSummary']
        return [i['CatalogName'] for i in self.DataCatalogs]

    
    def get_tables(self, database):
        self.TableMetadataList = self.list_table_metadata(CatalogName=self.wg_project.value, DatabaseName=database)
        return [i['Name'] for i in self.TableMetadataList]
    
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
    def __init__(self, profile_name="dbt_athena_dwh", target='prod'):
        super().__init__(profile_name=profile_name, target=target)
        self.DEFAULT_DBT_FOLDER = os.path.join(Path().home(), "projects", "data-aws", "dbt_athena_dwh")
        
    def source(self, schema, table):
        SOURCES, _ = self._sources_and_models()
        default_database = [i for i in map(self.profile_config.get, ['dbname', 'database', 'dataset']) if i][0]
        source = [
            {
                "database":item.get('database', default_database),
                "schema": item.get("name"),
                "table": table if table in [i["name"] for i in item.get("tables")] else "<! TABLE NOT FOUND in dbt project !>"

            } for item in SOURCES if item.get("name")==schema
        ]
        source = self._len_check(source, table_name=table)

        if source["database"]:
            results = '"{database}"."{schema}"."{table}"'.format(database=source['database'], schema=source['schema'], table=source['table'])
        else:
            results = '"{schema}"."{table}"'.format(schema=source['schema'], table=source['table'])
        return results

    def ref(self, table_name):
        custom_schema = self._get_custom_schema(table_name)
        default_schema = self.profile_config.get("schema")
        return (f'"{default_schema}_{custom_schema}"."{table_name}"', f'"{default_schema}"."{table_name}"')[self.target=='dev']

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
        PriceInDollar = (DataScannedInBytes*0.000005, 0.00005)[DataScannedInBytes<=10]
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
    @magic_arguments.argument('--n_output', '-n', default=5, help='')
    @magic_arguments.argument('--dataframe', '-df', default="df", help='The variable to return the results in.')
    @magic_arguments.argument('--parser', '-p', action='store_true', help='Translate Jinja.')
    @magic_arguments.argument('--params', default='', help='Add additional Jinja params.')
    @magic_arguments.argument('--profile', default='dbt_athena_dwh', help='')
    @magic_arguments.argument('--target', default='prod', help='')
    def athena(self, line, cell=None):
        """
---------------------------------------------------------------------------
%%athena

SELECT * FROM {{ ref('table_in_dbt_project') }}
---------------------------------------------------------------------------
---------------------------------------------------------------------------
asdf = {'a':'value','b':'value2'}
test_func = lambda x: x+1

%%athena --params $asdf -p 
{{test_func(41)}}
{{params}}
{{params.b, params.a}}
SELECT * FROM {{ ref('table_in_dbt_project') }}
---------------------------------------------------------------------------
"""
        if cell is None:
            dc = AthenaDataController()
            return dc()
        else:        
            args = magic_arguments.parse_argstring(self.athena, line)
            self.dbt_helper = dbtHelperAdapter(args.profile, args.target)
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
                df = df.head(int(args.n_output)) if type(df)==pd.DataFrame else None
                return df

def load_ipython_extension(ipython):
    js = """IPython.CodeCell.options_default.highlight_modes['magic_sql'] = {'reg':[/^%%(athena)/]};
    IPython.notebook.events.one('kernel_ready.Kernel', function(){
        IPython.notebook.get_cells().map(function(cell){
            if (cell.cell_type == 'code'){ cell.auto_highlight(); } }) ;
    });
    """
    display.display_javascript(js, raw=True)
    ipython.register_magics(SQLMagics)