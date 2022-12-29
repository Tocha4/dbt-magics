from time import time
import pandas as pd
from jinja2 import Template
import io
import os
from pathlib import Path

from IPython.core import magic_arguments, display
from IPython.core.magic import line_magic, cell_magic, line_cell_magic, Magics, magics_class
import ipywidgets as widgets

import boto3

from dbt_helper import dbtHelper, prStyle


class Adapter(dbtHelper):
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
asdf = str({'a':'value','b':'value2'}).replace(' ','')

%%athena --params $asdf -p 

{{params.b, params.a}}
SELECT * FROM {{ ref('table_in_dbt_project') }}
---------------------------------------------------------------------------
"""
        if cell is None:
            dc = DataController()
            return dc()
        else:        
            args = magic_arguments.parse_argstring(self.athena, line)
            self.dbt_helper = Adapter(args.profile, args.target)
            if args.params!='':
                params_var = ' {%-set params='+args.params+' -%}'
            else:
                params_var = ''

            macros_txt = self.dbt_helper.macros_txt
            jinja_statement = params_var+macros_txt+cell
            statement = Template(jinja_statement).render(source=self.dbt_helper.source, ref=self.dbt_helper.ref).strip()

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





class prStyle():
    BLACK = '\033[30m'
    RED = '\033[31m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    BLUE = '\033[34m'
    MAGENTA = '\033[35m'
    CYAN = '\033[36m'
    WHITE = '\033[37m'
    UNDERLINE = '\033[4m'
    RESET = '\033[0m'
    
class CB(widgets.HBox):
    def __init__(self, name, dtype):
        super().__init__()
        layout = widgets.Layout(max_width='450px')
        self.add_class('aen_cb_style')
        self.layout = layout
        self.check = widgets.Checkbox(value=True, description=name, indent=False)
        self.lab = widgets.Label(dtype.upper())
        dtype = dtype if "int" not in dtype else "int"
        dtype = 'decimal' if "decimal" in dtype else dtype
        dtype = 'string' if "varchar" in dtype else dtype
        self.lab.add_class(f'aen_cb_lab_style_{dtype}')
        self.children = (self.check, self.lab)

class DataController():
    def __init__(self):
        self.client = boto3.client('athena', region_name='eu-central-1')
        self.DataCatalogs = self.client.list_data_catalogs()['DataCatalogsSummary']
        self.catalog_names = [i['CatalogName'] for i in self.DataCatalogs]
        
        #----------- WIDGETS -----------#
        self.wg_catalog = widgets.Dropdown(options=self.catalog_names, value=None)
        self.wg_database = widgets.Dropdown(options=[])
        self.wg_search_table = widgets.Text(placeholder='<table>')
        self.wg_table = widgets.Dropdown(options=[])
        self.wg_style = widgets.HTML('''<style>
            .widget-text input[type="text"] {max-width:650px; background-color:#89d5c7; border-radius:7px; font-size: 13pt}
            div.widget-dropdown select {max-width:650px; background-color:#73c6e3; border-radius:7px; font-size: 13pt} 
            .wg_base_dropdowns {max-width:950px;}
            .p-Accordion-child {border-radius:7px; max-width:650px;}
            .widget-button {border-radius:7px; border: 2px solid green; font-family: "Monaco", monospace;}
            .aen_cb_style {border-radius:7px; background-color:#c8d8dd; border: 1px solid black;}
            .widget-label-basic span {font-size: 13pt;}
            .p-Widget.jupyter-widgets.widget-label.aen_cb_lab_style_string {background-color:#89d5c7; font-size: 19pt; border-radius:7px; text-align: center !important; color: black !important;}
            .p-Widget.jupyter-widgets.widget-label.aen_cb_lab_style_date {background-color:#7d91e7; font-size: 19pt; border-radius:7px; text-align: center !important; color: black !important;}
            .p-Widget.jupyter-widgets.widget-label.aen_cb_lab_style_int {background-color:#c0d539; font-size: 19pt; border-radius:7px; text-align: center !important; color: black !important;}
            .p-Widget.jupyter-widgets.widget-label.aen_cb_lab_style_float {background-color:#e1a25a; font-size: 19pt; border-radius:7px; text-align: center !important; color: black !important;}
            .p-Widget.jupyter-widgets.widget-label.aen_cb_lab_style_boolean {background-color:#f59cf4; font-size: 19pt; border-radius:7px; text-align: center !important; color: black !important;}
            .p-Widget.jupyter-widgets.widget-label.aen_cb_lab_style_timestamp {background-color:#adbaf1; font-size: 19pt; border-radius:7px; text-align: center !important; color: black !important;}
            .p-Widget.jupyter-widgets.widget-label.aen_cb_lab_style_decimal {background-color:#e18972; font-size: 19pt; border-radius:7px; text-align: center !important; color: black !important;}
            .p-Widget.jupyter-widgets.widget-label.aen_cb_lab_style_double {background-color:#e1a25a; font-size: 19pt; border-radius:7px; text-align: center !important; color: black !important;}
            </style>''')
        self.wg_base_dropdowns = widgets.VBox(children=[self.wg_catalog, self.wg_database, self.wg_search_table, self.wg_table])
        self.wg_base_dropdowns.add_class('wg_base_dropdowns')
        self.all_columns = widgets.Button(description="All Columns")
        self.wg_columns_container = widgets.VBox(children=[self.all_columns])
        self.wg_column = widgets.Accordion(children=[self.wg_columns_container], selected_index=None)    
        self.select_sql = widgets.Button(description="SELECT")
        self.output = widgets.Output()
        self.output.add_class("anton-enns")
        self.wg_output = widgets.Accordion(children=[self.output])
        self.pannel = widgets.HBox(
                children=[self.wg_base_dropdowns, self.wg_column, self.select_sql, self.wg_output],
                box_style='info' # one of 'success', 'info', 'warning' or 'danger', or ''
            )  
        
        #----------- WIDGET ACTIONS -----------#
        self.wg_catalog.observe(self.get_databases, names='value')
        self.wg_database.observe(self.get_tables, names='value')
        self.wg_table.observe(self.get_colums, names='value')
        self.select_sql.on_click(self.on_button_clicked)
        self.wg_search_table.observe(self.search_tables, names='value')
        self.all_columns.on_click(self.all_columns_handler)
        
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
    
    def list_databases(self, CatalogName):
        response = self.client.list_databases(CatalogName=CatalogName, MaxResults=50)
        DatabaseList = response['DatabaseList']
        while 'NextToken' in response:
            response = self.client.list_databases(CatalogName=CatalogName, MaxResults=50, NextToken=response["NextToken"])
            DatabaseList += response['DatabaseList']        
        return DatabaseList

    def get_databases(self, catalog):
        DatabaseList = self.list_databases(CatalogName=catalog['new'])
        self.wg_database.index = None
        self.wg_database.options = tuple([i['Name'] for i in DatabaseList])

    def get_tables(self, database):
        self.TableMetadataList = self.list_table_metadata(CatalogName=self.wg_catalog.value, DatabaseName=database['new'])
        self.wg_table.index = None
        self.wg_table.options = tuple([i['Name'] for i in self.TableMetadataList])

    def get_colums(self, table):
        f = lambda name, dtype: CB(name, dtype)
        for i in self.TableMetadataList:
            if i['Name']==table['new']:
                cols = [(i['Name'], i['Type']) for i in i['Columns']]
                # self.check_boxes = [f(f"{i['Name']} -- {i['Type']}") for i in i['Columns']]
                self.check_boxes = [f(i['Name'], i['Type']) for i in i['Columns']]
                self.wg_columns_container.children = [self.all_columns]+self.check_boxes
    
    def on_button_clicked(self, b):
        f = lambda name, dtype:  f'{name} {prStyle.BLUE}-- {dtype}{prStyle.RESET}'
        with self.output:
            self.output.clear_output()
            cols = "\n    , ".join([f(i.check.description, i.lab.value) for i in self.check_boxes if i.check.value])
            print(f'{prStyle.MAGENTA}SELECT{prStyle.RESET}\n    {cols} \n{prStyle.MAGENTA}FROM{prStyle.RESET} {prStyle.GREEN}"{self.wg_catalog.value}"."{self.wg_database.value}"."{self.wg_table.value}"{prStyle.RESET}')

    def search_tables(self, observation):
        self.wg_table.index = None
        self.wg_table.options = tuple([i['Name'] for i in self.TableMetadataList if observation['new'] in i['Name']])

    def all_columns_handler(self, observation):
        if all([i.check.value for i in self.check_boxes]):
            for box in self.check_boxes:
                box.check.value = False
        else:
            for box in self.check_boxes:
                box.check.value = True
        # self.wg_column.children = [widgets.VBox([self.all_columns]+self.check_boxes)]
    
    def __call__(self):
        return self.pannel