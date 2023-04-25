import os
from pathlib import Path
from time import time

import ipywidgets as widgets
import pandas as pd
from google.cloud import bigquery
from IPython.core import display, magic_arguments
from IPython.core.magic import Magics, line_cell_magic, magics_class
from jinja2 import Template

from dbt_magics.athenaMagics import CB, prStyle
from dbt_magics.dbt_helper import dbtHelper


class Adapter(dbtHelper):
    def __init__(self, profile_name="dwh_bigquery", target='prod', default_dbt_folder=os.path.join(Path().home(), "documents", "data-aws", "dwh_bigquery")):
        super().__init__(profile_name=profile_name, target=target, default_dbt_folder=default_dbt_folder)
        
    def source(self, schema_name, table):
        SOURCES, _ = self._sources_and_models()
        default_database = [i for i in map(self.profile_config.get, ['dbname', 'database', 'dataset']) if i][0]
        source = [
            {
                "database":item.get('database', default_database),
                "project": item.get('project', self.profile_config.get('project')),
                "table": table if table in [i["name"] for i in item.get("tables")] else "<! TABLE NOT FOUND in dbt project !>"
            } for item in SOURCES if item.get("name")==schema_name
        ]
        source = self._len_check(source, table_name=table)
        results = '`{project}`.`{database}`.`{table}`'.format(database=source['database'], project=source['project'], table=source['table'])
        return results

    def ref(self, table_name):
        custom_schema = self._get_custom_schema(table_name)
        default_schema = self.profile_config.get("dataset")
        default_project = self.profile_config.get("project")
        return f'`{default_project}`.`{custom_schema}`.`{table_name}`'


@magics_class
class SQLMagics(Magics):
    pd.set_option('display.max_columns', None)

    @line_cell_magic
    @magic_arguments.magic_arguments()
    @magic_arguments.argument('--n_output', '-n', default=5, help='')
    @magic_arguments.argument('--dataframe', '-df', default="df", help='The variable to return the results in.')
    @magic_arguments.argument('--parser', '-p', action='store_true', help='Translate Jinja.')
    @magic_arguments.argument('--params', default='', help='Add additional Jinja params.')
    @magic_arguments.argument('--profile', default='dwh_bigquery', help='')
    @magic_arguments.argument('--target', default='dev', help='')
    def bigquery(self, line, cell=None):
        """
        ---------------------------------------------------------------------------
        %%bigquery

        SELECT * FROM {{ ref('table_in_dbt_project') }}
        ---------------------------------------------------------------------------
        ---------------------------------------------------------------------------
        asdf = str({'a':'value','b':'value2'}).replace(' ','')

        %%bigquery --params $asdf -p 

        {{params.b, params.a}}
        SELECT * FROM {{ ref('table_in_dbt_project') }}
        ---------------------------------------------------------------------------
        """
        if cell == None:
            dc = DataController()
            return dc()

        args = magic_arguments.parse_argstring(self.bigquery, line)

        self.dbt_helper = Adapter(args.profile, args.target)
        if args.params!='':
            params_var = ' {%-set params='+args.params+' -%}'
        else:
            params_var = ''

        macros_txt = self.dbt_helper.macros_txt
        jinja_statement = params_var+macros_txt+cell
        statement = Template(jinja_statement).render(source=self.dbt_helper.source, ref=self.dbt_helper.ref).strip()
        start = time()
        if args.parser:
            print(statement)
        else:
            #--------------------------------------------- Start
            client = bigquery.Client()
            results = client.query(statement)
            flat_results = [dict(row) for row in results.result()]
            df = pd.DataFrame(flat_results)
            duration = time()-start
            # https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobStatistics2.FIELDS.total_bytes_billed
            # cost per GB 0,023 * 1e-9 = cost per byte
            PriceInDollar = str(results.estimated_bytes_processed * (0.023 * 1e-9) if results.estimated_bytes_processed != None else "") \
                + "$" if (results.total_bytes_billed != None) \
                    else "error calculating price"
            print(f'Execution time: {int(duration//60)} min. - {duration%60:.2f} sec.\
                | Cost: {PriceInDollar} Bytes Billed: {results.estimated_bytes_processed}') 
            #--------------------------------------------- End

            self.shell.user_ns[args.dataframe] = df
            df = df.head(int(args.n_output)) if type(df)==pd.DataFrame else None
            return df

def load_ipython_extension(ipython):
    js = """IPython.CodeCell.options_default.highlight_modes['magic_sql'] = {'reg':[/^%%(biggy)/]};
    IPython.notebook.events.one('kernel_ready.Kernel', function(){
        IPython.notebook.get_cells().map(function(cell){
            if (cell.cell_type == 'code'){ cell.auto_highlight(); } }) ;
    });
    """
    display.display_javascript(js, raw=True)
    ipython.register_magics(SQLMagics)



class DataController():
    def __init__(self):
        self.client =  bigquery.Client()
        self.projects = self.list_projects()
        
        #----------- WIDGETS -----------#
        # self.wg_catalog = widgets.Dropdown(options=self.catalog_names, value=None)
        self.wg_project = widgets.Dropdown(options=[p.project_id for p in self.projects])
       # self.wg_search_table = widgets.Text(placeholder='<table>')
        self.wg_database = widgets.Dropdown(options=[])
        self.wg_tables = widgets.Dropdown(options=[])
        self.get_dataset(self.wg_project.value)
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
        self.wg_base_dropdowns = widgets.VBox(children=[self.wg_project, self.wg_database, self.wg_tables])
        self.wg_base_dropdowns.add_class('wg_base_dropdowns')
        self.all_columns = widgets.Button(description="All Columns")
        self.wg_columns_container = widgets.VBox(children=[self.all_columns])
        self.wg_column = widgets.Accordion(children=[self.wg_columns_container], selected_index=None)    
        self.select_sql = widgets.Button(description="SELECT")
        self.select_sql_star = widgets.Button(description="SELECT ALL")
        self.select_box = widgets.VBox(children=[self.select_sql, self.select_sql_star])
        self.output = widgets.Output()
        self.output.add_class("anton-enns")
        self.wg_output = widgets.Accordion(children=[self.output])
        self.pannel = widgets.HBox(
                children=[self.wg_base_dropdowns, self.wg_column, self.select_box, self.wg_output],
                box_style='info' # one of 'success', 'info', 'warning' or 'danger', or ''
            )  
        
        #----------- WIDGET ACTIONS -----------#
        self.wg_project.observe(self.get_dataset, names='value')
        self.wg_database.observe(self.get_tables, names='value')
        self.wg_tables.observe(self.get_colums, names='value')
        self.select_sql.on_click(self.on_button_clicked)
        self.select_sql_star.on_click(lambda x: self.on_button_clicked(x, star=True))
      #  self.wg_search_table.observe(self.search_tables, names='value')
        self.all_columns.on_click(self.all_columns_handler)
        
    def list_dataset_metadata(self, ProjectName):
        self.client = bigquery.Client(ProjectName)
        datasets = list(self.client.list_datasets())  
        DatasetMetadataList = [d for d in datasets]   
        return DatasetMetadataList
    
    def list_projects(self):
        return bigquery.Client().list_projects()
    
    def list_tables(self, dataset_id):
        tables = self.client.list_tables(dataset_id)  # Make an API request.
        table_ids = [table.table_id for table in tables]

        return table_ids

    def get_dataset(self, database):
        self.DatasetMetadataList = self.list_dataset_metadata(database)
        self.wg_database.index = None
        self.wg_database.options = tuple([d.dataset_id for d in self.DatasetMetadataList])

    def get_tables(self, dataset):
        self.table_ids = self.list_tables(dataset.new)
        self.wg_tables.index = None
        self.wg_tables.options = tuple(self.table_ids)

    def get_colums(self, table):
        full_table_id = f"{self.wg_project.value}.{self.wg_database.value}.{table.new}"
        print(full_table_id)
        api_repr = self.client.get_table(full_table_id).to_api_repr()
        f = lambda name, dtype: CB(name, dtype)
        self.partition_columns = []
        #self.check_boxes = [] + self.partition_columns
        self.check_boxes = [f(i['name'], i['type']) for i in api_repr['schema']['fields']] + self.partition_columns
        self.wg_columns_container.children = [self.all_columns]+self.check_boxes
    
    def on_button_clicked(self, x, star=False):
        f = lambda name, dtype:  f'{name} {prStyle.BLUE}-- {dtype}{prStyle.RESET}'
        with self.output:
            self.output.clear_output()
            part = [i.check.description for i in self.partition_columns if i.lab.value in ('DATE(PART.)', 'STRING(PART.)')]
            if len(part):
                part_string = f"\n{prStyle.MAGENTA}WHERE{prStyle.RESET} DATE({part[0]})=current_date\n{prStyle.MAGENTA}LIMIT{prStyle.RESET} {prStyle.CYAN}100{prStyle.RESET}"
            else:
                part_string = f"\n{prStyle.MAGENTA}LIMIT{prStyle.RESET} {prStyle.CYAN}100{prStyle.RESET}"
            if star:
                cols = '*'
            else:
                cols = "\n    , ".join([f(i.check.description, i.lab.value) for i in self.check_boxes if i.check.value])
            print(f'{prStyle.RED}%%bigquery{prStyle.RESET}\n{prStyle.MAGENTA}SELECT{prStyle.RESET}\n    {cols} \n{prStyle.MAGENTA}FROM{prStyle.RESET} {prStyle.GREEN}{self.wg_project.value}"."{self.wg_database.value}"."{self.wg_tables.value}"{prStyle.RESET}{part_string}')

    def search_tables(self, observation):
        self.wg_database.index = None
        self.wg_database.options = tuple([i['Name'] for i in self.DatasetMetadataList if observation['new'] in i['Name']])

    def all_columns_handler(self, observation):
        if all([i.check.value for i in self.check_boxes]):
            for box in self.check_boxes:
                box.check.value = False
        else:
            for box in self.check_boxes:
                box.check.value = True
        self.wg_column.children = [widgets.VBox([self.all_columns]+self.check_boxes)]
    
    def __call__(self):
        return self.pannel