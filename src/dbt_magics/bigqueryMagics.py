import os
from pathlib import Path
from time import time

import pandas as pd
from google.cloud import bigquery
from IPython.core import display, magic_arguments
from IPython.core.magic import Magics, line_cell_magic, magics_class
from jinja2 import Template
from dbt_magics.datacontroller import CB, DataController

from dbt_magics.dbt_helper import dbtHelper

"""
Implementation of the BigQueryMagics class.
Implement abstract methods from DataController class.
"""
class BigQueryDataController(DataController):
    def __init__(self):
        self.client =  bigquery.Client()

        super().__init__()

    """
    Implemented Abstract methods
    """

    def set_dataset(self, database):
        self.DatasetMetadataList = self.get_dataset_metadata(database)
        self.wg_database.index = None
        self.wg_database.options = tuple([d.dataset_id for d in self.DatasetMetadataList])

    def set_tables(self, dataset):
        self.table_ids = self.get_tables(dataset.new)
        self.wg_tables.index = None
        self.wg_tables.options = tuple(self.table_ids)

    def set_columns(self, table):
        if table.new == None:
            return
        full_table_id = f"{self.wg_project.value}.{self.wg_database.value}.{table.new}"
        api_repr = self.client.get_table(full_table_id).to_api_repr()
        f = lambda name, dtype: CB(name, dtype)
        self.partition_columns = []

        #self.check_boxes = [] + self.partition_columns
        self.check_boxes = [f(i['name'], i['type']) for i in api_repr['schema']['fields']] + self.partition_columns
        self.wg_columns_container.children = [self.all_columns, self.wg_search_column]+self.check_boxes
        self.wg_check_boxes.children = self.check_boxes
   
    def get_projects(self):
        return [p.project_id for p in self.client.list_projects()]


    """
    Additional methods
    """

    def get_tables(self, dataset_id):
        tables = self.client.list_tables(dataset_id)  # Make an API request.
        table_ids = [table.table_id for table in tables]

        return table_ids
    
    def get_dataset_metadata(self, ProjectName):
        self.client = bigquery.Client(ProjectName)
        datasets = list(self.client.list_datasets())  
        DatasetMetadataList = [d for d in datasets]   
        return DatasetMetadataList

class dbtHelperAdapter(dbtHelper):
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
            dc = BigQueryDataController()
            return dc()

        args = magic_arguments.parse_argstring(self.bigquery, line)

        self.dbt_helper = dbtHelperAdapter(args.profile, args.target)
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

