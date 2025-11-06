import os
from pathlib import Path
from time import time

import pandas as pd
from google.cloud import bigquery
from IPython.core import display, magic_arguments
from IPython.core.magic import Magics, line_cell_magic, magics_class
from jinja2 import Environment, Template, meta

from dbt_magics.datacontroller import DataController, debounce
from dbt_magics.dbt_helper import dbtHelper

"""
Implementation of the BigQueryMagics class.
Implement abstract methods from DataController class.
"""
class BigQueryDataController(DataController):
    def __init__(self):
        # If you want to use a different project by default, set it here.
        self.client =  bigquery.Client()

        super().__init__(r"%%bigquery", includeLeadingQuotesInCellMagic=False, table_name_quote_sign='`')

    """
    Implemented Abstract methods
    """

    def get_datasets(self, database):
        DatasetMetadataList = self.get_dataset_metadata(database)
        return [d.dataset_id for d in DatasetMetadataList]

    def get_tables(self, dataset_id):
        tables = self.client.list_tables(dataset_id)  # Make an API request.
        table_ids = [table.table_id for table in tables]
        return table_ids
    
    def get_columns(self, table):
        full_table_id = f"{self.wg_project.value}.{self.wg_database.value}.{table}"
        api_repr = self.client.get_table(full_table_id).to_api_repr()
        return [(i['name'], i['type']) for i in api_repr['schema']['fields']]
   
    def get_projects(self):
        return [p.project_id for p in self.client.list_projects()]
    
    def get_dataset_metadata(self, ProjectName):
        self.client = bigquery.Client(ProjectName)
        datasets = list(self.client.list_datasets())  
        DatasetMetadataList = [d for d in datasets]   
        return DatasetMetadataList
    

class dbtHelperAdapter(dbtHelper):
    def __init__(self, adapter_name='bigquery', profile_name="poky", target='prod'):
        super().__init__(adapter_name=adapter_name, profile_name=profile_name, target=target)
        
    def source(self, schema_name, table):
        SOURCES, _ = self._sources_and_models()        
        default_project = [i for i in map(self.profile_config.get, ['project', 'dbname', 'database', 'dataset']) if i][0]
        source = self._search_for_source_table(SOURCES, target_schema=schema_name, target_table=table, default_database=default_project)
        results = '`{project}`.`{schema}`.`{table}`'.format(schema=source['schema'], project=default_project, table=source['table'])
        return results

    def ref(self, table_name):
        custom_schema = self._get_custom_schema(table_name)
        default_schema = self.profile_config.get("dataset")
        default_project = self.profile_config.get("project")
        return f'`{default_project}`.`{custom_schema}`.`{table_name}`'

@magics_class
class BigQuerySQLMagics(Magics):
    pd.set_option('display.max_columns', None)

    @line_cell_magic
    @magic_arguments.magic_arguments()
    @magic_arguments.argument('--n_output', '-n', default=5, help='')
    @magic_arguments.argument('--dataframe', '-df', default="df", help='The variable to return the results in.')
    @magic_arguments.argument('--parser', '-p', action='store_true', help='Translate Jinja.')
    @magic_arguments.argument('--params', default='', help='Add additional Jinja params.')
    @magic_arguments.argument('--profile', default='poky', help='')
    @magic_arguments.argument('--target', default='prod', help='')
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

        self.dbt_helper = dbtHelperAdapter('bigquery', args.profile, args.target)

        env = Environment()
        def ipython(variable):
            try: result = get_ipython().ev(variable)
            except: result = False
            return result

        kwargs = {i:ipython(i) for i in meta.find_undeclared_variables(env.parse(cell)) if ipython(i)}

        macros_txt = self.dbt_helper.macros_txt
        jinja_statement = macros_txt+cell
        statement = Template(jinja_statement).render(source=self.dbt_helper.source, ref=self.dbt_helper.ref, var=self.dbt_helper.var, **kwargs).strip()


        start = time()
        if args.parser:
            print(statement)
        else:
            #--------------------------------------------- Start
            client = bigquery.Client(project=self.dbt_helper.profile_config.get("project"), location=self.dbt_helper.profile_config.get("location"))
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
    js = """IPython.CodeCell.options_default.highlight_modes['magic_sql'] = {'reg':[/^%%(bigquery)/]};
    IPython.notebook.events.one('kernel_ready.Kernel', function(){
        IPython.notebook.get_cells().map(function(cell){
            if (cell.cell_type == 'code'){ cell.auto_highlight(); } }) ;
    });
    """
    display.display_javascript(js, raw=True)
    ipython.register_magics(BigQuerySQLMagics)