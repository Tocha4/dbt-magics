from time import time
import pandas as pd
from jinja2 import Template
import io

from IPython.core import magic_arguments, display
from IPython.core.magic import line_magic, cell_magic, line_cell_magic, Magics, magics_class

from google.cloud import bigquery

from dbt_magics.dbt_helper import dbtHelper


class Adapter(dbtHelper):
    def __init__(self, profile_name="gcp_dwh", target='prod'):
        super().__init__(profile_name=profile_name, target=target)
        
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

    @cell_magic
    @magic_arguments.magic_arguments()
    @magic_arguments.argument('--n_output', '-n', default=5, help='')
    @magic_arguments.argument('--dataframe', '-df', default="df", help='The variable to return the results in.')
    @magic_arguments.argument('--parser', '-p', action='store_true', help='Translate Jinja.')
    @magic_arguments.argument('--params', default='', help='Add additional Jinja params.')
    @magic_arguments.argument('--profile', default='gcp_dwh', help='')
    @magic_arguments.argument('--target', default='dev', help='')
    def biggy(self, line, cell):
        """
---------------------------------------------------------------------------
%%biggy

SELECT * FROM {{ ref('table_in_dbt_project') }}
---------------------------------------------------------------------------
---------------------------------------------------------------------------
asdf = str({'a':'value','b':'value2'}).replace(' ','')

%%biggy --params $asdf -p 

{{params.b, params.a}}
SELECT * FROM {{ ref('table_in_dbt_project') }}
---------------------------------------------------------------------------
"""
        args = magic_arguments.parse_argstring(self.biggy, line)
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
            print(f'Execution time: {int(duration//60)} min. - {duration%60:.2f} sec. | Billed: {results.total_bytes_billed/(9.5367431640625*10**7):.4f}')
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


