import sqlite3 as sql
from time import time

import pandas as pd
from IPython.core import display, magic_arguments
from IPython.core.magic import Magics, cell_magic, magics_class
from jinja2 import Template

from dbt_magics.dbt_helper import dbtHelper


class Adapter(dbtHelper):
    def __init__(self, profile_name="gcp_dwh", target='prod'):
        super().__init__(profile_name=profile_name, target=target)
        
    def ref(self, table_name):
        return f'main."{table_name}"'

    def source(self, source_name, table_name):
        SOURCES, _ = self._sources_and_models()
        database = [database for database in SOURCES if source_name==database['name']][0]
        schema = database['schema']
        return f'{schema}."{table_name}"'


@magics_class
class SQLMagics(Magics):
    pd.set_option('display.max_columns', None)

    @cell_magic
    @magic_arguments.magic_arguments()
    @magic_arguments.argument('--n_output', '-n', default=5, help='')
    @magic_arguments.argument('--dataframe', '-df', default="df", help='The variable to return the results in.')
    @magic_arguments.argument('--parser', '-p', action='store_true', help='Translate Jinja.')
    @magic_arguments.argument('--params', default='', help='Add additional Jinja params.')
    @magic_arguments.argument('--profile', default='dbt_sqlite', help='')
    @magic_arguments.argument('--target', default='dev', help='')
    @magic_arguments.argument('--ddl', action='store_true', help='')
    def sqlity(self, line, cell):
        """
---------------------------------------------------------------------------
%%sqlity

SELECT * FROM {{ ref('table_in_dbt_project') }}
---------------------------------------------------------------------------
---------------------------------------------------------------------------
asdf = str({'a':'value','b':'value2'}).replace(' ','')

%%sqlity --params $asdf -p 

{{params.b, params.a}}
SELECT * FROM {{ ref('table_in_dbt_project') }}
---------------------------------------------------------------------------
"""
        args = magic_arguments.parse_argstring(self.sqlity, line)
        self.dbt_helper = Adapter(args.profile, args.target)
        if args.params!='':
            params_var = ' {%-set params='+args.params+' -%}'
        else:
            params_var = ''

        macros_txt = self.dbt_helper.macros_txt
        jinja_statement = params_var+macros_txt+cell
        statement = Template(jinja_statement).render(source=self.dbt_helper.source, ref=self.dbt_helper.ref).strip()

        with sql.connect(self.dbt_helper.profile_config['schemas_and_paths']['main']) as conn:
            cursor = conn.cursor()
            conn.enable_load_extension(True)
            for extension in self.dbt_helper.profile_config['extensions']:
                conn.load_extension(extension)
            for key in self.dbt_helper.profile_config['schemas_and_paths'].keys():
                if key=='main':continue
                query = f"""attach '{self.dbt_helper.profile_config['schemas_and_paths'][key]}' as {key};"""
                cursor.execute(query)

            #---------------------- For Performance -----------------------#
            sql_script = """pragma journal_mode = WAL;
                            pragma synchronous = normal;
                            pragma temp_store = memory;
                            pragma mmap_size = 30000000000;"""
            results = cursor.executescript(sql_script)

            start = time()
            if args.parser:
                print(statement)
            elif args.ddl:
                results = cursor.executescript(statement)
                duration = time()-start
                print(f'Execution time: {int(duration//60)} min. - {duration%60:.2f} sec.')
                return f"Done. {results.fetchall()}"
            else:
                df = pd.read_sql(statement, conn)
                self.shell.user_ns[args.dataframe] = df
                df = df.head(int(args.n_output)) if type(df)==pd.DataFrame else None
                duration = time()-start
                print(f'Execution time: {int(duration//60)} min. - {duration%60:.2f} sec.')                
                return df


def load_ipython_extension(ipython):
    js = """IPython.CodeCell.options_default.highlight_modes['magic_sql'] = {'reg':[/^%%(sqlity|athena|redshift)/]};
    IPython.notebook.events.one('kernel_ready.Kernel', function(){
        IPython.notebook.get_cells().map(function(cell){
            if (cell.cell_type == 'code'){ cell.auto_highlight(); } }) ;
    });
    """
    display.display_javascript(js, raw=True)
    ipython.register_magics(SQLMagics)