import sqlite3 as sql
from time import time

import pandas as pd
from IPython.core import display, magic_arguments
from IPython import get_ipython
from IPython.core.magic import Magics, line_cell_magic, magics_class
from jinja2 import Environment, Template, meta

from dbt_magics.datacontroller import DataController, prStyle
from dbt_magics.dbt_helper import dbtHelper


class SQLiteDataController(DataController):
    def __init__(self):
        self.dbt_helper = dbtHelperAdapter(profile_name=None, target='prod')
        super().__init__(r"%%sqlity")

    """
    Implemented Abstract methods
    """
    def get_projects(self):
        return ['project']

    def get_datasets(self, database):
        
        return list(self.dbt_helper.profile_config['schemas_and_paths'].keys())

    
    def get_tables(self, database):
        if database:
            return sorted(self.get_data(f'select * from {database}.sqlite_master')['name'].values)
        else: 
            return []
    
    def get_columns(self, table):
        columns = self.get_data(f"PRAGMA {self.wg_database.value}.table_info({table});")[['name', 'type']].values
        return columns

    """ 
    Additional methods 
    """
    def get_data(self, statement, dataset='main'):
        return self.dbt_helper.run_query(
                            sql_statement=statement,
                            main_database=self.dbt_helper.profile_config['schemas_and_paths']['main'],
                            extensions=self.dbt_helper.profile_config['extensions'],
                            schemas_and_paths=self.dbt_helper.profile_config['schemas_and_paths'],
                            verbose=False
        )

    # Print the SQL statement to the output widget when the button is clicked
    def on_button_clicked(self, x, star=False):
        f = lambda name, dtype:  f'{name} {prStyle.BLUE}-- {dtype}{prStyle.RESET}'
        with self.output:
            self.output.clear_output()
            part_string = f"\n{prStyle.MAGENTA}LIMIT{prStyle.RESET} {prStyle.CYAN}100{prStyle.RESET}"
            if star:
                cols = '*'
            else:
                cols = "\n    , ".join([f(i.check.description, i.lab.value) for i in self.check_boxes if i.check.value])
            
            output_string = f'{prStyle.RED}{self.lineMagicName}{prStyle.RESET}\n{prStyle.MAGENTA}SELECT{prStyle.RESET}\n    {cols} \n{prStyle.MAGENTA}FROM{prStyle.RESET} {prStyle.GREEN}"{self.wg_database.value}"."{self.wg_tables.value}"{prStyle.RESET}{part_string}'

            if self.includeLeadingQuotesInCellMagic:
                print(output_string)
            else:
                print(output_string.replace('"', ''))


class dbtHelperAdapter(dbtHelper):
    def __init__(self, adapter_name='sqlite', profile_name=None, target=None):
        super().__init__(adapter_name=adapter_name, profile_name=profile_name, target=target)
        
    def ref(self, table_name):
        return f'main."{table_name}"'

    def source(self, source_name, table_name):
        # TODO: Rules development
        # SOURCES, _ = self._sources_and_models()
        # print(SOURCES)
        # database = [database for database in SOURCES if source_name==database['name']][0]
        # schema = database['schema']
        return f'{source_name}."{table_name}"'
    


    def run_query(self, sql_statement, main_database, extensions=[], schemas_and_paths=None, verbose=True):
        with sql.connect(main_database) as conn:
            cursor = conn.cursor()
            conn.enable_load_extension(True)
            for extension in extensions:
                conn.load_extension(extension)
            for key in schemas_and_paths.keys():
                if key=='main':continue
                query = f"""attach '{schemas_and_paths[key]}' as {key};"""
                cursor.execute(query)

            #---------------------- For Performance -----------------------#
            sql_script = """pragma journal_mode = WAL;
                            pragma synchronous = normal;
                            pragma temp_store = memory;
                            pragma mmap_size = 30000000000;"""
            results = cursor.executescript(sql_script)

            start = time()        
            try:
                df = pd.read_sql(sql_statement, conn)
            except Exception as e:
                print(f"{prStyle.RED}Not a SELECT statement.\n{e}")
                
                df = None
            duration = time()-start
            if verbose: print(f'{prStyle.GREEN}Execution time: {int(duration//60)} min. - {duration%60:.2f} sec.')
            return df
    

@magics_class
class SQLMagics(Magics):
    pd.set_option('display.max_columns', None)

    @line_cell_magic
    @magic_arguments.magic_arguments()
    @magic_arguments.argument('--n_output', '-n', default=5, help='')
    @magic_arguments.argument('--dataframe', '-df', default="df", help='The variable to return the results in.')
    @magic_arguments.argument('--parser', '-p', action='store_true', help='Translate Jinja.')
    @magic_arguments.argument('--profile', default=None, help='')
    @magic_arguments.argument('--target', default='prod', help='')
    def sqlity(self, line, cell=None):
        """
---------------------------------------------------------------------------
%%athena

SELECT * FROM {{ ref('table_in_dbt_project') }}
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
            dc = SQLiteDataController()
            return dc()
        else:        
            args = magic_arguments.parse_argstring(self.sqlity, line)
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
                df = self.dbt_helper.run_query(
                    sql_statement=statement,
                    main_database=self.dbt_helper.profile_config['schemas_and_paths']['main'],
                    extensions=self.dbt_helper.profile_config['extensions'],
                    schemas_and_paths=self.dbt_helper.profile_config['schemas_and_paths'],


                )
                self.shell.user_ns[args.dataframe] = df
                df = df.head(int(args.n_output)) if type(df)==pd.DataFrame else None
                return df



def load_ipython_extension(ipython):
    js = """IPython.CodeCell.options_default.highlight_modes['magic_sql'] = {'reg':[/^%%(sqlity)/]};
    IPython.notebook.events.one('kernel_ready.Kernel', function(){
        IPython.notebook.get_cells().map(function(cell){
            if (cell.cell_type == 'code'){ cell.auto_highlight(); } }) ;
    });
    """
    display.display_javascript(js, raw=True)
    ipython.register_magics(SQLMagics)
