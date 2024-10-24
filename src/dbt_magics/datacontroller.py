import asyncio
from abc import ABC, abstractmethod
import re
import ipywidgets as widgets
from pandas.io.clipboard import clipboard_set


# Styling
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

# Checkbox class
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

# Code for Timer and debounce
# is from https://ipywidgets.readthedocs.io/en/latest/examples/Widget%20Events.html#Debouncing
class Timer:
    def __init__(self, timeout, callback):
        self._timeout = timeout
        self._callback = callback

    async def _job(self):
        await asyncio.sleep(self._timeout)
        self._callback()

    def start(self):
        self._task = asyncio.ensure_future(self._job())

    def cancel(self):
        self._task.cancel()

def debounce(wait):
    """ Decorator that will postpone a function's
        execution until after `wait` seconds
        have elapsed since the last time it was invoked. """
    def decorator(fn):
        timer = None
        def debounced(*args, **kwargs):
            nonlocal timer
            def call_it():
                fn(*args, **kwargs)
            if timer is not None:
                timer.cancel()
            timer = Timer(wait, call_it)
            timer.start()
        return debounced
    return decorator


"""
Base class for a data controller GUI interface
Includes generic GUI functions
Specific data controllers should inherit from this class

Child classes should be implemented in their respective magics.py files 
due to the different dependencies (e.g. BigQuery, Athena, SQLite)
"""
class DataController(ABC):
    """
    lineMagicName: The name of the line magic that is displayed in the output widget, e.g. %bigquery or %athena
    """
    def __init__(self, lineMagicName, includeLeadingQuotesInCellMagic=True, table_name_quote_sign='"'):
        self.projects = self.get_projects()
        self.lineMagicName = lineMagicName
        self.includeLeadingQuotesInCellMagic = includeLeadingQuotesInCellMagic
        self.table_name_quote_sign = table_name_quote_sign

        #----------- WIDGETS -----------#
        # self.wg_catalog = widgets.Dropdown(options=self.catalog_names, value=None)
        self.wg_project = widgets.Dropdown(options=[p for p in self.projects])
        self.wg_search_table = widgets.Text(placeholder='Search for table...')
        self.wg_database = widgets.Dropdown(options=[])
        self.wg_tables = widgets.Select(options=[])
        self.set_dataset_options(self.wg_project.value)
        self.set_tables_options(self.wg_database.value)

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
        
        
        self.wg_base_dropdowns = widgets.VBox(children=[self.wg_project, self.wg_database, self.wg_search_table, self.wg_tables])
        self.wg_base_dropdowns.add_class('wg_base_dropdowns')
        self.all_columns = widgets.Button(description="All Columns")
        self.wg_search_column = widgets.Text(placeholder='<column>')
        self.wg_columns_and_search = widgets.HBox(children=[self.all_columns, self.wg_search_column])
        self.wg_check_boxes = widgets.VBox(children=[])
        self.wg_columns_container = widgets.VBox(children=[self.wg_columns_and_search, self.wg_check_boxes])
        self.wg_column = widgets.Accordion(children=[self.wg_columns_container], selected_index=None)    
        self.select_sql = widgets.Button(description="SELECT", icon='fa-copy')
        self.select_sql_star = widgets.Button(description="SELECT ALL", icon='fa-copy')
        self.select_box = widgets.VBox(children=[self.select_sql, self.select_sql_star])
        self.output = widgets.Output()
        self.output.add_class("anton-enns")
        self.wg_output = widgets.Accordion(children=[self.output])
        self.pannel = widgets.HBox(
                children=[self.wg_base_dropdowns, self.wg_column, self.select_box, self.wg_output],
                box_style='info' # one of 'success', 'info', 'warning' or 'danger', or ''
            )  
        
        #----------- WIDGET ACTIONS -----------#
        self.wg_project.observe(self.set_dataset_options, names='value')
        self.wg_database.observe(self.set_tables_options, names='value')
        self.wg_tables.observe(self.set_columns_options, names='value')
        self.select_sql.on_click(self.on_button_clicked)
        self.select_sql_star.on_click(lambda x: self.on_button_clicked(x, star=True))
        self.wg_search_table.observe(self.search_tables, names='value')
        self.all_columns.on_click(self.all_columns_handler)
        self.wg_search_column.observe(self.search_columns, names='value')
        

    """ Abstract methods """

    """ 
    Abstract method to be implemented by child class
    Should return a list of projects for a given data source

    E.g. for BigQuery, this is a list of projects
    E.g. for Athena, this is a list of DataCatalogs
    """
    @abstractmethod
    def get_projects(self):
        pass

    """
    Abstract method to be implemented by child class

    Should return a list of datasets for a given project (or database)
    """
    @abstractmethod
    def get_datasets(self, database):
        pass

    """
    Abstract method to be implemented by child class
    Should return a list of tables for a given dataset
    """
    @abstractmethod
    def get_tables(self, dataset):
        pass

    """
    Abstract method to be implemented by child class
    Should return a list of dicts of columns and data types for a given table
    A datatype may include a partition type, e.g. "DATE(PART.)"
    I.e.:
    [(column_name: string, data_type: string), ...]
    E.g.:
    [("my_date", DATE(PART.)), ("my_string", STRING), ...]
    """
    @abstractmethod
    def get_columns(self, table):
        pass

    """ Additional methods """

    # Set the options for the dataset dropdown
    @debounce(0.3)
    def set_dataset_options(self, observation):
        observation = observation if type(observation)==str else observation['new']
        datasets = self.get_datasets(observation)
        self.wg_database.index = None
        self.wg_database.options = tuple(datasets)

    # Set the options for the table dropdown
    @debounce(0.3)
    def set_tables_options(self, observation):
        observation = observation if type(observation)==str else observation['new']
        self.tables = self.get_tables(observation)
        self.wg_tables.index = None
        self.wg_tables.options = tuple(self.tables)

    # Set the options for the column checkboxes
    def set_columns_options(self, table):
        if table.new == None:
            return
        f = lambda name, dtype: CB(name, dtype)
        
        table_tuples = self.get_columns(table.new)

        # Partition columns are columns with a partition type, e.g. "DATE(PART.)"
        self.partition_columns = [f(*i) for i in table_tuples if "Part." in i[1]]
        self.check_boxes = [f(*i) for i in table_tuples] 
        self.wg_columns_container.children = [self.all_columns, self.wg_search_column]+self.check_boxes
        self.wg_check_boxes.children = self.check_boxes

    # Print the SQL statement to the output widget when the button is clicked
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
            
            output_string = f'{prStyle.RED}{self.lineMagicName}{prStyle.RESET}\n{prStyle.MAGENTA}SELECT{prStyle.RESET}\n    {cols} \n{prStyle.MAGENTA}FROM{prStyle.RESET} {prStyle.GREEN}"{self.wg_project.value}"."{self.wg_database.value}"."{self.wg_tables.value}"{prStyle.RESET}{part_string}'
            
            statement = output_string if self.includeLeadingQuotesInCellMagic else output_string.replace('"', self.table_name_quote_sign)
            print(statement)
            clipboard_set(re.sub(r'\x1b\[\d+m', '', statement, count=0, flags=0))

                
    # Search the tables dropdown
    # https://ipywidgets.readthedocs.io/en/latest/examples/Widget%20Events.html#Debouncing
    @debounce(0.3)
    def search_tables(self, observation):
        self.wg_tables.options = tuple([i for i in self.tables if observation['new'] in i])
        if len(self.wg_tables.options) > 0:
            self.wg_tables.index = 0
        else:
            self.wg_tables.index = None

    # Select all columns
    def all_columns_handler(self, observation):
        if all([i.check.value for i in self.check_boxes]):
            for box in self.check_boxes:
                box.check.value = False
        else:
            for box in self.check_boxes:
                box.check.value = True

    def __call__(self):
        return self.pannel
    
    # Search the columns and set the checkboxes
    def search_columns(self, observation):
        self.wg_columns_container.children = [self.all_columns, self.wg_search_column]+ [i for i in self.check_boxes if observation['new'] in i.check.description]