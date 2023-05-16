import ipywidgets as widgets
from abc import ABC, abstractmethod

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

"""
Superclass for a data controller GUI interface
Includes generic GUI functions
Specific data controllers should inherit from this class
"""
class DataController(ABC):
    def __init__(self):
        self.projects = self.get_projects()
        #----------- WIDGETS -----------#
        # self.wg_catalog = widgets.Dropdown(options=self.catalog_names, value=None)
        self.wg_project = widgets.Dropdown(options=[p for p in self.projects])
        self.wg_search_table = widgets.Text(placeholder='<table>')
        self.wg_database = widgets.Dropdown(options=[])
        self.wg_tables = widgets.Dropdown(options=[])
        self.set_dataset(self.wg_project.value)
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
        self.wg_project.observe(self.set_dataset, names='value')
        self.wg_database.observe(self.set_tables, names='value')
        self.wg_tables.observe(self.set_columns, names='value')
        self.select_sql.on_click(self.on_button_clicked)
        self.select_sql_star.on_click(lambda x: self.on_button_clicked(x, star=True))
        self.wg_search_table.observe(self.search_tables, names='value')
        self.all_columns.on_click(self.all_columns_handler)
        self.wg_search_column.observe(self.search_columns, names='value')

    """ 
    Abstract method to be implemented by child class
    Creates a list of projects/catalogues to be displayed in the dropdown
    E.g. for BigQuery, this is a list of projects
    E.g. for Athena, this is a list of DataCatalogs
    """
    @abstractmethod
    def get_projects(self):
        pass

    """
    Abstract method to be implemented by child class
    Creates a list of datasets to be displayed in the dropdown.
    """
    def set_dataset(self, database):
        pass

    """
    Abstract method to be implemented by child class
    Creates a list of tables to be displayed in the dropdown
    """
    @abstractmethod
    def set_tables(self, dataset):
        pass

    """
    Abstract method to be implemented by child class
    Creates the data for a list of columns to be displayed in the dropdown
    This method has to set the following variables:

    self.partition_columns
    self.check_boxes
    self.wg_columns_container
    self.wg_check_boxes
    """
    @abstractmethod
    def set_columns(self, table):
        """ example code:
        api_repr = client.get_table(full_table_id)
        f = lambda name, dtype: CB(name, dtype)
        self.partition_columns = []

        #self.check_boxes = [] + self.partition_columns
        self.check_boxes = [f(i['name'], i['type']) for i in api_repr['schema']['fields']] + self.partition_columns
        self.wg_columns_container.children = [self.all_columns, self.wg_search_column]+self.check_boxes
        self.wg_check_boxes.children = self.check_boxes
        """
        pass

    
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
        self.wg_tables.index = None
        self.wg_tables.options = tuple([i for i in self.table_ids if observation['new'] in i])

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
    
    def search_columns(self, observation):
        self.wg_check_boxes.children = [i for i in self.check_boxes if observation['new'] in i.check.description]

        for box in self.check_boxes:
            if observation['new'] in box.check.description:
                box.check.value = True
            else: 
                box.check.value = False
        self.wg_columns_container.children = [self.all_columns, self.wg_search_column]+ [i for i in self.check_boxes if observation['new'] in i.check.description]