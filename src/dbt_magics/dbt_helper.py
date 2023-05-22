import os
from pathlib import Path

import yaml


#################### CLASSES ####################

"""
Base class to help with dbt project

Child classes should be implemented in their respective magics.py files 
due to the different dependencies (e.g. BigQuery, Athena, SQLite)
"""
class dbtHelper():

    def __init__(self, profile_name="dbt_athena_dwh", target=None, \
                 default_dbt_folder=os.path.join(Path().home(), "projects", "data-aws", "dbt_athena_dwh")):
        self.DEFAULT_DBT_FOLDER = default_dbt_folder
        self.profile_name = profile_name
        try:
            self.profile = self._get_profiles()[profile_name]
        except:
            raise BaseException(f"Profile '{profile_name}' not found. Available Profiles: {tuple(self._get_profiles().keys())}.")
        
        self.target = (target if target else self.profile.get("target"))
        try:
            self.profile_config = self.profile["outputs"][self.target]
        except:
            raise BaseException(f"Profile-target '{self.target}' not found.")
        self.dbt_project = self._get_dbt_project()

    def _open_yaml(self, file_path):
        with open(file_path) as pf:
            results = yaml.safe_load(pf)
        return results

    def _len_check(self, source, table_name):
        if len(source)>1: 
            raise BaseException(f"Conflicting table name: {table_name}. Sources: {source}.")
        elif len(source)==0:
            raise BaseException(f"Not found table name {table_name}.")
        elif len(source)==1:
            source = source[0]
        return source

    def _get_macros(self, folder):
        macro_files = []
        for top, dirs, files in os.walk(folder):
            for nm in files:       
                macro_files.append(os.path.join(top, nm))
        return [i for i in macro_files if i.endswith(".sql")]        

    def _get_profiles(self):
        profiles_file_path = os.path.join(Path().home(), ".dbt", "profiles.yml")
        return self._open_yaml(profiles_file_path)

    def _get_dbt_project(self):
        dbt_project_file_path = os.path.join(self.profile_config.get("project_folder", self.DEFAULT_DBT_FOLDER), "dbt_project.yml")
        return self._open_yaml(dbt_project_file_path)

    def _sources_and_models(self):
        SOURCES, MODELS = [], []
        # dbt_project, dbt_project_folder = get_dbt_project()
        for mp in self.dbt_project.get("model-paths"):
            folder = os.path.join(self.profile_config.get("project_folder", self.DEFAULT_DBT_FOLDER), mp)
            for root, dirs, files in os.walk(folder):
                for f in files:
                    if f.endswith(".yml"):
                        file = self._open_yaml(os.path.join(root, f))
                        SOURCES += file.get("sources", [])
                    elif f.endswith(".sql"):
                        model_path = os.path.join(root, f).split(mp)[-1]
                        model = [i for i in os.path.normpath(model_path).split(os.path.sep) if i]
                        schema = model[0]
                        table = model[-1].replace(".sql","")
                        MODELS += [{table: schema}]
                        
        for mp in self.dbt_project.get("seed-paths"):
            folder = os.path.join(self.profile_config.get("project_folder", self.DEFAULT_DBT_FOLDER), mp)
            for root, dirs, files in os.walk(folder):
                for f in files:
                    if f.endswith(".yml"):
                        file = self._open_yaml(os.path.join(root, f))
                        MODELS += [{seed['name']: 'seeds'} for seed in file.get("seeds", [])]

        return (SOURCES, MODELS)

    def _get_custom_schema(self, table_name):
        _, MODELS = self._sources_and_models()
        table = [i for i in MODELS if i.get(table_name, False)]
        table = self._len_check(table, table_name=table_name)
        if table[table_name]=='seeds':
            custom_schema = self.dbt_project.get("seeds").get(self.profile_name).get('+schema')
        else:
            custom_schema = self.dbt_project.get("models").get(self.profile_name).get(table[table_name]).get('+schema')
        return custom_schema

    @property
    def macros_txt(self):
        folder = self.profile_config.get("project_folder", self.DEFAULT_DBT_FOLDER)
        macros_txt = ""
        for mp in self.dbt_project.get("macro-paths"):
            macros_files = self._get_macros(os.path.join(folder, mp))
            for mf in macros_files:
                with open(mf, encoding='utf-8') as file:
                    macros_txt += "".join(file.readlines()) + "\n"
        return macros_txt

    def var(self, value):
        return self.dbt_project['vars'].get(value, f'ERROR: NOT FOUND VALUE {value}')