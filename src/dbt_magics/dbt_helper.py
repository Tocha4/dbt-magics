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

    def __init__(self, adapter_name, profile_name=None, target=None):
        profiles = self._get_profiles()
        outputs = [i for i in profiles if any([profiles[i]['outputs'][j]['type']==adapter_name for j in profiles[i]['outputs']])] # Search for profiles matching adapter
        
        if len(outputs)>1:
            assert profile_name!=None, f'More then one profile for adapter={adapter_name}. Profiles: {outputs}\nPlease use --profiles flag like (%%athena --profiles {outputs[0]})'
            self.profile_name = profile_name
        elif profile_name!=None:
            assert profile_name in tuple(profiles.keys()), f'Selected profile not in ./dbt/profiles.yml.\nAvailable profiles: {tuple(profiles.keys())}.'
            self.profile_name = profile_name
        elif len(outputs)==1:
            self.profile_name = outputs[0] 
        else:
            assert outputs, f'No profiles found. Please use --profile flag or set (type: {adapter_name}) in ./dbt/profiles.yml'

        self.profile = profiles[self.profile_name]

        
        self.target = (target if target else self.profile.get("target"))
        try:
            self.profile_config = self.profile["outputs"][self.target]
        except:
            raise BaseException(f"Profile-target '{self.target}' not found.")
        self.dbt_project = self._get_dbt_project()

    @property
    def project_folder(self):
        pf = self.profile_config.get("project_folder", False)
        assert pf, f'Path to the project is not set. Please set project_folder in ./dbt/profiles.yml'
        return pf

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
        dbt_project_file_path = os.path.join(self.project_folder, "dbt_project.yml")
        return self._open_yaml(dbt_project_file_path)

    def _sources_and_models(self):
        SOURCES, MODELS = [], []
        # dbt_project, dbt_project_folder = get_dbt_project()
        for mp in self.dbt_project.get("model-paths"):
            folder = os.path.join(self.project_folder, mp)
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
            folder = os.path.join(self.project_folder, mp)
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
        folder = self.project_folder
        macros_txt = ""
        for mp in self.dbt_project.get("macro-paths"):
            macros_files = self._get_macros(os.path.join(folder, mp))
            for mf in macros_files:
                with open(mf, encoding='utf-8') as file:
                    macros_txt += "".join(file.readlines()) + "\n"
        return macros_txt

    def var(self, value):
        return self.dbt_project['vars'].get(value, f'ERROR: NOT FOUND VALUE {value}')