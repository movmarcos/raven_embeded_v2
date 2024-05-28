#%%
import os
import sys
import inspect
from mufg_snowflakeconn import sfconnection as m_sf
sys.path.insert(0, 'D:\GitHub\secrisk-raven-embedded')
from models.procedures.python.PY_STAGE_ME import run
from models.procedures.python.PY_STAGE_ME_INFER_SCHEMA import StageMe


###### Change here ######
environment = 'dvlp'
user_type = 'depl'
project_name = 'musbi'
wh_size = 'XS'
db_sufix = 'REGOPS'
#########################

env_letter = environment[:1]
role_type = 'batch' if user_type == 'batch' else 'owner'
role_name = f'{environment}_{project_name}_{role_type}'
role_name = f'BI_DEVELOPER'
user_name = f'ap{env_letter}_raptor_sfk_{user_type}@mufgsecurities.com'
user_name = f'marcos.magri@mufgsecurities.com'
wh_name = f'{environment}_{project_name}_WH_{wh_size}'
db_name = f'{environment}_{project_name}_{db_sufix}'

mufgconn = m_sf.MufgSnowflakeConn(environment,user_name)
session = mufgconn.get_snowflake_session_ext()

session.use_role(role_name)
session.use_database(db_name)

#%%
from initialize_raven import InitializeRaven
from raven_app import RavenTargetDB as raven_app

raven = raven_app(db_name,project_name)
build_configs, raven_file_paths = raven.get_raven_file_paths()
init_raven = InitializeRaven(session, db_name,project_name,raven_file_paths)
build_process = 'models'
#%%
exec_files = [f for key,value in raven_file_paths.items() if "nee" not in key for f in value for p in f.split("\\") if p == build_process]
exec_files = [x for x in exec_files if "__pycache__" not in x]
len(exec_files)
#%%
if build_process == 'models':
    seq_objects = ['file_formats', 'tables', 'functions', 'views', 'table_functions', 'sql', 'tasks']
    values = set(map(lambda x:x.split("\\")[-2], exec_files))
    dict_objs = {seq_objects.index(x):[y for y in exec_files if y.split("\\")[-2]==x] for x in values}
    newlist = []
    for key,item in sorted(dict_objs.items()):
        newlist = newlist + item
    exec_files = newlist                                                      
#%%
exec_result = {}
for file_name in exec_files:
    print(f"File executed: {file_name}")
    with open(file_name) as f: obj_cmd = f.read()

    if any("tables" == s for s in file_name.split("\\")):
        exec_cmd = create_table(self.session,obj_cmd)
    else:
        exec_cmd = self.session.sql(obj_cmd).collect()[0][0]

    exec_result[file_name.split(".")[0]] = exec_cmd
