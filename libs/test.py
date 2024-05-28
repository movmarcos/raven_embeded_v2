#%%
import pandas as pd
from utils import get_project_root
from mufg_snowflakeconn import sfconnection as m_sf     # new version
environment = 'dvlp'
user_name = 'apd_raptor_sfk_depl@mufgsecurities.com'
mufgconn = m_sf.MufgSnowflakeConn(environment,user_name)
session = mufgconn.get_snowflake_session()
db_name = 'DVLP_RAPTOR_ERAVEN'
wh_name = 'DVLP_RAPTOR_WH'
session.use_database(db_name)
session.use_warehouse(wh_name)
app_name = "raptor"

exec_result = {}

# %%
import os
root_path = get_project_root()
seed_path = f'{root_path}\\metadata\\seeds\\{app_name}'
seed_files = [f for (dirpath, dirnames, filenames) in os.walk(f"{seed_path}") for f in filenames]
session.use_schema("RAVEN")

for csv_name in seed_files:
    metadata_file = csv_name.split('.')[0]
    csv_path = f"{seed_path}\\{csv_name}"
    df_metadata = pd.read_csv(csv_path)
    session.write_pandas(df_metadata, f"TEMP_METADATA_SRC_{metadata_file}", auto_create_table=True, overwrite=True, table_type="temporary")

    sql_path = f"{root_path}\\metadata\\merging\\{metadata_file}.sql"
    with open(sql_path) as f: obj_cmd = f.read()
    exec_cmd = session.sql(obj_cmd).collect()[0][0]
    exec_result[sql_path.split(".")[0]] = exec_cmd
    print(f"File executed: {sql_path}")



#%%

# %%

from snowflake.snowpark.functions import when_matched, when_not_matched
target_df = session.create_dataframe([(10, "old"), (10, "too_old"), (11, "old")], schema=["key", "value"])
target_df.write.save_as_table("my_table", mode="overwrite", table_type="temporary")
target = session.table("my_table")
source = session.create_dataframe([(10, "new"), (12, "new"), (13, "old")], schema=["key", "value"])
target.merge(source, (target["key"] == source["key"]) & (target["value"] == "too_old"),
...              [when_matched().update({"value": source["value"]}), when_not_matched().insert({"key": source["key"]})])
MergeResult(rows_inserted=2, rows_updated=1, rows_deleted=0)
target.sort("key", "value").collect()
[Row(KEY=10, VALUE='new'), Row(KEY=10, VALUE='old'), Row(KEY=11, VALUE='old'), Row(KEY=12, VALUE=None), Row(KEY=13, VALUE=None)]
