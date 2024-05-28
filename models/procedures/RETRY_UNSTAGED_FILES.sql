CREATE OR REPLACE PROCEDURE RAVEN.RETRY_UNSTAGED_FILES(
    "COBID_START" VARCHAR(16777216), 
    "COBID_END" VARCHAR(16777216) )
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS caller
AS
$$

from snowflake.snowpark.functions import col

def run (session, cobid_start, cobid_end):

    # Retry call
    sf_retry = session \
        .table("RAVEN.VW_LOG_UNSTAGED_FILES") \
        .filter(col("RAVEN_COBID").between(cobid_start, cobid_end))

    for row in sf_retry.collect():
        file_name = row["FILE_NAME"]
        folder_path = row["FOLDER_PATH"]
        container_name = row["CONTAINER_NAME"]
        event_trigger = '''{"EVENT_SOURCE": 'SNOWFLAKE Task'}'''
        flag_create_table = False
        flag_create_pipe = False
        flag_create_csv_mapping = True
        database_target = ""
        
        session.call(
            "RAVEN.PY_STAGE_ME",
            file_name, 
            folder_path,
            container_name,
            event_trigger,
            flag_create_table,
            flag_create_pipe,
            flag_create_csv_mapping,
            database_target)
            
            
$$
;
