CREATE OR REPLACE PROCEDURE RAVEN.LIST_STORAGE_SPACES(
    "STAGE" STRING,
    "COBID_START" INT, 
    "COBID_END" INT )
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS caller
AS
$$

from snowflake.snowpark.functions import col, lit, when_matched, when_not_matched
from snowflake.snowpark.types import *
from snowflake.snowpark.exceptions import SnowparkSQLException

def list_blob(session,stage_name,pattern):
# List file(s) in the datalake according to pattern used to copy
    cmd_list_file = f"LIST @{stage_name} pattern = '{pattern}'"
    sf_blob_list = session.sql(cmd_list_file).collect()
    return sf_blob_list

def merge_log(session,insert_list):
    target = session.table("RAVEN.LOG_LIST_EXTERNAL_FILE")     
    source = session.create_dataframe([insert_list])

    target.merge(source, 
        (target["RAVEN_COBID"] == source["RAVEN_COBID"]) &
        (target["STAGE_NAME"] == source["STAGE_NAME"]) &
        (target["FOLDER_PATTERN"] == source["FOLDER_PATH_COB"]) &
        (target["FILE_EXTENSION"] == source["FILE_EXTENSION"]) &
        (target["FLAG_LAST_VERSION"] == lit(True))
        ,
        [when_matched().update({
            "FLAG_LAST_VERSION" : lit(False)
            })
        ])
    
    target.merge(source, 
        (target["RAVEN_COBID"] == source["RAVEN_COBID"]) &
        (target["STAGE_NAME"] == source["STAGE_NAME"]) &
        (target["FOLDER_PATTERN"] == source["FOLDER_PATH_COB"]) &
        (target["FILE_EXTENSION"] == source["FILE_EXTENSION"]) &
        (target["FLAG_LAST_VERSION"] == lit(True))
        ,
        [when_not_matched().insert({
            "RAVEN_COBID" : source["RAVEN_COBID"],
            "STAGE_NAME" : source["STAGE_NAME"],
            "STAGE_URL" : source["STAGE_URL"],
            "FOLDER_PATTERN" : source["FOLDER_PATH_COB"],
            "FILE_EXTENSION" : source["FILE_EXTENSION"],
            "FILE_LIST" : source["FILE_LIST"],
            "PROCESS_TIMESTAMP" : source["PROCESS_TIMESTAMP"],
            "FLAG_LAST_VERSION" : source["FLAG_LAST_VERSION"]})
        ])


def run (session, stage, cobid_start, cobid_end):
    try:
        # Current timestamp
        sf_config = session.sql("SELECT sysdate() AS CURRENT_TIMESTAMP").collect()[0]
        start_timestamp = sf_config["CURRENT_TIMESTAMP"]

        # Get the list of fold patterns
        sf_staged_files = session\
            .table("RAVEN.VW_METADATA_STAGE_ME_PARAMETERS_COB") \
            .filter(col("RAVEN_COBID").between(cobid_start, cobid_end)) \
            .filter(f"STAGE_NAME LIKE '%{stage}%'") \
            .select("RAVEN_COBID","STAGE_NAME","FOLDER_PATH_COB","FILE_EXTENSION") \
            .sort(col("RAVEN_COBID").desc()) \
            .distinct()

        # Get the external/internal file list
        count_storage_pattern = 0
        for row in sf_staged_files.collect():
            cobid = row["RAVEN_COBID"]
            stage_name = row["STAGE_NAME"]
            folder_path_cob = row["FOLDER_PATH_COB"]
            file_extension = row["FILE_EXTENSION"]
            folder = (folder_path_cob[1:] if folder_path_cob.startswith('/') else folder_path_cob)
            pattern = folder + ".*." +file_extension

            # Get STAGE URL
            stage_desc = session.sql("DESC STAGE " + stage_name) \
                .filter(col('"property"') == "URL") \
                .select('"property_value"') \
                .collect()[0][0]
            stage_url = stage_desc.translate({ord(i): None for i in '[]"'})

            # List file in stage
            blob_list = list_blob(session,stage_name,pattern)
            file_list =  [r.as_dict() for r in blob_list]

            insert_list = {
                    "RAVEN_COBID" : cobid,
                    "STAGE_NAME" : stage_name,
                    "STAGE_URL" : stage_url,
                    "FOLDER_PATH_COB" : folder,
                    "FILE_EXTENSION" : file_extension,
                    "FILE_LIST" : file_list,
                    "PROCESS_TIMESTAMP" : start_timestamp,
                    "FLAG_LAST_VERSION" : True
                }
            merge_log(session,insert_list)
            count_storage_pattern +=1
        
        return f"{count_storage_pattern} storage pattern folders were listed"
        
    except SnowparkSQLException as e:
        raise e.message
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        raise exc_value

$$
;
