/**
 * Creates or replaces a stored procedure to stage metadata files.
 *
 * @param FILE_NAME - The file name to stage. 
 * @param FOLDER_PATH - The folder path where the file is located. 
 * @param CONTAINER - The container where the file is located.
 * @param EVENT_TRIGGER - The event that triggered this procedure.
 * @param FLAG_CREATE_TABLE - Flag to create a table.
 * @param FLAG_CREATE_PIPE - Flag to create a pipe.
 * @param FLAG_CREATE_CSV_SELECT - Flag to create CSV select.
 * @param DATABASE_TARGET - The target database.
 */
CREATE OR REPLACE PROCEDURE RAVEN.PY_STAGE_ME(
    "FILE_NAME" VARCHAR(16777216), 
    "FOLDER_PATH" VARCHAR(16777216), 
    "CONTAINER" VARCHAR(16777216),
    "EVENT_TRIGGER" VARCHAR(16777216),
    "FLAG_CREATE_TABLE" BOOLEAN, 
    "FLAG_CREATE_PIPE" BOOLEAN, 
    "FLAG_CREATE_CSV_SELECT" BOOLEAN,
    "DATABASE_TARGET" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS caller
AS
$$

import json
import random
import sys
import re
from collections import Counter
from snowflake.snowpark.functions import col, sql_expr, parse_json, concat_ws, lit, when_matched, when_not_matched
from snowflake.snowpark.types import *

def list_blob(session,stage_name,pattern):
    """
    List file(s) in the datalake according to pattern used to copy

    session: session connection
    stage_name: Stage name where the file was trigger
    pattern: path and file name
    """
    cmd_list_file = f"LIST @{stage_name} pattern = '{pattern}'"
    sf_blob_list = session.sql(cmd_list_file).collect()
    return sf_blob_list

def is_cob(value):
    """
    If it is number, it means it is COB or part of the COB path.
    Example: hd_input/data/2023/202306/20230626
    The value in this function has to be only part of the pathe at the time
    How to use it:
    path_list = pattern_file.split("/")
    path_no_cob = [is_cob(i) for i in path_list]

    return the number or .*

    value: part of the file path (file path splitted by "/")
    """
    return value if not str.isdigit(value) else ".*"

def get_warehouse_name(session,size,database_name):
    """
    Base on the size and database name, finds the warehouse name

    return Warehouse Name

    session: session connection
    size: Value configured in the metadata STAGE_ME_PARAMETERS. If empty, the default value it is XS
    database_name: database destination for the file
    """
    # Get first and second part of the database name
    arr_db_name = database_name.split("_")
    wh_like = arr_db_name[0] + "%" + arr_db_name[1]

    # Show warehouses that matches with database name
    cmd_list_wh = f"SHOW WAREHOUSES LIKE '%{wh_like}_%'"
    sf_show_wh = session.sql(cmd_list_wh)

    # Filter by size from metadata value
    size = size if size else 'X-Small'
    sf_list_wh = sf_show_wh.filter(col('"size"') == size)

    # If does not retunr anything, use current connect WH
    if len(sf_list_wh.collect()) == 0:
        sf_list_wh = sf_show_wh.filter(col('"is_current"') == 'Y')

    wh_name = sf_list_wh.select(col('"name"')).collect()[0][0]

    return wh_name

def create_table(session,db_target,table_name,list_column_name_target,list_create_column_name_target):
    """
    Base on the size and database name, finds the warehouse name

    return Message from the command Create or Alter table

    session: session connection
    db_target: database destination for the file
    table_name: Name of the table configured in SOURCE_FILE. If the schema desti
    list_column_name_target: List of column names
    list_create_column_name_target: List of column names with data type
    """

    # Get the column names if the table exists
    cmd_table_check = f""" SELECT listagg('"'||UPPER(COLUMN_NAME)||'"', ',') within group (order by ORDINAL_POSITION ASC) LIST_COLUMN_NAME
                             FROM {db_target}.INFORMATION_SCHEMA."COLUMNS"
                            WHERE UPPER(CONCAT_WS('.',TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME)) = UPPER('{table_name}')
                         GROUP BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
                        """
    table_check = session.sql(cmd_table_check).collect()

    return_msg = ""
    # If there is a return from INFORMATION_SCHEMA, goes to the next step to check if it is necessary altering the table
    if len(table_check) > 0:
        list_column_name_original = (table_check[0]["LIST_COLUMN_NAME"]).split(",")
        # Check for new columns
        new_columns = [item.upper() for item in list_column_name_target if item not in list_column_name_original]

        # Alter the table 
        if len(new_columns) > 0:
            msg_error_table = f"{len(new_columns)} present in the metadata and not found in DB Target. This process does not have permission to alter the table."
            raise Exception(msg_error_table)
            """for idx,cl in enumerate(list_column_name_target):
                if cl.upper() in new_columns:
                    cmd_alter = f" ALTER TABLE {table_name} ADD {list_create_column_name_target[idx]}"
                    return_alter = session.sql(cmd_alter).collect()[0]
            return_msg = "Alter table executed" """
    else:
        create_column_names = ",".join(list_create_column_name_target)
        cmd_create_table = f""" CREATE TABLE IF NOT EXISTS {table_name}
                            (RAVEN_COBID INT, 
                            RAVEN_STAGE_SCOPE_FIELDS VARIANT, 
                            {create_column_names}, 
                            RAVEN_FILENAME STRING, 
                            RAVEN_FILE_ROW_NUMBER INT, 
                            RAVEN_STAGE_TIMESTAMP TIMESTAMP, 
                            RAVEN_DATASET_NAME VARCHAR(500) )"""

        sf_table_create = session.sql(cmd_create_table).collect()[0]
        return_msg = sf_table_create.as_dict()
        
    return return_msg

def merge_log(session,insert_log):
    """
    Insert or Update log table RAVEN.LOG_STAGE_ME_STATUS

    session: session connection
    insert_log (Dictionary): Detail about the COPY process 
    """
    target = session.table("RAVEN.LOG_STAGE_ME_STATUS")
    source = session.create_dataframe([insert_log])

    target.merge(source, target["ID"] == source["ID"],
        [when_matched().update({
            "DATASET_NAME" : source["DATASET_NAME"],
            "PROCESS_PARAMETERS" : source["PROCESS_PARAMETERS"],
            "PROCESS_RESULT" : source["PROCESS_RESULT"],
            "PROCESS_STATUS" : source["PROCESS_STATUS"],
            "START_TIMESTAMP" : source["START_TIMESTAMP"],
            "END_TIMESTAMP" : source["END_TIMESTAMP"],
            "BLOB_FILE" : source["BLOB_FILE"]}), 
        when_not_matched().insert({
            "ID" : source["ID"],
            "RAVEN_COBID" : source["RAVEN_COBID"],
            "DATASET_NAME" : source["DATASET_NAME"],
            "PROCESS_PARAMETERS" : source["PROCESS_PARAMETERS"],
            "PROCESS_RESULT" : source["PROCESS_RESULT"],
            "PROCESS_STATUS" : source["PROCESS_STATUS"],
            "START_TIMESTAMP" : source["START_TIMESTAMP"],
            "END_TIMESTAMP" : source["END_TIMESTAMP"],
            "BLOB_FILE" : source["BLOB_FILE"]})
        ])

def csv_mapping_file(list_csv_header,src_file_filed):
    csv_mapping_target_columns = []
    csv_mapping_source_seq = []
    csv_mapping_source_columns = []
    
    list_column_name_source = src_file_filed["LIST_COLUMN_NAME_SOURCE"]
    list_column_name_target = src_file_filed["LIST_COLUMN_NAME_TARGET"]
    list_data_type_name = src_file_filed["LIST_DATA_TYPE_NAME"]
    list_derived_expression = src_file_filed["LIST_DERIVED_EXPRESSION"]
    list_column_unknown_position_source_transform = src_file_filed["LIST_COLUMN_UNKNOWN_POSITION_SOURCE_TRANSFORM"]
    datetime_format = src_file_filed["TIMESTAMP_INPUT_FORMAT"]
    date_format = src_file_filed["DATE_INPUT_FORMAT"]

    for idx, field in enumerate(list_column_name_source): # loop for all fields listed in the metadata
        col_pos =[index for (index, item) in enumerate(list_csv_header) if item.replace('"',"").strip().upper() == field.strip().upper()] # Find the field position on the header
        
        if len(col_pos) == 1: # Can only find one field that matches: source metadata field name == header field name
            col_pos = col_pos[0]
            
            if list_derived_expression[idx]: # If source field has expression, add it to "csv_mapping_source_columns" and replace the field name with position
                file_position = "NULLIF($" + str(col_pos + 1) + ",'')" # Force NULL for empty values
                csv_mapping_source_columns.append(list_derived_expression[idx].replace(field,file_position))
            else: # Get the convert data from metadata configuration
                csv_mapping_source_columns.append(list_column_unknown_position_source_transform[idx].replace('|:REPLACE_POSITION:|','$' + str(col_pos + 1)))

            csv_mapping_target_columns.append(list_column_name_target[idx])
            csv_mapping_source_seq.append("$" + str(col_pos + 1))

    return csv_mapping_source_columns, csv_mapping_target_columns, csv_mapping_source_seq

def generate_log_id(start_timestamp,cobid,file_name,folder_path):
    """
    Generages a random ID for the table RAVEN.LOG_STAGE_ME_STATUS

    return a random number

    start_timestamp,cobid,file_name and folder_path are used for the hash code
    """
    # Generate Log ID
    random_num = random.randint(-1000000000000,1000000000000)
    log_id = hash( (start_timestamp,cobid,file_name,folder_path,random_num) )
    return log_id

def remove_bom_char(text_value):
    """
    Remove any BOM character from a string
    It works for: UTF-8[a], UTF-16 (BE), UTF-16 (LE)

    return Strging without BOM characters

    text_value: String with that contains BOM characters
    """
    return text_value.replace("ï»¿","").replace("þÿ","").replace("ÿþ","")


def run(session, file_name, folder_path, container, event_trigger, flag_create_table, flag_create_pipe, flag_create_csv_mapping, database_target):
    # Main function
    """ 
    session: mandatory parameter
    file_name: Name of the file in the datalake or internal stage
    folder_path: path without container or stage
    container: External Stage: container where the file is located | Internal Stage: pass empty string
    event_trigger: JSON Format | Indicates where the trigger started

    flag_create_table: If True, create the staging table based on metadata target fields
    flag_create_pipe: If True, create Snowpipe instead of execute the command COPY
    flag_create_csv_mapping: If True, get the file header position from the first row of the file
    database_target: If empty, use the current DB, else use the value in the parameter
    """
    process_parameters = {}
    insert_log = {}
    process_result = {}
    log_id = -1
    return_result = {}
    try:
        ###################################################### START: Prepare and Validate basic values ######################################################

        # Get initial timestamp, current database and session id
        sf_config = session.sql("SELECT sysdate() AS CURRENT_TIMESTAMP, current_database() AS CURRENT_DATABASE, current_session() AS CURRENT_SESSION").collect()[0]
        start_timestamp = sf_config["CURRENT_TIMESTAMP"]
        current_database = sf_config["CURRENT_DATABASE"]
        env_database = current_database.split("_")[0]
        session_id = sf_config["CURRENT_SESSION"]
        process_result["session_id"] = session_id
        
        # Get COBID from the folder path or file name
        full_file_path =  "/".join([folder_path.rstrip('/'),file_name])
        cob_list = [x for x in [int(s) for s in re.split('_|-|/|\.', full_file_path) if s.isdigit()] if x > 19000100]
        cobid = cob_list[0] if len(cob_list) > 0 else "19000101"

        # If parameter database_target is empty, use current DB (connection db)
        db_target = current_database if not database_target else database_target

        # Non PROD Environments have the environment version. Example: test3-<dataset name>, where "test3" is the version
        env_with_version = ["int_","dvlp","test","rlse"]
        arr_container_name = container.split("-")
        container_first_part = "int_" if "int" in arr_container_name[0] else arr_container_name[0]

        # Get container name without environment version
        container_name = container.replace(f"{arr_container_name[0]}-", "") if container_first_part[:4] in env_with_version else container
        
        # Remove any "/" at the beginning and end
        source_folder = "/".join(filter(None,folder_path.split("/")))

        # Convert ADF Evente Trigger values to Dictonary
        lst = [x.split(":") for x in event_trigger.split(",")]
        dct_et = {lst[i][0]: lst[i][1] for i in range(0, len(lst), 1)} if lst[0][0] else {'Adf': 'none'}

        # Procedure Parameters
        process_parameters = {
            "container_name": container_name,
            "event_trigger": dct_et,
            "file_stage_me": f"{source_folder}/{file_name}",
            "file_name": file_name,
            "folder_path": source_folder,
            "database_target": db_target,
            "flag_create_table": flag_create_table,
            "flag_create_pipe":flag_create_pipe,
            "flag_create_csv_mapping":flag_create_csv_mapping
        }

        ###################################################### END: Prepare and Validate basic values ######################################################

        
        # Check if the file has metadata
        sf_smp_cob = session.table("RAVEN.VW_METADATA_STAGE_ME_PARAMETERS_COB")
        sf_smp_cob = sf_smp_cob \
            .filter(f"""IS_ENABLED = TRUE 
                    AND RAVEN_COBID = {cobid}
                    AND UPPER('{file_name}') LIKE ARRAY_TO_STRING(SPLIT(UPPER(FILE_NAME_COB),'*'),'%')
                    AND UPPER('/{source_folder}/') LIKE UPPER(FOLDER_PATH_COB||'%')
                    AND IFNULL(NULLIF(CONTAINER_NAME,''), 'NO CONTAINER') = IFNULL(NULLIF('{container_name}',''),'NO CONTAINER')
                    """).collect()

        len_sf_smp_cob = len(sf_smp_cob)

        if len_sf_smp_cob == 0:
            log_id = generate_log_id(start_timestamp,cobid,file_name,folder_path)
            insert_log = {
                    "ID" : log_id,
                    "RAVEN_COBID" : cobid,
                    "DATASET_NAME" : None,
                    "PROCESS_PARAMETERS" : process_parameters,
                    "PROCESS_RESULT" : None,
                    "PROCESS_STATUS" : "FAILED",
                    "START_TIMESTAMP" : start_timestamp,
                    "END_TIMESTAMP" : None,
                    "BLOB_FILE" : None
                }
            msg_error_metadata = f"There is no metadata for the folder/file: {source_folder}/{file_name}"
            raise Exception(msg_error_metadata)
        
        for sf_smp in sf_smp_cob:
        
            log_id = generate_log_id(start_timestamp,cobid,file_name,folder_path)

            is_trigger_file = bool(sf_smp["IS_TRIGGER_FILE"])                   # If FALSE, read the file triggered direct, else read all files in the folder
            dataset_name = sf_smp["DATASET_NAME"]                               # RAVEN.METADATA_STAGE_ME_PARAMETERS Primary Key. It defines a unique file in data lake       
            file_name_pipe = sf_smp["SOURCE_FILE_NAME_PATTERN"]                 # File name pattern
            staging_scope_fields = json.loads(sf_smp["STAGING_SCOPE_FIELDS"])   # DEPARTMENT_CODE, ENTITY_CODE, MARKET, REGION
            
            # Insert Log
            insert_log = {
                "ID" : log_id,
                "RAVEN_COBID" : cobid,
                "DATASET_NAME" : dataset_name,
                "PROCESS_PARAMETERS" : process_parameters,
                "PROCESS_RESULT" : None,
                "PROCESS_STATUS" : "RUNNING",
                "START_TIMESTAMP" : start_timestamp,
                "END_TIMESTAMP" : None,
                "BLOB_FILE" : None
            }
            merge_log(session,insert_log)

            # Table destination setup
            src_file_filed = json.loads(sf_smp["SOURCE_FILE_AND_FIELD"])
            target_table = db_target + "." + src_file_filed["DESTINATION_FULL_TABLE_NAME"]
            flag_delete_by_file_name = src_file_filed["DELETE_STAGE_BY_FILE_NAME"] 
            stage_name = src_file_filed["STAGE_NAME"]
            file_format = src_file_filed["FILE_FORMAT"]
            skip_row_on_error = int(src_file_filed["SKIP_ROW_ON_ERROR"])
            list_column_name_source = src_file_filed["LIST_COLUMN_NAME_SOURCE"]
            list_column_name_target = src_file_filed["LIST_COLUMN_NAME_TARGET"]
            list_create_column_name_target = src_file_filed["LIST_CREATE_COLUMN_NAME_TARGET"]
            list_extra_field_derived_expression = src_file_filed["LIST_EXTRA_FIELD_DERIVED_EXPRESSION"]
            list_extra_derived_expression = src_file_filed["LIST_EXTRA_DERIVED_EXPRESSION"]

            ### Path and Pattern of the file ###
            # If it is TRIGGER file or PIPE, the pattern is found in RAVEN.METADATA_SOURCE_FILE table
            pattern_file_name = file_name_pipe if bool(is_trigger_file) or bool(flag_create_pipe) else file_name
            source_folder = source_folder if source_folder != "" else ".*"
            pattern_file = f"{source_folder}/{pattern_file_name}"


            # Get file format information
            sf_file_format_check = session.table("INFORMATION_SCHEMA.FILE_FORMATS")\
                .filter(col("FILE_FORMAT_NAME") == file_format)\
                .filter(col("FILE_FORMAT_SCHEMA") == "RAVEN")\
                .collect()
            if sf_file_format_check:
                sf_file_format = sf_file_format_check[0]
            else:
                raise Exception(f"File Format not found: {file_format}")

            file_format_type = sf_file_format["FILE_FORMAT_TYPE"]
            skip_header = sf_file_format["SKIP_HEADER"]
            file_delimiter = sf_file_format["FIELD_DELIMITER"]

            mapping_source_columns = []
            mapping_target_columns = []
            mapping_source_seq = []
            
            if flag_create_csv_mapping == True and skip_header == 1 : # Create CSV mapping based on file header and metadata
                # Select header
                cmd_select_header = f"SELECT $1 AS HEADER FROM @{stage_name} (FILE_FORMAT => 'RAVEN.TEXT_FORMAT_NO_HEADER', pattern => '.*{pattern_file}') LIMIT 1"
                header_return = session.sql(cmd_select_header).collect()

                # Find the field position and convert in date, time and other types
                if header_return:
                    header = header_return[0]["HEADER"]
                    csv_header = remove_bom_char(header)
                    list_csv_header = csv_header.split(file_delimiter)
                    list_csv_header = [x.strip() for x in list_csv_header]
                    process_result["csv_header"] = csv_header

                    if list_column_name_target: # Check if there is field metadata
                        mapping_source_columns, mapping_target_columns, mapping_source_seq = csv_mapping_file(list_csv_header,src_file_filed)
                    
                    else:
                        mapping_source_seq = ['$'+str(index+1) for (index,item) in enumerate(list_csv_header)]
                        mapping_source_columns = ["NULLIF($" + str(index+1) + ",'')::STRING AS " + f'"{item}"'  for (index,item) in enumerate(list_csv_header)]
                        mapping_target_columns = ['"'+re.sub('[^A-Za-z0-9]+', '_', item.upper())+'"' for item in list_csv_header]
                        list_column_name_target  = mapping_target_columns 
                        list_column_name_source = list_csv_header
                        list_create_column_name_target = [f'{item} STRING' for item in mapping_target_columns]

                else:
                    raise Exception("File not found in the location. Enable to create the SELECT statement from file header.")

            else: # If not using the file to get the field position, use it from the metadata
                mapping_target_columns = list_column_name_target
                mapping_source_columns = src_file_filed["LIST_COLUMN_POSITION_SOURCE_TRANSFORM"]
            
             # Create staging table
            if bool(flag_create_table): 
                create_table_result = create_table(session,db_target,target_table,list_column_name_target,list_create_column_name_target)
                process_result["create_table_result"] = create_table_result

            # Remove COB from path, If Snowpipe
            if bool(flag_create_pipe):
                path_list = pattern_file.split("/")
                path_no_cob = [is_cob(i) for i in path_list]        # Replace number (COB) with ".*" in the path
                pattern_file = "/".join(path_no_cob)                # Rejoin the path without COB in the path
                #pattern_file = f"pipelocalfolder/{pattern_file}"   # Temporary configuration where it is configure the QUEUE 

            # Set parameter ON_ERROR
            on_error = f"on_error = 'skip_file_{str(skip_row_on_error)}'" if skip_row_on_error > 0 else ""
            
            # Fields from file
            target_columns_file = ",".join(mapping_target_columns)
            source_columns_file = ",".join(mapping_source_columns)
            source_columns_file_seq = ",".join(mapping_source_seq)

            ##~ Log Information ~##
            process_result["source_columns_file"] = ",".join(list_column_name_source)
            process_result["source_columns_file_seq"] = source_columns_file_seq
            process_result["source_columns_file_transformed"] = source_columns_file
            process_result["target_columns_file"] = target_columns_file

            # Case there are extra fields as expression for only Target fields 
            source_columns_expression = ",".join(list_extra_derived_expression)
            target_columns_expression = ",".join(list_extra_field_derived_expression)

            # Raven metadata fields
            target_columns_raven = "RAVEN_COBID,RAVEN_STAGE_SCOPE_FIELDS,RAVEN_FILENAME,RAVEN_FILE_ROW_NUMBER,RAVEN_STAGE_TIMESTAMP,RAVEN_DATASET_NAME"
            source_columns_raven = f"RAVEN.FN_FIND_COB(metadata$filename) AS RAVEN_COBID,{staging_scope_fields}::OBJECT AS RAVEN_STAGE_SCOPE_FIELDS,metadata$filename AS RAVEN_FILENAME,metadata$file_row_number AS RAVEN_FILE_ROW_NUMBER,current_timestamp AS RAVEN_STAGE_TIMESTAMP,'{dataset_name}' AS RAVEN_DATASET_NAME"

            # Join fields for target and sources
            target_columns = ",".join( filter(None,[target_columns_file, target_columns_raven, target_columns_expression]) )
            source_columns = ",".join( filter(None,[source_columns_file, source_columns_raven, source_columns_expression]) )

            # Build file format and pattern
            ff_and_pattern = f" (file_format => '{file_format}', pattern => '.*{pattern_file}') "

            # Select data from file based on pattern
            cmd_select = f" SELECT {source_columns} FROM @{stage_name} {ff_and_pattern} "
            
            ##~ Log Information ~##
            process_result["cmd_select"] = cmd_select
            process_result["target_table"] = target_table
            
            # Create COPY command that can be used for Snowpipe and direct copy
            cmd_copy = f" COPY INTO {target_table} ({target_columns}) FROM ({cmd_select}) {on_error}"
            
            # Set RAVEN schema
            session.sql("USE SCHEMA RAVEN").collect()

            if bool(flag_create_pipe) : # Create PIPE
                cmd = f"CREATE PIPE IF NOT EXISTS {dataset_name} AUTO_INGEST = TRUE INTEGRATION = '{env_database}_SNOWPIPE_RAPTOR' AS {cmd_copy}"
                
                ##~ Log Information ~##
                process_result["cmd_pipe"] = cmd

            else: # Run COPY INTO command
                # Get file details: last_modified, md5, name, size
                blob_list = list_blob(session,stage_name,pattern_file)
                file_list =  [r.as_dict() for r in blob_list]
                blob_file = {"FILE_LIST": file_list, "FILE_COUNT":len(file_list)}
                insert_log["BLOB_FILE"] = blob_file

                # Copy command with FORCE = TRUE to allow load same file (re-run)
                cmd = cmd_copy + " FORCE = TRUE " if bool(sf_smp["ALLOW_RELOAD"]) else ""

                # Set Warehouse
                warehouse_size = sf_smp["WAREHOUSE_SIZE"]
                warehouse_name = get_warehouse_name(session,warehouse_size,current_database)
                session.sql(f"USE WAREHOUSE {warehouse_name}").collect()

                process_result["warehouse_size"] = warehouse_size
                process_result["warehouse_name"] = warehouse_name

                ##### Delete older version of the file in the staging table ####
                raven_file =  f"{source_folder}/{file_name}".replace("/", "")

                delete_option = f"replace(RAVEN_FILENAME,'/','') = '{raven_file}'" if flag_delete_by_file_name else f"RAVEN_DATASET_NAME = '{dataset_name}'"

                check_delete_result = session.table(target_table) \
                    .filter(f"RAVEN_COBID = {cobid}") \
                    .filter(delete_option) \
                    .count()
                
                cmd_delete = f" DELETE FROM {target_table} WHERE RAVEN_COBID = {cobid} AND {delete_option}"

                if check_delete_result > 0:
                    delete_result = session.sql(cmd_delete).collect()   
                    msg_delete_result = delete_result[0].as_dict()
                else:
                    msg_delete_result = "{'number of rows deleted': 0}"

                ##~ Log Information ~##
                process_result["cmd_delete"] = cmd_delete
                process_result["cmd_copy"] = cmd
                process_result["msg_delete_result"] = msg_delete_result
                
            # Execute copy command
            copy_result = session.sql(cmd).collect()
            msg_copy_result = [r.as_dict() for r in copy_result]

            ##~ Log Information ~##
            process_result["msg_copy_result"] = msg_copy_result
            insert_log["PROCESS_RESULT"] = process_result
            insert_log["PROCESS_STATUS"] = "SUCCESS"
            insert_log["is_error"] = False
            insert_log["END_TIMESTAMP"] = session.sql("SELECT sysdate() AS CURRENT_TIMESTAMP").collect()[0][0]
        
            merge_log(session,insert_log)
            return_result[log_id] = "SUCCESS"

    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        process_result["exception"] = str(exc_value)
        process_result["is_error"] = True
        insert_log["PROCESS_RESULT"] = process_result
        insert_log["PROCESS_STATUS"] = "FAILED"
        insert_log["END_TIMESTAMP"] = session.sql("SELECT sysdate() AS CURRENT_TIMESTAMP").collect()[0][0]
        merge_log(session,insert_log)
        return_result[log_id] = "FAILED"
        raise exc_type(exc_value).with_traceback(exc_traceback)
    return return_result


$$
;
