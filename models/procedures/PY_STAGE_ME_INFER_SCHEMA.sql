/**
 * Creates or replaces a stored procedure to stage data from a file into Snowflake.
 * 
 * Parameters:
 * - FILE_NAME: Name of the file to stage from 
 * - FOLDER_PATH: Path to the file, excluding container/stage name
 * - CONTAINER: External stage container or leave empty for internal stage 
 * - EVENT_TRIGGER: JSON describing what triggered the staging
 * - FLAG_CREATE_TABLE: Whether to create a staging table based on metadata
 * - FLAG_CREATE_PIPE: Whether to create a Snowpipe instead of COPY command
 * - FLAG_CREATE_CSV_SELECT: Whether to infer CSV header positions
 * - DATABASE_TARGET: Target database, defaults to current if empty
*/
CREATE OR REPLACE PROCEDURE RAVEN.PY_STAGE_ME_INFER_SCHEMA(
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
PACKAGES = ('snowflake-snowpark-python','pandas','numpy')
HANDLER = 'run'
EXECUTE AS caller
AS
$$

# Imports
import sys, re, random, json
import pandas as pd
from collections import Counter
from snowflake.snowpark.functions import col, concat_ws, lit, when_matched, when_not_matched
from snowflake.snowpark.types import StructField, StructType, StringType
from snowflake.snowpark import DataFrame

def run(session,file_name, folder_path, container, event_trigger, flag_create_table, flag_create_pipe, flag_create_csv_mapping, database_target):
	stage_me = StageMe(session,file_name, folder_path, container, event_trigger, flag_create_table, flag_create_pipe, flag_create_csv_mapping, database_target)
	return stage_me.run()


class StageMe:
    def __init__(self,session, file_name, folder_path, container, event_trigger, flag_create_table, flag_create_pipe, flag_create_csv_mapping, database_target):
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
        self.session = session
        self.file_name = file_name
        self.folder_path = folder_path
        self.container = container
        self.event_trigger = event_trigger
        self.flag_create_table = flag_create_table
        self.flag_create_pipe = flag_create_pipe
        self.flag_create_csv_mapping = flag_create_csv_mapping
        self.database_target = database_target
        self.process_parameters = {}
        self.insert_log = {}
        self.process_result = {}
        self.log_id = -1
        self.return_result = {}

        # Remove any "/" at the beginning and end
        self.source_folder = "/".join(filter(None,self.folder_path.split("/")))

    def list_blob(self,stage_name: str,pattern: str) -> list:
        """
        List file(s) in the datalake according to pattern used to copy

        stage_name: Stage name where the file was trigger
        pattern: path and file name
        """
        cmd_list_file = f"LIST @{stage_name} pattern = '{pattern}'"
        sf_blob_list = self.session.sql(cmd_list_file).collect()
        return sf_blob_list

    def is_cob(value: str) -> str:
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

    def get_warehouse_name(self,size: str,database_name: str) -> str:
        """
        Base on the size and database name, finds the warehouse name
        return Warehouse Name

        size: Value configured in the metadata STAGE_ME_PARAMETERS. If empty, the default value it is XS
        database_name: database destination for the file
        """
        # Get first and second part of the database name
        arr_db_name = database_name.split("_")
        wh_like = arr_db_name[0] + "%" + arr_db_name[1]

        # Show warehouses that matches with database name
        cmd_list_wh = f"SHOW WAREHOUSES LIKE '%{wh_like}_%'"
        sf_show_wh = self.session.sql(cmd_list_wh)

        # Filter by size from metadata value
        size = size if size else 'X-Small'
        sf_list_wh = sf_show_wh.filter(col('"size"') == size)

        # If does not retunr anything, use current connect WH
        if len(sf_list_wh.collect()) == 0:
            sf_list_wh = sf_show_wh.filter(col('"is_current"') == 'Y')

        wh_name = sf_list_wh.select(col('"name"')).collect()[0][0]

        return wh_name

    def create_table(self,table_name: str,df_schema: str) -> str:
        """
        Base on the size and database name, finds the warehouse name

        return Message from the command Create or Alter table

        table_name: Name of the table configured in SOURCE_FILE. If the schema desti
        df_schema: dataframe with schema
        """
        db_target = table_name.split('.')[0]
        list_column_name_target = [x for x in list(df_schema['COLUMN_NAME_TARGET']) if x is not None]
        list_create_column_name_target = [x for x in list(df_schema['CREATE_COLUMN_NAME_TARGET']) if x is not None]

        # Get the column names if the table exists
        cmd_table_check = f""" SELECT 
                                    listagg('"'||UPPER(COLUMN_NAME)||'"', ',') within group (order by ORDINAL_POSITION ASC) LIST_COLUMN_NAME
                                FROM {db_target}.INFORMATION_SCHEMA."COLUMNS"
                                WHERE UPPER(CONCAT_WS('.',TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME)) = UPPER('{table_name}')
                            GROUP BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
                            """
        table_check = self.session.sql(cmd_table_check).collect()

        return_msg = ""
        list_column_name_target = list(map(lambda x: x.upper(), list_column_name_target))

        # If there is a return from INFORMATION_SCHEMA, goes to the next step to check if it is necessary altering the table
        if len(table_check) > 0:
            if df_schema.shape[0] == 0:
                return_msg = "The table already exists, but the schema could not be validated since the file provided is empty"
            else:
                list_column_name_original = (table_check[0]["LIST_COLUMN_NAME"]).split(",")
                # Check for new columns
                new_columns = [item.upper() for item in list_column_name_target if item not in list_column_name_original]

                # Alter the table 
                if len(new_columns) > 0:
                    for idx,cl in enumerate(list_column_name_target):
                        if cl.upper() in new_columns:
                            cmd_alter = f" ALTER TABLE {table_name} ADD {list_create_column_name_target[idx]}"
                            return_alter = self.session.sql(cmd_alter).collect()[0]
                    return_msg = "Alter table executed"
        else:
            if df_schema.shape[0] == 0:
                return_msg = "No metadata was found and schema inference did not return any results. It will be necessary to manually create the staging table" 
                raise Exception(f"No columns to create staging table {table_name}: {return_msg}")
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

                sf_table_create = self.session.sql(cmd_create_table).collect()[0]
                return_msg = sf_table_create.as_dict()
            
        return return_msg

    def merge_log(self):
        """
        Insert or Update log table RAVEN.LOG_STAGE_ME_STATUS

        """
        target = self.session.table("RAVEN.LOG_STAGE_ME_STATUS")
        source = self.session.create_dataframe([self.insert_log])

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

    def generate_log_id(self,start_timestamp: str,cobid: str) -> int:
        """
        Generages a random ID for the table RAVEN.LOG_STAGE_ME_STATUS
        return a random number
        start_timestamp,cobid,file_name and folder_path are used for the hash code
        """
        # Generate Log ID
        random_num = random.randint(-1000000000000,1000000000000)
        log_id = hash( (start_timestamp,cobid,self.file_name,self.folder_path,random_num) )
        return log_id

    def get_container_name(self):
        # Non PROD Environments have the environment version. Example: test3-<dataset name>, where "test3" is the version
        env_with_version = ["int_","dvlp","test","rlse"]
        arr_container_name = self.container.split("-")
        container_first_part = "int_" if "int" in arr_container_name[0] else arr_container_name[0]

        # Get container name without environment version
        container_name = self.container.replace(f"{arr_container_name[0]}-", "") if container_first_part[:4] in env_with_version else self.container
        return container_name

    def split_join_str_uppercase(ini_str: str) -> str:
        normal_string = ""
        if ini_str.upper() == "COBID":
            normal_string = "COBID"
        else:
            # A list of special characters
            special_characters=['@','#','$','*','&','-','(',')','[',']','+','/','\\','|','{','}','<','>','?','!','~','ï','Ï','»','¿','þ','ÿ',"'"]

            # using lambda function to find if the special characters are in the list
            # Converting list to string using the join method
            normal_string = "".join(filter(lambda char: char not in special_characters , ini_str))
            normal_string = re.sub(' +', ' ',normal_string)
            normal_string = normal_string.replace(' ','_')
            split = [s for s in re.split('([A-Z]{1,}[^A-Z]*)', normal_string) if s]
            normal_string = "_".join(split).upper()
            normal_string = re.sub('_+', '_',normal_string)
            normal_string = (normal_string[1:0] if normal_string[0] == "_" else normal_string)

        return normal_string

    def infer_schema(self,src_file_field: str,stage_name: str,pattern_file_parse: str,file_format: str,target_table: str,file_format_type: str):

        def isNaN(num):
            return num != num

        # Get all metadata Fields
        list_column_name_source = src_file_field["LIST_COLUMN_NAME_SOURCE"]
        list_column_name_target = src_file_field["LIST_COLUMN_NAME_TARGET"]
        list_create_column_name_target = src_file_field["LIST_CREATE_COLUMN_NAME_TARGET_NO_DERIVED_EXPRESSION"]
        list_column_unknown_position_source_transform = src_file_field["LIST_COLUMN_UNKNOWN_POSITION_SOURCE_TRANSFORM"]
        list_extra_field_derived_expression = src_file_field["LIST_EXTRA_FIELD_DERIVED_EXPRESSION"]
        list_extra_derived_expression = src_file_field["LIST_EXTRA_DERIVED_EXPRESSION"]
        list_extra_create_field_derived_expression = src_file_field["LIST_EXTRA_CREATE_FIELD_DERIVED_EXPRESSION"]

        # Make all arrays the same size
        arrays = [list_column_name_source,list_column_name_target,list_create_column_name_target,list_column_unknown_position_source_transform]
        max_length = 0
        for array in arrays:
            max_length = max(max_length, len(array))
        for array in arrays:
            array += [None] * (max_length - len(array))

        # Create pandas dataframe
        df_metadata = pd.DataFrame({
            'COLUMN_NAME_SOURCE': list_column_name_source, 
            'COLUMN_NAME_TARGET': list_column_name_target,
            'CREATE_COLUMN_NAME_TARGET': list_create_column_name_target,
            'COLUMN_UNKNOWN_POSITION_SOURCE_TRANSFORM': list_column_unknown_position_source_transform
            })

        file_format_parse = f'{file_format}_PARSE_HEADER' if file_format_type.upper() == "CSV" else file_format
        
        # Infer Schema from file
        cmd_infer = f"""
            SELECT 
            FILENAMES,TYPE, 
            TRIM(COLUMN_NAME) AS COLUMN_NAME_SOURCE,         
                'NULLIF('||SPLIT_PART(EXPRESSION,'::',1)||','''')'||'::'||TYPE||' AS "'||COLUMN_NAME_SOURCE||'"'  AS SELECT_EXPRESSION,
                SPLIT_PART(EXPRESSION,'::',1) AS SELECT_POSITION
            FROM TABLE(
                INFER_SCHEMA(
                LOCATION=>'@{stage_name}/{pattern_file_parse}'
                , FILE_FORMAT=>'RAVEN.{file_format_parse}'
                , MAX_RECORDS_PER_FILE => 10000
                )
            )
        """
        # Transform to pandas dataframe
        df_infer = self.session.sql(cmd_infer).to_pandas()
        
        # Split uppercase strings
        df_infer["COLUMN_NAME_TARGET_INFER"] = '"'+ df_infer["COLUMN_NAME_SOURCE"].map(StageMe.split_join_str_uppercase) + '"' 
        df_infer["CREATE_FIELD"] = df_infer["COLUMN_NAME_TARGET_INFER"] + ' ' + df_infer["TYPE"]

        df_metadata['COLUMN_NAME_SOURCE']=df_metadata['COLUMN_NAME_SOURCE'].astype(str)
        df_infer['COLUMN_NAME_SOURCE']=df_infer['COLUMN_NAME_SOURCE'].astype(str)

        # Join data from infer schema with metadata
        df_merge = df_infer.merge(df_metadata, on='COLUMN_NAME_SOURCE', how='left')

        # Replace "|:REPLACE_POSITION:|" with "SELECT_POSITION" in column: COLUMN_UNKNOWN_POSITION_SOURCE_TRANSFORM
        df_merge['COPY_INTO_SELECT'] = df_merge.apply(lambda x: None
                                                        if isNaN(x["COLUMN_UNKNOWN_POSITION_SOURCE_TRANSFORM"]) 
                                                        else (str(x["COLUMN_UNKNOWN_POSITION_SOURCE_TRANSFORM"])).replace("|:REPLACE_POSITION:|",x["SELECT_POSITION"]), 
                                                        axis=1)

        # Replace missing values with "SELECT_EXPRESSION" in column: 'COPY_INTO_SELECT'
        df_merge['COPY_INTO_SELECT'] = df_merge['COPY_INTO_SELECT'].fillna(df_merge['SELECT_EXPRESSION'])

        # Drop column: 'COLUMN_UNKNOWN_POSITION_SOURCE_TRANSFORM'
        df_merge.drop(columns=['COLUMN_UNKNOWN_POSITION_SOURCE_TRANSFORM'],inplace=True)

        # Replace missing values with "COLUMN_NAME_TARGET_INFER" in column: 'COLUMN_NAME_TARGET'
        df_merge['COLUMN_NAME_TARGET'] = df_merge['COLUMN_NAME_TARGET'].fillna(df_merge['COLUMN_NAME_TARGET_INFER'])

        # Replace missing values with "CREATE_FIELD" in column: 'CREATE_COLUMN_NAME_TARGET'
        df_merge['CREATE_COLUMN_NAME_TARGET'] = df_merge['CREATE_COLUMN_NAME_TARGET'].fillna(df_merge['CREATE_FIELD'])

        # Create lists
        df_mapping = df_merge[['COLUMN_NAME_TARGET','COPY_INTO_SELECT','CREATE_COLUMN_NAME_TARGET','SELECT_POSITION']]

        # Create Dataframe for Expressions
        select_position = [None] * len(list_extra_field_derived_expression)
        df_expression = pd.DataFrame({
            'COLUMN_NAME_TARGET': list_extra_field_derived_expression,
            'COPY_INTO_SELECT': list_extra_derived_expression,
            'CREATE_COLUMN_NAME_TARGET': list_extra_create_field_derived_expression,
            'SELECT_POSITION': select_position
            })

        df_schema = pd.concat([df_mapping, df_expression], 
                  ignore_index = True)

        # Create staging table
        if bool(self.flag_create_table) and (target_table.split('_')[0]).upper() == "DVLP":
            create_table_result = StageMe.create_table(self,target_table,df_schema)           
            ##~ Log Information ~##
            self.process_result["create_table_result"] = create_table_result
        elif bool(self.flag_create_table) and (target_table.split('_')[0]).upper() != "DVLP":
            ##~ Log Information ~##
            self.process_result["create_table_result"] = "Staging table creation requested, but creation is only permitted in then DVLP environment."
        else:
            ##~ Log Information ~##
            self.process_result["create_table_result"] = "Staging table creation not requested."
        
        # Match the source header with table fields    
        df_match = StageMe.match_by_column_name(self,df_schema,target_table)

        mapping_target_columns = [x for x in list(df_match['COLUMN_NAME_TARGET']) if x is not None]
        mapping_source_columns = [x for x in list(df_match['COPY_INTO_SELECT']) if x is not None]
        mapping_create_columns = [x for x in list(df_match['CREATE_COLUMN_NAME_TARGET']) if x is not None]
        mapping_source_seq = [x for x in list(df_match['SELECT_POSITION']) if x is not None]

        return mapping_target_columns, mapping_source_columns, mapping_create_columns, mapping_source_seq

    def match_by_column_name(self,df_mapping: DataFrame,target_table: str) -> DataFrame:
        table_split = target_table.split('.')

        sf_columns = self.session.table('INFORMATION_SCHEMA.COLUMNS') \
            .filter(f"""
                upper(TABLE_SCHEMA) =  upper('{table_split[1]}')
            AND upper(TABLE_NAME) = upper('{table_split[2]}')
                """) \
            .sort("ORDINAL_POSITION") \
            .select((col("COLUMN_NAME")).as_("COLUMN_NAME_TARGET"))

        df_columns = sf_columns.to_pandas()
        df_columns = df_columns.apply(lambda x: '"'+x+'"')
        df = df_mapping.merge(df_columns, on='COLUMN_NAME_TARGET', how='inner')
        return df
        
    # TODO: Create a return and test the file
    def rewrite_files(self,stage_name: str,pattern_file_parse: str,skip_header: str):
        # Define the schema for the data in the CSV file.
        schema = StructType([StructField("all", StringType())])
        file = f'{stage_name}/{pattern_file_parse}'
        df = session.read.options(
            {'skip_header': skip_header, 'field_delimiter': '¬'}).schema(user_schema).csv(file)

        file_name = pattern_file_parse.split('\\')[-1]
        unload_location = f'{stage_name}/{pattern_file_parse}'
        df.write.copy_into_location(
                unload_location,
                file_format_type="csv",
                format_type_options=dict(
                    compression="none",
                    field_delimiter="\t",
                ),
                single=False,
            )
        
    def run(self):
        # Main function

        try:
            ###################################################### START: Prepare and Validate basic values ######################################################

            # Get initial timestamp, current database and session id
            sf_config = self.session.sql("""SELECT sysdate() AS CURRENT_TIMESTAMP,current_database() AS CURRENT_DATABASE,current_session() AS CURRENT_SESSION""").collect()[0]
            start_timestamp = sf_config["CURRENT_TIMESTAMP"]
            current_database = sf_config["CURRENT_DATABASE"]
            env_database = current_database.split("_")[0]
            session_id = sf_config["CURRENT_SESSION"]
            self.process_result["session_id"] = session_id
            
            # Get COBID from the folder path or file name
            full_file_path =  "/".join([self.folder_path.rstrip('/'),self.file_name])
            cob_list = [x for x in [int(s) for s in re.split('_|-|/|\.', full_file_path) if s.isdigit()] if x > 19000100]
            cobid = cob_list[0] if len(cob_list) > 0 else "19000101"

            # If parameter database_target is empty, use current DB (connection db)
            db_target = current_database if not self.database_target else self.database_target

            # Get container name without environment version
            container_name = StageMe.get_container_name(self)

            # Convert ADF Evente Trigger values to Dictonary
            lst = [x.split(":") for x in self.event_trigger.split(",")]
            dct_et = {lst[i][0]: lst[i][1] for i in range(0, len(lst), 1)} if lst[0][0] else {'Adf': 'none'}

            # Procedure Parameters

            self.process_parameters = {
                "container_name": container_name,
                "event_trigger": dct_et,
                "file_stage_me": f"{self.source_folder}/{self.file_name}",
                "file_name": self.file_name,
                "folder_path": self.source_folder,
                "database_target": db_target,
                "flag_create_table": self.flag_create_table,
                "flag_create_csv_mapping": self.flag_create_csv_mapping
            }

            ###################################################### END: Prepare and Validate basic values ######################################################

            
            # Check if the file has metadata
            sf_smp_cob = self.session.table("RAVEN.VW_METADATA_STAGE_ME_PARAMETERS_COB") \
                .filter(f"""IS_ENABLED = TRUE 
                        AND RAVEN_COBID = {cobid}
                        AND UPPER('{self.file_name}') LIKE ARRAY_TO_STRING(SPLIT(UPPER(FILE_NAME_COB),'*'),'%')
                        AND UPPER('/{self.source_folder}/') LIKE UPPER(FOLDER_PATH_COB||'%')
                        AND IFNULL(NULLIF(CONTAINER_NAME,''), 'NO CONTAINER') = IFNULL(NULLIF('{container_name}',''),'NO CONTAINER')
                        """).collect()

            len_sf_smp_cob = len(sf_smp_cob)

            if len_sf_smp_cob == 0:
                self.log_id = StageMe.generate_log_id(self,start_timestamp,cobid)
                self.insert_log = {
                        "ID" : self.log_id,
                        "RAVEN_COBID" : cobid,
                        "DATASET_NAME" : None,
                        "PROCESS_PARAMETERS" : self.process_parameters,
                        "PROCESS_RESULT" : None,
                        "PROCESS_STATUS" : "FAILED",
                        "START_TIMESTAMP" : start_timestamp,
                        "END_TIMESTAMP" : None,
                        "BLOB_FILE" : None
                    }
                msg_error_metadata = f"There is no metadata for the folder/file: {self.source_folder}/{self.file_name}"
                raise Exception(msg_error_metadata)
            
            for sf_smp in sf_smp_cob:
            
                self.log_id = StageMe.generate_log_id(self,start_timestamp,cobid)

                is_trigger_file = bool(sf_smp["IS_TRIGGER_FILE"])                   # If FALSE, read the file triggered direct, else read all files in the folder
                dataset_name = sf_smp["DATASET_NAME"]                               # RAVEN.METADATA_STAGE_ME_PARAMETERS Primary Key. It defines a unique file in data lake       
                file_name_pipe = sf_smp["SOURCE_FILE_NAME_PATTERN"]                 # File name pattern
                staging_scope_fields = json.loads(sf_smp["STAGING_SCOPE_FIELDS"])   # DEPARTMENT_CODE, ENTITY_CODE, MARKET, REGION
                allow_infer_schema = bool(sf_smp["ALLOW_INFER_SCHEMA"])             # If FALSE, the file must have all the fields configured in metadata
                
                ##~ Log Information ~##
                self.process_result["source_system_code"] = sf_smp["SOURCE_SYSTEM_CODE"] # SOURCE_SYSTEM_CODE
                self.process_result["source_feed_code"] = sf_smp["SOURCE_FEED_CODE"]     # SOURCE_FEED_CODE

                # Insert Log
                self.insert_log = {
                    "ID" : self.log_id,
                    "RAVEN_COBID" : cobid,
                    "DATASET_NAME" : dataset_name,
                    "PROCESS_PARAMETERS" : self.process_parameters,
                    "PROCESS_RESULT" : None,
                    "PROCESS_STATUS" : "RUNNING",
                    "START_TIMESTAMP" : start_timestamp,
                    "END_TIMESTAMP" : None,
                    "BLOB_FILE" : None
                }
                StageMe.merge_log(self)

                # Table destination setup
                src_file_field = json.loads(sf_smp["SOURCE_FILE_AND_FIELD"])
                target_table = db_target + "." + src_file_field["DESTINATION_FULL_TABLE_NAME"]
                flag_delete_by_file_name = src_file_field["DELETE_STAGE_BY_FILE_NAME"] 
                stage_name = src_file_field["STAGE_NAME"]
                file_format = src_file_field["FILE_FORMAT"]
                skip_row_on_error = int(src_file_field["SKIP_ROW_ON_ERROR"])

                ### Path and Pattern of the file ###
                # If it is TRIGGER file, the pattern is found in RAVEN.METADATA_SOURCE_FILE table
                pattern_file_name = file_name_pipe if bool(is_trigger_file) else self.file_name
                self.source_folder = self.source_folder if self.source_folder != "" else ".*"
                pattern_file = f"{self.source_folder}/{pattern_file_name}"
                pattern_file_parse = self.source_folder if bool(is_trigger_file) else pattern_file
                
                # Get file details: last_modified, md5, name, size
                blob_list = StageMe.list_blob(self,stage_name,pattern_file)
                file_list =  [r.as_dict() for r in blob_list]
                blob_file = {"FILE_LIST": file_list, "FILE_COUNT":len(file_list)}
                self.insert_log["BLOB_FILE"] = blob_file
                
                if len(file_list) == 0:
                    raise Exception(f"File not found: {pattern_file}")

                # Get file format information
                sf_file_format_check = self.session.table("INFORMATION_SCHEMA.FILE_FORMATS")\
                    .filter(col("FILE_FORMAT_NAME") == file_format)\
                    .filter(col("FILE_FORMAT_SCHEMA") == "RAVEN")\
                    .collect()
                if sf_file_format_check:
                    sf_file_format = sf_file_format_check[0]
                else:
                    raise Exception(f"File Format not found: {file_format}")

                file_format_type = sf_file_format["FILE_FORMAT_TYPE"]
                skip_header = sf_file_format["SKIP_HEADER"]
                
                # If header is not the first row, re-write the file to skip the rows before
                if self.flag_create_csv_mapping == True and skip_header > 1:
                    StageMe.rewrite_files(self,stage_name,pattern_file_parse)

                mapping_source_columns = []
                mapping_target_columns = []
                mapping_source_seq = []

                if len(file_list) > 0 and self.flag_create_csv_mapping == True and skip_header != 0 and allow_infer_schema: 
                # Infer Schema if the file format has header (SKIPE_HEADER = 1 or NULL) 
                    mapping_target_columns, mapping_source_columns, mapping_create_columns, mapping_source_seq = StageMe.infer_schema(self,src_file_field,stage_name,pattern_file_parse,file_format,target_table,file_format_type)

                else: 
                # If not using the file to get the field position, use it from the metadata
                    mapping_target_columns = src_file_field["LIST_COLUMN_NAME_TARGET"]+src_file_field["LIST_EXTRA_FIELD_DERIVED_EXPRESSION"]
                    mapping_source_columns = src_file_field["LIST_COLUMN_POSITION_SOURCE_TRANSFORM"]+src_file_field["LIST_EXTRA_DERIVED_EXPRESSION"]
                    mapping_create_columns = src_file_field["LIST_CREATE_COLUMN_NAME_TARGET_NO_DERIVED_EXPRESSION"]+src_file_field["LIST_EXTRA_CREATE_FIELD_DERIVED_EXPRESSION"]

                # Set parameter ON_ERROR
                on_error = f"on_error = 'skip_file_{str(skip_row_on_error)}'" if skip_row_on_error > 0 else ""
                
                # Fields from file
                target_columns_file = ",".join(mapping_target_columns)
                source_columns_file = ",".join(mapping_source_columns)
                source_columns_file_seq = ",".join(mapping_source_seq)

                ##~ Log Information ~##
                self.process_result["source_columns_file"] = ",".join(mapping_target_columns)
                self.process_result["source_columns_file_seq"] = source_columns_file_seq
                self.process_result["source_columns_file_transformed"] = source_columns_file
                self.process_result["target_columns_file"] = target_columns_file

                # Raven metadata fields
                target_columns_raven = "RAVEN_COBID,RAVEN_STAGE_SCOPE_FIELDS,RAVEN_FILENAME,RAVEN_FILE_ROW_NUMBER,RAVEN_STAGE_TIMESTAMP,RAVEN_DATASET_NAME"
                source_columns_raven = f"RAVEN.FN_FIND_COB(metadata$filename) AS RAVEN_COBID,{staging_scope_fields}::OBJECT AS RAVEN_STAGE_SCOPE_FIELDS,metadata$filename AS RAVEN_FILENAME,metadata$file_row_number AS RAVEN_FILE_ROW_NUMBER,current_timestamp AS RAVEN_STAGE_TIMESTAMP,'{dataset_name}' AS RAVEN_DATASET_NAME"

                # Join fields for target and sources
                target_columns = ",".join( filter(None,[target_columns_file, target_columns_raven]) )
                source_columns = ",".join( filter(None,[source_columns_file, source_columns_raven]) )

                # Build file format and pattern
                ff_and_pattern = f" (file_format => '{file_format}', pattern => '.*{pattern_file}') "

                # Select data from file based on pattern
                cmd_select = f" SELECT {source_columns} FROM @{stage_name} {ff_and_pattern} "
                
                ##~ Log Information ~##
                self.process_result["cmd_select"] = cmd_select
                self.process_result["target_table"] = target_table
                
                # Create COPY command that can be used for Snowpipe and direct copy
                cmd_copy = f" COPY INTO {target_table} ({target_columns}) FROM ({cmd_select}) {on_error}"
                
                # Set RAVEN schema and Begin Transaction
                self.session.sql("USE SCHEMA RAVEN").collect()
                self.session.sql('BEGIN TRANSACTION').collect()

                # Copy command with FORCE = TRUE to allow load same file (re-run)
                cmd = cmd_copy + " FORCE = TRUE " if bool(sf_smp["ALLOW_RELOAD"]) else ""

                # Set Warehouse
                warehouse_size = sf_smp["WAREHOUSE_SIZE"]
                warehouse_name = StageMe.get_warehouse_name(self,warehouse_size,current_database)
                self.session.sql(f"USE WAREHOUSE {warehouse_name}").collect()

                ##~ Log Information ~##
                self.process_result["warehouse_size"] = warehouse_size
                self.process_result["warehouse_name"] = warehouse_name

                ##### Delete older version of the file in the staging table #####
                raven_file =  f"{self.source_folder}/{self.file_name}".replace("/", "")

                delete_option = f"replace(RAVEN_FILENAME,'/','') = '{raven_file}'" if flag_delete_by_file_name else f"RAVEN_DATASET_NAME = '{dataset_name}'"

                check_delete_result = self.session.table(target_table) \
                    .filter(f"RAVEN_COBID = {cobid}") \
                    .filter(delete_option) \
                    .count()
                
                cmd_delete = f" DELETE FROM {target_table} WHERE RAVEN_COBID = {cobid} AND {delete_option}"

                if check_delete_result > 0:
                    delete_result = self.session.sql(cmd_delete).collect()   
                    msg_delete_result = delete_result[0].as_dict()
                else:
                    msg_delete_result = "{'number of rows deleted': 0}"

                ##~ Log Information ~##
                self.process_result["cmd_delete"] = cmd_delete
                self.process_result["cmd_copy"] = cmd
                self.process_result["msg_delete_result"] = msg_delete_result
                    
                # Execute copy command
                copy_result = self.session.sql(cmd).collect()
                msg_copy_result = [r.as_dict() for r in copy_result]

                ##~ Log Information ~##
                self.process_result["msg_copy_result"] = msg_copy_result
                self.insert_log["PROCESS_RESULT"] = self.process_result
                self.insert_log["PROCESS_STATUS"] = "SUCCESS"
                self.insert_log["is_error"] = False
                self.insert_log["END_TIMESTAMP"] = self.session.sql("SELECT sysdate() AS CURRENT_TIMESTAMP").collect()[0][0]
            
                StageMe.merge_log(self)
                self.return_result[self.log_id] = "SUCCESS"
                self.session.sql('COMMIT').collect()

        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.session.sql('ROLLBACK').collect()
            ##~ Log Information ~##
            self.process_result["exception"] = str(exc_value)
            self.process_result["is_error"] = True

            self.insert_log["PROCESS_RESULT"] = self.process_result
            self.insert_log["PROCESS_STATUS"] = "FAILED"
            self.insert_log["END_TIMESTAMP"] = self.session.sql("SELECT sysdate() AS CURRENT_TIMESTAMP").collect()[0][0]
            StageMe.merge_log(self)
            self.return_result[self.log_id] = "FAILED"
            raise exc_type(exc_value).with_traceback(exc_traceback)
        return self.return_result


$$
;
