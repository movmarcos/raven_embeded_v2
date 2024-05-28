"""
This code is for the upload_metadata method of the BuildMetadata class.

The purpose of this method is to upload metadata CSV files from a specified seed path into Snowflake tables, execute associated SQL scripts to merge the uploaded data, and return a dictionary containing the executed SQL file paths and command results.

It takes the following inputs:

self: The BuildMetadata object instance, which contains attributes like the Snowflake session and paths to the metadata files.
It produces the following output:

A dictionary with keys as the merged SQL file paths, and values as the SQL command execution results.
The logic works as follows:

- Initialize an empty dictionary to store the execution results.
- Get a list of all CSV file names in the seed path folder.
- Loop through each CSV file name.
- Extract the metadata name from the CSV file name by splitting on '.'
- Build the full path to the CSV file.
- Read the CSV into a Pandas DataFrame.
- Write the DataFrame into a Snowflake temporary table named with the metadata name.
- Build the path to the associated SQL merge script file using the metadata name.
- Open the SQL file, read the contents into a string, and execute it via Snowflake.
- Store the SQL execution result in the dictionary, using the SQL file path as the key.
- Return the final dictionary containing all SQL execution results after looping through all CSVs.
- This allows the metadata CSVs to be uploaded and merged in a simple automated way. The SQL execution results are captured to enable checking for any errors.

"""
import os, sys
from snowflake.snowpark.functions import col, concat_ws, lit, listagg, upper
import pandas as pd
from utils import get_project_root

class BuildMetadata:
     
    def __init__(self, session, database_name, app_name = ""):
        self.database_name = database_name
        self.app_name = app_name.lower()
        self.root_path = get_project_root()
        self.session = session
        self.session.use_database(database_name)
        self.seed_path = f'{self.root_path}\\metadata\\seeds\\{self.app_name}'
        self.source_file_path = f'{self.seed_path}\\SOURCE_FILE.csv'
        self.source_field_path = f'{self.seed_path}\\SOURCE_FIELD.csv'
        self.stage_me_parameters_path = f'{self.seed_path}\\STAGE_ME_PARAMETERS.csv'
    
    def test_metadata(self):
        # Validates metadata by reading in CSVs, checking for duplicates, 
        # comparing to database objects, and raising errors if issues are found.
        try:
            # SOURCE_FILE.csv
            df_source_file = pd.read_csv(self.source_file_path)
            # SOURCE_FIELD.csv
            df_source_field = pd.read_csv(self.source_field_path)
            # STAGE_ME_PARAMETERS.csv
            df_stage_me_parameters = pd.read_csv(self.stage_me_parameters_path)

            # Get distinct value for FILE FORMAT and STAGE
            list_src_file_format = df_source_file["FILE_FORMAT"].drop_duplicates().tolist()
            list_src_stage = df_source_file["STAGE_NAME"].drop_duplicates().tolist()


            # List all FILE FORMATS and STAGES in RAVEN schema
            df_show_file_format = self.session.sql("SHOW FILE FORMATS IN SCHEMA RAVEN") # Get file formats create by RAVEN
            df_stage = self.session.sql("SHOW STAGES IN DATABASE") # Get the list of stages
            df_stage = df_stage.with_column('"full_stage_name"', concat_ws(lit("."),df_stage['"schema_name"'],df_stage['"name"']))

            list_sf_file_format = pd.DataFrame(df_show_file_format.collect()).name.tolist()
            list_sf_stage = pd.DataFrame(df_stage.collect()).full_stage_name.tolist()

            ############################# Validate Metadata #############################

            file_format_not_valid = [x for x in list_src_file_format if x not in list_sf_file_format]
            stage_not_valid = [x for x in list_src_stage if x not in list_sf_stage]

            # Raise an exception if there are FILE FORMATS and/or STAGES listed in the metadata and not present in RAVEN schema
            if (len(file_format_not_valid) > 0 or len(stage_not_valid) > 0) :
                err_fileformat = "File Formats: " + ",".join(file_format_not_valid)
                err_stages = "Stages: " + ",".join(stage_not_valid)
                error_message = "Metadata not valid: \n" + "\n".join([err_stages,err_fileformat])
                raise ValueError(error_message)

            pk_source_file = df_source_file[["SOURCE_SYSTEM_CODE", "SOURCE_FEED_CODE"]]
            dpk_source_file = pk_source_file[pk_source_file.duplicated(keep=False)]

            pk_source_field = df_source_field[["SOURCE_SYSTEM_CODE", "SOURCE_FEED_CODE","SOURCE_FIELD_NAME"]]
            dpk_source_field = pk_source_field[pk_source_field.duplicated(keep=False)]

            pk_target_field = df_source_field[["SOURCE_SYSTEM_CODE", "SOURCE_FEED_CODE","TARGET_FIELD_NAME"]]
            dpk_target_field = pk_target_field[pk_target_field.duplicated(keep=False)]

            pk_stage_me_parameters = df_stage_me_parameters[["DATASET_NAME"]]
            dpk_stage_me_parameters = pk_stage_me_parameters[pk_stage_me_parameters.duplicated(keep=False)]

            if (len(dpk_source_file) > 0 or len(dpk_source_field) > 0 or len(dpk_target_field) > 0 or len(dpk_stage_me_parameters) > 0):
                err_source_file = "CSV SOURCE_FILE: " + str(dpk_source_file.to_dict('index'))
                err_source_field_src = "CSV SOURCE FIELD SRC: " + str(dpk_source_field.to_dict('index'))
                err_source_field_tgt = "CSV SOURCE FIELD TGT: " + str(dpk_target_field.to_dict('index'))
                err_stage_me_parameters = "CSV STAGE ME PARAMETERS: " + str(dpk_stage_me_parameters.to_dict('index'))
                error_message = "Metadata not valid. Duplicate keys for: \n" + "\n".join([err_source_file,err_source_field_src,err_source_field_tgt,err_stage_me_parameters])
                raise ValueError(error_message)
            
            return  "Metadata was successfully verified."
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            raise exc_value

    def upload_metadata(self):
        # Uploads metadata CSV files from the seed path into Snowflake, executes 
        # associated SQL scripts to merge the data, and returns a dict of the
        # executed SQL file paths and command results.
        try:
            
            exec_result = {}
            seed_files = [f for (dirpath, dirnames, filenames) in os.walk(f"{self.seed_path}") for f in filenames]
            self.session.use_schema("RAVEN")

            for csv_name in seed_files:
                metadata_file = csv_name.split('.')[0]
                csv_path = f"{self.seed_path}\\{csv_name}"
                df_metadata = pd.read_csv(csv_path)
                self.session.write_pandas(df_metadata, f"TEMP_METADATA_SRC_{metadata_file}", auto_create_table=True, overwrite=True, table_type="temporary")

                sql_path = f"{self.root_path}\\metadata\\merging\\{metadata_file}.sql"
                with open(sql_path) as f: obj_cmd = f.read()
                exec_cmd = self.session.sql(obj_cmd).collect()[0][0]
                exec_result[sql_path.split(".")[0]] = exec_cmd
                print(f"File executed: {sql_path}")
                
            return exec_result
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            raise exc_value
