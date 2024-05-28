"""
The purpose of this InitializeRaven class is to initialize and set up the Raven application in a Snowflake database.

It takes as input:
- A Snowflake session object to connect to the database
- The name of the database to use
- The name of the Raven application
- A dictionary of file paths pointing to different Raven components like models, connectors, etc.

It doesn't directly output anything, but sets up the database by:
- Uploading Raven files from the provided file paths into internal Snowflake stages
- Executing commands from the uploaded Raven files to create tables, views, procedures etc.
- Granting permissions to a role so it can access the Raven schema

The main logic flows are:

1. The constructor which saves the input parameters as attributes of the InitializeRaven instance. 
It also tells the Snowflake session which database to use.

2. The upload_files method which loops through the provided file paths, uploads each file to a Snowflake internal stage, 
and returns a dictionary of info about each uploaded file.

3. The execute_commands method which loops through Raven files, executes the SQL commands in them to create database objects, 
and returns a dictionary of the execution results.

4. The grant_permission_raven method which grants permissions to a role so it can access the Raven schema where these objects are created.

5. There are also methods to build metadata about the Raven setup.

So in summary, this InitializeRaven class handles all the initial setup steps needed to get a Raven application ready to use inside a Snowflake database. 
The methods upload necessary files, create database objects by executing those files, and set permissions.
"""
import sys
from snowflake.snowpark.types import *
from snowflake.snowpark.functions import listagg

from metadata import BuildMetadata
from create_tables import create_table
from create_calendar import create_calendar
from utils import get_project_root

class InitializeRaven:
     
    def __init__(self, session, database_name, app_name = "", raven_file_paths = ""):
        self.database_name = database_name
        self.app_name = app_name.lower()
        self.raven_file_paths = raven_file_paths
        self.session = session
        self.session.use_database(database_name)
        
    def upload_files(self,build_process: str) -> dict:
        """
        Uploads Raven component files to a Snowflake internal stage.
        
        This method loops through the provided file paths for Raven components like models, 
        connectors etc., uploads each file to a Snowflake internal stage, and returns a 
        dictionary containing metadata about each uploaded file.
        
        The filepath for the stage is constructed by removing the filename, replacing the 
        project root path, and joining the remaining path segments. The file is then uploaded
        using the Snowflake PUT command.
        
        Parameters:
            build_process (str): The type of Raven component (e.g. "models"). Used to filter
                                filepaths.
        
        Returns:
            dict: Metadata about each uploaded file, with the filename as the key.
        """
        root_path = get_project_root()

        upload_result = {}
  
        upload_files = [f for key,value in self.raven_file_paths.items() if "nee" in key for f in value for p in f.split("\\") if p == build_process]
        upload_files = [x for x in upload_files if "__pycache__" not in x]

        for file_obj in upload_files:
            # Get only file name
            filename = file_obj.split("\\")[-1]

            # Prepare the path in the Intenal Stage
            filepath = file_obj.replace(filename,"").replace(root_path,"")
            filepath = filepath.split("\\")
            filepath_stage = "/".join([f for f in filepath if f.upper() != self.app_name.upper()])
            filepath_stage = f"@RAVEN.INTSTAGE_RAVEN_FILES{filepath_stage}"

            # Upload the file
            file_upload = self.session.file.put(file_obj, filepath_stage, auto_compress=False, overwrite=True)
            upload_result[filename.split(".")[0]] = file_upload[0]._asdict()
        
        return upload_result


    def execute_commands(self,build_process: str) -> dict:
        """
        Executes Raven component SQL files.

        This function executes the SQL files for different Raven components like models, connectors etc. 
        It first filters the file paths to only include the relevant component type based on the build_process parameter.

        The files are then sorted if it is a models build, to ensure they are created in the correct sequence.

        Each file is then executed using Snowflake SQL commands and the results are captured in a dictionary.
        """

        exec_files = [f for key,value in self.raven_file_paths.items() if "nee" not in key for f in value for p in f.split("\\") if p == build_process]
        exec_files = [x for x in exec_files if "__pycache__" not in x]

        if build_process == 'models':
            seq_objects = ['file_formats', 'tables', 'functions', 'views', 'table_functions', 'procedures', 'tasks']
            values = set(map(lambda x:x.split("\\")[-2], exec_files))
            dict_objs = {seq_objects.index(x):[y for y in exec_files if y.split("\\")[-2]==x] for x in values}
            newlist = []
            for key,item in sorted(dict_objs.items()):
                newlist = newlist + item
            exec_files = newlist                                                      

        exec_result = {}
        for file_name in exec_files:
            print(f"File executed: {file_name}")
            with open(file_name, encoding="utf8") as f: obj_cmd = f.read()

            if any("tables" == s for s in file_name.split("\\")):
                exec_cmd = create_table(self.session,obj_cmd)
            else:
                exec_cmd = self.session.sql(obj_cmd).collect()[0][0]

            exec_result[file_name.split(".")[0]] = exec_cmd
        
        return exec_result

    def grant_permission_raven(self) -> dict:
        """
        Grants permissions to the RAVEN schema and objects for the given role.

        This grants usage and privileges on the RAVEN schema, procedures, 
        functions, file formats, stages, tables, and views to the provided role name.

        It also grants privileges to create tables and full access to the STAGING schema.

        The grants are executed and results are captured in a dictionary.
        """

        env = self.database_name.split("_")[0]

        batch_role_list = [f"{env}_RAVEN_BATCH", f"{env}_RAPTOR_BATCH"]
        rw_role_list = [f"{env}_{self.app_name}_RW"]

        batch_permission_list = [
        "grant USAGE on SCHEMA RAVEN TO ROLE |:role_name:|",

        "grant USAGE on ALL PROCEDURES IN SCHEMA RAVEN TO ROLE |:role_name:|",

        "grant USAGE on ALL FUNCTIONS IN SCHEMA RAVEN TO ROLE |:role_name:|",

        "grant USAGE on ALL FILE FORMATS IN SCHEMA RAVEN TO ROLE |:role_name:|",

        "grant READ on ALL STAGES in SCHEMA RAVEN TO ROLE |:role_name:|",

        "grant DELETE,INSERT,SELECT,UPDATE on ALL TABLES IN SCHEMA RAVEN TO ROLE |:role_name:|",

        "grant SELECT on ALL VIEWS IN SCHEMA RAVEN TO ROLE |:role_name:|",

        "grant CREATE TABLE ON SCHEMA STAGING TO ROLE |:role_name:|",

        "grant USAGE on ALL STAGES in SCHEMA STAGING TO ROLE |:role_name:|",

        "grant ALL PRIVILEGES on SCHEMA STAGING TO ROLE |:role_name:|",

        "grant DELETE,INSERT,SELECT,UPDATE on ALL TABLES IN SCHEMA STAGING TO ROLE |:role_name:|"

        ]

        rw_permission_list = [
        "grant ALL PRIVILEGES on SCHEMA STAGING TO ROLE |:role_name:|",

        "grant DELETE,INSERT,SELECT,UPDATE on ALL TABLES IN SCHEMA STAGING TO ROLE |:role_name:|"

        ]

        grant_dict = {
            "rw": {"role_list": rw_role_list, "permission_list": rw_permission_list}, 
            "batch": {"role_list": batch_role_list, "permission_list": batch_permission_list}
        }

        grant_result = {}
        for role_type,role_data in grant_dict.items():
            for role in role_data["role_list"]:
                print(f"---------------- {role} ----------------")
                for permission in role_data["permission_list"]:
                    permission = permission.replace("|:role_name:|", role)
                    print(permission)
                    try:
                        grant_return = self.session.sql(permission).collect()[0][0]
                        grant_result[permission] = grant_return
                    except:
                        exc_type, exc_value, exc_traceback = sys.exc_info()
                        error_msg = f'ERROR | {exc_type} - {exc_value}'
                        grant_result[grant_cmd] = error_msg

        return grant_result
        
    def build_metadata(self,step: str) -> dict:
        """
        Builds Snowflake metadata for the given database and saves it to the metadata table.
        
        Args:
          step: The step to perform - either "test_metadata" or "upload_metadata". 
        
        Returns:
          The results of the metadata test or upload.
        """
        metadata = BuildMetadata(self.session, self.database_name,self.app_name)
        if step == "test_metadata":
            return metadata.test_metadata()
        elif step == "upload_metadata":
            return metadata.upload_metadata()
