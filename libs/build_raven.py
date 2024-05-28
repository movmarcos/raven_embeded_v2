import sys, os, inspect, logging, time
from snowflake.snowpark.types import *
from snowflake.snowpark.functions import listagg

from create_calendar import create_calendar
from raven_app import RavenTargetDB as raven_app
from initialize_raven import InitializeRaven
from utils import get_project_root

class BuildRaven:
     
     def __init__(self, database_name = "", app_name = "", flag_metadata = False, flag_build = False, flag_grant = False, env_number = 0):
          self.database_name = database_name
          self.database_env = (database_name.split("_")[0]).upper()
          self.database_name_app = database_name.split("_")[1]
          self.app_name = app_name.lower()
          self.env_number = env_number
          self.flag_metadata = bool(flag_metadata)
          self.flag_build = bool(flag_build)
          self.flag_grant= bool(flag_grant)


     def build_raven(self):
          try:
               root_path = get_project_root()
               
               timestr = time.strftime("%Y%m%d-%H%M%S")
               logger = logging.getLogger(__name__)
               logger.setLevel(logging.DEBUG)
               log_path = f"{root_path}\\logs\\{self.app_name}"
               os.makedirs(log_path, exist_ok=True) 
               ch = logging.FileHandler(f"{log_path}\\{timestr}_initialize_raven.log")
               ch.setLevel(logging.DEBUG)
               ch.setFormatter(logging.Formatter('%(asctime)s: %(lineno)d - %(levelname)s - %(message)s'))
               logger.addHandler(ch)

               # Get config and Create session
               raven = raven_app(self.database_name,self.app_name)
               build_configs, raven_file_paths = raven.get_raven_file_paths()
               params = raven.get_db_parameters()
               session = raven.get_snowflake_session()

               init_raven = InitializeRaven(session, self.database_name,self.app_name,raven_file_paths)

               result_dict = {}

               if self.flag_build:
                    # Create schema RAVEN
                    create_schema = session.sql("CREATE SCHEMA IF NOT EXISTS RAVEN").collect()[0][0]
                    logger.debug('Create schema:: %s',create_schema)
                    result_dict["Create Schema"] = create_schema
                    session.use_schema("RAVEN")

                    print("Creating objects ...")
                    # Create objects
                    exec_models_result = init_raven.execute_commands("models")
                    logger.debug('Create objects | %s',exec_models_result)
                    result_dict["Create Objects"] = exec_models_result
                    
                    if self.database_env == "PROD":
                         print("Resume tasks ...")
                         result_tasks = session.call("RAVEN.ALTER_TASKS")
                         logger.debug('Resume tasks | %s',result_tasks)
                    
                    # Create Stages in STAGING schema
                    print("Creating stages ...")
                    exec_stages_result = session.call("RAVEN.CREATE_EXTERNAL_STAGE",'STAGING',self.env_number)
                    logger.debug('Create External Stages | %s',exec_stages_result)
                    result_dict["Create External Stages"] = exec_stages_result

               if self.flag_grant:                    
                    print("Grating permissions ...")
                    grant_result = init_raven.grant_permission_raven()
                    logger.debug('Grant permission on RAVEN schema| %s',grant_result)
                    result_dict["Grant permission on RAVEN schema"] = grant_result

               if self.flag_metadata:
                    print("Creating CALENDAR.csv")
                    df_calendar = create_calendar(params["country"],params["subdiv"],params["holiday_type"],params["fin_market"])
                    df_calendar.to_csv(f"{root_path}/{build_configs['seed-path']}/{self.app_name}/CALENDAR.csv",index=False)

                    print("Testing metadata ...")
                    result_validate_metadata = init_raven.build_metadata("test_metadata")
                    logger.debug('Test Metadata | %s',result_validate_metadata)
                    result_dict["Test Metadata"] = result_validate_metadata

                    print("Initializing metadata ...")
                    exec_metadata_result = init_raven.build_metadata("upload_metadata")
                    logger.debug('Staging Metadata | %s',exec_metadata_result)
                    result_dict["Staging Metadata"] = exec_metadata_result


               print("----------- PROCESS COMPLETED -----------")
               return result_dict

          except:
               exc_type, exc_value, exc_traceback = sys.exc_info()
               error_msg = f'{exc_type} - {exc_value}'
               logger.debug(error_msg)
               raise Exception(error_msg)
