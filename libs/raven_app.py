#%%
import os
import yaml
from mufg_snowflakeconn import sfconnection as m_sf
from snowflake.snowpark.session import Session
from utils import get_project_root

#%%
class RavenTargetDB:

    def __init__(self, database_name = "", app_name = ""):
        """
        Initializes RavenTargetDB instance
        
        Args:
            database_name: Name of the database 
            app_name: Name of the application
        
        Attributes:
            database_name: Name of the database
            environment: Environment extracted from database_name 
            environment_letter: First letter of environment
            app_name: Name of the application
            root_path: Project root path  
        """
        self.database_name = database_name
        self.environment = database_name.split("_")[0]
        self.environment_letter = self.environment[0]
        self.app_name = app_name
        self.root_path = get_project_root()
        

    def get_build_configs(self):
        """Gets the build config YAML file path specific to the Raven app."""
        return RavenTargetDB.open_config_file(f"{self.root_path}/configs/eraven.yml")


    def get_db_parameters(self) -> dict:
        """
        Gets database connection parameters from the config file
        specific to the current app and environment.
        
        Returns:
            dict: Dictionary containing database connection parameters.
        """
        
        config = RavenTargetDB.open_config_file(f"{self.root_path}/configs/app_config.yml")

        params_dict = {}
        params_dict["sf_account"] = config[self.app_name.upper()]['ACCOUNT']
        params_dict["holiday_type"] = config[self.app_name.upper()]['HOLIDAY_TYPE']
        params_dict["fin_market"] = config[self.app_name.upper()]['FINANCIAL_MARKET']
        params_dict["country"] = config[self.app_name.upper()]['COUNTRY']
        params_dict["subdiv"] = config[self.app_name.upper()]['SUBDIVISION']
        params_dict["user_name"] = config[self.app_name.upper()]['USER_NAME']\
            .format(self.environment_letter,self.app_name)        
        params_dict["conn_type"] = config[self.app_name.upper()]['CONNECTION_TYPE']
        params_dict["role"] = config[self.app_name.upper()]['ROLE']\
            .format(self.environment,self.app_name)
        params_dict["warehouse"] = config[self.app_name.upper()]['WAREHOUSE']\
            .format(self.environment,self.app_name)

        return params_dict

    def open_config_file(file_path):
        with open(file_path, "r") as stream:
            try:
                return yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)

    def get_raven_file_paths(self):    
        """
        Gets the file paths for Raven artifacts based on the build config.
    
        Filters the file paths to only include those for the current app, 
        and separates executable vs non-executable file extensions.
        
        Returns:
            build_configs: The parsed build config YAML
            raven_dict: A dict containing lists of executable and non-executable
                file paths, keyed by the path config names.
        """

        build_configs = RavenTargetDB.get_build_configs(self)

        path_dict = {key: build_configs[key] for key in build_configs if "-path" in key}
        apps = build_configs["apps"]
        upload_extensions = build_configs["non-executable-extensions"]
        not_selected_apps = list(filter(lambda x : x != self.app_name, apps))
        raven_dict = {}

        for key,item in path_dict.items():

            # Get all files
            file_list = [os.path.join(dirpath,f) for (dirpath, dirnames, filenames) in os.walk(f"{self.root_path}\\{item}") for f in filenames]
            # Get what is not app selected
            list_not_selected_app = [f for f in file_list for p in f.split("\\") if p in not_selected_apps]
            # Remove not app selected
            app_file_list = list(filter(lambda x : x not in list_not_selected_app, file_list))
            
            # Filter only non executable extensions (nee)
            objs_nee = [f_path for f_path in app_file_list for f in f_path.split(".") if f in upload_extensions]
            if len(objs_nee) > 0:
                raven_dict[f"{key}-nee"] = objs_nee
            
            # Filter executable files
            objs_ee = list(filter(lambda x : x not in objs_nee, app_file_list))
            if len(objs_ee) > 0:
                raven_dict[f"{key}"] = objs_ee
        
        return build_configs, raven_dict


    def get_snowflake_session(self) -> Session:    
        """
        Gets a Snowflake session object configured for the app.
        Returns:
            session: A Snowflake session object
        """

        params_dict = RavenTargetDB.get_db_parameters(self)

        if params_dict["conn_type"] == 'externalbrowser':
            mufgconn = m_sf.MufgSnowflakeConn(self.environment,params_dict["user_name"])
            session = mufgconn.get_snowflake_session_ext()

        elif params_dict["conn_type"] == 'privatekey':
            mufgconn = m_sf.MufgSnowflakeConn(self.environment,params_dict["user_name"])
            session = mufgconn.get_snowflake_session()

        # Set session with Role, Database and Warehouse
        session.use_role(params_dict["role"])
        session.use_database(self.database_name)
        session.use_warehouse(params_dict["warehouse"])

        return session
