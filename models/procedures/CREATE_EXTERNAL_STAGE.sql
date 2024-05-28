CREATE OR REPLACE PROCEDURE RAVEN.CREATE_EXTERNAL_STAGE("SCHEMA_DESTINATION" VARCHAR,"ENV_VERSION_NUMBER" INT)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS caller
AS
$$

import pandas as pd
import sys
from snowflake.snowpark.exceptions import SnowparkSQLException

def run(session,schema_destination,env_version_number):
    try:
        
        # Current Database Info
        environment_sql = """SELECT current_database() AS CURRENT_DB, 
                                    SPLIT_PART(current_database(),'_', 1) AS ENVIRONMENT,
                                    SPLIT_PART(current_database(),'_', 2) AS DB_TYPE,
                                    IFNULL(TRY_TO_NUMBER(SPLIT_PART(current_database(),'_', 3)),1) AS DB_VERSION 
                        """
        environment = session.sql(environment_sql).collect()[0]

        curr_db = environment["CURRENT_DB"]
        env = environment["ENVIRONMENT"]
        db_type = environment["DB_TYPE"]
        db_version_number = int(environment["DB_VERSION"])

        # Use the db number if the proc parameter is equal to 0
        version_number = db_version_number if env_version_number == 0 else int(env_version_number)

        # If version is equal 1, do not use numbers 
        # (Exception for dvlp environment in quic plus datalake - changed in the CREATE STAGE command)
        env_version = (env if version_number == 1 else f"{env}{version_number}").lower()

        # Integrations name
        integration_names = ["RAPTORSTORAGE","QPSTORAGE"]

        count_stages = 0
        for int_name in integration_names:

            if int_name == "QPSTORAGE" and env == "DVLP":
                if version_number == 1:
                    env_version = env_version.replace("dvlp", "int1")
                else:
                    env_version = env_version.replace("dvlp", "int")

            storage_integration_name = f"{env}_AZUREDLINTEG_{int_name}"
            datalake_owner = "RAPTORDATA" if int_name == "RAPTORSTORAGE" else "QUICPLUS"

            # Gets all allowed location from Integration
            lt_integration = session.sql(f"DESC INTEGRATION {storage_integration_name}").collect()          # Describe Inegration
            df_integration = pd.DataFrame(lt_integration)                                                   # Transform to Pandas DF
            df_integration.set_index('property', inplace=True)                                              # Set Property column as index
            allowed_loc = (df_integration.loc["STORAGE_ALLOWED_LOCATIONS", "property_value"]).split(",")    # Get the values for allowed location

            # Transform, create da dataframe and filter the result by environment version
            container_list = [[item, item.split("/")[-1], (item.split("/")[-1]).split("-")[0]] for item in allowed_loc]
            df_container = pd.DataFrame(container_list, columns = ["container_path", "container_name", "env_version"])            
            if env.upper() != "PROD": # Prod does not have version, no need for filter
                df_container = df_container[df_container.env_version == env_version].reset_index()
            
            # Iterate over all allowed storage
            for index, row in df_container.iterrows():
                container_path = row["container_path"]
                container_name = row["container_name"]
                dataset_name = container_name.replace(f"{env_version}-","").replace("-","_").upper()
                dataset_name_concat = "_" + dataset_name if dataset_name != datalake_owner else ""
                stage_name = f"EXTSTAGE_AZUREDL_{datalake_owner}{dataset_name_concat}"
                
                # Check if stage exists
                stage_check = session.sql(f"SHOW STAGES LIKE '{stage_name}' IN SCHEMA {schema_destination}").collect()

                sql_stage_instruction = ""
                if len(stage_check) == 0:
                    sql_stage_instruction = f"CREATE STAGE {schema_destination}.{stage_name}"
                else:
                    sql_stage_instruction = f"ALTER STAGE {schema_destination}.{stage_name} SET"

                sql_stage = f"{sql_stage_instruction} STORAGE_INTEGRATION = {storage_integration_name} URL = '{container_path}'"

                session.sql(sql_stage).collect()
                count_stages +=1
        return f"Number of stages created: {str(count_stages)}"
    except SnowparkSQLException as e:
        raise e.message
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        raise exc_value

$$
;
