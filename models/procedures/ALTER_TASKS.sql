CREATE OR REPLACE PROCEDURE RAVEN.ALTER_TASKS()
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS caller
AS
$$

import sys
from snowflake.snowpark.functions import col
from snowflake.snowpark.exceptions import SnowparkSQLException

def run(session):
    try:
        # Get current db
        df_current_db = session.sql("SELECT current_database() AS CURRENT_DATABASE").collect()[0]
        current_database = df_current_db["CURRENT_DATABASE"]
        env_database = "_".join(current_database.split("_")[0:2]) # Env + Db project

        # List all taks in RAVEN schema
        df_tasks = session.sql("SHOW TASKS in RAVEN")

        # Alter warehouse
        df_tasks_wh = df_tasks.filter('"warehouse" is not null')
        
        wh_name = session.sql("SHOW WAREHOUSES") \
            .filter(f''' "name" like '{env_database}%' AND "size" = 'X-Small' ''') \
            .select('"name"').limit(1).collect()[0][0]
        
        for wh_row in df_tasks_wh.collect():
            task_name = wh_row["name"]
            session.sql(f"ALTER TASK RAVEN.{task_name} SET warehouse = {wh_name}").collect()

        # Resume Tasks
        df_tasks_resume = df_tasks \
            .filter('ARRAY_SIZE("predecessors") = 0') \
            .filter(''' "state" = 'suspended' ''')
        for row in df_tasks_resume.collect():
            task_name = row["name"]
            session.sql(f"SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('RAVEN.{task_name}')").collect()

    except SnowparkSQLException as e:
        raise e.message
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        raise exc_value

$$
;
