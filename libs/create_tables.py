from snowflake.snowpark.functions import listagg, col
import sys
import pandas as pd


def create_table(session,obj_cmd):
    try:
        obj_name = ((obj_cmd.split("TABLE")[1]).split("(")[0]).strip()

        session.use_schema("RAVEN")

        if "." not in obj_name:
            raise Exception("Create table command has not schema")
        else:
            table_schema, table_name = obj_name.split(".")

        # Use Schema
        session.use_schema(table_schema)

        # Check tables exists
        tbl = session.sql(f"SHOW TABLES LIKE '{table_name}'")
        tbl_check = tbl.count()

        if tbl_check > 0:

            # Create intermediated table
            obj_name_inter = f"{obj_name}_INT"
            table_name_inter = obj_name.split(".")[1]
            obj_cmd = obj_cmd.replace(obj_name, obj_name_inter)
            obj_create = session.sql(obj_cmd).collect()[0][0]

            # Get columns from both versions
            old_columns = session.table(obj_name).columns
            new_columns = session.table(obj_name_inter).columns

            # Compare the schema
            df = session.table("INFORMATION_SCHEMA.COLUMNS")\
                .filter(
                    (col("TABLE_SCHEMA") == table_schema) &
                    ((col("TABLE_NAME") == table_name) | (col("TABLE_NAME") == table_name_inter))
                )
            df_table = df.filter(col("TABLE_NAME") == table_name).drop(col("TABLE_NAME")).to_pandas()
            df_table_int = df.filter(col("TABLE_NAME") == table_name_inter).drop(col("TABLE_NAME")).to_pandas()

            df_diff = pd.concat([df_table, df_table_int]).drop_duplicates(keep=False)
            
            if old_columns != new_columns or df_diff.shape[0]>0:
                # Insert Intersection Columns
                inter_columns = [value for value in new_columns if value in old_columns]
                srt_inter_columns = ",".join(inter_columns)
                session.sql(f"INSERT INTO {obj_name_inter} ({srt_inter_columns}) SELECT {srt_inter_columns} FROM {obj_name}").collect()
                # Swap and drop table
                session.sql(f"ALTER TABLE {obj_name_inter} SWAP WITH {obj_name}").collect()

            else:
                obj_create = f"Table {obj_name} already exists"
 
            # Drop intermediated table   
            session.sql(f"DROP TABLE IF EXISTS {obj_name_inter}").collect()

        else:
            print(f"Create Table: {table_name}")
            obj_create = session.sql(obj_cmd).collect()[0][0]

    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        obj_create = exc_value
        sys.exit(1)

    return obj_create
