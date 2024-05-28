from dataclasses import dataclass
from operator import methodcaller
from typing import ClassVar
import pandas as pd
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col


@dataclass
class CompareMetadata:
    def __init__(self, database_name_base = "", database_name_target = ""):
        # Class variables
        self.database_name_base = database_name_base
        self.database_name_target= database_name_target

    def compare_metadata(self, table_name, session : Session):

        df_metadata_base = session.table(f"{self.database_name_base}.{table_name}").drop(col("LAST_MODIFIED"),col("FIRST_TIME_INSERTED")).to_pandas()
        df_metadata_target = session.table(f"{self.database_name_target}.{table_name}").drop(col("LAST_MODIFIED"),col("FIRST_TIME_INSERTED")).to_pandas()
        
        df_metadata_base.insert(loc=0, column='CURRENT_DATABASE_NAME', value=self.database_name_base)
        df_metadata_target.insert(loc=0, column='CURRENT_DATABASE_NAME', value=self.database_name_target)

        df = pd.concat([df_metadata_base, df_metadata_target])
        df_diff = df.drop_duplicates(keep=False,subset=df.columns.difference(['CURRENT_DATABASE_NAME']))

        df_diff_base = df_diff[df_diff.CURRENT_DATABASE_NAME==self.database_name_base]
        df_diff_target = df_diff[df_diff.CURRENT_DATABASE_NAME==self.database_name_target]

        df2 = pd.concat([df_diff_base, df_diff_target])
        df2 = df2.reset_index(drop=True)
        df2_gpby = df2.groupby(list(df2.columns))
        idx = [x[0] for x in df2_gpby.groups.values() if len(x) == 1]

        df_diff_summary = df2.reindex(idx)
        df_compare = df_diff_base.reset_index().compare(df_diff_target.reset_index())

        return df_diff_summary,df_compare
