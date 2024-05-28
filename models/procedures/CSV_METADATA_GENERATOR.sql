CREATE OR REPLACE PROCEDURE RAVEN.CSV_METADATA_GENERATOR(STAGE_NAME STRING, FULL_FILE_PATH STRING)
RETURNS TABLE ()
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python','pandas','numpy')
HANDLER = 'run'
EXECUTE AS caller
AS
$$

import pandas as pd
import numpy as np
import io
from snowflake.snowpark.types import StructType, StructField, StringType
from snowflake.snowpark.functions import col
from snowflake.snowpark.exceptions import SnowparkSQLException

def buffer_file(session,stage_name,full_file_path):
        
    # Create a Schema that has only one String column 
    schema = StructType([StructField("all", StringType())])
            
    # Read the CSV file from Snowflake into a DataFrame
    result = session.read.option('field_delimiter', '¬')\
        .schema(schema)\
        .csv(f'@{stage_name}/{full_file_path}')\
        .limit(100)\
        .collect()

    #Read result row objects to strings
    csv_data = [row[0] for row in result]

    # Find delimiter
    delimiters = [',','|',';','\t']
    header = csv_data[0]
    count_delimiter = {}
    for d in delimiters:
        count_char = header.count(d)
        count_delimiter[d] = count_char
        
    # Get the key with max value        
    delimiter = max(count_delimiter, key=count_delimiter.get)
        
    # Create the buffer object
    buffer = io.StringIO()
        
    # Write the strings to the buffer
    for s in csv_data:
        buffer.write(s+'\n')      
    
    # Reset the buffer's position to the beginning
    buffer.seek(0)
    return buffer, delimiter
    
def run(session,stage_name,full_file_path):
    try:
        # Put the first 100 rows in a buffer and delimiter
        buffer, delimiter = buffer_file(session,stage_name,full_file_path)

        # Read from the buffer
        df = pd.read_csv(buffer, delimiter=delimiter, encoding = 'utf-8')

        # Field type mapping
        df_type_mapping = pd.DataFrame(
            {
                'TYPE': ['string','integer', 'floating'],
                'DATA_TYPE_NAME': ['VARCHAR','NUMERIC','NUMERIC'],
                'DATA_TYPE_LENGTH': [1000,0,0],
                'DATA_TYPE_PRECISION':[0,38,38],
                'DATA_TYPE_SCALE':[0,0,18]
            },
            index=[0, 1, 2]
        )

        # Schema
        infer_type = lambda x: pd.api.types.infer_dtype(x, skipna=True)
        df.apply(infer_type, axis=0)

        # DataFrame with column names & new types
        df_type = pd.DataFrame(df.apply(pd.api.types.infer_dtype, axis=0)).reset_index().rename(columns={'index': 'SOURCE_FIELD_NAME', 0: 'TYPE'})

        # Add destination field name
        df_type['TARGET_FIELD_NAME'] = df_type['SOURCE_FIELD_NAME']\
            .str.replace('[\W]', '')\
            .str.replace('(?<!^)([A-Z])', r'_\1')\
            .str.upper()

        # Add field position
        df_type['FIELD_ORDINAL'] = df_type.index +1

        # Mapping field type and drop TYPE column
        df_type = pd.merge(df_type, df_type_mapping, how="inner", on=["TYPE"])
        df_type.drop('TYPE', axis=1, inplace=True)

        # Add default values
        df_type[
            [
                'IS_IN_SOURCE',
                'IS_IN_TARGET',
                'IS_DELETED',
                'DERIVED_EXPRESSION'
            ]
        ] = pd.DataFrame([['TRUE', 'TRUE', 'FALSE', np.nan]], index=df_type.index)
        
        #Return a dataframe
        return session.create_dataframe(df_type).sort(col('FIELD_ORDINAL'))
    
    except SnowparkSQLException as e:
        raise e.message
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        raise exc_value

$$        
