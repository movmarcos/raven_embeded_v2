CREATE OR REPLACE VIEW RAVEN.VW_METADATA_SOURCE_FILE_AND_FIELD_CONCAT AS
WITH DATA_TYPE AS (
	SELECT
     SOURCE_SYSTEM_CODE
    ,SOURCE_FEED_CODE
    ,SOURCE_FILE_NAME_PATTERN
    ,SOURCE_FILE_PATH
    ,CURRENT_DATABASE_NAME
    ,DESTINATION_SCHEMA_NAME
    ,DESTINATION_TABLE_NAME
    ,DESTINATION_FULL_TABLE_NAME
    ,SOURCE_FIELD_NAME
    ,'"'||TARGET_FIELD_NAME||'"' 	AS TARGET_FIELD_NAME
    ,IFNULL(FIELD_ORDINAL,1)		AS FIELD_ORDINAL
    ,IS_IN_SOURCE
    ,IFNULL(IS_IN_TARGET,TRUE)		AS IS_IN_TARGET
    ,FILE_FORMAT
    ,STAGE_NAME
    ,DELETE_STAGE_BY_FILE_NAME
    ,ENVIRONMENT
    ,DERIVED_EXPRESSION
    ,DATE_INPUT_FORMAT
    ,TIMESTAMP_INPUT_FORMAT
    ,TIMEZONE
    ,SKIP_ROW_ON_ERROR
    ,DATA_TYPE_NAME
	  ,DATA_TYPE_SIZE
    ,DATA_TYPE_PRECISION
    ,DATA_TYPE_SCALE
    ,SNOWPARK_DATA_TYPE
	FROM RAVEN.VW_METADATA_SOURCE_FILE_AND_FIELD
)
SELECT
     SOURCE_SYSTEM_CODE
    ,SOURCE_FEED_CODE
    ,MAX(SOURCE_FILE_NAME_PATTERN)    AS SOURCE_FILE_NAME_PATTERN
    ,MAX(SOURCE_FILE_PATH)            AS SOURCE_FILE_PATH
    ,MAX(CURRENT_DATABASE_NAME)		    AS CURRENT_DATABASE_NAME
    ,MAX(DESTINATION_SCHEMA_NAME)		  AS DESTINATION_SCHEMA_NAME
    ,MAX(DESTINATION_TABLE_NAME)		  AS DESTINATION_TABLE_NAME
    ,MAX(DESTINATION_FULL_TABLE_NAME)	AS DESTINATION_FULL_TABLE_NAME
    ,MAX(STAGE_NAME)					        AS STAGE_NAME
    ,MAX(FILE_FORMAT)					        AS FILE_FORMAT
    ,MAX(ENVIRONMENT)					        AS ENVIRONMENT
    ,MAX(DELETE_STAGE_BY_FILE_NAME)	  AS DELETE_STAGE_BY_FILE_NAME
    ,MAX(DATE_INPUT_FORMAT) 			    AS DATE_INPUT_FORMAT
    ,MAX(TIMESTAMP_INPUT_FORMAT) 		  AS TIMESTAMP_INPUT_FORMAT
    ,MAX(SKIP_ROW_ON_ERROR) 			    AS SKIP_ROW_ON_ERROR
    ,ARRAY_AGG(DATA_TYPE_SIZE)                                    						 			WITHIN GROUP (ORDER BY FIELD_ORDINAL,SOURCE_FIELD_NAME) LIST_DATA_TYPE_NAME
    -- FIELDS LIST_COLUMN_NAME_SOURCE AND LIST_COLUMN_NAME_TARGET ONLY LIST FIELDS IN SOURCE, THE OTHERS WILL BE AT LIST_EXTRA_FIELD_DERIVED_EXPRESSION
    ,ARRAY_AGG(IFF(IS_IN_SOURCE = 1, SOURCE_FIELD_NAME,NULL))        					 			WITHIN GROUP (ORDER BY FIELD_ORDINAL,SOURCE_FIELD_NAME) LIST_COLUMN_NAME_SOURCE
    ,ARRAY_AGG(IFF(IS_IN_SOURCE = 1, FIELD_ORDINAL,NULL))        					 			    WITHIN GROUP (ORDER BY FIELD_ORDINAL,SOURCE_FIELD_NAME) LIST_COLUMN_POSITION_SOURCE
    ,ARRAY_AGG(IFF(IS_IN_SOURCE = 1, UPPER(TARGET_FIELD_NAME),NULL)) 					 			WITHIN GROUP (ORDER BY FIELD_ORDINAL,SOURCE_FIELD_NAME) LIST_COLUMN_NAME_TARGET
    ,ARRAY_AGG(UPPER(TARGET_FIELD_NAME) || ' ' || DATA_TYPE_SIZE) 						 			WITHIN GROUP (ORDER BY FIELD_ORDINAL,SOURCE_FIELD_NAME) LIST_CREATE_COLUMN_NAME_TARGET
    ,ARRAY_AGG(IFF(IS_IN_SOURCE = 1, UPPER(TARGET_FIELD_NAME)||' '||DATA_TYPE_SIZE,NULL)) WITHIN GROUP (ORDER BY FIELD_ORDINAL,SOURCE_FIELD_NAME) LIST_CREATE_COLUMN_NAME_TARGET_NO_DERIVED_EXPRESSION
    ,ARRAY_AGG(IFF(IS_IN_SOURCE = 1, SNOWPARK_DATA_TYPE, NULL)) 									WITHIN GROUP (ORDER BY FIELD_ORDINAL,SOURCE_FIELD_NAME) AS LIST_SNOWPARK_FIELD_TYPE
    ,ARRAY_AGG(IFF(IS_IN_SOURCE = 1, UPPER(TARGET_FIELD_NAME) || '#' ||  SNOWPARK_DATA_TYPE, NULL)) WITHIN GROUP (ORDER BY FIELD_ORDINAL,SOURCE_FIELD_NAME) AS LIST_SNOWPARK_STRUCT_FIELD
    ,ARRAY_AGG(IFF(
    			IS_IN_SOURCE = 1,
    			IFF(LENGTH(DERIVED_EXPRESSION) > 0,  REPLACE(DERIVED_EXPRESSION, REPLACE(SOURCE_FIELD_NAME,'"',''), '$' || FIELD_ORDINAL),
    			CASE 
	    		WHEN UPPER(DATA_TYPE_NAME) = 'DATE' THEN UPPER('DATE(NULLIF(' || 
	    								IFF(FILE_FORMAT LIKE '%PARQUET%' OR FILE_FORMAT LIKE '%JSON%', '$1:' || SOURCE_FIELD_NAME, '$' || FIELD_ORDINAL) || 
	    								',''''),''' || DATE_INPUT_FORMAT || ''')' )
	    		WHEN UPPER(DATA_TYPE_NAME) = 'DATETIME' THEN 'CONVERT_TIMEZONE('''|| TIMEZONE || UPPER(''', ''UTC'', TO_TIMESTAMP(NULLIF(' || 
	    								IFF(FILE_FORMAT LIKE '%PARQUET%' OR FILE_FORMAT LIKE '%JSON%', '$1:' || SOURCE_FIELD_NAME, '$' || FIELD_ORDINAL) || 
	    								',''''),''' || TIMESTAMP_INPUT_FORMAT || '''))' )
	    		ELSE 'NULLIF(' || IFF(FILE_FORMAT LIKE '%PARQUET%' OR FILE_FORMAT LIKE '%JSON%', '$1:' || SOURCE_FIELD_NAME, '$' || FIELD_ORDINAL) ||','''')::'|| DATA_TYPE_SIZE
	    	 	END), 
	    	 	NULL) || ' AS "' || SOURCE_FIELD_NAME || '"'
    		 ) WITHIN GROUP (ORDER BY FIELD_ORDINAL ASC) AS LIST_COLUMN_POSITION_SOURCE_TRANSFORM
    ,ARRAY_AGG(IFF(
    			IS_IN_SOURCE = 1,
    			IFF(LENGTH(DERIVED_EXPRESSION) > 0,  REPLACE(DERIVED_EXPRESSION, REPLACE(SOURCE_FIELD_NAME,'"',''), '$' || FIELD_ORDINAL),
    			CASE 
	    		WHEN UPPER(DATA_TYPE_NAME) = 'DATE' THEN UPPER('DATE(NULLIF(' || 
	    								IFF(FILE_FORMAT LIKE '%PARQUET%' OR FILE_FORMAT LIKE '%JSON%', '$1:' || SOURCE_FIELD_NAME, '|:REPLACE_POSITION:|') || 
	    								',''''),''' || DATE_INPUT_FORMAT || ''')' )
	    		WHEN UPPER(DATA_TYPE_NAME) = 'DATETIME' THEN 'CONVERT_TIMEZONE('''|| TIMEZONE || UPPER(''', ''UTC'', TO_TIMESTAMP(NULLIF(' || 
	    								IFF(FILE_FORMAT LIKE '%PARQUET%' OR FILE_FORMAT LIKE '%JSON%', '$1:' || SOURCE_FIELD_NAME, '|:REPLACE_POSITION:|') || 
	    								',''''),''' || TIMESTAMP_INPUT_FORMAT || '''))' )
	    		ELSE 'NULLIF(' || IFF(FILE_FORMAT LIKE '%PARQUET%' OR FILE_FORMAT LIKE '%JSON%', '$1:' || SOURCE_FIELD_NAME, '|:REPLACE_POSITION:|') ||','''')::'|| DATA_TYPE_SIZE
	    	 	END), 
	    	 	NULL) || ' AS "' || SOURCE_FIELD_NAME || '"'
    		 ) WITHIN GROUP (ORDER BY FIELD_ORDINAL ASC) AS LIST_COLUMN_UNKNOWN_POSITION_SOURCE_TRANSFORM
    ,ARRAY_AGG(CASE WHEN LENGTH(DERIVED_EXPRESSION) > 0 AND IS_IN_SOURCE = 1 THEN DERIVED_EXPRESSION ELSE ''END) 	WITHIN GROUP (ORDER BY FIELD_ORDINAL,SOURCE_FIELD_NAME) LIST_DERIVED_EXPRESSION
    ,ARRAY_AGG(CASE WHEN LENGTH(DERIVED_EXPRESSION) > 0 AND IS_IN_SOURCE = 0 THEN UPPER(TARGET_FIELD_NAME)  END) 	WITHIN GROUP (ORDER BY FIELD_ORDINAL,SOURCE_FIELD_NAME) LIST_EXTRA_FIELD_DERIVED_EXPRESSION
    ,ARRAY_AGG(CASE WHEN LENGTH(DERIVED_EXPRESSION) > 0 AND IS_IN_SOURCE = 0 THEN DERIVED_EXPRESSION        END) 	WITHIN GROUP (ORDER BY FIELD_ORDINAL,SOURCE_FIELD_NAME) LIST_EXTRA_DERIVED_EXPRESSION
    ,ARRAY_AGG(CASE WHEN LENGTH(DERIVED_EXPRESSION) > 0 AND IS_IN_SOURCE = 0 THEN UPPER(TARGET_FIELD_NAME) || ' ' || DATA_TYPE_SIZE END)  WITHIN GROUP (ORDER BY FIELD_ORDINAL,SOURCE_FIELD_NAME) LIST_EXTRA_CREATE_FIELD_DERIVED_EXPRESSION
    ,IFF(COUNT(IS_IN_SOURCE) > 0, TRUE, FALSE) AS HAS_FIELD_DATA
 FROM DATA_TYPE
WHERE FIELD_ORDINAL >= 0
  AND IS_IN_TARGET = TRUE
GROUP BY SOURCE_SYSTEM_CODE,SOURCE_FEED_CODE
;
