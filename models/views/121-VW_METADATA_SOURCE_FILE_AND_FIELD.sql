CREATE OR REPLACE VIEW RAVEN.VW_METADATA_SOURCE_FILE_AND_FIELD AS
SELECT
 FL.SOURCE_SYSTEM_CODE
,FL.SOURCE_FEED_CODE
,FL.STAGE_NAME
,FL.SOURCE_FILE_PATH
,FL.SOURCE_FILE_NAME_PATTERN
,FL.DESTINATION_SCHEMA_NAME
,FL.DESTINATION_TABLE_NAME
,CASE WHEN FL.DESTINATION_SCHEMA_NAME LIKE '%STAGING%'
	THEN DESTINATION_SCHEMA_NAME || '.' || FL.SOURCE_SYSTEM_CODE || '_'  || FL.DESTINATION_TABLE_NAME 
	ELSE DESTINATION_SCHEMA_NAME || '.' || FL.DESTINATION_TABLE_NAME
END AS DESTINATION_FULL_TABLE_NAME
,FL.IS_ENABLED
,FL.DELETE_STAGE_BY_FILE_NAME
,FL.FILE_FORMAT
,FL.DATE_INPUT_FORMAT
,FL.TIMESTAMP_INPUT_FORMAT
,FL.SKIP_ROW_ON_ERROR
,IFNULL(FL.TIMEZONE,'Europe/London') AS TIMEZONE
,FD.SOURCE_FIELD_NAME
,FD.TARGET_FIELD_NAME
,FD.FIELD_ORDINAL
,FD.DATA_TYPE_NAME
,FD.DATA_TYPE_LENGTH
,FD.DATA_TYPE_PRECISION
,FD.DATA_TYPE_SCALE
,FD.DATA_TYPE_NAME ||
 CASE
	WHEN DATA_TYPE_NAME in ('DOUBLE','FLOAT','BOOLEAN','DATE','DATETIME','STRING','TIME','TIMESTAMP') THEN ''
	WHEN FD.DATA_TYPE_LENGTH > 0 							   THEN '(' || FD.DATA_TYPE_LENGTH || ')'                                                                                          	
	WHEN FD.DATA_TYPE_PRECISION > 0 AND FD.DATA_TYPE_SCALE = 0 THEN '(' || FD.DATA_TYPE_PRECISION || ')'                                                        	
	WHEN FD.DATA_TYPE_PRECISION = 0 AND FD.DATA_TYPE_SCALE = 0 THEN '(38)'                                                             	
	WHEN FD.DATA_TYPE_PRECISION > 0 AND FD.DATA_TYPE_SCALE > 0 THEN '(' || FD.DATA_TYPE_PRECISION || ',' || FD.DATA_TYPE_SCALE || ')'                                  	
	ELSE ''                                                                                                                                                	
 END DATA_TYPE_SIZE
,CASE 
	WHEN DATA_TYPE_NAME = 'DATE' 		    THEN 'DateType()'
	WHEN DATA_TYPE_NAME = 'TIME' 		    THEN 'TimeType()'
	WHEN DATA_TYPE_NAME = 'DATETIME' 	  	THEN 'TimestampType()'
	WHEN DATA_TYPE_NAME = 'TIMESTAMP' 		THEN 'TimestampType()'
	WHEN DATA_TYPE_NAME = 'STRING' 			THEN 'StringType()'
	WHEN DATA_TYPE_NAME = 'VARCHAR' 		THEN 'StringType()'
	WHEN DATA_TYPE_NAME = 'BOOLEAN' 		THEN 'BooleanType()'
	WHEN DATA_TYPE_NAME = 'NUMERIC' 
		AND DATA_TYPE_PRECISION = 0 	  	THEN 'IntegerType()'
	WHEN DATA_TYPE_NAME = 'NUMERIC' 
		AND DATA_TYPE_PRECISION > 0 	  	THEN 'DecimalType(' || DATA_TYPE_PRECISION || ',' || DATA_TYPE_SCALE || ')' 
	ELSE 'StringType()'
END AS SNOWPARK_DATA_TYPE
,FD.IS_IN_SOURCE
,FD.IS_IN_TARGET
,FD.IS_DELETED	AS IS_DELETED_FIELD
,FD.DERIVED_EXPRESSION
,CURRENT_DATABASE() AS CURRENT_DATABASE_NAME
,SPLIT_PART(CURRENT_DATABASE(),'_',0) AS ENVIRONMENT
FROM RAVEN.METADATA_SOURCE_FILE FL
LEFT JOIN RAVEN.METADATA_SOURCE_FIELD FD ON FL.SOURCE_SYSTEM_CODE = FD.SOURCE_SYSTEM_CODE
                                             AND FL.SOURCE_FEED_CODE = FD.SOURCE_FEED_CODE
											 AND FD.IS_DELETED = FALSE
;
