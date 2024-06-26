MERGE INTO RAVEN.METADATA_SOURCE_FILE AS tgt
USING (
	SELECT
		 IFNULL(src.SOURCE_SYSTEM_CODE, tgt.SOURCE_SYSTEM_CODE) SOURCE_SYSTEM_CODE
		,IFNULL(src.SOURCE_FEED_CODE, tgt.SOURCE_FEED_CODE) SOURCE_FEED_CODE
		,src.STAGE_NAME
		,src.SOURCE_FILE_PATH
		,src.SOURCE_FILE_NAME_PATTERN
		,src.DESTINATION_SCHEMA_NAME
		,src.DESTINATION_TABLE_NAME
		,src.IS_ENABLED
		,src.FILE_FORMAT
		,src.DATE_INPUT_FORMAT
		,src.TIMESTAMP_INPUT_FORMAT
		,src.SKIP_ROW_ON_ERROR
		,src.DELETE_STAGE_BY_FILE_NAME
		,src.TIMEZONE
		,IFF(src.SOURCE_SYSTEM_CODE IS NULL, 'DISABLE', 'UPDATE') AS ACTION_UPDATE
	FROM RAVEN.TEMP_METADATA_SRC_SOURCE_FILE src
	FULL OUTER JOIN RAVEN.METADATA_SOURCE_FILE tgt ON src.SOURCE_SYSTEM_CODE = tgt.SOURCE_SYSTEM_CODE AND src.SOURCE_FEED_CODE = tgt.SOURCE_FEED_CODE
) AS src
ON (
	tgt.SOURCE_SYSTEM_CODE=src.SOURCE_SYSTEM_CODE 
AND tgt.SOURCE_FEED_CODE=src.SOURCE_FEED_CODE
)
WHEN MATCHED AND ACTION_UPDATE = 'UPDATE'
THEN UPDATE SET
tgt.STAGE_NAME=src.STAGE_NAME, 
tgt.SOURCE_FILE_PATH=src.SOURCE_FILE_PATH,
tgt.SOURCE_FILE_NAME_PATTERN=src.SOURCE_FILE_NAME_PATTERN,
tgt.DESTINATION_SCHEMA_NAME=src.DESTINATION_SCHEMA_NAME, 
tgt.DESTINATION_TABLE_NAME=src.DESTINATION_TABLE_NAME, 
tgt.IS_ENABLED=src.IS_ENABLED, 
tgt.FILE_FORMAT=src.FILE_FORMAT, 
tgt.DATE_INPUT_FORMAT=src.DATE_INPUT_FORMAT, 
tgt.TIMESTAMP_INPUT_FORMAT=src.TIMESTAMP_INPUT_FORMAT, 
tgt.SKIP_ROW_ON_ERROR=src.SKIP_ROW_ON_ERROR, 
tgt.DELETE_STAGE_BY_FILE_NAME=src.DELETE_STAGE_BY_FILE_NAME,
tgt.TIMEZONE = src.TIMEZONE,
tgt.LAST_MODIFIED=CURRENT_TIMESTAMP(),
tgt.FIRST_TIME_INSERTED=nvl(tgt.FIRST_TIME_INSERTED,CURRENT_TIMESTAMP())
WHEN MATCHED AND ACTION_UPDATE = 'DISABLE'
THEN DELETE
WHEN NOT MATCHED
THEN INSERT (
SOURCE_SYSTEM_CODE, 
SOURCE_FEED_CODE, 
STAGE_NAME, 
SOURCE_FILE_PATH,
SOURCE_FILE_NAME_PATTERN,
DESTINATION_SCHEMA_NAME, 
DESTINATION_TABLE_NAME, 
IS_ENABLED, 
FILE_FORMAT, 
DATE_INPUT_FORMAT, 
TIMESTAMP_INPUT_FORMAT, 
SKIP_ROW_ON_ERROR, 
DELETE_STAGE_BY_FILE_NAME,
TIMEZONE,
LAST_MODIFIED,
FIRST_TIME_INSERTED)
VALUES (
src.SOURCE_SYSTEM_CODE, 
src.SOURCE_FEED_CODE, 
src.STAGE_NAME, 
src.SOURCE_FILE_PATH,
src.SOURCE_FILE_NAME_PATTERN,
src.DESTINATION_SCHEMA_NAME, 
src.DESTINATION_TABLE_NAME, 
src.IS_ENABLED, 
src.FILE_FORMAT, 
src.DATE_INPUT_FORMAT, 
src.TIMESTAMP_INPUT_FORMAT, 
src.SKIP_ROW_ON_ERROR, 
src.DELETE_STAGE_BY_FILE_NAME,
src.TIMEZONE,
CURRENT_TIMESTAMP(),
CURRENT_TIMESTAMP())
;
