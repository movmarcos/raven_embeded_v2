MERGE INTO RAVEN.METADATA_STAGE_ME_PARAMETERS AS tgt
USING (
	SELECT
		IFNULL(src.DATASET_NAME COLLATE 'utf8', tgt.DATASET_NAME) DATASET_NAME
	  ,src.CONTAINER_NAME		
    ,IFF(LEFT(TRIM(src.FILE_PATH),1)='/','','/') || TRIM(src.FILE_PATH) || IFF(RIGHT(TRIM(src.FILE_PATH),1)='/','','/')	AS SRC_FILE_PATH
	  ,IFNULL(SRC_FILE_PATH, tgt.FILE_PATH) FILE_PATH
		,IFNULL(src.FILE_NAME, tgt.FILE_NAME) FILE_NAME
		,src.SOURCE_SYSTEM_CODE
		,src.SOURCE_FEED_CODE
		,src.IS_ENABLED
		,src.IS_TRIGGER_FILE
		,src.ALLOW_RELOAD
		,src.ENTITY_CODE
		,src.DEPARTMENT_CODE
		,src.REGION
		,src.MARKET
		,src.IS_CLOUD_COPY
		,src.EXPECTED_STAGE_TIME
		,src.WAREHOUSE_SIZE
		,NVL(src.ALLOW_INFER_SCHEMA, TRUE) as ALLOW_INFER_SCHEMA
		,IFF(src.FILE_PATH IS NULL, 'DISABLE', 'UPDATE') AS ACTION_UPDATE
		,ARRAY_EXCEPT(SPLIT(src.TAGS,'#'),['']) AS TAGS
	FROM RAVEN.TEMP_METADATA_SRC_STAGE_ME_PARAMETERS src
	FULL OUTER JOIN RAVEN.METADATA_STAGE_ME_PARAMETERS tgt ON src.DATASET_NAME COLLATE 'utf8' = tgt.DATASET_NAME
) AS src
ON 
	tgt.DATASET_NAME=src.DATASET_NAME
WHEN MATCHED AND ACTION_UPDATE = 'UPDATE'
THEN UPDATE SET
	tgt.CONTAINER_NAME=src.CONTAINER_NAME,
	tgt.FILE_PATH=src.FILE_PATH,
	tgt.FILE_NAME=src.FILE_NAME,
	tgt.SOURCE_SYSTEM_CODE=src.SOURCE_SYSTEM_CODE,
	tgt.SOURCE_FEED_CODE=src.SOURCE_FEED_CODE,
	tgt.IS_ENABLED=src.IS_ENABLED, 
	tgt.IS_TRIGGER_FILE=src.IS_TRIGGER_FILE, 
	tgt.ALLOW_RELOAD=src.ALLOW_RELOAD, 
	tgt.ENTITY_CODE=src.ENTITY_CODE, 
	tgt.DEPARTMENT_CODE=src.DEPARTMENT_CODE, 
	tgt.REGION=src.REGION, 
	tgt.MARKET=src.MARKET, 
	tgt.IS_CLOUD_COPY=src.IS_CLOUD_COPY, 
	tgt.EXPECTED_STAGE_TIME=src.EXPECTED_STAGE_TIME,
	tgt.WAREHOUSE_SIZE=src.WAREHOUSE_SIZE,
	tgt.ALLOW_INFER_SCHEMA=src.ALLOW_INFER_SCHEMA,
	tgt.TAGS=src.TAGS,
	tgt.LAST_MODIFIED=CURRENT_TIMESTAMP(),
	tgt.FIRST_TIME_INSERTED=nvl(tgt.FIRST_TIME_INSERTED,CURRENT_TIMESTAMP())
WHEN MATCHED AND ACTION_UPDATE = 'DISABLE'
THEN DELETE
WHEN NOT MATCHED
THEN INSERT 
	(
	DATASET_NAME,
	CONTAINER_NAME,
	FILE_PATH, 
	FILE_NAME, 
	SOURCE_SYSTEM_CODE, 
	SOURCE_FEED_CODE, 
	IS_ENABLED, 
	IS_TRIGGER_FILE, 
	ALLOW_RELOAD, 
	ENTITY_CODE, 
	DEPARTMENT_CODE, 
	REGION, 
	MARKET, 
	IS_CLOUD_COPY, 
	EXPECTED_STAGE_TIME,
	WAREHOUSE_SIZE,
	ALLOW_INFER_SCHEMA,
	TAGS,
	LAST_MODIFIED,
	FIRST_TIME_INSERTED
	)
	VALUES 
	(
	src.DATASET_NAME,
	src.CONTAINER_NAME,
	src.FILE_PATH, 
	src.FILE_NAME, 
	src.SOURCE_SYSTEM_CODE, 
	src.SOURCE_FEED_CODE, 
	src.IS_ENABLED, 
	src.IS_TRIGGER_FILE, 
	src.ALLOW_RELOAD, 
	src.ENTITY_CODE, 
	src.DEPARTMENT_CODE, 
	src.REGION, 
	src.MARKET, 
	src.IS_CLOUD_COPY, 
	src.EXPECTED_STAGE_TIME,
	src.WAREHOUSE_SIZE,
	src.ALLOW_INFER_SCHEMA,
	src.TAGS,
	CURRENT_TIMESTAMP(),
	CURRENT_TIMESTAMP()
	)
	;