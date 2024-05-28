CREATE or replace TABLE RAVEN.METADATA_STAGE_ME_PARAMETERS (
	DATASET_NAME VARCHAR(5000) NOT NULL COLLATE 'UTF8',
	CONTAINER_NAME VARCHAR(5000),
	FILE_PATH VARCHAR(500) NOT NULL,
	FILE_NAME VARCHAR(500) NOT NULL,
	SOURCE_SYSTEM_CODE VARCHAR(200) NOT NULL,
	SOURCE_FEED_CODE VARCHAR(200) NOT NULL,
	IS_ENABLED BOOLEAN NOT NULL DEFAULT TRUE,
	IS_TRIGGER_FILE BOOLEAN NOT NULL DEFAULT FALSE,
	ALLOW_RELOAD BOOLEAN NOT NULL DEFAULT TRUE,
	ENTITY_CODE VARCHAR(20),
	DEPARTMENT_CODE VARCHAR(200),
	REGION VARCHAR(200),
	MARKET VARCHAR(200),
	IS_CLOUD_COPY BOOLEAN NOT NULL DEFAULT FALSE,
	EXPECTED_STAGE_TIME VARCHAR(8),
	WAREHOUSE_SIZE VARCHAR(200),
	FIRST_TIME_INSERTED TIMESTAMP_TZ(9),
	LAST_MODIFIED TIMESTAMP_TZ(9) DEFAULT CURRENT_TIMESTAMP(),
	ALLOW_INFER_SCHEMA BOOLEAN NOT NULL DEFAULT TRUE,
	TAGS VARIANT,
	constraint PK_METADATA_STAGE_ME_PARAMETERS primary key (DATASET_NAME)
)
;
