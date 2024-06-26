CREATE or replace TABLE RAVEN.LOG_LIST_EXTERNAL_FILE (
	ID NUMBER(38,0) NOT NULL autoincrement,
	RAVEN_COBID NUMBER(38,0) NOT NULL,
	STAGE_NAME VARCHAR(200),
	STAGE_URL VARCHAR(500),
	FOLDER_PATTERN VARCHAR(200),
	FILE_EXTENSION VARCHAR(200),
	FILE_LIST VARIANT,
	PROCESS_TIMESTAMP TIMESTAMP_TZ(9),
	FLAG_LAST_VERSION BOOLEAN,
	constraint PK_LOG_LIST_EXTERNAL_FILE primary key (ID)
)
;
