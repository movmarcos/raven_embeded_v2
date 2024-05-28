CREATE or replace TABLE RAVEN.METADATA_SOURCE_FIELD (
	SOURCE_SYSTEM_CODE VARCHAR(200) NOT NULL COLLATE 'en-ci',
	SOURCE_FEED_CODE VARCHAR(200) NOT NULL COLLATE 'en-ci',
	SOURCE_FIELD_NAME VARCHAR(100) NOT NULL COLLATE 'en-ci',
	TARGET_FIELD_NAME VARCHAR(100) NOT NULL COLLATE 'en-ci',
	FIELD_ORDINAL NUMBER(38,0),
	DATA_TYPE_NAME VARCHAR(20) NOT NULL COLLATE 'en-ci',
	DATA_TYPE_LENGTH NUMBER(38,0) NOT NULL,
	DATA_TYPE_PRECISION NUMBER(38,0) NOT NULL,
	DATA_TYPE_SCALE NUMBER(38,0) NOT NULL,
	IS_IN_SOURCE BOOLEAN NOT NULL,
	IS_IN_TARGET BOOLEAN NOT NULL,
	IS_DELETED BOOLEAN NOT NULL DEFAULT FALSE,
	DERIVED_EXPRESSION STRING COLLATE 'en-ci',
	FIRST_TIME_INSERTED TIMESTAMP_TZ(9),
	LAST_MODIFIED TIMESTAMP_TZ(9) DEFAULT CURRENT_TIMESTAMP(),
	constraint PK_METADATA_SOURCE_FIELD primary key (SOURCE_SYSTEM_CODE, SOURCE_FEED_CODE, SOURCE_FIELD_NAME)
)
;