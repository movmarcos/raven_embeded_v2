CREATE OR REPLACE FILE FORMAT RAVEN.CSV_FORMAT_PARSE_HEADER
	ESCAPE_UNENCLOSED_FIELD = 'NONE'
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
	ENCODING = 'windows-1252'
	PARSE_HEADER = TRUE
;
