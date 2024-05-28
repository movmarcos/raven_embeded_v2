CREATE OR REPLACE FUNCTION RAVEN.FN_LIST_UPLOAD_FILES("cobid" VARCHAR, "environment" VARCHAR)
RETURNS TABLE (FILE_LIST VARIANT)
LANGUAGE SQL
AS $$
    WITH SOURCE_FILE AS (
        SELECT 
            SOURCE_SYSTEM_CODE,
            SOURCE_FEED_CODE,
            LISTAGG(
                IFF(REPLACE(VALUE, ':ENVIRONMENT:', SPLIT_PART(environment,'_',1)) LIKE ':%:', 
                    TO_CHAR(TO_DATE(cobid,'YYYYMMDD'),REPLACE(VALUE,':','')), 
                    REPLACE(VALUE, ':ENVIRONMENT:', SPLIT_PART(LOWER(environment),'_',1))), 
                '') WITHIN GROUP (ORDER BY INDEX ASC) AS SOURCE_FILE_PATH
        FROM RAVEN.METADATA_SOURCE_FILE SPLITTABLE, LATERAL STRTOK_SPLIT_TO_TABLE(SPLITTABLE.SOURCE_FILE_PATH, '|')
        WHERE IS_ENABLED = TRUE
        GROUP BY SOURCE_SYSTEM_CODE, SOURCE_FEED_CODE
    )
    ,FILE_DETAIL AS(
        SELECT 
            SF.SOURCE_FILE_PATH,
            SP.FILE_NAME_COB    AS FILE_NAME,
            SP.FOLDER_PATH_COB  AS DESTINATION_FILE_PATH
        FROM SOURCE_FILE SF
        INNER JOIN RAVEN.VW_METADATA_STAGE_ME_PARAMETERS_COB SP
            ON SF.SOURCE_SYSTEM_CODE = SP.SOURCE_SYSTEM_CODE 
            AND SF.SOURCE_FEED_CODE = SP.SOURCE_FEED_CODE
        WHERE SP.RAVEN_COBID = cobid
    )
	SELECT DISTINCT 
	TO_JSON(OBJECT_CONSTRUCT(
		'SOURCE_FILE_PATH', SOURCE_FILE_PATH,
		'DESTINATION_FILE_PATH', DESTINATION_FILE_PATH,
		'FILE_LIST', ARRAY_AGG(FILE_NAME)  WITHIN GROUP (ORDER BY FILE_NAME ASC)
	))::VARIANT AS FILE_LIST
	FROM FILE_DETAIL
	GROUP BY SOURCE_FILE_PATH, DESTINATION_FILE_PATH
    
$$