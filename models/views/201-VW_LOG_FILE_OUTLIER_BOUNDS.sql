CREATE OR REPLACE VIEW RAVEN.VW_LOG_FILE_OUTLIER_BOUNDS AS
WITH STAGE_ME_STATUS_TOTAL_ROW AS(
SELECT
	 ID,
	 SUM(CAST(VALUE:rows_loaded AS INT)) 	AS ROWS_LOADED,
	 SUM(CAST(VALUE:rows_parsed AS INT))	AS ROWS_PARSED
FROM RAVEN.LOG_STAGE_ME_STATUS AS S,TABLE (flatten(S.PROCESS_RESULT ,'msg_copy_result', outer => FALSE)) F
GROUP BY ID
)
,STAGE_ME AS (
	SELECT 
		S.DATASET_NAME,
		S.ID,
		S.RAVEN_COBID,
		ROWS_LOADED/1.0	AS TOTAL_ROWS_STAGED,
		PROCESS_STATUS AS STAGE_ME_STATUS
	FROM RAVEN.LOG_STAGE_ME_STATUS S
  LEFT JOIN STAGE_ME_STATUS_TOTAL_ROW SR ON S.ID = SR.ID
)
,ORDERED_LIST AS (
SELECT
	DATASET_NAME,
	TOTAL_ROWS_STAGED,
	COUNT(*) OVER (PARTITION BY DATASET_NAME) ROW_C,
	CASE 
		WHEN ROW_NUMBER() OVER (PARTITION BY DATASET_NAME ORDER BY TOTAL_ROWS_STAGED) = FLOOR(COUNT(*) OVER (PARTITION BY DATASET_NAME) * 0.75) THEN 'Q_THREE'
		WHEN ROW_NUMBER() OVER (PARTITION BY DATASET_NAME ORDER BY TOTAL_ROWS_STAGED) = FLOOR(COUNT(*) OVER (PARTITION BY DATASET_NAME) * 0.25) THEN 'Q_ONE'
	END QUARTILE ,
	ROW_NUMBER() OVER (PARTITION BY DATASET_NAME ORDER BY TOTAL_ROWS_STAGED) AS ROW_N,
	NTILE(4) OVER (PARTITION BY DATASET_NAME ORDER BY TOTAL_ROWS_STAGED)
FROM STAGE_ME
WHERE STAGE_ME_STATUS = 'SUCCESS'
)
,OUTLIER_CALC AS (
SELECT
 DATASET_NAME
,A.TOTAL_ROWS_STAGED
,(SELECT MAX(B.TOTAL_ROWS_STAGED) FROM ORDERED_LIST B WHERE QUARTILE = 'Q_THREE' AND A.DATASET_NAME = B.DATASET_NAME)					AS Q_THREE
,(SELECT MAX(B.TOTAL_ROWS_STAGED) FROM ORDERED_LIST B WHERE QUARTILE = 'Q_ONE' AND A.DATASET_NAME = B.DATASET_NAME) 	 				AS Q_ONE
,ROUND(1.5*( Q_THREE - Q_ONE) ,0)		AS OUTLIER_RANGE
FROM ORDERED_LIST A
)
SELECT 
 DATASET_NAME
,MAX(Q_THREE)						AS Q_THREE
,MAX(Q_ONE)							AS Q_ONE
,MAX(OUTLIER_RANGE)					AS OUTLIER_RANGE
,IFF(MAX(Q_ONE) - MAX(OUTLIER_RANGE) < 0, 0, MAX(Q_ONE) - MAX(OUTLIER_RANGE))	AS LOWER_BOUND
,MAX(Q_THREE) + MAX(OUTLIER_RANGE)	AS UPPER_BOUND
,'['|| TRIM(TO_CHAR(ROUND(LOWER_BOUND,0),'999,999,999,999')) || ' - ' || TRIM(TO_CHAR(ROUND(UPPER_BOUND,0),'999,999,999,999')) || ']' 		AS BOUNDS
FROM OUTLIER_CALC
GROUP BY  DATASET_NAME
;