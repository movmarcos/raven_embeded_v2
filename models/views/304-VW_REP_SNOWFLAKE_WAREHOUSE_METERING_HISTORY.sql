create or replace view RAVEN.VW_REP_SNOWFLAKE_WAREHOUSE_METERING_HISTORY(
	START_TIME,
	END_TIME,
	WAREHOUSE_ID,
	WAREHOUSE_NAME,
	CREDITS_USED,
	START_DATE,
	END_DATE,
	WAREHOUSE_OPERATION_HOURS,
	TIME_OF_DAY
) as
SELECT
	START_TIME,
    END_TIME,
    WAREHOUSE_ID,
    WAREHOUSE_NAME,
    CREDITS_USED,
    TO_DATE(START_TIME) AS START_DATE,
    TO_DATE(END_TIME) AS END_DATE,
    DATEDIFF(HOUR, START_TIME, END_TIME) AS WAREHOUSE_OPERATION_HOURS,
    TO_CHAR(TO_TIME(START_TIME)) AS TIME_OF_DAY
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
