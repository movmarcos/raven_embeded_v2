CREATE or replace TASK RAVEN.TASK_LIST_STORAGE_SPACES
    WAREHOUSE = 'DEMO_WH'
    SCHEDULE = '60 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    SUSPEND_TASK_AFTER_NUM_FAILURES = 15
AS
DECLARE
  COBID_START INTEGER;
  COBID_END   INTEGER;
  res RESULTSET DEFAULT 
  (
    SELECT 
      MIN(RAVEN_COBID) COBID_START, 
      MAX(RAVEN_COBID) COBID_END 
		FROM (SELECT DISTINCT RAVEN_COBID 
						FROM RAVEN.LOG_STAGE_ME_STATUS 
            ORDER BY RAVEN_COBID DESC LIMIT 3)
  );
  c1 CURSOR FOR res;
BEGIN
  FOR row_variable IN c1 DO
  	COBID_START := row_variable.COBID_START;
    COBID_END := row_variable.COBID_END;
    CALL RAVEN.LIST_STORAGE_SPACES('',:COBID_START,:COBID_END);
  END FOR;
END;