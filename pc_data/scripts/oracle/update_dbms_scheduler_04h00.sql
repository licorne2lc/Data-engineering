-- =============================================================================
-- Modification des jobs DBMS_SCHEDULER : 07:30 UTC -> 02:00 UTC (04:00 CEST)
-- Objectif : tables chargees avant dag_check_pipeline (05:15 CEST)
-- Chaine : dag_oracle_load (02:30 CEST) -> DBMS_SCHEDULER (04:00 CEST) -> check (05:15 CEST)
-- =============================================================================

BEGIN
  DBMS_SCHEDULER.SET_ATTRIBUTE('JOB_LOAD_CALENDRIER',        'repeat_interval', 'FREQ=DAILY;BYHOUR=2;BYMINUTE=0;BYSECOND=0');
  DBMS_SCHEDULER.SET_ATTRIBUTE('JOB_LOAD_ENEDIS_30MIN',      'repeat_interval', 'FREQ=DAILY;BYHOUR=2;BYMINUTE=0;BYSECOND=0');
  DBMS_SCHEDULER.SET_ATTRIBUTE('JOB_LOAD_ENEDIS_HORAIRE',    'repeat_interval', 'FREQ=DAILY;BYHOUR=2;BYMINUTE=0;BYSECOND=0');
  DBMS_SCHEDULER.SET_ATTRIBUTE('JOB_LOAD_ENEDIS_JOURNALIER', 'repeat_interval', 'FREQ=DAILY;BYHOUR=2;BYMINUTE=0;BYSECOND=0');
  DBMS_SCHEDULER.SET_ATTRIBUTE('JOB_LOAD_FINANCE_COTATIONS', 'repeat_interval', 'FREQ=DAILY;BYHOUR=2;BYMINUTE=0;BYSECOND=0');
  DBMS_SCHEDULER.SET_ATTRIBUTE('JOB_LOAD_METEO_BRESSER',     'repeat_interval', 'FREQ=DAILY;BYHOUR=2;BYMINUTE=0;BYSECOND=0');
  DBMS_SCHEDULER.SET_ATTRIBUTE('JOB_LOAD_TUYA_15MIN',        'repeat_interval', 'FREQ=DAILY;BYHOUR=2;BYMINUTE=0;BYSECOND=0');
  DBMS_SCHEDULER.SET_ATTRIBUTE('JOB_LOAD_TUYA_HORAIRE',      'repeat_interval', 'FREQ=DAILY;BYHOUR=2;BYMINUTE=0;BYSECOND=0');
  DBMS_SCHEDULER.SET_ATTRIBUTE('JOB_LOAD_TUYA_JOURNALIER',   'repeat_interval', 'FREQ=DAILY;BYHOUR=2;BYMINUTE=0;BYSECOND=0');
  DBMS_SCHEDULER.SET_ATTRIBUTE('JOB_LOAD_TUYA_MENSUEL',      'repeat_interval', 'FREQ=DAILY;BYHOUR=2;BYMINUTE=0;BYSECOND=0');
END;
/

-- Verification apres modification
SELECT job_name, repeat_interval, next_run_date
FROM user_scheduler_jobs
WHERE job_name LIKE 'JOB_LOAD%'
ORDER BY job_name;
