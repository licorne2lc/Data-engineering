-- ============================================================
-- JOBS FINAUX — format JSON avec dateformat + timestampformat
-- Exécuter dans SQL Worksheet (Run Script F5)
-- ============================================================

BEGIN
    FOR j IN (SELECT job_name FROM user_scheduler_jobs WHERE job_name LIKE 'JOB_LOAD_%')
    LOOP DBMS_SCHEDULER.DROP_JOB(j.job_name, force => TRUE); END LOOP;
END;
/

-- 1. CALENDRIER — DATE : YYYY-MM-DD
BEGIN
  DBMS_SCHEDULER.CREATE_JOB(
    job_name        => 'JOB_LOAD_CALENDRIER',
    job_type        => 'PLSQL_BLOCK',
    job_action      => q'[
      BEGIN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE calendrier';
        DBMS_CLOUD.COPY_DATA(
          table_name      => 'CALENDRIER',
          credential_name => 'OCI_DATAOZ',
          file_uri_list   => 'https://objectstorage.eu-paris-1.oraclecloud.com/n/axdo67cv3ayo/b/dataoz-curated/o/calendrier.csv',
          format          => '{"type":"CSV","skipheaders":"1","delimiter":";","characterset":"AL32UTF8","blankasnull":"true","ignoremissingcolumns":"true","dateformat":"YYYY-MM-DD"}'
        );
        COMMIT;
      END;
    ]',
    start_date      => SYSTIMESTAMP,
    repeat_interval => 'FREQ=DAILY;BYHOUR=7;BYMINUTE=30;BYSECOND=0',
    enabled         => TRUE,
    comments        => 'Chargement quotidien calendrier depuis Object Storage'
  );
END;
/

-- 2. METEO_BRESSER — TIMESTAMP : YYYY-MM-DD HH24:MI:SS
BEGIN
  DBMS_SCHEDULER.CREATE_JOB(
    job_name        => 'JOB_LOAD_METEO_BRESSER',
    job_type        => 'PLSQL_BLOCK',
    job_action      => q'[
      BEGIN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE meteo_bresser';
        DBMS_CLOUD.COPY_DATA(
          table_name      => 'METEO_BRESSER',
          credential_name => 'OCI_DATAOZ',
          file_uri_list   => 'https://objectstorage.eu-paris-1.oraclecloud.com/n/axdo67cv3ayo/b/dataoz-curated/o/meteo_bresser.csv',
          format          => '{"type":"CSV","skipheaders":"1","delimiter":";","characterset":"AL32UTF8","blankasnull":"true","ignoremissingcolumns":"true","timestampformat":"YYYY-MM-DD HH24:MI:SS"}'
        );
        COMMIT;
      END;
    ]',
    start_date      => SYSTIMESTAMP,
    repeat_interval => 'FREQ=DAILY;BYHOUR=7;BYMINUTE=30;BYSECOND=0',
    enabled         => TRUE,
    comments        => 'Chargement quotidien meteo_bresser depuis Object Storage'
  );
END;
/

-- 3. ENEDIS_30MIN — TIMESTAMP : YYYY-MM-DD HH24:MI:SS
BEGIN
  DBMS_SCHEDULER.CREATE_JOB(
    job_name        => 'JOB_LOAD_ENEDIS_30MIN',
    job_type        => 'PLSQL_BLOCK',
    job_action      => q'[
      BEGIN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE enedis_30min';
        DBMS_CLOUD.COPY_DATA(
          table_name      => 'ENEDIS_30MIN',
          credential_name => 'OCI_DATAOZ',
          file_uri_list   => 'https://objectstorage.eu-paris-1.oraclecloud.com/n/axdo67cv3ayo/b/dataoz-curated/o/enedis_30min.csv',
          format          => '{"type":"CSV","skipheaders":"1","delimiter":";","characterset":"AL32UTF8","blankasnull":"true","ignoremissingcolumns":"true","timestampformat":"YYYY-MM-DD HH24:MI:SS"}'
        );
        COMMIT;
      END;
    ]',
    start_date      => SYSTIMESTAMP,
    repeat_interval => 'FREQ=DAILY;BYHOUR=7;BYMINUTE=30;BYSECOND=0',
    enabled         => TRUE,
    comments        => 'Chargement quotidien enedis_30min depuis Object Storage'
  );
END;
/

-- 4. ENEDIS_JOURNALIER — DATE : YYYY-MM-DD
BEGIN
  DBMS_SCHEDULER.CREATE_JOB(
    job_name        => 'JOB_LOAD_ENEDIS_JOURNALIER',
    job_type        => 'PLSQL_BLOCK',
    job_action      => q'[
      BEGIN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE enedis_journalier';
        DBMS_CLOUD.COPY_DATA(
          table_name      => 'ENEDIS_JOURNALIER',
          credential_name => 'OCI_DATAOZ',
          file_uri_list   => 'https://objectstorage.eu-paris-1.oraclecloud.com/n/axdo67cv3ayo/b/dataoz-curated/o/enedis_journalier.csv',
          format          => '{"type":"CSV","skipheaders":"1","delimiter":";","characterset":"AL32UTF8","blankasnull":"true","ignoremissingcolumns":"true","dateformat":"YYYY-MM-DD"}'
        );
        COMMIT;
      END;
    ]',
    start_date      => SYSTIMESTAMP,
    repeat_interval => 'FREQ=DAILY;BYHOUR=7;BYMINUTE=30;BYSECOND=0',
    enabled         => TRUE,
    comments        => 'Chargement quotidien enedis_journalier depuis Object Storage'
  );
END;
/

-- 5. TUYA_15MIN — TIMESTAMP : YYYY-MM-DD HH24:MI:SS
BEGIN
  DBMS_SCHEDULER.CREATE_JOB(
    job_name        => 'JOB_LOAD_TUYA_15MIN',
    job_type        => 'PLSQL_BLOCK',
    job_action      => q'[
      BEGIN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE tuya_15min';
        DBMS_CLOUD.COPY_DATA(
          table_name      => 'TUYA_15MIN',
          credential_name => 'OCI_DATAOZ',
          file_uri_list   => 'https://objectstorage.eu-paris-1.oraclecloud.com/n/axdo67cv3ayo/b/dataoz-curated/o/tuya_15min.csv',
          format          => '{"type":"CSV","skipheaders":"1","delimiter":";","characterset":"AL32UTF8","blankasnull":"true","ignoremissingcolumns":"true","timestampformat":"YYYY-MM-DD HH24:MI:SS"}'
        );
        COMMIT;
      END;
    ]',
    start_date      => SYSTIMESTAMP,
    repeat_interval => 'FREQ=DAILY;BYHOUR=7;BYMINUTE=30;BYSECOND=0',
    enabled         => TRUE,
    comments        => 'Chargement quotidien tuya_15min depuis Object Storage'
  );
END;
/

-- 6. TUYA_HORAIRE — TIMESTAMP : YYYY-MM-DD HH24:MI:SS
BEGIN
  DBMS_SCHEDULER.CREATE_JOB(
    job_name        => 'JOB_LOAD_TUYA_HORAIRE',
    job_type        => 'PLSQL_BLOCK',
    job_action      => q'[
      BEGIN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE tuya_horaire';
        DBMS_CLOUD.COPY_DATA(
          table_name      => 'TUYA_HORAIRE',
          credential_name => 'OCI_DATAOZ',
          file_uri_list   => 'https://objectstorage.eu-paris-1.oraclecloud.com/n/axdo67cv3ayo/b/dataoz-curated/o/tuya_horaire.csv',
          format          => '{"type":"CSV","skipheaders":"1","delimiter":";","characterset":"AL32UTF8","blankasnull":"true","ignoremissingcolumns":"true","timestampformat":"YYYY-MM-DD HH24:MI:SS"}'
        );
        COMMIT;
      END;
    ]',
    start_date      => SYSTIMESTAMP,
    repeat_interval => 'FREQ=DAILY;BYHOUR=7;BYMINUTE=30;BYSECOND=0',
    enabled         => TRUE,
    comments        => 'Chargement quotidien tuya_horaire depuis Object Storage'
  );
END;
/

-- 7. TUYA_JOURNALIER — DATE : YYYY-MM-DD
BEGIN
  DBMS_SCHEDULER.CREATE_JOB(
    job_name        => 'JOB_LOAD_TUYA_JOURNALIER',
    job_type        => 'PLSQL_BLOCK',
    job_action      => q'[
      BEGIN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE tuya_journalier';
        DBMS_CLOUD.COPY_DATA(
          table_name      => 'TUYA_JOURNALIER',
          credential_name => 'OCI_DATAOZ',
          file_uri_list   => 'https://objectstorage.eu-paris-1.oraclecloud.com/n/axdo67cv3ayo/b/dataoz-curated/o/tuya_journalier.csv',
          format          => '{"type":"CSV","skipheaders":"1","delimiter":";","characterset":"AL32UTF8","blankasnull":"true","ignoremissingcolumns":"true","dateformat":"YYYY-MM-DD"}'
        );
        COMMIT;
      END;
    ]',
    start_date      => SYSTIMESTAMP,
    repeat_interval => 'FREQ=DAILY;BYHOUR=7;BYMINUTE=30;BYSECOND=0',
    enabled         => TRUE,
    comments        => 'Chargement quotidien tuya_journalier depuis Object Storage'
  );
END;
/

-- 8. TUYA_MENSUEL — pas de colonne date/timestamp
BEGIN
  DBMS_SCHEDULER.CREATE_JOB(
    job_name        => 'JOB_LOAD_TUYA_MENSUEL',
    job_type        => 'PLSQL_BLOCK',
    job_action      => q'[
      BEGIN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE tuya_mensuel';
        DBMS_CLOUD.COPY_DATA(
          table_name      => 'TUYA_MENSUEL',
          credential_name => 'OCI_DATAOZ',
          file_uri_list   => 'https://objectstorage.eu-paris-1.oraclecloud.com/n/axdo67cv3ayo/b/dataoz-curated/o/tuya_mensuel.csv',
          format          => '{"type":"CSV","skipheaders":"1","delimiter":";","characterset":"AL32UTF8","blankasnull":"true","ignoremissingcolumns":"true"}'
        );
        COMMIT;
      END;
    ]',
    start_date      => SYSTIMESTAMP,
    repeat_interval => 'FREQ=DAILY;BYHOUR=7;BYMINUTE=30;BYSECOND=0',
    enabled         => TRUE,
    comments        => 'Chargement quotidien tuya_mensuel depuis Object Storage'
  );
END;
/

-- 9. FINANCE_COTATIONS — DATE : YYYY-MM-DD
BEGIN
  DBMS_SCHEDULER.CREATE_JOB(
    job_name        => 'JOB_LOAD_FINANCE_COTATIONS',
    job_type        => 'PLSQL_BLOCK',
    job_action      => q'[
      BEGIN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE finance_cotations';
        DBMS_CLOUD.COPY_DATA(
          table_name      => 'FINANCE_COTATIONS',
          credential_name => 'OCI_DATAOZ',
          file_uri_list   => 'https://objectstorage.eu-paris-1.oraclecloud.com/n/axdo67cv3ayo/b/dataoz-curated/o/finance_cotations.csv',
          format          => '{"type":"CSV","skipheaders":"1","delimiter":";","characterset":"AL32UTF8","blankasnull":"true","ignoremissingcolumns":"true","dateformat":"YYYY-MM-DD"}'
        );
        COMMIT;
      END;
    ]',
    start_date      => SYSTIMESTAMP,
    repeat_interval => 'FREQ=DAILY;BYHOUR=7;BYMINUTE=30;BYSECOND=0',
    enabled         => TRUE,
    comments        => 'Chargement quotidien finance_cotations depuis Object Storage'
  );
END;
/

-- Vérification
SELECT job_name, enabled, state, next_run_date
FROM user_scheduler_jobs
WHERE job_name LIKE 'JOB_LOAD_%'
ORDER BY job_name;
