-- ============================================================
-- CORRECTION : remplacement JSON_OBJECT par string JSON littéral
-- DBMS_CLOUD.COPY_DATA attend un CLOB/VARCHAR2, pas un type JSON
-- Exécuter dans SQL Worksheet (Run Script F5)
-- ============================================================

-- Suppression de tous les jobs existants
BEGIN
    FOR j IN (
        SELECT job_name FROM user_scheduler_jobs
        WHERE job_name LIKE 'JOB_LOAD_%'
    ) LOOP
        DBMS_SCHEDULER.DROP_JOB(j.job_name, force => TRUE);
    END LOOP;
END;
/

-- 1. CALENDRIER
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
          format          => '{"type":"CSV","skipheaders":"1","delimiter":";","characterset":"AL32UTF8","blankasnull":"true","ignoremissingcolumns":"true"}',
          column_list     => 'date_jour,jour_semaine,jour_sem,num_semaine_iso,sem_impaire,utc,nom_jour_ferie,vac_scol_a,vac_scol_b,vac_scol_c'
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

-- 2. METEO_BRESSER
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
          format          => '{"type":"CSV","skipheaders":"1","delimiter":";","characterset":"AL32UTF8","blankasnull":"true","ignoremissingcolumns":"true"}',
          column_list     => 'ts,source,qualite,temp_interieure,hum_interieure,temp_exterieure,hum_exterieure,ressenti,point_rosee,indice_chaleur,refroidissement_eolien,pression_abs,pression_rel,vent_vitesse,vent_rafale,vent_direction,pluie_taux,pluie_horaire,uvi,luminosite,temp_etage,hum_etage,temp_cave,hum_cave'
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

-- 3. ENEDIS_30MIN
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
          format          => '{"type":"CSV","skipheaders":"1","delimiter":";","characterset":"AL32UTF8","blankasnull":"true","ignoremissingcolumns":"true"}',
          column_list     => 'ts,source,conso_w'
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

-- 4. ENEDIS_JOURNALIER
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
          format          => '{"type":"CSV","skipheaders":"1","delimiter":";","characterset":"AL32UTF8","blankasnull":"true","ignoremissingcolumns":"true"}',
          column_list     => 'date_jour,source,conso_kwh'
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

-- 5. TUYA_15MIN
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
          format          => '{"type":"CSV","skipheaders":"1","delimiter":";","characterset":"AL32UTF8","blankasnull":"true","ignoremissingcolumns":"true"}',
          column_list     => 'periode_15min,ts,ballon_eau_chaude,chauffage,frigo,jaccuzzi,loan,parfum_salon,prise_pc,prise_parfum_ch,teleprojecteur,tv_chambre,tv_salon,total_kwh'
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

-- 6. TUYA_HORAIRE
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
          format          => '{"type":"CSV","skipheaders":"1","delimiter":";","characterset":"AL32UTF8","blankasnull":"true","ignoremissingcolumns":"true"}',
          column_list     => 'heure,ts,ballon_eau_chaude,chauffage,frigo,prise_pc,teleprojecteur,tv_chambre,total_kwh'
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

-- 7. TUYA_JOURNALIER
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
          format          => '{"type":"CSV","skipheaders":"1","delimiter":";","characterset":"AL32UTF8","blankasnull":"true","ignoremissingcolumns":"true"}',
          column_list     => 'jour,date_jour,ballon_eau_chaude,chauffage,frigo,jaccuzzi,loan,parfum_salon,prise_pc,prise_parfum_ch,teleprojecteur,tv_chambre,tv_salon,total_kwh'
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

-- 8. TUYA_MENSUEL
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
          format          => '{"type":"CSV","skipheaders":"1","delimiter":";","characterset":"AL32UTF8","blankasnull":"true","ignoremissingcolumns":"true"}',
          column_list     => 'mois,date_lisible,ballon_eau_chaude,chauffage,frigo,jaccuzzi,loan,parfum_salon,prise_pc,prise_parfum_ch,teleprojecteur,tv_chambre,tv_salon,total_kwh'
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

-- 9. FINANCE_COTATIONS
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
          format          => '{"type":"CSV","skipheaders":"1","delimiter":";","characterset":"AL32UTF8","blankasnull":"true","ignoremissingcolumns":"true"}',
          column_list     => 'date_import,label,symbol,isin,mnemonic,dernier,precedent,haut,bas,variation,volume,exchange_code,categorie,secteur,pays,risk_level,eligibility,elig_pea'
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
SELECT job_name, enabled, state, repeat_interval, next_run_date
FROM   user_scheduler_jobs
WHERE  job_name LIKE 'JOB_LOAD_%'
ORDER  BY job_name;
