-- ============================================================
-- CORRECTION : délimiteur JOB_LOAD_METEO_BRESSER
-- Le job avait été créé avec delimiter=',' mais le CSV produit
-- par upload_to_bucket.py utilise ';' pour TOUS les fichiers.
-- Exécuter une seule fois dans SQL Worksheet (Run Script F5)
-- ============================================================

BEGIN
    DBMS_SCHEDULER.DROP_JOB('JOB_LOAD_METEO_BRESSER', force => TRUE);
END;
/

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
          format          => JSON_OBJECT(
            'type'          VALUE 'CSV',
            'skipheaders'   VALUE '1',
            'delimiter'     VALUE ';',
            'characterset'  VALUE 'AL32UTF8',
            'blankasnull'   VALUE 'true',
            'ignoremissingcolumns' VALUE 'true'
          ),
          column_list => 'ts,source,qualite,temp_interieure,hum_interieure,temp_exterieure,hum_exterieure,ressenti,point_rosee,indice_chaleur,refroidissement_eolien,pression_abs,pression_rel,vent_vitesse,vent_rafale,vent_direction,pluie_taux,pluie_horaire,uvi,luminosite,temp_etage,hum_etage,temp_cave,hum_cave'
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

-- Vérification
SELECT job_name, enabled, state, repeat_interval, next_run_date
FROM   user_scheduler_jobs
WHERE  job_name = 'JOB_LOAD_METEO_BRESSER';
