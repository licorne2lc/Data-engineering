-- ============================================================
-- DATAOZ — Jobs DBMS_SCHEDULER + DBMS_CLOUD.COPY_DATA
-- Chargement automatique depuis Oracle Object Storage
-- Namespace : axdo67cv3ayo  |  Bucket : dataoz-curated
-- Region    : eu-paris-1 (France Central)
-- Planif.   : tous les jours à 07h30 UTC
--             (après le dépôt des CSV par le DAG Airflow ~06h15)
-- ============================================================
-- Exécuter ce script UNE SEULE FOIS dans SQL Worksheet (Run Script F5)
-- ============================================================

-- Base URL Object Storage
-- https://objectstorage.eu-paris-1.oraclecloud.com/n/axdo67cv3ayo/b/dataoz-curated/o/

-- ────────────────────────────────────────────────────────────
-- 0. Supprimer les jobs existants (si re-exécution du script)
-- ────────────────────────────────────────────────────────────
BEGIN
    FOR j IN (
        SELECT job_name FROM user_scheduler_jobs
        WHERE job_name LIKE 'JOB_LOAD_%'
    ) LOOP
        DBMS_SCHEDULER.DROP_JOB(j.job_name, force => TRUE);
    END LOOP;
END;
/

-- ────────────────────────────────────────────────────────────
-- 1. CALENDRIER
-- ────────────────────────────────────────────────────────────
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
          format          => JSON_OBJECT(
            'type'          VALUE 'CSV',
            'skipheaders'   VALUE '1',
            'delimiter'     VALUE ';',
            'characterset'  VALUE 'AL32UTF8',
            'blankasnull'   VALUE 'true',
            'ignoremissingcolumns' VALUE 'true'
          ),
          column_list => 'date_jour,jour_semaine,jour_sem,num_semaine_iso,sem_impaire,utc,nom_jour_ferie,vac_scol_a,vac_scol_b,vac_scol_c'
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

-- ────────────────────────────────────────────────────────────
-- 2. METEO_BRESSER
-- ────────────────────────────────────────────────────────────
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

-- ────────────────────────────────────────────────────────────
-- 3. ENEDIS_30MIN
-- ────────────────────────────────────────────────────────────
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
          format          => JSON_OBJECT(
            'type'          VALUE 'CSV',
            'skipheaders'   VALUE '1',
            'delimiter'     VALUE ';',
            'characterset'  VALUE 'AL32UTF8',
            'blankasnull'   VALUE 'true',
            'ignoremissingcolumns' VALUE 'true'
          ),
          column_list => 'ts,source,conso_w'
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

-- ────────────────────────────────────────────────────────────
-- 4. ENEDIS_JOURNALIER
-- ────────────────────────────────────────────────────────────
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
          format          => JSON_OBJECT(
            'type'          VALUE 'CSV',
            'skipheaders'   VALUE '1',
            'delimiter'     VALUE ';',
            'characterset'  VALUE 'AL32UTF8',
            'blankasnull'   VALUE 'true',
            'ignoremissingcolumns' VALUE 'true'
          ),
          column_list => 'date_jour,source,conso_kwh'
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

-- ────────────────────────────────────────────────────────────
-- 5. ENEDIS_HORAIRE — TIMESTAMP : YYYY-MM-DD HH24:MI:SS
-- ────────────────────────────────────────────────────────────
BEGIN
  DBMS_SCHEDULER.CREATE_JOB(
    job_name        => 'JOB_LOAD_ENEDIS_HORAIRE',
    job_type        => 'PLSQL_BLOCK',
    job_action      => q'[
      BEGIN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE enedis_horaire';
        DBMS_CLOUD.COPY_DATA(
          table_name      => 'ENEDIS_HORAIRE',
          credential_name => 'OCI_DATAOZ',
          file_uri_list   => 'https://objectstorage.eu-paris-1.oraclecloud.com/n/axdo67cv3ayo/b/dataoz-curated/o/enedis_horaire.csv',
          format          => '{"type":"CSV","skipheaders":"1","delimiter":";","characterset":"AL32UTF8","blankasnull":"true","ignoremissingcolumns":"true","timestampformat":"YYYY-MM-DD HH24:MI:SS"}'
        );
        COMMIT;
      END;
    ]',
    start_date      => SYSTIMESTAMP,
    repeat_interval => 'FREQ=DAILY;BYHOUR=7;BYMINUTE=30;BYSECOND=0',
    enabled         => TRUE,
    comments        => 'Chargement quotidien enedis_horaire depuis Object Storage'
  );
END;
/

-- ────────────────────────────────────────────────────────────
-- 6. TUYA_15MIN
-- ────────────────────────────────────────────────────────────
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
          format          => JSON_OBJECT(
            'type'          VALUE 'CSV',
            'skipheaders'   VALUE '1',
            'delimiter'     VALUE ';',
            'characterset'  VALUE 'AL32UTF8',
            'blankasnull'   VALUE 'true',
            'ignoremissingcolumns' VALUE 'true'
          ),
          column_list => 'periode_15min,ts,ballon_eau_chaude,chauffage,frigo,jaccuzzi,loan,parfum_salon,prise_pc,prise_parfum_ch,teleprojecteur,tv_chambre,tv_salon,total_kwh'
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

-- ────────────────────────────────────────────────────────────
-- 6. TUYA_HORAIRE
-- ────────────────────────────────────────────────────────────
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
          format          => JSON_OBJECT(
            'type'          VALUE 'CSV',
            'skipheaders'   VALUE '1',
            'delimiter'     VALUE ';',
            'characterset'  VALUE 'AL32UTF8',
            'blankasnull'   VALUE 'true',
            'ignoremissingcolumns' VALUE 'true'
          ),
          column_list => 'heure,ts,ballon_eau_chaude,chauffage,frigo,prise_pc,teleprojecteur,tv_chambre,total_kwh'
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

-- ────────────────────────────────────────────────────────────
-- 7. TUYA_JOURNALIER
-- ────────────────────────────────────────────────────────────
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
          format          => JSON_OBJECT(
            'type'          VALUE 'CSV',
            'skipheaders'   VALUE '1',
            'delimiter'     VALUE ';',
            'characterset'  VALUE 'AL32UTF8',
            'blankasnull'   VALUE 'true',
            'ignoremissingcolumns' VALUE 'true'
          ),
          column_list => 'jour,date_jour,ballon_eau_chaude,chauffage,frigo,jaccuzzi,loan,parfum_salon,prise_pc,prise_parfum_ch,teleprojecteur,tv_chambre,tv_salon,total_kwh'
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

-- ────────────────────────────────────────────────────────────
-- 8. TUYA_MENSUEL
-- ────────────────────────────────────────────────────────────
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
          format          => JSON_OBJECT(
            'type'          VALUE 'CSV',
            'skipheaders'   VALUE '1',
            'delimiter'     VALUE ';',
            'characterset'  VALUE 'AL32UTF8',
            'blankasnull'   VALUE 'true',
            'ignoremissingcolumns' VALUE 'true'
          ),
          column_list => 'mois,date_lisible,ballon_eau_chaude,chauffage,frigo,jaccuzzi,loan,parfum_salon,prise_pc,prise_parfum_ch,teleprojecteur,tv_chambre,tv_salon,total_kwh'
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

-- ────────────────────────────────────────────────────────────
-- 9. FINANCE_COTATIONS  (via table de staging)
--    Le CSV (16 col.) contient une colonne "open" absente de la
--    table Oracle et a VOLUME/VARIATION dans l'ordre inverse.
--    Solution : charger dans FINANCE_COTATIONS_STAGE (16 col.)
--    puis INSERT SELECT avec mapping explicite.
--    PRÉ-REQUIS : créer la table de staging une seule fois :
--      CREATE TABLE finance_cotations_stage (
--          date_import DATE, label VARCHAR2(100),
--          symbol VARCHAR2(30), isin VARCHAR2(20),
--          mnemonic VARCHAR2(20), dernier NUMBER,
--          precedent NUMBER, haut NUMBER, bas NUMBER,
--          open_price NUMBER, volume NUMBER, variation NUMBER,
--          exchange_code VARCHAR2(10), categorie VARCHAR2(20),
--          secteur VARCHAR2(100), pays VARCHAR2(50)
--      );
-- ────────────────────────────────────────────────────────────
BEGIN
  DBMS_SCHEDULER.CREATE_JOB(
    job_name        => 'JOB_LOAD_FINANCE_COTATIONS',
    job_type        => 'PLSQL_BLOCK',
    job_action      => q'[
      BEGIN
        -- 1. Charger le CSV brut dans la table de staging (match exact)
        EXECUTE IMMEDIATE 'TRUNCATE TABLE finance_cotations_stage';
        DBMS_CLOUD.COPY_DATA(
          table_name      => 'FINANCE_COTATIONS_STAGE',
          credential_name => 'OCI_DATAOZ',
          file_uri_list   => 'https://objectstorage.eu-paris-1.oraclecloud.com/n/axdo67cv3ayo/b/dataoz-curated/o/finance_cotations.csv',
          format          => '{"type":"CSV","skipheaders":"1","delimiter":";","characterset":"AL32UTF8","blankasnull":"true","ignoremissingcolumns":"true","dateformat":"YYYY-MM-DD"}'
        );
        -- 2. Copier dans la table finale avec mapping explicite
        --    (open_price ignoré, RISK_LEVEL/ELIGIBILITY/ELIG_PEA laissés NULL)
        EXECUTE IMMEDIATE 'TRUNCATE TABLE finance_cotations';
        INSERT INTO finance_cotations (
            date_import, label, symbol, isin, mnemonic,
            dernier, precedent, haut, bas,
            variation, volume,
            exchange_code, categorie, secteur, pays
        )
        SELECT
            date_import, label, symbol, isin, mnemonic,
            dernier, precedent, haut, bas,
            variation, volume,
            exchange_code, categorie, secteur, pays
        FROM finance_cotations_stage;
        COMMIT;
      END;
    ]',
    start_date      => SYSTIMESTAMP,
    repeat_interval => 'FREQ=DAILY;BYHOUR=7;BYMINUTE=30;BYSECOND=0',
    enabled         => TRUE,
    comments        => 'Chargement quotidien finance_cotations depuis Object Storage (via staging)'
  );
END;
/

-- ────────────────────────────────────────────────────────────
-- VÉRIFICATION — liste des jobs créés
-- ────────────────────────────────────────────────────────────
SELECT job_name, enabled, state, repeat_interval, next_run_date
FROM   user_scheduler_jobs
WHERE  job_name LIKE 'JOB_LOAD_%'
ORDER  BY job_name;
