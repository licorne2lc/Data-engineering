-- ============================================================
-- DATAOZ — Ajout table ENEDIS_HORAIRE + job DBMS_SCHEDULER
-- Exécuter dans SQL Worksheet (Run Script F5) UNE SEULE FOIS
-- ============================================================
-- Table  : enedis_horaire
-- Source : database_enedis_horaire.csv (bucket dataoz-curated)
-- Colonnes CSV : ts ; source ; conso_kwh
--   ts        → YYYY-MM-DD HH:00:00  (début de l'heure)
--   source    → 'agregat_30min'
--   conso_kwh → kWh sur l'heure (somme des 2 tranches 30 min)
-- ============================================================


-- ────────────────────────────────────────────────────────────
-- 1. CREATE TABLE
-- ────────────────────────────────────────────────────────────

CREATE TABLE enedis_horaire (
    ts         TIMESTAMP    NOT NULL,
    source     VARCHAR2(30),
    conso_kwh  NUMBER(10,4),
    CONSTRAINT pk_enedis_horaire PRIMARY KEY (ts)
);

CREATE INDEX idx_enedis_horaire_ts ON enedis_horaire (ts DESC);


-- ────────────────────────────────────────────────────────────
-- 2. Supprimer le job s'il existe déjà (re-exécution safe)
-- ────────────────────────────────────────────────────────────

BEGIN
    DBMS_SCHEDULER.DROP_JOB('JOB_LOAD_ENEDIS_HORAIRE', force => TRUE);
EXCEPTION
    WHEN OTHERS THEN NULL;  -- job inexistant : on ignore
END;
/


-- ────────────────────────────────────────────────────────────
-- 3. DBMS_SCHEDULER — chargement quotidien à 07h30 UTC
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
-- 4. Vérification
-- ────────────────────────────────────────────────────────────

-- Confirme que la table est vide (sera chargée au prochain run)
SELECT COUNT(*) AS nb_lignes FROM enedis_horaire;

-- Confirme que le job est planifié
SELECT job_name, enabled, state, next_run_date
FROM   user_scheduler_jobs
WHERE  job_name = 'JOB_LOAD_ENEDIS_HORAIRE';
