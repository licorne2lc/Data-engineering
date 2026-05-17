-- ============================================================
-- CORRECTION : suppression de la colonne ID (GENERATED ALWAYS)
-- DBMS_CLOUD.COPY_DATA mappe par position → ID en col 1 bloque tout
-- Exécuter dans SQL Worksheet (Run Script F5)
-- ============================================================

ALTER TABLE calendrier        DROP COLUMN id;
ALTER TABLE meteo_bresser     DROP COLUMN id;
ALTER TABLE enedis_30min      DROP COLUMN id;
ALTER TABLE enedis_journalier DROP COLUMN id;
ALTER TABLE tuya_15min        DROP COLUMN id;
ALTER TABLE tuya_horaire      DROP COLUMN id;
ALTER TABLE tuya_journalier   DROP COLUMN id;
ALTER TABLE tuya_mensuel      DROP COLUMN id;
ALTER TABLE finance_cotations DROP COLUMN id;

-- Vérification : plus aucune colonne identité
SELECT table_name, column_name
FROM user_tab_columns
WHERE identity_column = 'YES'
ORDER BY table_name;

-- Test immédiat sur calendrier
BEGIN
  EXECUTE IMMEDIATE 'TRUNCATE TABLE calendrier';
  DBMS_CLOUD.COPY_DATA(
    table_name      => 'CALENDRIER',
    credential_name => 'OCI_DATAOZ',
    file_uri_list   => 'https://objectstorage.eu-paris-1.oraclecloud.com/n/axdo67cv3ayo/b/dataoz-curated/o/calendrier.csv',
    format          => '{"type":"CSV","skipheaders":"1","delimiter":";","characterset":"AL32UTF8","blankasnull":"true","ignoremissingcolumns":"true"}'
  );
  COMMIT;
  DBMS_OUTPUT.PUT_LINE('OK — calendrier chargé');
END;
/

SELECT COUNT(*) AS nb_lignes FROM calendrier;
