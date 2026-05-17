-- ============================================================
--  SCHEMA PostgreSQL - Projet DataOZ
--  Généré automatiquement depuis les fichiers curated
--  Chaque table dispose d'un id SERIAL (PK auto-incrémenté)
--  + contrainte UNIQUE sur la clé naturelle temporelle
-- ============================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ------------------------------------------------------------
-- 1. MÉTÉO - Station Bresser (granularité 30 min)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meteo_bresser (
    id                      SERIAL          PRIMARY KEY,
    timestamp               TIMESTAMPTZ     NOT NULL UNIQUE,
    source                  VARCHAR(20),
    qualite                 VARCHAR(20),
    -- Intérieur
    temp_interieure         FLOAT,
    hum_interieure          INT,
    -- Extérieur
    temp_exterieure         FLOAT,
    hum_exterieure          INT,
    ressenti                FLOAT,
    point_rosee             FLOAT,
    indice_chaleur          FLOAT,
    refroidissement_eolien  FLOAT,
    -- Pression
    pression_abs            FLOAT,
    pression_rel            FLOAT,
    -- Vent
    vent_vitesse            FLOAT,
    vent_rafale             FLOAT,
    vent_direction          INT,
    -- Précipitations
    pluie_taux              FLOAT,
    pluie_horaire           FLOAT,
    -- Ensoleillement
    uvi                     FLOAT,
    luminosite              FLOAT,
    -- Sondes supplémentaires
    temp_etage              FLOAT,
    hum_etage               INT,
    temp_cave               FLOAT,
    hum_cave                INT,
    temp_ch3                FLOAT,
    hum_ch3                 INT,
    temp_ch4                FLOAT,
    hum_ch4                 INT,
    temp_ch5                FLOAT,
    hum_ch5                 INT,
    temp_ch6                FLOAT,
    hum_ch6                 INT,
    temp_ch7                FLOAT,
    hum_ch7                 INT
);

CREATE INDEX IF NOT EXISTS idx_meteo_bresser_ts ON meteo_bresser (timestamp DESC);

-- ------------------------------------------------------------
-- 2. ENEDIS - Consommation réseau 30 minutes
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS enedis_30min (
    id          SERIAL          PRIMARY KEY,
    timestamp   TIMESTAMPTZ     NOT NULL UNIQUE,
    source      VARCHAR(30),
    conso_w     FLOAT
);

CREATE INDEX IF NOT EXISTS idx_enedis_30min_ts ON enedis_30min (timestamp DESC);

-- ------------------------------------------------------------
-- 3. ENEDIS - Consommation réseau journalière
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS enedis_journalier (
    id          SERIAL          PRIMARY KEY,
    date        DATE            NOT NULL UNIQUE,
    source      VARCHAR(30),
    conso_kwh   FLOAT
);

CREATE INDEX IF NOT EXISTS idx_enedis_journalier_date ON enedis_journalier (date DESC);

-- ------------------------------------------------------------
-- 4. TUYA - Consommation appareils 15 minutes
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tuya_15min (
    id                      SERIAL          PRIMARY KEY,
    periode_15min           VARCHAR(15)     NOT NULL UNIQUE,  -- ex: 202604240900
    timestamp               TIMESTAMPTZ,
    ballon_eau_chaude       FLOAT           DEFAULT 0,
    chauffage               FLOAT           DEFAULT 0,
    frigo                   FLOAT           DEFAULT 0,
    jaccuzzi                FLOAT           DEFAULT 0,
    loan                    FLOAT           DEFAULT 0,
    parfum_salon            FLOAT           DEFAULT 0,
    prise_pc                FLOAT           DEFAULT 0,
    prise_parfum_ch_parents FLOAT           DEFAULT 0,
    teleprojecteur          FLOAT           DEFAULT 0,
    tv_chambre              FLOAT           DEFAULT 0,
    tv_salon                FLOAT           DEFAULT 0,
    total_kwh               FLOAT           DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_tuya_15min_ts ON tuya_15min (timestamp DESC);

-- ------------------------------------------------------------
-- 5. TUYA - Consommation appareils horaire
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tuya_horaire (
    id                      SERIAL          PRIMARY KEY,
    heure                   VARCHAR(12)     NOT NULL UNIQUE,  -- ex: 2026040109
    timestamp               TIMESTAMPTZ,
    ballon_eau_chaude       FLOAT           DEFAULT 0,
    chauffage               FLOAT           DEFAULT 0,
    frigo                   FLOAT           DEFAULT 0,
    prise_pc                FLOAT           DEFAULT 0,
    teleprojecteur          FLOAT           DEFAULT 0,
    tv_chambre              FLOAT           DEFAULT 0,
    total_kwh               FLOAT           DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_tuya_horaire_ts ON tuya_horaire (timestamp DESC);

-- ------------------------------------------------------------
-- 6. TUYA - Consommation appareils journalière
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tuya_journalier (
    id                      SERIAL          PRIMARY KEY,
    jour                    VARCHAR(10)     NOT NULL UNIQUE,  -- ex: 20231027
    date                    DATE,
    ballon_eau_chaude       FLOAT           DEFAULT 0,
    chauffage               FLOAT           DEFAULT 0,
    frigo                   FLOAT           DEFAULT 0,
    jaccuzzi                FLOAT           DEFAULT 0,
    loan                    FLOAT           DEFAULT 0,
    parfum_salon            FLOAT           DEFAULT 0,
    prise_pc                FLOAT           DEFAULT 0,
    prise_parfum_ch_parents FLOAT           DEFAULT 0,
    teleprojecteur          FLOAT           DEFAULT 0,
    tv_chambre              FLOAT           DEFAULT 0,
    tv_salon                FLOAT           DEFAULT 0,
    total_kwh               FLOAT           DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_tuya_journalier_date ON tuya_journalier (date DESC);

-- ------------------------------------------------------------
-- 7. TUYA - Consommation appareils mensuelle
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tuya_mensuel (
    id                      SERIAL          PRIMARY KEY,
    mois                    VARCHAR(8)      NOT NULL UNIQUE,  -- ex: 202310
    date_lisible            VARCHAR(8),                       -- ex: 2023-10
    ballon_eau_chaude       FLOAT           DEFAULT 0,
    chauffage               FLOAT           DEFAULT 0,
    frigo                   FLOAT           DEFAULT 0,
    jaccuzzi                FLOAT           DEFAULT 0,
    loan                    FLOAT           DEFAULT 0,
    parfum_salon            FLOAT           DEFAULT 0,
    prise_pc                FLOAT           DEFAULT 0,
    prise_parfum_ch_parents FLOAT           DEFAULT 0,
    teleprojecteur          FLOAT           DEFAULT 0,
    tv_chambre              FLOAT           DEFAULT 0,
    tv_salon                FLOAT           DEFAULT 0,
    total_kwh               FLOAT           DEFAULT 0
);

-- ------------------------------------------------------------
-- 8. CALENDRIER
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS calendrier (
    id              SERIAL          PRIMARY KEY,
    date            DATE            NOT NULL UNIQUE,
    jour_semaine    VARCHAR(20),
    jour_sem        VARCHAR(20),
    num_semaine_iso INT,
    sem_impaire     INT,
    utc             VARCHAR(15),
    nom_jour_ferie  VARCHAR(60),
    vac_scol_a      VARCHAR(60),
    vac_scol_b      VARCHAR(60),
    vac_scol_c      VARCHAR(60)
);

CREATE INDEX IF NOT EXISTS idx_calendrier_date ON calendrier (date DESC);

-- ------------------------------------------------------------
-- 9. FINANCE - Cotations Boursorama
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS finance_cotations (
    id              SERIAL          PRIMARY KEY,
    date_import     DATE            NOT NULL DEFAULT CURRENT_DATE,
    label           VARCHAR(100),
    symbol          VARCHAR(30),
    isin            VARCHAR(20),
    mnemonic        VARCHAR(20),
    dernier         FLOAT,
    precedent       FLOAT,
    haut            FLOAT,
    bas             FLOAT,
    variation       FLOAT,
    volume          FLOAT,
    exchange_code   VARCHAR(10),
    categorie       VARCHAR(20),
    secteur         VARCHAR(100),
    pays            VARCHAR(50),
    UNIQUE (date_import, symbol)   -- un snapshot par jour et par valeur
);

CREATE INDEX IF NOT EXISTS idx_finance_symbol ON finance_cotations (symbol);
CREATE INDEX IF NOT EXISTS idx_finance_date   ON finance_cotations (date_import DESC);

-- ------------------------------------------------------------
-- VUE PRATIQUE : météo + enedis alignés sur le jour
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW v_meteo_enedis_journalier AS
SELECT
    DATE(m.timestamp)                   AS date,
    AVG(m.temp_exterieure)              AS temp_ext_moy,
    MIN(m.temp_exterieure)              AS temp_ext_min,
    MAX(m.temp_exterieure)              AS temp_ext_max,
    AVG(m.hum_exterieure)               AS hum_ext_moy,
    SUM(m.pluie_horaire)                AS pluie_totale,
    MAX(m.vent_rafale)                  AS vent_rafale_max,
    AVG(m.uvi)                          AS uvi_moy,
    e.conso_kwh                         AS conso_enedis_kwh,
    tj.total_kwh                        AS conso_tuya_kwh,
    c.nom_jour_ferie,
    c.jour_semaine,
    c.vac_scol_b
FROM meteo_bresser m
LEFT JOIN enedis_journalier e   ON DATE(m.timestamp) = e.date
LEFT JOIN tuya_journalier tj    ON DATE(m.timestamp) = tj.date
LEFT JOIN calendrier c          ON DATE(m.timestamp) = c.date
GROUP BY DATE(m.timestamp), e.conso_kwh, tj.total_kwh, c.nom_jour_ferie, c.jour_semaine, c.vac_scol_b
ORDER BY date DESC;
