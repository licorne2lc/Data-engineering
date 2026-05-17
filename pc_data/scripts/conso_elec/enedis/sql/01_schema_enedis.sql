-- =============================================================================
-- Schéma `enedis` — Consommation électrique côté DISTRIBUTEUR (réseau Enedis)
-- =============================================================================
-- Cible       : PostgreSQL 15+ (instance dataoz_postgres)
-- Source      : DAG dag_conso_elec_enedis (appels Enedis Data Hub via OAuth2)
-- Périmètre   : courbe de charge 30 min, agrégats jour, puissance max quotidienne
-- Idempotent  : OUI — exécuté à chaque run du DAG (tâche `init_schema`).
-- =============================================================================
-- Rappel unités :
--   * Enedis 30 min       : energie en Wh sur chaque pas     -> kWh = Wh / 1000
--   * Enedis daily        : energie en Wh agregee a la journee
--   * Enedis daily pmax   : puissance max atteinte en VA
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS enedis;

COMMENT ON SCHEMA enedis IS
    'Consommation électrique côté distributeur Enedis — API Data Hub';


-- =============================================================================
-- DIMENSION : point de livraison (PRM)
-- =============================================================================
-- Un foyer peut avoir plusieurs PRM (logement principal + résidence secondaire).
-- Le consentement Enedis est donné PRM par PRM.

CREATE TABLE IF NOT EXISTS enedis.dim_prm (
    prm           TEXT        PRIMARY KEY,          -- identifiant 14 chiffres
    libelle       TEXT,
    adresse       TEXT,
    consent_from  DATE,                             -- début de consentement
    consent_to    DATE,                             -- fin de consentement (nullable)
    actif         BOOLEAN     NOT NULL DEFAULT TRUE,
    cree_le       TIMESTAMPTZ NOT NULL DEFAULT now(),
    maj_le        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_enedis_dim_prm_actif
    ON enedis.dim_prm (actif) WHERE actif;


-- =============================================================================
-- FACT : courbe de charge 30 MIN (endpoint consumption_load_curve)
-- =============================================================================
-- Granularité native Enedis : Wh par pas de 30 min (pas de 15 min côté Enedis).

CREATE TABLE IF NOT EXISTS enedis.f_conso_30min (
    prm           TEXT          NOT NULL REFERENCES enedis.dim_prm(prm) ON DELETE CASCADE,
    ts_debut      TIMESTAMPTZ   NOT NULL,
    wh            INTEGER       NOT NULL CHECK (wh >= 0),
    kwh           NUMERIC(10,4) GENERATED ALWAYS AS (wh::NUMERIC / 1000) STORED,
    source_file   TEXT,
    loaded_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (prm, ts_debut)
);

CREATE INDEX IF NOT EXISTS ix_enedis_c30_ts
    ON enedis.f_conso_30min (ts_debut);
-- Note : pas d'index fonctionnel sur (ts_debut::date) car le cast
-- TIMESTAMPTZ->DATE n'est pas IMMUTABLE en Postgres. La PK (prm, ts_debut)
-- sert très bien les filtres de plage (ts_debut >= X AND ts_debut < Y).


-- =============================================================================
-- FACT : consommation QUOTIDIENNE (endpoint daily_consumption)
-- =============================================================================
-- Agrégat journalier publié directement par Enedis (meter_reading value_wh).

CREATE TABLE IF NOT EXISTS enedis.f_conso_jour (
    prm           TEXT          NOT NULL REFERENCES enedis.dim_prm(prm) ON DELETE CASCADE,
    jour          DATE          NOT NULL,
    wh            INTEGER       NOT NULL CHECK (wh >= 0),
    kwh           NUMERIC(10,3) GENERATED ALWAYS AS (wh::NUMERIC / 1000) STORED,
    source_file   TEXT,
    loaded_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (prm, jour)
);


-- =============================================================================
-- FACT : puissance MAX quotidienne (endpoint daily_consumption_max_power)
-- =============================================================================

CREATE TABLE IF NOT EXISTS enedis.f_pmax_jour (
    prm           TEXT          NOT NULL REFERENCES enedis.dim_prm(prm) ON DELETE CASCADE,
    jour          DATE          NOT NULL,
    pmax_va       INTEGER       NOT NULL CHECK (pmax_va >= 0),
    ts_pmax       TIMESTAMPTZ,                       -- horodatage précis du pic (si fourni)
    source_file   TEXT,
    loaded_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (prm, jour)
);


-- =============================================================================
-- OPERATIONS : journal des appels API
-- =============================================================================
-- Traçabilité des appels Data Hub (utile pour debugger rate-limit & erreurs).

CREATE TABLE IF NOT EXISTS enedis.api_call_log (
    call_id       BIGSERIAL     PRIMARY KEY,
    ts_call       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    endpoint      TEXT          NOT NULL,             -- 'consumption_load_curve' etc.
    prm           TEXT,
    date_debut    DATE,
    date_fin      DATE,
    http_status   INTEGER,
    n_points      INTEGER,                            -- nb de mesures retournées
    duree_ms      INTEGER,
    erreur        TEXT
);

CREATE INDEX IF NOT EXISTS ix_enedis_api_call_log_ts
    ON enedis.api_call_log (ts_call DESC);
CREATE INDEX IF NOT EXISTS ix_enedis_api_call_log_endpoint
    ON enedis.api_call_log (endpoint, ts_call DESC);


-- =============================================================================
-- COMMENTAIRES
-- =============================================================================

COMMENT ON TABLE  enedis.dim_prm            IS 'Référentiel des Points de Référence Mesure (14 chiffres Linky)';
COMMENT ON TABLE  enedis.f_conso_30min      IS 'Courbe de charge Enedis — énergie (Wh) par pas de 30 min';
COMMENT ON TABLE  enedis.f_conso_jour       IS 'Consommation quotidienne Enedis (Wh) — endpoint daily_consumption';
COMMENT ON TABLE  enedis.f_pmax_jour        IS 'Puissance max atteinte dans la journée (VA) — endpoint daily_consumption_max_power';
COMMENT ON TABLE  enedis.api_call_log       IS 'Journal des appels Enedis Data Hub (OAuth2 client_credentials)';

COMMENT ON COLUMN enedis.dim_prm.prm                IS 'Numéro PRM Linky à 14 chiffres';
COMMENT ON COLUMN enedis.f_conso_30min.kwh          IS 'Colonne générée = wh / 1000';
COMMENT ON COLUMN enedis.f_conso_jour.kwh           IS 'Colonne générée = wh / 1000';

-- =============================================================================
-- FIN du DDL
-- =============================================================================
