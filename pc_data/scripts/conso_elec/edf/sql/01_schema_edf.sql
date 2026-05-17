-- =============================================================================
-- Schéma `edf` — Consommation électrique côté FOURNISSEUR (facturation EDF)
-- =============================================================================
-- Cible       : PostgreSQL 15+ (instance dataoz_postgres)
-- Source      : DAG dag_conso_elec_edf (parsing des ZIP exportés depuis
--               l'espace client particulier.edf.fr)
-- Périmètre   : mensuel, journalier, index HP/HC, puissance max quotidienne
--               et courbe de charge 30 min (puissance instantanée en W).
-- Idempotent  : OUI — exécuté à chaque run du DAG (tâche `init_schema`).
-- =============================================================================
-- Rappel unités :
--   * EDF 30 min  = PUISSANCE atteinte en W (VA)   -> kWh = W * 0.5 / 1000
--   * Enedis 30 min = ENERGIE en Wh                -> kWh = Wh / 1000
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS edf;

COMMENT ON SCHEMA edf IS
    'Consommation électrique côté fournisseur EDF — exports ZIP espace client';


-- =============================================================================
-- FACT : consommation MENSUELLE (ma-conso-mensuelle.csv)
-- =============================================================================

CREATE TABLE IF NOT EXISTS edf.f_conso_mois (
    annee_mois    DATE          NOT NULL,          -- 1er du mois (YYYY-MM-01)
    kwh_total     NUMERIC(12,3) NOT NULL CHECK (kwh_total >= 0),
    nature        TEXT,                            -- 'Mesuré' / 'Estimé'
    source_file   TEXT,
    loaded_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (annee_mois)
);


-- =============================================================================
-- FACT : consommation QUOTIDIENNE (ma-conso-quotidienne.csv)
-- =============================================================================

CREATE TABLE IF NOT EXISTS edf.f_conso_jour (
    jour          DATE          NOT NULL,
    kwh_total     NUMERIC(12,3) NOT NULL CHECK (kwh_total >= 0),
    nature        TEXT,                            -- 'Mesuré' / 'Estimé' / 'Corrigé'
    source_file   TEXT,
    loaded_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (jour)
);

CREATE INDEX IF NOT EXISTS ix_edf_conso_jour_nature
    ON edf.f_conso_jour (nature);


-- =============================================================================
-- FACT : puissance MAX quotidienne (ma-puissance-max.csv)
-- =============================================================================

CREATE TABLE IF NOT EXISTS edf.f_pmax_jour (
    jour          DATE          NOT NULL,
    pmax_va       INTEGER       NOT NULL CHECK (pmax_va >= 0),
    nature        TEXT,
    source_file   TEXT,
    loaded_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (jour)
);


-- =============================================================================
-- FACT : index Linky HP/HC (mes-index-elec.csv)
-- =============================================================================
-- Index cumulatif en Wh relevé quotidiennement.

CREATE TABLE IF NOT EXISTS edf.f_index_jour (
    jour          DATE          NOT NULL,
    index_hp_wh   BIGINT,                          -- index Heures Pleines (cumulatif)
    index_hc_wh   BIGINT,                          -- index Heures Creuses (cumulatif)
    type_index    TEXT,                            -- 'Réel' / 'Estimé'
    source_file   TEXT,
    loaded_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (jour)
);


-- =============================================================================
-- FACT : courbe de charge 30 MIN (mes-puissances-atteintes-30min.csv)
-- =============================================================================
-- EDF publie la puissance atteinte (W) sur chaque tranche de 30 min.
-- kWh déductibles par : puissance_w * 0.5 / 1000  (colonne générée).

CREATE TABLE IF NOT EXISTS edf.f_puissance_30min (
    ts_debut        TIMESTAMPTZ   NOT NULL,
    puissance_w     INTEGER       NOT NULL CHECK (puissance_w >= 0),
    kwh_derive      NUMERIC(10,4) GENERATED ALWAYS AS
                      (puissance_w::NUMERIC * 0.5 / 1000) STORED,
    nature          TEXT,
    source_file     TEXT,
    loaded_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (ts_debut)
);

-- Note : pas d'index fonctionnel sur (ts_debut::date) — cast non-IMMUTABLE.
-- La PK (ts_debut) sert les filtres de plage.


-- =============================================================================
-- COMMENTAIRES
-- =============================================================================

COMMENT ON TABLE  edf.f_conso_mois        IS 'Conso EDF kWh agrégée par mois (export ma-conso-mensuelle.csv)';
COMMENT ON TABLE  edf.f_conso_jour        IS 'Conso EDF kWh agrégée par jour (export ma-conso-quotidienne.csv)';
COMMENT ON TABLE  edf.f_pmax_jour         IS 'Puissance max atteinte dans la journée (VA)';
COMMENT ON TABLE  edf.f_index_jour        IS 'Index Linky cumulatifs HP/HC (Wh)';
COMMENT ON TABLE  edf.f_puissance_30min   IS 'Courbe de charge 30 min — puissance atteinte (W)';

COMMENT ON COLUMN edf.f_conso_jour.nature IS 'Qualité de la donnée selon EDF : Mesuré / Estimé / Corrigé';
COMMENT ON COLUMN edf.f_puissance_30min.kwh_derive IS 'kWh déduit de la puissance 30 min (W * 0.5 / 1000)';

-- =============================================================================
-- FIN du DDL
-- =============================================================================
