-- =============================================================================
-- Schéma `tuya` — Base de consommation électrique SmartLife
-- =============================================================================
-- Cible       : PostgreSQL 15+ (instance dataoz_postgres)
-- Source      : DAG dag_conso_elec_tuya
-- Périmètre   : granularités FINES uniquement (15 min + heure)
--               Les agrégats mois / jour restent en CSV (data/curated/conso_elec).
-- Idempotent  : OUI — exécuté à chaque run du DAG (tâche `init_schema`).
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS tuya;

COMMENT ON SCHEMA tuya IS
    'Consommation électrique SmartLife — granularités fines (heure, 15min)';


-- =============================================================================
-- DIMENSION : appareils
-- =============================================================================

CREATE TABLE IF NOT EXISTS tuya.dim_appareil (
    appareil_id          TEXT        PRIMARY KEY,
    nom                  TEXT        NOT NULL,
    model                TEXT,
    category             TEXT,
    piece                TEXT,
    actif                BOOLEAN     NOT NULL DEFAULT TRUE,
    cree_le              TIMESTAMPTZ NOT NULL DEFAULT now(),
    maj_le               TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_dim_appareil_actif
    ON tuya.dim_appareil (actif) WHERE actif;


-- =============================================================================
-- FACT : consommation HORAIRE
-- =============================================================================
-- Tuya renvoie uniquement les heures > 0 (CHECK enforced).

CREATE TABLE IF NOT EXISTS tuya.f_conso_heure (
    appareil_id   TEXT          NOT NULL REFERENCES tuya.dim_appareil(appareil_id) ON DELETE CASCADE,
    ts_debut      TIMESTAMPTZ   NOT NULL,
    kwh           NUMERIC(12,3) NOT NULL CHECK (kwh > 0),
    source_file   TEXT,
    loaded_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (appareil_id, ts_debut)
);

CREATE INDEX IF NOT EXISTS ix_conso_heure_ts
    ON tuya.f_conso_heure (ts_debut);


-- =============================================================================
-- FACT : consommation 15 MIN
-- =============================================================================
-- Toutes les valeurs stockées (y compris 0). Partial index pour les non-zéros.

CREATE TABLE IF NOT EXISTS tuya.f_conso_15min (
    appareil_id   TEXT          NOT NULL REFERENCES tuya.dim_appareil(appareil_id) ON DELETE CASCADE,
    ts_debut      TIMESTAMPTZ   NOT NULL,
    kwh           NUMERIC(12,3) NOT NULL,
    is_zero       BOOLEAN       GENERATED ALWAYS AS (kwh = 0) STORED,
    source_file   TEXT,
    loaded_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (appareil_id, ts_debut)
);

CREATE INDEX IF NOT EXISTS ix_conso_15min_ts
    ON tuya.f_conso_15min (ts_debut);
CREATE INDEX IF NOT EXISTS ix_conso_15min_non_zero
    ON tuya.f_conso_15min (appareil_id, ts_debut) WHERE NOT is_zero;


-- =============================================================================
-- COMMENTAIRES
-- =============================================================================

COMMENT ON TABLE  tuya.dim_appareil      IS 'Référentiel des appareils SmartLife (1 ligne par appareil)';
COMMENT ON COLUMN tuya.dim_appareil.appareil_id IS 'ID Tuya complet (ex. bf28133c02cbbd0433cefp)';
COMMENT ON COLUMN tuya.dim_appareil.actif IS 'TRUE si listé dans la dernière sync API Tuya';

COMMENT ON TABLE tuya.f_conso_heure   IS 'Conso kWh par heure (uniquement heures > 0)';
COMMENT ON TABLE tuya.f_conso_15min   IS 'Conso kWh par quart d''heure (toutes valeurs, y compris 0)';

-- =============================================================================
-- FIN du DDL
-- =============================================================================
