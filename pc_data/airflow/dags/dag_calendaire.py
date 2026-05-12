# -*- coding: utf-8 -*-
"""
dag_calendaire.py
=================
DAG Airflow -- Calendrier de référence (socle + sources externes).

===============================================================================
ARCHITECTURE -- ÉTAPES 1→4 : SOCLE + DOWNLOADS + ENRICHISSEMENTS COMPLETS
===============================================================================

Objectif : produire et tenir à jour la database calendaire enrichie
`data/curated/calendaire/calendrier.csv` avec les colonnes :

    Date ; Jour de la semaine ; jour Sem ; N° semaine ISO ; Sem. Impaire ;
    UTC ; nom_jour_ferie ; vac_scol_A ; vac_scol_B ; vac_scol_C

(format aligné sur le fichier 'exemple calendrier.csv' fourni par le métier)

  ÉTAPES 1→4 -- ce DAG (toutes livrées)
  --------------------------------------
  generate_socle ──────────┐
                           ├──> enrichir_vacances ──┐
  download_vacances ───────┘                        ├──> enrichir_jours_feries
                                                    │             │
  download_jours_feries ────────────────────────────┘             │
                                                                  ▼
                                                          pipeline_summary

    1. generate_socle
         Script : scripts/calendaire/transform/socle_calendrier.py
         Génère le socle 2010-01-01 → 2035-12-31 (9 496 jours) avec
         Date / Jour de la semaine / jour Sem / N° semaine ISO / Sem. Impaire /
         UTC. UTC calculé via zoneinfo Europe/Paris (aucune table statique).
         Les colonnes nom_jour_ferie / vac_scol_A/B/C sont initialisées
         à '--' (placeholders).
         Sortie : data/curated/calendaire/socle_calendrier.csv

    2a. download_vacances
         Script : scripts/calendaire/extract/download_vacances_scolaires.py
         Télécharge le dataset open data des vacances scolaires depuis
         data.education.gouv.fr (endpoint export CSV utilisé dans
         le script historique v3.4 lignes 906-908).
         Sortie : data/raw/calendrier/vacances/vacances_scolaires_YYYYMMDD.csv
                  + alias vacances_scolaires.csv (latest)

    2b. download_jours_feries
         Script : scripts/calendaire/extract/download_jours_feries.py
         Télécharge le dataset open data des jours fériés métropole
         maintenu par Etalab (etalab.github.io/jours-feries-france-data).
         Format identique au fichier déjà présent dans le projet :
         date,annee,zone,nom_jour_ferie.
         Sortie : data/raw/calendrier/jours_feries/
                  jours_feries_metropole_YYYYMMDD.csv
                  + alias jours_feries_metropole.csv (latest)

    3. enrichir_vacances
         Script : scripts/calendaire/transform/enrichir_vacances_scolaires.py
         Consomme socle_calendrier.csv + vacances_scolaires.csv pour produire
         calendrier.csv enrichi (vac_scol_A/B/C remplies avec le nom de la
         période -- "Vacances d'Hiver", "Vacances de Noël", etc.).
         Filtre Zones France métropolitaine (A/B/C) et dédoublonne par
         académies. Conversion timezone UTC -> Europe/Paris pour les bornes.
         Sortie : data/curated/calendaire/calendrier.csv

    4. enrichir_jours_feries
         Script : scripts/calendaire/transform/enrichir_jours_feries.py
         Consomme calendrier.csv (sortie de l'étape 3) +
         jours_feries_metropole.csv (téléchargé en 2b) pour remplir la
         colonne nom_jour_ferie ('1er janvier', 'Lundi de Pâques', etc.).
         Jointure simple par date sur la zone Métropole.
         Sortie : data/curated/calendaire/calendrier.csv (écrasement en place)

Planification : tous les jours à 04:30 Europe/Paris (avant les DAG
consommateurs comme dag_conso_elec_enedis qui tourne à 05:00).
"""
from __future__ import annotations

import importlib
import importlib.util
import logging
import sys
import traceback
from datetime import date, datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator


# -- PYTHONPATH : scripts calendaire --------------------------------------------
_CAL_ROOT = Path("/opt/airflow/scripts/calendaire")
for _sub in ["", "extract", "transform", "load"]:
    _p = str(_CAL_ROOT / _sub) if _sub else str(_CAL_ROOT)
    if _p not in sys.path:
        sys.path.insert(0, _p)


# -- Chemins (container Docker) -------------------------------------------------
# Convention historique du projet :
#   RAW     -> data/raw/calendrier/...      (orthographe française)
#   CURATED -> data/curated/calendaire/...  (typo conservée pour compat)
RAW_DIR     = Path("/opt/airflow/data/raw/calendrier")
CURATED_DIR = Path("/opt/airflow/data/curated/calendaire")

# Sortie du socle (fichier intermédiaire qui servira de base à l'enrichissement)
SOCLE_CSV = CURATED_DIR / "socle_calendrier.csv"

# Cibles des téléchargements (raw)
VACANCES_DIR     = RAW_DIR / "vacances"
VACANCES_CSV     = VACANCES_DIR / "vacances_scolaires.csv"             # alias latest
JOURS_FERIES_DIR = RAW_DIR / "jours_feries"
JOURS_FERIES_CSV = JOURS_FERIES_DIR / "jours_feries_metropole.csv"     # alias latest

# Cible de l'enrichissement (database calendaire finale)
CALENDRIER_CSV = CURATED_DIR / "calendrier.csv"

# Plage couverte par le socle (alignée sur la décision projet 2010 → 2035)
SOCLE_START = date(2010, 1, 1)
SOCLE_END   = date(2035, 12, 31)


default_args = {
    "owner":            "dataoz",
    "retries":          2,
    "retry_delay":      timedelta(minutes=15),
    "email_on_failure": False,
}


# ==============================================================================
# CALLABLES
# ==============================================================================

def task_generate_socle(**context):
    """
    Phase 1 -- Génère le socle calendaire 2010 → 2035 dans CURATED_DIR.

    Délègue à scripts/calendaire/transform/socle_calendrier.py
    (fonctions build_socle + export_socle).
    """
    try:
        import socle_calendrier
        importlib.reload(socle_calendrier)

        df = socle_calendrier.build_socle(SOCLE_START, SOCLE_END)
        socle_calendrier.export_socle(df, SOCLE_CSV)

        n_lignes = len(df)
        utcs     = sorted(df["UTC"].unique().tolist())
        result   = {
            "status":   "ok",
            "start":    SOCLE_START.isoformat(),
            "end":      SOCLE_END.isoformat(),
            "lignes":   n_lignes,
            "utc_uniq": utcs,
            "output":   str(SOCLE_CSV),
        }
        print(f"[generate_socle] {SOCLE_START} → {SOCLE_END}  "
              f"{n_lignes} jours  UTC={utcs}", flush=True)
        print(f"[generate_socle] -> {SOCLE_CSV}", flush=True)
        return result

    except Exception as e:
        print(f"[ERROR] generate_socle -> {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


def task_download_vacances(**context):
    """
    Phase 2a -- Télécharge le CSV des vacances scolaires depuis
    data.education.gouv.fr et le dépose dans data/raw/calendrier/vacances/.

    Délègue à scripts/calendaire/extract/download_vacances_scolaires.py
    (fonction download_vacances).
    """
    try:
        import download_vacances_scolaires as dl
        importlib.reload(dl)

        result = dl.download_vacances(VACANCES_DIR)

        print(f"[download_vacances] {result['bytes']:,} bytes  "
              f"sep={result['sep']!r}  cols={result['columns']}", flush=True)
        print(f"[download_vacances] snapshot -> "
              f"{Path(result['snapshot_path']).name}", flush=True)
        print(f"[download_vacances] latest   -> "
              f"{Path(result['latest_path']).name}", flush=True)
        return result

    except Exception as e:
        print(f"[ERROR] download_vacances -> {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


def task_download_jours_feries(**context):
    """
    Phase 2b -- Télécharge le CSV des jours fériés métropole depuis
    le dataset open data Etalab et le dépose dans
    data/raw/calendrier/jours_feries/.

    Délègue à scripts/calendaire/extract/download_jours_feries.py
    (fonction download_jours_feries).
    """
    try:
        import download_jours_feries as dl_jf
        importlib.reload(dl_jf)

        result = dl_jf.download_jours_feries(JOURS_FERIES_DIR)

        print(f"[download_jours_feries] {result['bytes']:,} bytes  "
              f"sep={result['sep']!r}  cols={result['columns']}", flush=True)
        print(f"[download_jours_feries] snapshot -> "
              f"{Path(result['snapshot_path']).name}", flush=True)
        print(f"[download_jours_feries] latest   -> "
              f"{Path(result['latest_path']).name}", flush=True)
        return result

    except Exception as e:
        print(f"[ERROR] download_jours_feries -> {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


def task_enrichir_vacances(**context):
    """
    Phase 3 -- Enrichit le socle avec les colonnes vac_scol_A/B/C.

    Consomme :
        SOCLE_CSV     (généré par generate_socle)
        VACANCES_CSV  (téléchargé par download_vacances)

    Produit CALENDRIER_CSV en convertissant les bornes UTC -> Europe/Paris
    et en dédoublonnant les périodes par académies.

    Délègue à scripts/calendaire/transform/enrichir_vacances_scolaires.py
    (fonction enrichir).
    """
    try:
        import enrichir_vacances_scolaires as ev
        importlib.reload(ev)

        result = ev.enrichir(
            socle_path    = SOCLE_CSV,
            vacances_path = VACANCES_CSV,
            output_path   = CALENDRIER_CSV,
        )

        print(f"[enrichir_vacances] {result['lignes']} lignes  "
              f"couverture {result['couverture_min']} → {result['couverture_max']}",
              flush=True)
        print(f"[enrichir_vacances] jours vacances : "
              f"A={result['jours_vac_A']}  "
              f"B={result['jours_vac_B']}  "
              f"C={result['jours_vac_C']}", flush=True)
        print(f"[enrichir_vacances] -> {result['output']}", flush=True)
        return result

    except Exception as e:
        print(f"[ERROR] enrichir_vacances -> {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


def task_enrichir_jours_feries(**context):
    """
    Phase 4 -- Remplit la colonne nom_jour_ferie du calendrier à partir
    du CSV téléchargé en 2b.

    Délègue à scripts/calendaire/transform/enrichir_jours_feries.py
    (fonction enrichir).
    """
    try:
        import enrichir_jours_feries as ejf
        importlib.reload(ejf)

        result = ejf.enrichir(
            calendrier_path = CALENDRIER_CSV,
            feries_path     = JOURS_FERIES_CSV,
            output_path     = CALENDRIER_CSV,
        )

        print(f"[enrichir_jours_feries] {result['lignes']} lignes  "
              f"couverture {result['couverture_min']} → "
              f"{result['couverture_max']}", flush=True)
        print(f"[enrichir_jours_feries] {result['feries_appliques']} jours "
              f"fériés appliqués sur {result['feries_indexes']} dispo "
              f"({result['feries_hors_periode']} hors période)", flush=True)
        print(f"[enrichir_jours_feries] -> {result['output']}", flush=True)
        return result

    except Exception as e:
        print(f"[ERROR] enrichir_jours_feries -> {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


def task_pipeline_summary(**context):
    """Résumé des phases (étapes 1→4)."""
    ti       = context["ti"]
    socle    = ti.xcom_pull(task_ids="generate_socle")        or {}
    vac_dl   = ti.xcom_pull(task_ids="download_vacances")     or {}
    jf_dl    = ti.xcom_pull(task_ids="download_jours_feries") or {}
    enrich   = ti.xcom_pull(task_ids="enrichir_vacances")     or {}
    enrich_f = ti.xcom_pull(task_ids="enrichir_jours_feries") or {}

    print("=" * 70, flush=True)
    print("DAG CALENDAIRE -- ÉTAPES 1→4 -- RÉSUMÉ", flush=True)
    print(f"   Execution : {context.get('ds', 'N/A')}", flush=True)
    print("", flush=True)

    # -- Socle ----------------------------------------------------------------
    print("   -- 1. SOCLE CALENDAIRE --", flush=True)
    s_status = socle.get("status", "?")
    if s_status == "ok":
        print(f"   Plage    : {socle.get('start')} → {socle.get('end')}",
              flush=True)
        print(f"   Lignes   : {socle.get('lignes', '?')}", flush=True)
        print(f"   UTC      : {socle.get('utc_uniq', '?')}", flush=True)
        print(f"   Sortie   : {socle.get('output', '?')}", flush=True)
    else:
        print(f"   Statut   : {s_status}", flush=True)
    print("", flush=True)

    # -- Download vacances ----------------------------------------------------
    print("   -- 2a. DOWNLOAD VACANCES SCOLAIRES --", flush=True)
    v_status = vac_dl.get("status", "?")
    if v_status == "ok":
        print(f"   Bytes    : {vac_dl.get('bytes', '?'):,}", flush=True)
        print(f"   Sep      : {vac_dl.get('sep', '?')!r}", flush=True)
        print(f"   Colonnes : {vac_dl.get('columns', '?')}", flush=True)
        print(f"   Snapshot : {vac_dl.get('snapshot_path', '?')}", flush=True)
        print(f"   Latest   : {vac_dl.get('latest_path', '?')}", flush=True)
    else:
        print(f"   Statut   : {v_status}", flush=True)
    print("", flush=True)

    # -- Download jours fériés ------------------------------------------------
    print("   -- 2b. DOWNLOAD JOURS FÉRIÉS --", flush=True)
    j_status = jf_dl.get("status", "?")
    if j_status == "ok":
        print(f"   Bytes    : {jf_dl.get('bytes', '?'):,}", flush=True)
        print(f"   Sep      : {jf_dl.get('sep', '?')!r}", flush=True)
        print(f"   Colonnes : {jf_dl.get('columns', '?')}", flush=True)
        print(f"   Snapshot : {jf_dl.get('snapshot_path', '?')}", flush=True)
        print(f"   Latest   : {jf_dl.get('latest_path', '?')}", flush=True)
    else:
        print(f"   Statut   : {j_status}", flush=True)
    print("", flush=True)

    # -- Enrichissement vacances ---------------------------------------------
    print("   -- 3. ENRICHISSEMENT VACANCES --", flush=True)
    e_status = enrich.get("status", "?")
    if e_status == "ok":
        print(f"   Lignes      : {enrich.get('lignes', '?')}", flush=True)
        print(f"   Couverture  : {enrich.get('couverture_min')} → "
              f"{enrich.get('couverture_max')}", flush=True)
        print(f"   Jours Zone A: {enrich.get('jours_vac_A', '?')}", flush=True)
        print(f"   Jours Zone B: {enrich.get('jours_vac_B', '?')}", flush=True)
        print(f"   Jours Zone C: {enrich.get('jours_vac_C', '?')}", flush=True)
        print(f"   Sortie      : {enrich.get('output', '?')}", flush=True)
    else:
        print(f"   Statut      : {e_status}", flush=True)
    print("", flush=True)

    # -- Enrichissement jours fériés ------------------------------------------
    print("   -- 4. ENRICHISSEMENT JOURS FÉRIÉS --", flush=True)
    f_status = enrich_f.get("status", "?")
    if f_status == "ok":
        print(f"   Lignes        : {enrich_f.get('lignes', '?')}", flush=True)
        print(f"   Source dispo  : {enrich_f.get('feries_indexes', '?')}",
              flush=True)
        print(f"   Appliqués     : {enrich_f.get('feries_appliques', '?')}",
              flush=True)
        print(f"   Hors période  : {enrich_f.get('feries_hors_periode', '?')}",
              flush=True)
        print(f"   Sortie        : {enrich_f.get('output', '?')}", flush=True)
    else:
        print(f"   Statut        : {f_status}", flush=True)
    print("", flush=True)

    print(f"   RAW       : {RAW_DIR}", flush=True)
    print(f"   CURATED   : {CURATED_DIR}", flush=True)
    print(f"   DATABASE  : {CALENDRIER_CSV}", flush=True)
    print("=" * 70, flush=True)


# ==============================================================================
# DEFINITION DU DAG
# ==============================================================================

with DAG(
    dag_id="dag_calendaire",
    description=(
        "Calendrier de référence -- "
        "Étapes 1→4 complètes : socle 2010→2035 (UTC dynamique) + download "
        "vacances (data.education.gouv.fr) + download jours fériés (Etalab) "
        "+ enrichissement vac_scol_A/B/C + enrichissement nom_jour_ferie."
    ),
    default_args=default_args,
    start_date=datetime(2026, 4, 29),
    schedule_interval="15 1 * * *",     # 01:15 -- avant les DAG consommateurs
    catchup=False,
    max_active_runs=1,
    tags=["calendaire", "calendrier", "dataoz"],
) as dag:

    # -- ÉTAPE 1 : socle -------------------------------------------------------
    t_socle = PythonOperator(
        task_id="generate_socle",
        python_callable=task_generate_socle,
        execution_timeout=timedelta(minutes=2),
    )

    # -- ÉTAPE 2 : downloads sources externes (parallèle) ----------------------
    t_vacances = PythonOperator(
        task_id="download_vacances",
        python_callable=task_download_vacances,
        execution_timeout=timedelta(minutes=5),
    )

    t_jours_feries = PythonOperator(
        task_id="download_jours_feries",
        python_callable=task_download_jours_feries,
        execution_timeout=timedelta(minutes=5),
    )

    # -- ÉTAPE 3 : enrichissement vac_scol_A/B/C -------------------------------
    t_enrich_vac = PythonOperator(
        task_id="enrichir_vacances",
        python_callable=task_enrichir_vacances,
        execution_timeout=timedelta(minutes=3),
    )

    # -- ÉTAPE 4 : enrichissement nom_jour_ferie -------------------------------
    t_enrich_feries = PythonOperator(
        task_id="enrichir_jours_feries",
        python_callable=task_enrichir_jours_feries,
        execution_timeout=timedelta(minutes=2),
    )

    t_summary = PythonOperator(
        task_id="pipeline_summary",
        python_callable=task_pipeline_summary,
        trigger_rule="all_done",
    )

    # -- Dépendances -----------------------------------------------------------
    #
    #  generate_socle ──────────┐
    #                           ├──> enrichir_vacances ──┐
    #  download_vacances ───────┘                        ├──> enrichir_jours_feries ──┐
    #                                                    │                            │
    #  download_jours_feries ────────────────────────────┘                            │
    #                                                                                 ▼
    #                                                                       pipeline_summary
    #
    #  Cascade d'enrichissement :
    #    enrichir_vacances     consomme SOCLE_CSV + VACANCES_CSV     -> calendrier.csv
    #    enrichir_jours_feries consomme calendrier.csv + JOURS_FER