# -*- coding: utf-8 -*-
"""
dag_boursorama_cotation.py
===========================
DAG Airflow — Pipeline quotidien Boursorama Cotations

Enchaîne deux étapes :
  1. update_master    → scrape les pages cotations + enrichit les ISIN ETF
  2. update_history   → télécharge les données 5J et 10A via Playwright (Chromium)
  3. pipeline_summary → résumé dans les logs Airflow

Planification : tous les jours à 06h00 (heure de Paris)
  (avant le DAG news à 07h00 pour avoir les cotations fraîches)
Déclenchement manuel possible via l'interface Airflow.
"""

import sys
import importlib
import traceback
from argparse import Namespace
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ── Ajout du dossier finance/cotation/extract au PYTHONPATH ─────────────────
# scripts/finance/cotation/extract/ → update_cotation.py
_COTATION_SCRIPTS = Path("/opt/airflow/scripts/finance/cotation")
for _sub in ["extract", "transform", "load", ""]:
    _p = str(_COTATION_SCRIPTS / _sub)
    if _p not in sys.path:
        sys.path.insert(0, _p)


# ── Paramètres par défaut ────────────────────────────────────────────────────
default_args = {
    "owner": "dataoz",
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "email_on_failure": False,
}


# ─────────────────────────────────────────────────────────────────────────────
# HELPER — Construit AppConfig depuis les variables d'environnement Docker
# ─────────────────────────────────────────────────────────────────────────────
def _make_args() -> Namespace:
    """
    Simule les arguments argparse avec les valeurs Docker.
    Les chemins sont lus depuis les variables d'environnement injectées
    par docker-compose.yml.

    Architecture raw → curated :
      raw_path  (DATAOZ_COTATION_RAW)  → données brutes téléchargées
                                          · boursorama_cotations.csv (scraping brut)
                                          · cotation/5d_updates/    (fichiers 5J Playwright)
                                          · archives/               (snapshots master)
                                          · update_cotation_report.csv

      base_path (DATAOZ_COTATION_BASE) → bases consolidées (curated)
                                          · boursorama_cotations_enriched.csv
                                          · cotation/intraday_db/   (série intraday agrégée)
                                          · ohlc_10a/               (OHLC 10A incrémental)
    """
    import os
    finance_root = os.environ.get(
        "DATAOZ_FINANCE_ROOT",
        "/opt/airflow/data/curated/finance",
    )
    return Namespace(
        # ── Curated : bases consolidées ──────────────────────────────────────
        base_path=os.environ.get(
            "DATAOZ_COTATION_BASE",
            "/opt/airflow/data/curated/finance/cotations",
        ),
        # ── Raw : données brutes téléchargées ────────────────────────────────
        raw_path=os.environ.get(
            "DATAOZ_COTATION_RAW",
            "/opt/airflow/data/raw/finance/cotations",
        ),
        base_url="https://www.boursorama.com/bourse/actions/cotations/",
        total_pages=8,
        etf_path=os.environ.get(
            "DATAOZ_ETF_PATH",
            "/opt/airflow/data/curated/finance/valeurs/ETF",
        ),
        master_csv=None,        # calculé automatiquement → raw_path/boursorama_cotations.csv
        # enriched_csv est dans curated/finance/valeurs/ — pas dans cotations/
        enriched_csv=os.path.join(finance_root, "valeurs", "boursorama_cotations_enriched.csv"),
        report_csv=None,        # calculé automatiquement → raw_path/update_cotation_report.csv
        no_archive=False,
        headless=1,
        sleep_between=2.0,
        retry_per_action=2,
        connect_timeout=20,
        read_timeout=60,
        retries=5,
        backoff=0.8,
        skip_master=False,
        skip_history=False,
    )


# ─────────────────────────────────────────────────────────────────────────────
# TÂCHE 1 — Scraping master CSV (update_master_csv)
# ─────────────────────────────────────────────────────────────────────────────
def run_update_master(**context):
    """
    Scrape les pages de cotations Boursorama et enrichit les ISIN ETF.
    Produit : boursorama_cotations.csv + boursorama_cotations_enriched.csv
    """
    try:
        import update_cotation
        importlib.reload(update_cotation)

        args = _make_args()
        cfg = update_cotation.build_config(args)

        # Répertoires curated (bases consolidées)
        cfg.base_path.mkdir(parents=True, exist_ok=True)
        cfg.cotation_dir.mkdir(parents=True, exist_ok=True)
        cfg.ohlc_10a_dir.mkdir(parents=True, exist_ok=True)
        # Répertoires raw (données brutes téléchargées)
        cfg.raw_base_path.mkdir(parents=True, exist_ok=True)
        cfg.updates_5d_dir.mkdir(parents=True, exist_ok=True)
        cfg.archive_dir.mkdir(parents=True, exist_ok=True)

        master_path = update_cotation.update_master_csv(cfg)

        result = {"master_csv": str(master_path)}
        print(f"[update_master] résultat : {result}", flush=True)
        context["ti"].xcom_push(key="master_result", value=result)
        return result

    except Exception as e:
        print(f"[ERROR] update_master -> {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


# ─────────────────────────────────────────────────────────────────────────────
# TÂCHE 2 — Téléchargement historique 5J + 10A (update_history_from_csv)
# ─────────────────────────────────────────────────────────────────────────────
def run_update_history(**context):
    """
    Pour chaque symbole du CSV enrichi, télécharge via Playwright :
      - les données intraday 5 jours (5J)
      - les données OHLC 10 ans (10A) si pas encore initialisées
    Produit : fichiers TSV/CSV dans cotation/intraday_db/ et ohlc_10a/
    """
    try:
        import update_cotation
        importlib.reload(update_cotation)

        args = _make_args()
        cfg = update_cotation.build_config(args)

        update_cotation.update_history_from_csv(cfg)

        # Lecture du rapport CSV pour pousser un résumé dans XCom
        result = {"report_csv": str(cfg.report_csv)}
        if cfg.report_csv.exists():
            import pandas as pd
            df_report = pd.read_csv(cfg.report_csv, sep=";", dtype=str)
            result["total"] = len(df_report)
            result["success"] = int((df_report["status"] == "success").sum())
            result["partial"] = int((df_report["status"] == "partial").sum())
            result["failed"] = int((df_report["status"] == "failed").sum())

        print(f"[update_history] résultat : {result}", flush=True)
        context["ti"].xcom_push(key="history_result", value=result)
        return result

    except Exception as e:
        print(f"[ERROR] update_history -> {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


# ─────────────────────────────────────────────────────────────────────────────
# TÂCHE 3 — Résumé du pipeline
# ─────────────────────────────────────────────────────────────────────────────
def log_pipeline_summary(**context):
    ti = context["ti"]
    master_result  = ti.xcom_pull(task_ids="update_master",  key="master_result")  or {}
    history_result = ti.xcom_pull(task_ids="update_history", key="history_result") or {}

    total   = history_result.get("total",   "?")
    success = history_result.get("success", "?")
    partial = history_result.get("partial", "?")
    failed  = history_result.get("failed",  "?")

    print("=" * 60, flush=True)
    print("✅ PIPELINE BOURSORAMA COTATIONS — RÉSUMÉ", flush=True)
    print(f"   Date               : {context['ds']}", flush=True)
    print(f"   Master CSV         : {master_result.get('master_csv', '?')}", flush=True)
    print(f"   Symboles traités   : {total}", flush=True)
    print(f"   ✅ Succès          : {success}", flush=True)
    print(f"   ⚠️  Partiel         : {partial}", flush=True)
    print(f"   ❌ Échecs          : {failed}", flush=True)
    print(f"   Rapport            : {history_result.get('report_csv', '?')}", flush=True)
    print("=" * 60, flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# DÉFINITION DU DAG
# ─────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="dag_boursorama_cotation",
    description="Pipeline quotidien : scraping cotations Boursorama → 5J intraday + 10A OHLC",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="20 1 * * 1-5",  # Lundi–Vendredi à 01:20 (Paris)
    catchup=False,
    tags=["boursorama", "cotation", "dataoz"],
) as dag:

    t1 = PythonOperator(
        task_id="update_master",
        python_callable=run_update_master,
        execution_timeout=timedelta(minutes=30),
    )

    t2 = PythonOperator(
        task_id="update_history",
        python_ca