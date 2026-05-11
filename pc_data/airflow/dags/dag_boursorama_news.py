# -*- coding: utf-8 -*-
"""
dag_boursorama_news.py
=======================
DAG Airflow — Pipeline quotidien Boursorama News

Enchaîne deux étapes :
  1. update_news  → scrape les nouvelles news + génère les PDFs
  2. merge_news   → fusionne les PDFs/manifests dans la base principale

Planification : tous les jours à 07h00 (heure de Paris)
Déclenchement manuel possible via l'interface Airflow.
"""

import sys
import importlib
import traceback
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Ajout des sous-dossiers finance/news au PYTHONPATH ──────────────────────
# scripts/finance/news/extract/   → update_news.py
# scripts/finance/news/load/      → merge_news.py
_NEWS_SCRIPTS = Path("/opt/airflow/scripts/finance/news")
for _sub in ["extract", "load", "transform", ""]:
    _p = str(_NEWS_SCRIPTS / _sub)
    if _p not in sys.path:
        sys.path.insert(0, _p)


# ── Paramètres par défaut ────────────────────────────────────────────────────
default_args = {
    "owner": "dataoz",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}


# ─────────────────────────────────────────────────────────────────────────────
# TÂCHE 1 — Scraping & génération PDFs (update_news)
# ─────────────────────────────────────────────────────────────────────────────
def run_update_news(**context):
    """
    Importe et exécute update_news.main().
    Pousse le résultat dans XCom pour monitoring.
    """
    try:
        import update_news
        importlib.reload(update_news)   # force rechargement en cas de DAG reload
        result = update_news.main()
        print(f"[update_news] résultat : {result}", flush=True)
        context["ti"].xcom_push(key="update_result", value=result)
        return result
    except Exception as e:
        print(f"[ERROR] update_news -> {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


# ─────────────────────────────────────────────────────────────────────────────
# TÂCHE 2 — Merge & déduplication (merge_news)
# ─────────────────────────────────────────────────────────────────────────────
def run_merge_news(**context):
    """
    Importe et exécute merge_news.main().
    Récupère le résultat de la tâche précédente via XCom et logge un résumé.
    """
    try:
        import merge_news
        importlib.reload(merge_news)
        result = merge_news.main()
        print(f"[merge_news] résultat : {result}", flush=True)
        context["ti"].xcom_push(key="merge_result", value=result)
        return result
    except Exception as e:
        print(f"[ERROR] merge_news -> {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


# ─────────────────────────────────────────────────────────────────────────────
# TÂCHE 3 — Résumé du pipeline
# ─────────────────────────────────────────────────────────────────────────────
def log_pipeline_summary(**context):
    ti = context["ti"]
    update_result = ti.xcom_pull(task_ids="update_news", key="update_result") or {}
    merge_result  = ti.xcom_pull(task_ids="merge_news",  key="merge_result")  or {}

    symbols_scraped   = update_result.get("symbols", 0)
    symbols_with_news = update_result.get("symbols_with_news", 0)
    pdfs_created      = update_result.get("pdfs_created", 0)
    total_moved       = merge_result.get("total_moved", 0)
    symbols_merged    = merge_result.get("symbols_merged", 0)
    symbols_kept      = merge_result.get("symbols_kept", [])

    print("=" * 60, flush=True)
    print("✅ PIPELINE BOURSORAMA NEWS — RÉSUMÉ", flush=True)
    print(f"   Date           : {context['ds']}", flush=True)
    print(f"   Symboles scrapés    : {symbols_scraped}", flush=True)
    print(f"   Symboles avec news  : {symbols_with_news}", flush=True)
    print(f"   PDFs créés          : {pdfs_created}", flush=True)
    print(f"   PDFs déplacés       : {total_moved}", flush=True)
    print(f"   Symboles mergés OK  : {symbols_merged}", flush=True)
    if symbols_kept:
        print(f"   ⚠️  Symboles en attente : {symbols_kept}", flush=True)
    print("=" * 60, flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# DÉFINITION DU DAG
# ─────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="dag_boursorama_news",
    description="Pipeline quotidien : scraping Boursorama → PDFs → merge manifests",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 7 * * *",   # Tous les jours à 07h00 (Paris)
    catchup=False,
    tags=["boursorama", "news", "dataoz"],
) as dag:

    t1 = PythonOperator(
        task_id="update_news",
        python_callable=run_update_news,
    )

    t2 = PythonOperator(
        task_id="merge_news",
        python_callable=run_merge_news,
    )

    t3 = PythonOperator(
        task_id="pipeline_summary",
        python_callable=log_pipeline_summary,
    )

    # Ordre : scraping → merge → résumé
    t1 >> t2 >> t3
