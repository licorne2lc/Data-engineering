# -*- coding: utf-8 -*-
"""
dag_boursorama_valeurs.py
==========================
DAG Airflow — Pipeline de gestion du référentiel de valeurs boursières

Rôle dans l'architecture :
  Ce DAG alimente boursorama_cotations_enriched.csv, qui est la SOURCE DE VÉRITÉ
  lue par dag_boursorama_cotation pour savoir quels symboles télécharger.

  Ordre d'exécution recommandé :
    dag_boursorama_valeurs  →  dag_boursorama_cotation  →  dag_boursorama_news

Déclenchement :
  - Automatique : chaque lundi à 05h00 (avant dag_boursorama_cotation à 06h00)
  - Manuel      : via l'interface Airflow dès qu'on ajoute un CSV dans valeurs/
  - Arrêt automatique si aucun changement détecté (manifest_imports.json identique)

Étape unique :
  update_valeurs → lit les CSV de référence (ETF/, premieres/, specifique/),
                   scrape Boursorama par ISIN pour les nouvelles valeurs,
                   fusionne dans boursorama_cotations_enriched.csv.
"""

import sys
import importlib
import traceback
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Ajout du dossier finance/valeurs/extract au PYTHONPATH ──────────────────
_VALEURS_SCRIPTS = Path("/opt/airflow/scripts/finance/valeurs/extract")
if str(_VALEURS_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_VALEURS_SCRIPTS))


# ── Paramètres par défaut ────────────────────────────────────────────────────
default_args = {
    "owner": "dataoz",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}


# ─────────────────────────────────────────────────────────────────────────────
# TÂCHE — Mise à jour du référentiel de valeurs
# ─────────────────────────────────────────────────────────────────────────────
def run_update_valeurs(**context):
    """
    Importe et exécute update_valeurs.main().

    Comportement :
      - Si aucun changement dans les CSV source → arrêt rapide (quelques secondes)
      - Si nouvelles valeurs détectées → scraping Boursorama + fusion enriched.csv
        (peut prendre plusieurs minutes selon le nombre de nouvelles valeurs)

    Résumé poussé dans XCom pour monitoring :
      status        : "ok" | "no_change" | "no_files" | "no_new_values" | "enrich_empty"
      new_values    : nombre de nouvelles valeurs détectées dans les CSV source
      added         : nombre de lignes effectivement ajoutées dans enriched.csv
      total_enriched: taille totale du fichier enriched.csv après fusion
    """
    try:
        import update_valeurs
        importlib.reload(update_valeurs)

        result = update_valeurs.main()
        print(f"[update_valeurs] résultat : {result}", flush=True)
        context["ti"].xcom_push(key="valeurs_result", value=result)
        return result

    except Exception as e:
        print(f"[ERROR] update_valeurs -> {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


# ─────────────────────────────────────────────────────────────────────────────
# TÂCHE — Résumé du pipeline
# ─────────────────────────────────────────────────────────────────────────────
def log_pipeline_summary(**context):
    ti     = context["ti"]
    result = ti.xcom_pull(task_ids="update_valeurs", key="valeurs_result") or {}

    status         = result.get("status", "?")
    new_values     = result.get("new_values", 0)
    added          = result.get("added", 0)
    total_enriched = result.get("total_enriched", "?")
    before         = result.get("before", "?")
    enriched_csv   = result.get("enriched_csv", "?")

    print("=" * 60, flush=True)
    print("✅ PIPELINE BOURSORAMA VALEURS — RÉSUMÉ", flush=True)
    print(f"   Date                        : {context['ds']}", flush=True)
    print(f"   Statut                      : {status}", flush=True)
    print(f"   Nouvelles valeurs détectées : {new_values}", flush=True)
    print(f"   Lignes avant fusion         : {before}", flush=True)
    print(f"   Lignes ajoutées             : {added}", flush=True)
    print(f"   Total fichier enrichi       : {total_enriched}", flush=True)
    print(f"   Fichier enrichi             : {enriched_csv}", flush=True)
    print("=" * 60, flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# DÉFINITION DU DAG
# ─────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="dag_boursorama_valeurs",
    description=(
        "Référentiel valeurs : lit les CSV source (ETF/premieres/specifique), "
        "scrape Boursorama par ISIN → met à jour boursorama_cotations_enriched.csv"
    ),
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    # Lundi à 05h00 → avant dag_boursorama_cotation (06h00) et dag_boursorama_news (07h00)
    # Changer en "0 5 * * *" si on veut vérifier chaque jour (arrêt rapide si rien n'a changé)
    schedule_interval="35 1 * * 1",  # Lundi à 01:35
    catchup=False,
    tags=["boursorama", "valeurs", "referentiel", "dataoz"],
) as dag:

    t1 = PythonOperator(
        task_id="update_valeurs",
        python_callable=run_update_valeurs,
        # Timeout généreux : scraping par ISIN peut prendre du temps sur de nombreuses valeurs
        execution_timeout=timedelta(hours=2),
    