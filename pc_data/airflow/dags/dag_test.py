"""
DAG de test DataOZ
------------------
Vérifie que tout fonctionne bout en bout :
1. Télécharge un fichier JSON depuis une API publique
2. Le sauvegarde dans data/raw/
3. Vérifie que le fichier est bien présent
4. Log un résumé dans Airflow
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import os
from pathlib import Path

# ── Dossier de stockage (volume monté depuis le PC hôte) ──
DATA_RAW = Path("/opt/airflow/data/raw")

# ── Paramètres par défaut du DAG ──────────────────────────
default_args = {
    "owner": "dataoz",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

# ─────────────────────────────────────────────────────────
# TÂCHE 1 — Télécharger un fichier
# ─────────────────────────────────────────────────────────
def telecharger_fichier(**context):
    url = "https://jsonplaceholder.typicode.com/todos/1"
    date_run = context["ds"]  # format YYYY-MM-DD

    print(f"📥 Téléchargement depuis : {url}")
    response = requests.get(url, timeout=10)
    response.raise_for_status()

    data = response.json()
    print(f"✅ Données reçues : {data}")

    # Nom du fichier avec convention : source__type__date__id
    nom_fichier = f"test__json__{date_run}__todo_1.json"
    chemin = DATA_RAW / nom_fichier

    # Créer le dossier si besoin
    DATA_RAW.mkdir(parents=True, exist_ok=True)

    # Sauvegarder le fichier
    with open(chemin, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"💾 Fichier sauvegardé : {chemin}")

    # Passer le chemin à la tâche suivante via XCom
    return str(chemin)


# ─────────────────────────────────────────────────────────
# TÂCHE 2 — Vérifier le fichier
# ─────────────────────────────────────────────────────────
def verifier_fichier(**context):
    # Récupérer le chemin depuis la tâche précédente
    ti = context["ti"]
    chemin = ti.xcom_pull(task_ids="telecharger_fichier")

    if not chemin:
        raise ValueError("❌ Aucun chemin reçu de la tâche précédente")

    chemin = Path(chemin)

    if not chemin.exists():
        raise FileNotFoundError(f"❌ Fichier introuvable : {chemin}")

    taille = chemin.stat().st_size
    print(f"✅ Fichier vérifié : {chemin}")
    print(f"   Taille : {taille} octets")

    with open(chemin, "r", encoding="utf-8") as f:
        contenu = json.load(f)
    print(f"   Contenu : {contenu}")

    return f"OK - {chemin.name} ({taille} octets)"


# ─────────────────────────────────────────────────────────
# TÂCHE 3 — Résumé
# ─────────────────────────────────────────────────────────
def log_resume(**context):
    ti = context["ti"]
    resultat = ti.xcom_pull(task_ids="verifier_fichier")
    print("=" * 50)
    print("✅ DAG TEST DATAOZ — SUCCÈS")
    print(f"   Résultat : {resultat}")
    print(f"   Date     : {context['ds']}")
    print("=" * 50)
    print("🎉 Airflow + PostgreSQL + Stockage fonctionnent correctement !")


# ─────────────────────────────────────────────────────────
# DÉFINITION DU DAG
# ─────────────────────────────────────────────────────────
with DAG(
    dag_id="dag_test_dataoz",
    description="DAG de test — vérifie téléchargement et stockage",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,   # Déclenchement manuel uniquement
    catchup=False,
    tags=["test", "dataoz"],
) as dag:

    t1 = PythonOperator(
        task_id="telecharger_fichier",
        python_callable=telecharger_fichier,
    )

    t2 = PythonOperator(
        task_id="verifier_fichier",
        python_callable=verifier_fichier,
    )

    t3 = PythonOperator(
        task_id="log_resume",
        python_callable=log_resume,
    )

    # Ordre d'exécution : télécharger → vérifier → résumé
    t1 >> t2 >> t3
