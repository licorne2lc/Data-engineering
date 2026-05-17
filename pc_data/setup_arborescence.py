"""
Script de création de l'arborescence pc_data
Chemin de base : D:\projet_dataoz\pc_data
"""

import os
from pathlib import Path

BASE = Path(r"D:\projet_dataoz\pc_data")

dossiers = [
    # Airflow
    "airflow/dags",
    "airflow/plugins",
    "airflow/logs",
    "airflow/config",
    # ── Données RAW : fichiers bruts téléchargés ────────────────────────────
    # Cotations Boursorama (scraping + Playwright)
    "data/raw/finance/cotations/archives",          # snapshots du master CSV brut
    "data/raw/finance/cotations/cotation/5d_updates", # téléchargements 5J par symbole
    # News Boursorama
    "data/raw/finance/news",
    # ── Données STAGING : données intermédiaires transformées ───────────────
    "data/staging/finance",
    # ── Données CURATED : bases consolidées, source de vérité ───────────────
    # Cotations
    "data/curated/finance/cotations/cotation/intraday_db", # série intraday agrégée
    "data/curated/finance/cotations/ohlc_10a",             # OHLC 10A consolidé
    "data/curated/finance/valeurs/ETF",                    # compositions ETF
    # News
    "data/curated/finance/news",
    # ── Météo Bresser ────────────────────────────────────────────────────────
    # Données brutes reçues par le serveur récepteur (organisées par YYYY/MM/)
    "data/curated/météo/bresser",
    # Stats journalières consolidées (organisées par YYYY/stats/)
    # (créés dynamiquement par le DAG)
    # ── Archive ─────────────────────────────────────────────────────────────
    "data/archive",
    # Scripts Python
    "scripts/finance/cotation/extract",
    "scripts/finance/cotation/transform",
    "scripts/finance/cotation/load",
    "scripts/finance/news/extract",
    "scripts/finance/news/transform",
    "scripts/finance/news/load",
    # Scripts météo
    "scripts/meteo/bresser",
    # Tests
    "tests",
]

print(f"\n📁 Création de l'arborescence dans : {BASE}\n")

for dossier in dossiers:
    chemin = BASE / dossier
    chemin.mkdir(parents=True, exist_ok=True)
    print(f"  ✅ {chemin}")

# Créer les fichiers de base vides
fichiers = {
    "airflow/requirements.txt": "# Dépendances Python pour les DAGs Airflow\napache-airflow==2.8.0\n",
    "data/raw/finance/cotations/.gitkeep": "",
    "data/raw/finance/news/.gitkeep": "",
    "data/staging/finance/.gitkeep": "",
    "data/curated/finance/cotations/.gitkeep": "",
    "data/curated/finance/news/.gitkeep": "",
    "data/archive/.gitkeep": "",
    "tests/.gitkeep": "",
}

print()
for fichier, contenu in fichiers.items():
    chemin = BASE / fichier
    chemin.write_text(contenu, encoding="utf-8")
    print(f"  📄 {chemin}")

print(f"\n✅ Arborescence créée avec succès dans {BASE}\n")
print("Prochaine étape : copier docker-compose.yml et .env dans D:\\projet_dataoz\\")
