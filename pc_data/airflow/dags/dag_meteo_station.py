# -*- coding: utf-8 -*-
"""
dag_weathercloud_bresser.py
============================
DAG Airflow — Pipeline données météo Bresser MeteoChamp HD

═══════════════════════════════════════════════════════════════════════════════
ARCHITECTURE — 2 processus INDÉPENDANTS
═══════════════════════════════════════════════════════════════════════════════

┌─────────────────────────────────────────────────────────────────────────┐
│ PIPELINE A — WEATHERCLOUD (automatique, Playwright)                     │
│                                                                         │
│  download_csv ──► transform_wc ──────────────────────┐                  │
│                                                       ├──► summary      │
│ PIPELINE B — CLÉ USB (manuel, dépôt de fichiers CSV) │                  │
│                                                       │                  │
│  usb_extract ──► transform_usb ──────────────────────┘                  │
└─────────────────────────────────────────────────────────────────────────┘

PIPELINE A — Weathercloud
  1. download_csv → Playwright, login Weathercloud, export CSV mensuel
  2. transform_wc → [30 min filter] + [catalog FR→EN mapping]
                    Filtrage HH:00/HH:30 + mapping catalog + split Date/Time
                    + colonnes absentes=NaN + ordre = common_weather_database

PIPELINE B — Clé USB
  1. usb_extract  → inbox_bresser/ → vérifications → pretraited.csv
  2. transform_usb → vérif colonnes + nettoyage -.- + doublons
                    + renommage CH1→Etage / CH2→Cave → transformed.csv

═══════════════════════════════════════════════════════════════════════════════
FICHIERS PRODUITS (séparés et vérifiables indépendamment)
═══════════════════════════════════════════════════════════════════════════════

  [A1] raw/météo_bresser/weathercloud/weathercloud_bresser_YYYY-MM-DD.csv
       → CSV brut Weathercloud (UTF-16, ;, colonnes FR)

  [A2] raw/météo_bresser/weathercloud/météo_bresser_transformed.csv
       → CSV colonnes EN alignées sur common_weather_database (UTF-8, ,)

  [B1] raw/météo_bresser/clé_usb/bresser_YYYYMMDD_transformed.csv
       → CSV propre typé aligné sur common_weather_database (UTF-8, ,)

═══════════════════════════════════════════════════════════════════════════════
CATALOGUE DE COLONNES (catalog.json)
═══════════════════════════════════════════════════════════════════════════════

  Weathercloud                     ↔  USB (common_weather_database)
  ──────────────────────────────────────────────────────────────────
  Date (Europe/Paris)              ↔  Date + Time
  Température (°C)                 ↔  Out Temperature
  Température intérieur (°C)       ↔  IN Temperature
  Humidité (%)                     ↔  Out Humidity
  Rafale maximale de vent (m/s)    ↔  Wind Gust
  Pression atmosphérique (hPa)     ↔  Baro Pressure Rel
  … (voir catalog.json)

Variables d'environnement :
  WEATHERCLOUD_EMAIL        Email de connexion Weathercloud
  WEATHERCLOUD_PASSWORD     Mot de passe
  WEATHERCLOUD_STATION_ID   ID station (ex : 92fc230f9ee474d9)

Chemins Docker :
  Weathercloud raw  : /opt/airflow/data/raw/météo_bresser/weathercloud/
  Catalog           : /opt/airflow/data/curated/météo/bresser/catalog.json
  USB inbox         : /opt/airflow/data/raw/météo_bresser/clé_usb/inbox_bresser/
  USB sortie        : /opt/airflow/data/raw/météo_bresser/clé_usb/
  USB archive       : /opt/airflow/data/raw/météo_bresser/clé_usb/inbox_bresser/archive/
  Curated commun    : /opt/airflow/data/curated/météo/bresser/
  Tmp               : /opt/airflow/data/tmp/weathercloud_dl/
"""

import importlib
import os
import sys
import traceback
from datetime import datetime, timedelta, date
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ── PYTHONPATH : scripts météo Bresser ───────────────────────────────────────
_BRESSER_SCRIPTS = Path("/opt/airflow/scripts/meteo/bresser")
if str(_BRESSER_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_BRESSER_SCRIPTS))

# ── Chemins (dans le container Docker) ───────────────────────────────────────
# Catalog de correspondance colonnes WC ↔ USB
CATALOG_FILE  = Path("/opt/airflow/data/curated/météo/bresser/catalog.json")

# Pipeline A — Weathercloud
WC_RAW_DIR    = Path("/opt/airflow/data/raw/météo_bresser/weathercloud")

# Pipeline B — Clé USB
USB_RAW_DIR   = Path("/opt/airflow/data/raw/météo_bresser/clé_usb")
USB_INBOX     = USB_RAW_DIR / "inbox_bresser"           # dépôt des fichiers manuels
USB_ARCHIVE   = USB_INBOX   / "archive"                 # après traitement
# Temporaire Playwright
TMP_DIR       = Path("/opt/airflow/data/tmp/weathercloud_dl")
# Load — base de données originale à mettre à jour
DATABASE_FILE = Path("/opt/airflow/data/curated/météo/bresser/common_weather_database.csv")

# ── Paramètres par défaut ─────────────────────────────────────────────────────
default_args = {
    "owner":            "dataoz",
    "retries":          2,
    "retry_delay":      timedelta(minutes=10),
    "email_on_failure": False,
}


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _get_target_month(execution_date=None):
    """Retourne (année, mois) pour sélectionner le bon mois dans Weathercloud."""
    if execution_date is None:
        execution_date = datetime.now()
    ref = execution_date.date() if isinstance(execution_date, datetime) else execution_date
    return ref.year, ref.month


# ═════════════════════════════════════════════════════════════════════════════
# PIPELINE A — WEATHERCLOUD
# ═════════════════════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────────────────────
# A1 — Téléchargement CSV Weathercloud
# ─────────────────────────────────────────────────────────────────────────────

def task_download_csv(**context):
    """
    Télécharge le CSV mensuel depuis Weathercloud via Playwright.

    Sortie : weathercloud_bresser_YYYY-MM-DD.csv
    Dossier: /opt/airflow/data/raw/météo_bresser/weathercloud/
    """
    try:
        import download_weathercloud
        importlib.reload(download_weathercloud)

        email      = os.environ["WEATHERCLOUD_EMAIL"]
        password   = os.environ["WEATHERCLOUD_PASSWORD"]
        station_id = os.environ["WEATHERCLOUD_STATION_ID"]

        execution_date = context.get("execution_date") or datetime.now()
        year, month    = _get_target_month(execution_date)
        run_date       = (execution_date.date()
                         if isinstance(execution_date, datetime)
                         else execution_date)

        print(f"[download] Mois cible   : {year}-{month:02d}", flush=True)
        print(f"[download] Date fichier : {run_date}", flush=True)
        print(f"[download] Répertoire   : {WC_RAW_DIR}", flush=True)
        print(f"[download] Station ID   : {station_id}", flush=True)

        raw_file = download_weathercloud.download_csv(
            email=email,
            password=password,
            station_id=station_id,
            tmp_dir=str(TMP_DIR),
            raw_dir=str(WC_RAW_DIR),
            year=year,
            month=month,
            run_date=run_date,
        )

        result = {
            "status":   "ok",
            "raw_file": str(raw_file),
            "year":     year,
            "month":    month,
            "run_date": str(run_date),
        }
        print(f"[download] ✅ Fichier : {raw_file}", flush=True)
        context["ti"].xcom_push(key="download_result", value=result)
        return result

    except Exception as e:
        print(f"[ERROR] download_csv → {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


# ─────────────────────────────────────────────────────────────────────────────
# A2 — Transform Weathercloud (30 min + alignement colonnes)
# ─────────────────────────────────────────────────────────────────────────────

def task_transform_wc(**context):
    """
    Transforme le CSV brut Weathercloud en deux étapes enchaînées :

    Étape 1 — Filtrage 30 min (transform_weathercloud)
      Source : weathercloud_bresser_YYYY-MM-DD.csv   (brut UTF-16)
      Sortie : weathercloud_bresser_YYYY-MM-DD_30min.csv  [intermédiaire, supprimé après]
      Règles : conservation des créneaux HH:00 et HH:30, date ISO, lignes vides

    Étape 2 — Alignement colonnes (transform_wc_columns)
      Source : weathercloud_bresser_YYYY-MM-DD_30min.csv
      Catalog: /opt/airflow/data/curated/météo/bresser/catalog.json
      Sortie : bresser_wc_YYYY-MM-DD.csv
      Règles : séparation Date/Time, renommage FR→EN, colonnes absentes=NaN,
               normalisation numérique (virgule→point), ordre common_weather_database

    Nettoyage : suppression du _30min.csv et du brut weathercloud_bresser_YYYY-MM-DD.csv
      → un seul fichier bresser_wc_YYYY-MM-DD.csv conservé dans weathercloud/
    """
    try:
        import transform_weathercloud
        import transform_wc_columns
        importlib.reload(transform_weathercloud)
        importlib.reload(transform_wc_columns)

        ti              = context["ti"]
        download_result = ti.xcom_pull(task_ids="download_csv",
                                       key="download_result") or {}

        raw_file = download_result.get("raw_file")
        if not raw_file:
            raise ValueError("Chemin du fichier Raw non trouvé dans XCom.")
        if not Path(raw_file).exists():
            raise FileNotFoundError(f"Fichier Raw introuvable : {raw_file}")

        if not CATALOG_FILE.exists():
            raise FileNotFoundError(f"Catalog introuvable : {CATALOG_FILE}")

        # ── Étape 1 : filtrage 30 min ─────────────────────────────────────────
        print(f"[transform_wc] ── Étape 1 : filtrage 30 min ──", flush=True)
        print(f"[transform_wc] Source  : {raw_file}", flush=True)
        print(f"[transform_wc] Sortie  : {WC_RAW_DIR}", flush=True)

        t30_result = transform_weathercloud.run_transform(
            raw_file=raw_file,
            out_dir=str(WC_RAW_DIR),
        )

        wc_30min_file = t30_result.get("out_file")
        print(f"[transform_wc] ✔ Fichier 30 min : {wc_30min_file}", flush=True)
        print(f"[transform_wc]   Lignes brutes   : {t30_result.get('lignes_brutes')}", flush=True)
        print(f"[transform_wc]   Lignes 30 min   : {t30_result.get('lignes_30min')}", flush=True)
        print(f"[transform_wc]   Période         : {t30_result.get('debut')} → {t30_result.get('fin')}", flush=True)

        if not wc_30min_file or not Path(wc_30min_file).exists():
            raise FileNotFoundError(f"Fichier 30 min introuvable après étape 1 : {wc_30min_file}")

        # ── Étape 2 : alignement colonnes FR → EN ────────────────────────────
        print(f"[transform_wc] ── Étape 2 : alignement colonnes FR→EN ──", flush=True)
        print(f"[transform_wc] Source  : {wc_30min_file}", flush=True)
        print(f"[transform_wc] Catalog : {CATALOG_FILE}", flush=True)
        print(f"[transform_wc] Sortie  : {WC_RAW_DIR}", flush=True)

        col_result = transform_wc_columns.run_transform_columns(
            wc_30min_file=wc_30min_file,
            catalog_file=str(CATALOG_FILE),
            output_dir=str(WC_RAW_DIR),
        )

        print(f"[transform_wc] ✅ Transformé     : {col_result.get('out_file')}", flush=True)
        print(f"[transform_wc]    Lignes          : {col_result.get('lignes')}", flush=True)
        print(f"[transform_wc]    Colonnes        : {col_result.get('colonnes')}", flush=True)
        print(f"[transform_wc]    Période         : {col_result.get('debut')} → {col_result.get('fin')}", flush=True)
        if col_result.get("colonnes_ignorees"):
            print(f"[transform_wc]    ⚠ Ignorées      : {col_result.get('colonnes_ignorees')}", flush=True)

        # ── Nettoyage : suppression des fichiers intermédiaires ──────────────
        # _30min.csv (intermédiaire étape 1)
        try:
            Path(wc_30min_file).unlink()
            print(f"[transform_wc] 🗑  Supprimé : {Path(wc_30min_file).name}", flush=True)
        except OSError as exc:
            print(f"[transform_wc] ⚠  Impossible de supprimer _30min : {exc}", flush=True)

        # Brut weathercloud_bresser_YYYY-MM-DD.csv
        try:
            Path(raw_file).unlink()
            print(f"[transform_wc] 🗑  Supprimé : {Path(raw_file).name}", flush=True)
        except OSError as exc:
            print(f"[transform_wc] ⚠  Impossible de supprimer le brut : {exc}", flush=True)

        # Résultat combiné
        wc_result = {
            "status":          "ok",
            "lignes_brutes":   t30_result.get("lignes_brutes"),
            "lignes_30min":    t30_result.get("lignes_30min"),
            "out_file":        col_result.get("out_file"),
            "lignes":          col_result.get("lignes"),
            "colonnes":        col_result.get("colonnes"),
            "debut":           col_result.get("debut"),
            "fin":             col_result.get("fin"),
            "colonnes_mappees":        col_result.get("colonnes_mappees"),
            "colonnes_absentes_ajout": col_result.get("colonnes_absentes_ajout"),
            "colonnes_ignorees":       col_result.get("colonnes_ignorees"),
            "taille_ko":       col_result.get("taille_ko"),
        }

        context["ti"].xcom_push(key="wc_result", value=wc_result)
        return wc_result

    except Exception as e:
        print(f"[ERROR] transform_wc → {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


# ═════════════════════════════════════════════════════════════════════════════
# PIPELINE B — CLÉ USB
# ═════════════════════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────────────────────
# B1 — Extract USB
# ─────────────────────────────────────────────────────────────────────────────

def task_usb_extract(**context):
    """
    EXTRACT USB — lit les fichiers Data_*.csv déposés dans inbox_bresser/,
    applique les vérifications (colonnes, nombre, doublons, format date),
    et produit un seul fichier prétraité prêt pour la suite du pipeline.

    Vérifications :
      ✔ Nombre de colonnes (tolérance ±2 autour de 33)
      ✔ Présence des colonnes obligatoires (catalog.json)
      ✔ Format date dd/mm/yyyy → yyyy-mm-dd
      ✔ Doublons sur Date + Time
      ✔ Tri chronologique

    Source : /opt/airflow/data/raw/météo_bresser/clé_usb/inbox_bresser/Data_*.csv
    Sortie : /opt/airflow/data/raw/météo_bresser/clé_usb/bresser_YYYYMMDD_pretraited.csv
    Archive: /opt/airflow/data/raw/météo_bresser/clé_usb/inbox_bresser/archive/
    """
    try:
        import etl_usb_bresser
        importlib.reload(etl_usb_bresser)

        execution_date = context.get("execution_date") or datetime.now()
        run_date = (execution_date.date()
                    if isinstance(execution_date, datetime)
                    else execution_date)

        # Vérification qu'il y a des fichiers à traiter
        inbox_files = list(USB_INBOX.glob("*.csv"))
        if not inbox_files:
            msg = f"Aucun fichier CSV dans {USB_INBOX} — tâche ignorée."
            print(f"[usb_extract] ⚠️  {msg}", flush=True)
            result = {"status": "no_files", "message": msg}
            context["ti"].xcom_push(key="usb_extract_result", value=result)
            return result

        print(f"[usb_extract] {len(inbox_files)} fichier(s) dans inbox :", flush=True)
        for f in inbox_files:
            print(f"[usb_extract]   → {f.name}", flush=True)

        usb_result = etl_usb_bresser.run_extract_usb(
            inbox_dir=str(USB_INBOX),
            output_dir=str(USB_RAW_DIR),
            archive_dir=str(USB_ARCHIVE),
            run_date=run_date,
        )

        print(f"[usb_extract] ✅ Fichier prétraité : {usb_result.get('out_file')}", flush=True)
        print(f"[usb_extract] Fichiers traités     : {usb_result.get('fichiers_traites')} / {usb_result.get('fichiers_inbox')}", flush=True)
        print(f"[usb_extract] Fichiers rejetés     : {usb_result.get('fichiers_rejetes')}", flush=True)
        print(f"[usb_extract] Relevés totaux       : {usb_result.get('total_releves')}", flush=True)
        print(f"[usb_extract] Doublons supprimés   : {usb_result.get('doublons_supprimes')}", flush=True)
        print(f"[usb_extract] Période              : {usb_result.get('debut')} → {usb_result.get('fin')}", flush=True)

        for w in usb_result.get("warnings", []):
            print(f"[usb_extract] ⚠  {w}", flush=True)
        for e in usb_result.get("erreurs_fichiers", []):
            print(f"[usb_extract] ✗  {e}", flush=True)

        context["ti"].xcom_push(key="usb_extract_result", value=usb_result)
        return usb_result

    except Exception as e:
        print(f"[ERROR] usb_extract → {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


# ─────────────────────────────────────────────────────────────────────────────
# B2 — Transform USB
# ─────────────────────────────────────────────────────────────────────────────

def task_transform_usb(**context):
    """
    TRANSFORM USB — transforme le fichier prétraité en fichier propre et typé.

    Opérations :
      ✔ Vérification concordance des colonnes (16 obligatoires, 17 optionnelles)
      ✔ Nettoyage valeurs "sans capteur" : -.- et - - → NaN
      ✔ Suppression des doublons résiduels (Date + Time)
      ✔ Normalisation types numériques
      ✔ Tri chronologique

    Source : /opt/airflow/data/raw/météo_bresser/clé_usb/bresser_YYYYMMDD_pretraited.csv
    Sortie : /opt/airflow/data/raw/météo_bresser/clé_usb/bresser_YYYYMMDD_transformed.csv
    """
    try:
        import transform_usb
        importlib.reload(transform_usb)

        ti             = context["ti"]
        extract_result = ti.xcom_pull(task_ids="usb_extract",
                                      key="usb_extract_result") or {}

        # Si l'extract n'a trouvé aucun fichier, on passe
        if extract_result.get("status") == "no_files":
            msg = "Aucun fichier prétraité disponible — transform_usb ignoré."
            print(f"[transform_usb] ⚠️  {msg}", flush=True)
            result = {"status": "no_files", "message": msg}
            context["ti"].xcom_push(key="usb_transform_result", value=result)
            return result

        pretraited_file = extract_result.get("out_file")
        if not pretraited_file:
            raise ValueError("Chemin du fichier prétraité non trouvé dans XCom.")
        if not Path(pretraited_file).exists():
            raise FileNotFoundError(f"Fichier prétraité introuvable : {pretraited_file}")

        print(f"[transform_usb] Source : {pretraited_file}", flush=True)
        print(f"[transform_usb] Sortie : {USB_RAW_DIR}", flush=True)

        transform_result = transform_usb.run_transform_usb(
            pretraited_file=pretraited_file,
            output_dir=str(USB_RAW_DIR),
        )

        print(f"[transform_usb] ✅ Transformé          : {transform_result.get('out_file')}", flush=True)
        print(f"[transform_usb]    Lignes               : {transform_result.get('lignes')}", flush=True)
        print(f"[transform_usb]    Colonnes             : {transform_result.get('colonnes')}", flush=True)
        print(f"[transform_usb]    Période              : {transform_result.get('debut')} → {transform_result.get('fin')}", flush=True)
        print(f"[transform_usb]    Doublons supprimés   : {transform_result.get('doublons_supprimes')}", flush=True)
        print(f"[transform_usb]    Valeurs sans capteur : {transform_result.get('valeurs_sans_capteur')}", flush=True)

        for w in transform_result.get("warnings", []):
            print(f"[transform_usb] ⚠  {w}", flush=True)

        context["ti"].xcom_push(key="usb_transform_result", value=transform_result)
        return transform_result

    except Exception as e:
        print(f"[ERROR] transform_usb → {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


# ═════════════════════════════════════════════════════════════════════════════
# LOAD — Fusion USB + WC
# ═════════════════════════════════════════════════════════════════════════════

def task_load(**context):
    """
    LOAD — intègre bresser_usb et bresser_wc dans la base originale.

    Sécurités :
      ✔ [1] Contrôle cohérence USB ↔ WC (temp ≤1°C, humidity ≤15%)
      ✔ [2] Backup horodaté avant toute écriture
      ✔ [3] Contrôle schéma (colonnes)
      ✔ [4] Déduplication (créneaux déjà présents ignorés)
      ✔ [5] Validation de la fusion (lignes, période)
      ✔ [6] Écriture atomique (tmp → rename)
      ✔ [7] Rotation des backups (30 derniers conservés)
      ✔ [8] Audit log

    Source A : /opt/airflow/data/raw/météo_bresser/weathercloud/bresser_wc_YYYY-MM-DD.csv
    Source B : /opt/airflow/data/raw/météo_bresser/clé_usb/bresser_usb_YYYY-MM-DD.csv
    Base     : /opt/airflow/data/curated/météo/bresser/common_weather_database.csv
    """
    try:
        import load_bresser
        importlib.reload(load_bresser)

        ti       = context["ti"]
        wc       = ti.xcom_pull(task_ids="transform_wc",  key="wc_result")            or {}
        usb_tr   = ti.xcom_pull(task_ids="transform_usb", key="usb_transform_result") or {}

        execution_date = context.get("execution_date") or datetime.now()
        run_date = (execution_date.date()
                    if isinstance(execution_date, datetime)
                    else execution_date)

        wc_file  = wc.get("out_file")      or None
        usb_file = usb_tr.get("out_file")  or None

        if not wc_file and not usb_file:
            raise RuntimeError("Aucun fichier transformé disponible pour le Load.")

        print(f"[load] Source WC  : {wc_file  or '—'}", flush=True)
        print(f"[load] Source USB : {usb_file or '—'}", flush=True)
        print(f"[load] Base       : {DATABASE_FILE}", flush=True)

        load_result = load_bresser.run_load(
            usb_file=usb_file,
            wc_file=wc_file,
            database_file=str(DATABASE_FILE),
            run_date=run_date,
        )

        print(f"[load] ✅ Base mise à jour       : {DATABASE_FILE.name}", flush=True)
        print(f"[load]    Lignes avant            : {load_result.get('lignes_avant')}", flush=True)
        print(f"[load]    Lignes ajoutées         : {load_result.get('lignes_ajoutees')}", flush=True)
        print(f"[load]    Lignes après            : {load_result.get('lignes_apres')}", flush=True)
        print(f"[load]    Doublons ignorés        : {load_result.get('doublons_ignores')}", flush=True)
        c = load_result.get("coherence", {})
        print(f"[load]    Cohérence USB/WC        : {c.get('lignes_communes', 0)} créneaux communs, "
              f"taux incohérence = {c.get('taux_incoherence', 0):.1%}", flush=True)
        print(f"[load]    Période                 : {load_result.get('debut')} → {load_result.get('fin')}", flush=True)
        print(f"[load]    Backup                  : {load_result.get('backup')}", flush=True)

        for w in load_result.get("warnings", []):
            print(f"[load] ⚠  {w}", flush=True)

        context["ti"].xcom_push(key="load_result", value=load_result)
        return load_result

    except Exception as e:
        print(f"[ERROR] load → {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


# ═════════════════════════════════════════════════════════════════════════════
# RÉSUMÉ PIPELINE
# ═════════════════════════════════════════════════════════════════════════════

def task_pipeline_summary(**context):
    """Affiche un résumé lisible des 2 pipelines dans les logs Airflow."""
    ti       = context["ti"]
    dl       = ti.xcom_pull(task_ids="download_csv",  key="download_result")      or {}
    wc       = ti.xcom_pull(task_ids="transform_wc",  key="wc_result")            or {}
    usb_ext  = ti.xcom_pull(task_ids="usb_extract",   key="usb_extract_result")   or {}
    usb_tr   = ti.xcom_pull(task_ids="transform_usb", key="usb_transform_result") or {}
    load     = ti.xcom_pull(task_ids="load",           key="load_result")          or {}

    print("=" * 70, flush=True)
    print("🌤️  PIPELINE MÉTÉO BRESSER — RÉSUMÉ GLOBAL", flush=True)
    print(f"   Exécution : {context.get('ds', 'N/A')}", flush=True)
    print("", flush=True)

    print("   ══ PIPELINE A — WEATHERCLOUD (automatique) ══", flush=True)
    print(f"   [A1] Raw brut        : {dl.get('raw_file', '—')}", flush=True)
    print(f"        Date fichier     : {dl.get('run_date', '—')}", flush=True)
    if wc:
        print(f"   [A2] Transformé      : {wc.get('out_file', '—')}", flush=True)
        print(f"        Relevés          : {wc.get('lignes_brutes', '—')} bruts"
              f" → {wc.get('lignes_30min', '—')} à 30 min", flush=True)
        print(f"        Période          : {wc.get('debut', '—')} → {wc.get('fin', '—')}", flush=True)
        if wc.get("colonnes_ignorees"):
            print(f"        ⚠ Ignorées       : {wc.get('colonnes_ignorees')}", flush=True)
    print("", flush=True)

    print("   ══ PIPELINE B — CLÉ USB (manuel) ══", flush=True)
    if usb_ext.get("status") == "no_files":
        print(f"   ⚠️  {usb_ext.get('message', 'Aucun fichier dans inbox_bresser')}", flush=True)
    elif usb_ext.get("status") == "ok":
        print(f"   [B1] Prétraité         : {usb_ext.get('out_file', '—')}", flush=True)
        print(f"        Fichiers traités   : {usb_ext.get('fichiers_traites', '—')} / {usb_ext.get('fichiers_inbox', '—')}", flush=True)
        print(f"        Période            : {usb_ext.get('debut', '—')} → {usb_ext.get('fin', '—')}", flush=True)
        if usb_tr.get("status") == "ok":
            print(f"   [B2] Transformé        : {usb_tr.get('out_file', '—')}", flush=True)
            print(f"        Lignes             : {usb_tr.get('lignes', '—')}", flush=True)
            print(f"        Période            : {usb_tr.get('debut', '—')} → {usb_tr.get('fin', '—')}", flush=True)
        elif usb_tr.get("status") == "no_files":
            print(f"   [B2] ⚠️  {usb_tr.get('message', 'Transform USB ignoré')}", flush=True)
    print("", flush=True)

    print("   ══ LOAD — MISE À JOUR BASE ORIGINALE ══", flush=True)
    if load:
        c = load.get("coherence", {})
        print(f"   [L]  Statut                  : {load.get('status', '—')}", flush=True)
        print(f"        Lignes avant             : {load.get('lignes_avant', '—')}", flush=True)
        print(f"        Lignes ajoutées          : {load.get('lignes_ajoutees', '—')}", flush=True)
        print(f"        Lignes après             : {load.get('lignes_apres', '—')}", flush=True)
        print(f"        Doublons ignorés         : {load.get('doublons_ignores', '—')}", flush=True)
        print(f"        Lignes WC conservées     : {load.get('lignes_wc_conserv', '—')}", flush=True)
        print(f"        Cohérence USB/WC         : {c.get('lignes_communes', 0)} créneaux communs", flush=True)
        print(f"        Taux incohérence         : {c.get('taux_incoherence', 0):.1%}", flush=True)
        print(f"        Période                  : {load.get('debut', '—')} → {load.get('fin', '—')}", flush=True)
        print(f"        Backup                   : {load.get('backup', '—')}", flush=True)
        if load.get("warnings"):
            for w in load["warnings"]:
                print(f"        ⚠  {w}", flush=True)
    else:
        print(f"   ⚠️  Load non exécuté", flush=True)
    print("=" * 70, flush=True)


# ═════════════════════════════════════════════════════════════════════════════
# DÉFINITION DU DAG
# ═════════════════════════════════════════════════════════════════════════════

with DAG(
    dag_id="dag_meteo_station",
    description=(
        "Météo Bresser : Pipeline A (Weathercloud → raw → transform) "
        "et Pipeline B (clé USB → extract → transform) — indépendants"
    ),
    default_args=default_args,
    start_date=datetime(2026, 4, 15),
    schedule_interval="0 1 * * *",   # Tous les jours à 01:00
    catchup=False,
    tags=["bresser", "météo", "weathercloud", "usb", "dataoz"],
) as dag:

    # ── Pipeline A — Weathercloud ─────────────────────────────────────────────
    tA1 = PythonOperator(
        task_id="download_csv",
        python_callable=task_download_csv,
        execution_timeout=timedelta(minutes=15),
    )

    tA2 = PythonOperator(
        task_id="transform_wc",
        python_callable=task_transform_wc,
        execution_timeout=timedelta(minutes=10),
    )

    # ── Pipeline B — Clé USB ─────────────────────────────────────────────────
    tB1 = PythonOperator(
        task_id="usb_extract",
        python_callable=task_usb_extract,
        execution_timeout=timedelta(minutes=10),
    )

    tB2 = PythonOperator(
        task_id="transform_usb",
        python_callable=task_transform_usb,
        execution_timeout=timedelta(minutes=5),
    )

    # ── Load — Fusion USB + WC ────────────────────────────────────────────────
    tL = PythonOperator(
        task_id="load",
        python_callable=task_load,
        trigger_rule="all_done",   # s'exécute même si un seul pipeline a produit un fichier
        execution_timeout=timedelta(minutes=10),
    )

    # ── Résumé global ───────────────────────────────────────────────────────────────
    tS = PythonOperator(
        task_id="pipeline_summary",
        python_callable=task_pipeline_summary,
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=5),
    )

    t_trigger = TriggerDagRunOperator(
        task_id="trigger_check_pipeline",
        trigger_dag_id="dag_check_pipeline",
        wait_for_completion=False,
        trigger_rule="all_done",
    )

    # ── Dépendances ──────────────────────────────────────────────────────────
    # Pipeline A (Weathercloud) : téléchargement → transformation
    tA1 >> tA2
    # Pipeline B (USB Bresser)  : extraction → transformation
    tB1 >> tB2
    # Load : fusion des deux pipelines (all_done = un seul suffit)
    [tA2, tB2] >> tL
    # Résumé → déclenchement du check
    tL >> tS >> t_trigger
