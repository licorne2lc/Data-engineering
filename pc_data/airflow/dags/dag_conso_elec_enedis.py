# -*- coding: utf-8 -*-
"""
dag_conso_elec_enedis.py
=========================
DAG Airflow -- Consommation electrique Enedis.

===============================================================================
ARCHITECTURE -- 2 CANAUX ACTIFS + 1 CANAL EN PAUSE
===============================================================================

Database UNIQUE alimentee par 2 canaux complementaires :
    Database_Enedis_30_min.csv  (.../curated/conso_elec/enedis/)

  CANAL B -- ETL INBOX MANUEL (file-only, ad-hoc, souvent vide)
  -------------------------------------------------------------
  extract_inbox --> transform_inbox --> load_inbox --> pipeline_summary

    Source : XLSX deposes PONCTUELLEMENT par l'utilisateur dans
             inbox_enedis/ (export espace client Enedis).
    Finalite : completer la database depuis des fichiers manuels.
    Priorite sur les donnees scrap : keep="last" en cas de divergence
    (le manuel ECRASE le scrap sur les (Date,Time) communs).

    1. extract_inbox  : tri calendaire des XLSX, concatenation
                        -> _manuel/new_data_enedis_YYYYMMDD.xlsx
                        -> archive sources (max 10 XLSX conserves)
    2. transform_inbox: XLSX -> CSV, traitement DST
                        -> _manuel/new_data_enedis_YYYYMMDD.csv
    3. load_inbox     : audit divergences + fusion + archive
                        -> Database_Enedis_30_min.csv (UNIQUE)
                        -> archive/Database_Enedis_30_min_YYYYMMDD.csv
                          (snapshot versionne)

  CANAL C -- ETL SCRAPPING (Playwright, automatique, quotidien)
  -------------------------------------------------------------
  scrap_download --> scrap_extract --> scrap_transform --> scrap_load --> scrap_summary

    Source : XLSX telecharge automatiquement via Playwright depuis
             l'espace client Enedis (Courbe de charge J-5 -> J-2),
             depose dans inbox_enedis_scrap/.
    Finalite : alimenter la database au quotidien.
    En cas de divergence avec le manuel : keep="first" (la DB conserve
    la valeur en place, le scrap RESPECTE le manuel).

    1. scrap_download  : Playwright se connecte au compte, telecharge
                         le XLSX -> inbox_enedis_scrap/.
    2. scrap_extract   : tri + concat -> _scrap/new_data_*.xlsx
    3. scrap_transform : XLSX -> CSV (DST) -> _scrap/new_data_*.csv
    4. scrap_load      : audit divergences + fusion + archive
                         -> Database_Enedis_30_min.csv (MEME DB)
                         -> archive/Database_Enedis_30_min_YYYYMMDD.csv

  ORDRE D'EXECUTION (sequentiel pour eviter race condition) :
      load_inbox (manuel) >> scrap_load (scrap)
      Le manuel passe en premier (priorite), le scrap complete ensuite.

  CANAL A -- API DATA HUB  [EN PAUSE - SANDBOX]
  ---------------------------------------------
  init_schema --> ensure_prm --+--> fetch_load_curve        --+
                               +--> fetch_daily_consumption --+--> api_verify
                               +--> fetch_daily_max_power   --+

  Statut : DESACTIVE dans le graphe d'execution (operateurs commentes).
  Le code Python (callables et helpers) est conserve pour reactivation
  ulterieure lors du passage en production.

Planification : tous les jours a 05:00 Europe/Paris.

Variables d'environnement :
    ENEDIS_API_KEY, ENEDIS_SECRET_KEY   -- OAuth2
    ENEDIS_ENV                          -- 'sandbox' (defaut) ou 'prod'
    PRM_ID                              -- PRM reel (prod)
    ENEDIS_TEST_PRM                     -- override sandbox
    ENEDIS_TOKEN_CACHE                  -- cache token OAuth2
    CONSO_ELEC_DB_URL                   -- URL Postgres
    ENEDIS_IDENTIFIANT                  -- email compte particulier (Canal C)
    ENEDIS_PASSWORD                     -- mot de passe (Canal C)
"""
from __future__ import annotations

import importlib
import json
import os
import sys
import time
import traceback
from datetime import date, datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


# -- PYTHONPATH : scripts Enedis -------------------------------------------------
_ENEDIS_ROOT = Path("/opt/airflow/scripts/conso_elec/enedis")
for _sub in ["", "extract", "transform", "load"]:
    _p = str(_ENEDIS_ROOT / _sub) if _sub else str(_ENEDIS_ROOT)
    if _p not in sys.path:
        sys.path.insert(0, _p)


# -- Chemins (container Docker) --------------------------------------------------
RAW_DIR      = Path("/opt/airflow/data/raw/conso_elec/enedis")
CURATED_DIR  = Path("/opt/airflow/data/curated/conso_elec/enedis")
_SQL_SCHEMA  = _ENEDIS_ROOT / "sql" / "01_schema_enedis.sql"

# -- CANAL A : API sandbox (EN PAUSE) - dossiers conserves pour reactivation
RAW_API_DIR = RAW_DIR / "_api"
RAW_CLC     = RAW_API_DIR / "consumption_load_curve"
RAW_DC      = RAW_API_DIR / "daily_consumption"
RAW_DCMP    = RAW_API_DIR / "daily_consumption_max_power"

# -- CANAL B : MANUEL (inbox + intermediaires dedies)
INBOX_DIR     = RAW_DIR / "inbox_enedis"
INBOX_ARCHIVE = INBOX_DIR / "archive"
RAW_DIR_MANUEL = RAW_DIR / "_manuel"   # new_data_*.xlsx/csv intermediaires manuel

# -- CANAL C : SCRAPPING (inbox + intermediaires dedies)
INBOX_DIR_SCRAP     = RAW_DIR / "inbox_enedis_scrap"
INBOX_ARCHIVE_SCRAP = INBOX_DIR_SCRAP / "archive"
RAW_DIR_SCRAP       = RAW_DIR / "_scrap"           # new_data_*.xlsx/csv intermediaires
SCRAP_TMP_DIR       = Path("/opt/airflow/data/tmp/enedis_scrap")  # screenshots Playwright

# -- Parametres ------------------------------------------------------------------
PRM_SANDBOX_TEST = "22516914714270"   # PRM demo Enedis (sandbox)
PRM_REAL_DEFAULT = "22130390723840"   # PRM reel par defaut (si PRM_ID absent)

# CSV historique monte dans le container
HIST_CSV = RAW_DIR / "_historique" / "Database_Enedis_30_min.csv"


# -- Helpers ---------------------------------------------------------------------

def _resolve_prm() -> tuple[str, str]:
    """PRM sandbox/prod selon ENEDIS_ENV."""
    env      = os.environ.get("ENEDIS_ENV", "sandbox").lower()
    override = os.environ.get("ENEDIS_TEST_PRM", "").strip()
    if override:
        return override, "ENEDIS_TEST_PRM"
    if env == "sandbox":
        return PRM_SANDBOX_TEST, "sandbox"
    return os.environ.get("PRM_ID", "").strip(), "PRM_ID (prod)"


def _window(jours: int = 7) -> tuple[date, date]:
    today = date.today()
    return today - timedelta(days=jours), today


default_args = {
    "owner":            "dataoz",
    "retries":          2,
    "retry_delay":      timedelta(minutes=15),
    "email_on_failure": False,
}


# ==============================================================================
# TACHES COMMUNES (utilisees par CANAL A en pause - conservees pour reactivation)
# ==============================================================================

def task_init_schema(**context):
    """Cree le schema enedis + tables Postgres si necessaire."""
    try:
        from load import db
        importlib.reload(db)
        if not _SQL_SCHEMA.exists():
            raise FileNotFoundError(f"SQL introuvable : {_SQL_SCHEMA}")
        db.execute_sql_file(_SQL_SCHEMA)
        print(f"[init_schema] schema enedis initialise via {_SQL_SCHEMA}", flush=True)
        return {"sql_file": str(_SQL_SCHEMA)}
    except Exception as e:
        print(f"[ERROR] init_schema -> {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


def task_ensure_prm(**context):
    """Upsert du PRM dans enedis.dim_prm."""
    try:
        from load import load_enedis
        importlib.reload(load_enedis)

        prm, source = _resolve_prm()
        if not prm:
            raise RuntimeError("PRM introuvable -- verifier PRM_ID ou ENEDIS_TEST_PRM")
        n = load_enedis.upsert_prm(prm, libelle=f"Auto-created via DAG ({source})")
        print(f"[ensure_prm] PRM={prm}  source={source}  ({n} ligne)", flush=True)
        context["ti"].xcom_push(key="prm",    value=prm)
        context["ti"].xcom_push(key="source", value=source)
        return {"prm": prm, "source": source, "upserts": n}
    except Exception as e:
        print(f"[ERROR] ensure_prm -> {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


def _prm_from_xcom(context) -> str:
    return context["ti"].xcom_pull(task_ids="ensure_prm", key="prm") or ""


def _save_raw_json(dossier: Path, prm: str,
                   start: date, end: date, payload: dict) -> Path:
    dossier.mkdir(parents=True, exist_ok=True)
    fichier = dossier / f"{prm}_{start.isoformat()}_{end.isoformat()}.json"
    fichier.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                       encoding="utf-8")
    return fichier


def _fetch_and_load(endpoint_name, raw_dir, client_method,
                    parse_fn_name, upsert_fn_name, context):
    from extract import enedis_client
    from transform import parse as parse_mod
    from load import load_enedis
    importlib.reload(enedis_client)
    importlib.reload(parse_mod)
    importlib.reload(load_enedis)

    prm        = _prm_from_xcom(context)
    start, end = _window(7)
    client     = enedis_client.EnedisClient()
    parse_fn   = getattr(parse_mod, parse_fn_name)
    upsert_fn  = getattr(load_enedis, upsert_fn_name)
    method     = getattr(client, client_method)

    t0 = time.time()
    http_status = None
    n_points    = 0
    erreur      = None
    try:
        payload     = method(prm, start, end)
        http_status = 200
        source_file = str(_save_raw_json(raw_dir, prm, start, end, payload))
        rows        = parse_fn(payload, source_file=source_file)
        n_points    = len(rows)
        nb_upsert   = upsert_fn(rows)
        duree_ms    = int((time.time() - t0) * 1000)
        load_enedis.log_api_call(
            endpoint=endpoint_name, prm=prm,
            date_debut=start, date_fin=end,
            http_status=http_status, n_points=n_points, duree_ms=duree_ms,
        )
        print(f"[{endpoint_name}] {start} -> {end}  "
              f"{n_points} pts  {nb_upsert} upserts  {duree_ms}ms", flush=True)
        return {
            "prm": prm, "start": start.isoformat(), "end": end.isoformat(),
            "n_points": n_points, "n_upserts": nb_upsert,
            "source_file": source_file, "duree_ms": duree_ms,
        }
    except Exception as e:
        duree_ms = int((time.time() - t0) * 1000)
        erreur   = str(e)[:500]
        try:
            load_enedis.log_api_call(
                endpoint=endpoint_name, prm=prm,
                date_debut=start, date_fin=end,
                http_status=http_status, n_points=n_points,
                duree_ms=duree_ms, erreur=erreur,
            )
        except Exception:
            pass
        print(f"[ERROR] {endpoint_name} -> {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


# ==============================================================================
# CANAL A -- API DATA HUB (EN PAUSE : callables conserves, operateurs commentes)
# ==============================================================================

def task_fetch_load_curve(**context):
    return _fetch_and_load(
        "consumption_load_curve", RAW_CLC,
        "consumption_load_curve", "parse_load_curve", "upsert_conso_30min",
        context,
    )


def task_fetch_daily_consumption(**context):
    return _fetch_and_load(
        "daily_consumption", RAW_DC,
        "daily_consumption", "parse_daily_consumption", "upsert_conso_jour",
        context,
    )


def task_fetch_daily_max_power(**context):
    return _fetch_and_load(
        "daily_consumption_max_power", RAW_DCMP,
        "daily_consumption_max_power", "parse_daily_max_power", "upsert_pmax_jour",
        context,
    )


def task_api_verify(**context):
    """
    CANAL A -- Verification finale (en pause).
    Confirme que les 3 appels API sandbox ont abouti et que les fichiers JSON
    bruts ont ete enregistres. PAS de lien avec la database CSV.
    Reconnexion prevue lors du passage en production.
    """
    ti       = context["ti"]
    prm_info = ti.xcom_pull(task_ids="ensure_prm")              or {}
    clc      = ti.xcom_pull(task_ids="fetch_load_curve")        or {}
    daily    = ti.xcom_pull(task_ids="fetch_daily_consumption") or {}
    pmax     = ti.xcom_pull(task_ids="fetch_daily_max_power")   or {}

    ok_clc   = clc.get("n_points",  0) or 0
    ok_daily = daily.get("n_points", 0) or 0
    ok_pmax  = pmax.get("n_points",  0) or 0
    all_ok   = ok_clc > 0 and ok_daily > 0 and ok_pmax > 0

    print("=" * 70, flush=True)
    print("CANAL A -- API ENEDIS (BAC A SABLE) -- VERIFICATION", flush=True)
    print(f"   Execution   : {context.get('ds', 'N/A')}", flush=True)
    print(f"   Env         : {os.environ.get('ENEDIS_ENV', 'sandbox')}", flush=True)
    print(f"   PRM sandbox : {prm_info.get('prm', '?')}  [{prm_info.get('source', '?')}]",
          flush=True)
    print("", flush=True)
    status_clc   = "OK" if ok_clc   > 0 else "ECHEC"
    status_daily = "OK" if ok_daily > 0 else "ECHEC"
    status_pmax  = "OK" if ok_pmax  > 0 else "ECHEC"
    print(f"   [{status_clc:<5s}] fetch_load_curve        : "
          f"{ok_clc} mesures  | {clc.get('duree_ms', '?')} ms", flush=True)
    print(f"   [{status_daily:<5s}] fetch_daily_consumption : "
          f"{ok_daily} jours   | {daily.get('duree_ms', '?')} ms", flush=True)
    print(f"   [{status_pmax:<5s}] fetch_daily_max_power   : "
          f"{ok_pmax} jours   | {pmax.get('duree_ms', '?')} ms", flush=True)
    print("", flush=True)
    if all_ok:
        print("   Telechargement sandbox : SUCCES", flush=True)
    else:
        print("   Telechargement sandbox : PARTIEL ou ECHEC -- voir logs ci-dessus",
              flush=True)
    print("", flush=True)
    print("   NOTE : ces donnees sandbox ne sont PAS reliees a la database CSV.", flush=True)
    print("   Reconnexion prevue lors du passage en production.", flush=True)
    print("=" * 70, flush=True)

    return {
        "env":          os.environ.get("ENEDIS_ENV", "sandbox"),
        "prm":          prm_info.get("prm"),
        "clc_points":   ok_clc,
        "daily_points": ok_daily,
        "pmax_points":  ok_pmax,
        "all_ok":       all_ok,
    }


# ==============================================================================
# CANAL B -- ETL INBOX MANUEL (file-only, ad-hoc)
# ==============================================================================

# Database UNIQUE alimentee par les 2 canaux (manuel + scrap).
# Archive UNIQUE pour les snapshots versionnes Database_Enedis_30_min_YYYYMMDD.csv.
DATABASE_CSV = CURATED_DIR / "Database_Enedis_30_min.csv"
DB_ARCHIVE   = CURATED_DIR / "archive"
DST_TABLE    = Path("/opt/airflow/data/curated/calendaire/chgt_heure/table_chgt_heure.csv")

# Cle XCom pour transmettre le chemin du xlsx entre Extract et Transform
_XCOM_NEW_DATA_XLSX = "new_data_xlsx_path"
# Cle XCom pour transmettre le chemin du csv entre Transform et Load
_XCOM_NEW_DATA_CSV  = "new_data_csv_path"

# Cles XCom dediees pour la branche scrap (independantes du manuel)
_XCOM_SCRAP_XLSX = "scrap_xlsx_path"
_XCOM_SCRAP_CSV  = "scrap_csv_path"


def task_extract_inbox(**context):
    """
    CANAL B -- Phase 1 EXTRACT.
    Scan inbox_enedis/, tri calendaire par nom de fichier, concatenation de
    tous les XLSX en un seul new_data_enedis_YYYYMMDD.xlsx.
    Archive les XLSX sources (max 10 conserves).
    Pousse le chemin du fichier produit dans XCom pour la phase suivante.
    """
    try:
        import etl_inbox_enedis
        importlib.reload(etl_inbox_enedis)

        RAW_DIR_MANUEL.mkdir(parents=True, exist_ok=True)

        result = etl_inbox_enedis.phase_extract(
            inbox_dir   = INBOX_DIR,
            archive_dir = INBOX_ARCHIVE,
            output_dir  = RAW_DIR_MANUEL,   # _manuel/ : intermediaires dedies canal manuel
        )

        status = result.get("status", "?")
        if status == "no_files":
            print(f"[extract_inbox] Inbox vide : {result.get('message')}", flush=True)
        elif status == "no_data":
            print("[extract_inbox] XLSX trouves mais aucune donnee lisible", flush=True)
        else:
            print(f"[extract_inbox] {result['fichiers']} fichier(s) "
                  f"| {result['lignes']} lignes "
                  f"| -> {Path(str(result['output'])).name}", flush=True)
            for r in result.get("rejetes", []):
                print(f"[extract_inbox]   KO {r}", flush=True)

        # Pousse le chemin du xlsx produit (ou None si inbox vide)
        out = result.get("output")
        context["ti"].xcom_push(key=_XCOM_NEW_DATA_XLSX,
                                value=str(out) if out else None)
        return result

    except Exception as e:
        print(f"[ERROR] extract_inbox -> {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


def task_transform_inbox(**context):
    """
    CANAL B -- Phase 2 TRANSFORM.
    Charge le new_data xlsx (chemin recu via XCom), applique le traitement DST
    (fall-back='add', spring-forward='low'), convertit Debut -> Fin,
    exporte new_data_enedis_YYYYMMDD.csv.
    Pousse le chemin du CSV produit dans XCom pour la phase Load.
    """
    try:
        import etl_inbox_enedis
        importlib.reload(etl_inbox_enedis)

        xlsx_path_str = context["ti"].xcom_pull(
            task_ids="extract_inbox", key=_XCOM_NEW_DATA_XLSX
        )
        if not xlsx_path_str:
            print("[transform_inbox] Aucun fichier new_data recu (inbox vide) -- skip",
                  flush=True)
            context["ti"].xcom_push(key=_XCOM_NEW_DATA_CSV, value=None)
            return {"status": "skipped", "reason": "no xlsx from extract"}

        result = etl_inbox_enedis.phase_transform(
            new_data_path = Path(xlsx_path_str),
            output_dir    = RAW_DIR_MANUEL,  # _manuel/ : intermediaires dedies canal manuel
            dst_table     = DST_TABLE,
        )

        status = result.get("status", "?")
        if status == "ok":
            print(f"[transform_inbox] {result['lignes_input']} lignes in "
                  f"-> {result['lignes_output']} lignes out "
                  f"| DST traite "
                  f"| -> {Path(str(result['output'])).name}", flush=True)
        else:
            print(f"[transform_inbox] Statut : {status}", flush=True)

        out = result.get("output")
        context["ti"].xcom_push(key=_XCOM_NEW_DATA_CSV,
                                value=str(out) if out else None)
        return result

    except Exception as e:
        print(f"[ERROR] transform_inbox -> {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


def task_load_inbox(**context):
    """
    CANAL B -- Phase 3 LOAD (source="manuel", priorite haute).

    Source "manuel" -> keep="last" : en cas de divergence sur (Date,Time)
    deja presents dans la DB (provenant typiquement d'un precedent run scrap),
    la nouvelle valeur manuelle ECRASE l'ancienne.

    Audit divergence : log WARNING si valeurs differentes sur cles communes.

    Snapshot : archive/Database_Enedis_30_min_YYYYMMDD.csv (rotation, 30 max).
    """
    try:
        import etl_inbox_enedis
        importlib.reload(etl_inbox_enedis)

        csv_path_str = context["ti"].xcom_pull(
            task_ids="transform_inbox", key=_XCOM_NEW_DATA_CSV
        )
        if not csv_path_str:
            print("[load_inbox] Aucun CSV new_data recu (etapes precedentes vides) -- skip",
                  flush=True)
            return {"status": "skipped", "reason": "no csv from transform"}

        result = etl_inbox_enedis.phase_load(
            new_csv_path  = Path(csv_path_str),
            database_path = DATABASE_CSV,
            archive_dir   = DB_ARCHIVE,
            source        = "manuel",      # priorite haute : keep="last"
            keep_versioned = 30,
        )

        if result.get("status") == "ok":
            print(f"[load_inbox] +{result['lignes_ajoutees']} lignes ajoutees "
                  f"| total {result['lignes_apres']} lignes "
                  f"| dernier jour {result['last_date']}", flush=True)
            vp = result.get("versioned")
            if vp:
                print(f"[load_inbox] Copie versionnee : {Path(str(vp)).name}", flush=True)

        return result

    except Exception as e:
        print(f"[ERROR] load_inbox -> {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


def task_pipeline_summary(**context):
    """CANAL B -- Resume ETL de la mise a jour de la database."""
    ti  = context["ti"]
    ext = ti.xcom_pull(task_ids="extract_inbox")   or {}
    trn = ti.xcom_pull(task_ids="transform_inbox") or {}
    ld  = ti.xcom_pull(task_ids="load_inbox")      or {}

    print("=" * 70, flush=True)
    print("CANAL B -- ETL INBOX MANUEL -- RESUME", flush=True)
    print(f"   Execution : {context.get('ds', 'N/A')}", flush=True)
    print("", flush=True)

    # -- Extract ---------------------------------------------------------------
    print("   -- 1. EXTRACT --", flush=True)
    ext_status = ext.get("status", "?")
    if ext_status in ("no_files", "no_data"):
        print(f"   {ext_status} : {ext.get('message', ext.get('reason', ''))}", flush=True)
    elif ext_status == "ok":
        print(f"   XLSX traites  : {ext.get('fichiers', '?')}", flush=True)
        print(f"   Lignes brutes : {ext.get('lignes', '?')}", flush=True)
        rej = ext.get("rejetes", [])
        if rej:
            print(f"   Rejetes       : {rej}", flush=True)
    print("", flush=True)

    # -- Transform -------------------------------------------------------------
    print("   -- 2. TRANSFORM (DST) --", flush=True)
    trn_status = trn.get("status", "?")
    if trn_status == "skipped":
        print(f"   Skipped : {trn.get('reason', '')}", flush=True)
    elif trn_status == "ok":
        print(f"   Lignes input  : {trn.get('lignes_input', '?')}", flush=True)
        print(f"   Lignes output : {trn.get('lignes_output', '?')}", flush=True)
        print(f"   Dernier jour  : {trn.get('last_date', '?')}", flush=True)
    print("", flush=True)

    # -- Load ------------------------------------------------------------------
    print("   -- 3. LOAD --", flush=True)
    ld_status = ld.get("status", "?")
    if ld_status == "skipped":
        print(f"   Skipped : {ld.get('reason', '')}", flush=True)
    elif ld_status == "ok":
        print(f"   Lignes avant  : {ld.get('lignes_avant', '?')}", flush=True)
        print(f"   Lignes apres  : {ld.get('lignes_apres', '?')}", flush=True)
        print(f"   Lignes ajout. : +{ld.get('lignes_ajoutees', '?')}", flush=True)
        print(f"   Dernier jour  : {ld.get('last_date', '?')}", flush=True)
        vp = ld.get("versioned")
        if vp:
            print(f"   Version       : {Path(str(vp)).name}", flush=True)
    print("", flush=True)

    print(f"   INBOX   : {INBOX_DIR}", flush=True)
    print(f"   CURATED : {CURATED_DIR}", flush=True)
    print(f"   DATABASE: {DATABASE_CSV}", flush=True)
    print("=" * 70, flush=True)


# ==============================================================================
# CANAL C -- ETL SCRAPPING ENEDIS (Playwright, automatique quotidien)
# ==============================================================================
#
# Inbox et intermediaires DEDIES, mais la DATABASE est UNIQUE (partagee avec
# le canal manuel) :
#   - inbox    : INBOX_DIR_SCRAP        (.../inbox_enedis_scrap/)
#   - intermed : RAW_DIR_SCRAP          (.../_scrap/new_data_*.{xlsx,csv})
#   - DB       : DATABASE_CSV           (.../Database_Enedis_30_min.csv) UNIQUE
#   - source   : "scrap" -> keep="first" (le manuel ECRASE, le scrap RESPECTE)
#
# Sequencement : t_load (manuel) >> t_scrap_load (scrap) garantit la priorite.
# Execution : AUTOMATIQUE a chaque run du DAG (cron 05:00).

def task_scrap_download(**context):
    """
    CANAL C / 1 -- TELECHARGEMENT.

    Se connecte au compte particulier Enedis via Playwright et telecharge
    le fichier "Courbe de charge" sur la fenetre J-3 -> J-2.
    Le XLSX est depose dans INBOX_DIR_SCRAP, qui est l'inbox DEDIEE de la
    branche scrap (separe de l'inbox manuelle).

    Variables d'environnement requises :
        ENEDIS_IDENTIFIANT
        ENEDIS_PASSWORD
    """
    # 1. Recuperation des identifiants ----------------------------------------
    identifiant = os.environ.get("ENEDIS_IDENTIFIANT", "").strip()
    password    = os.environ.get("ENEDIS_PASSWORD", "").strip()
    if not identifiant or not password:
        raise RuntimeError(
            "Variables ENEDIS_IDENTIFIANT / ENEDIS_PASSWORD introuvables -- "
            "verifier le fichier .env (D:/projet_dataoz/.env) et le bloc "
            "x-airflow-common.environment de docker-compose.yml."
        )

    # 2. Lancement du scrapping -----------------------------------------------
    try:
        from extract import scrapping_enedis
        importlib.reload(scrapping_enedis)

        INBOX_DIR_SCRAP.mkdir(parents=True, exist_ok=True)
        SCRAP_TMP_DIR.mkdir(parents=True, exist_ok=True)

        ref_date       = date.today()
        d_start, d_end = scrapping_enedis.get_target_window(ref_date)

        print(f"[scrap_download] ref_date    : {ref_date}", flush=True)
        print(f"[scrap_download] periode     : {d_start} -> {d_end}", flush=True)
        print(f"[scrap_download] inbox scrap : {INBOX_DIR_SCRAP}", flush=True)
        print(f"[scrap_download] tmp         : {SCRAP_TMP_DIR}", flush=True)

        t0 = time.time()
        fichier = scrapping_enedis.download_courbe_de_charge(
            identifiant = identifiant,
            password    = password,
            tmp_dir     = str(SCRAP_TMP_DIR),
            inbox_dir   = str(INBOX_DIR_SCRAP),
            ref_date    = ref_date,
        )
        duree_ms = int((time.time() - t0) * 1000)

        result = {
            "status":   "ok",
            "file":     str(fichier),
            "d_start":  d_start.isoformat(),
            "d_end":    d_end.isoformat(),
            "duree_ms": duree_ms,
        }
        print(f"[scrap_download] OK  fichier : {fichier}", flush=True)
        print(f"[scrap_download]     duree   : {duree_ms} ms", flush=True)
        return result

    except Exception as e:
        print(f"[ERROR] scrap_download -> {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


def task_scrap_extract(**context):
    """
    CANAL C / 2 -- EXTRACT.

    Equivalent de task_extract_inbox MAIS sur l'inbox DEDIEE scrap :
      - inbox_dir   = INBOX_DIR_SCRAP
      - archive_dir = INBOX_ARCHIVE_SCRAP
      - output_dir  = RAW_DIR_SCRAP   (sub-dossier dedie scrap)
    """
    try:
        import etl_inbox_enedis
        importlib.reload(etl_inbox_enedis)

        INBOX_DIR_SCRAP.mkdir(parents=True, exist_ok=True)
        INBOX_ARCHIVE_SCRAP.mkdir(parents=True, exist_ok=True)
        RAW_DIR_SCRAP.mkdir(parents=True, exist_ok=True)

        result = etl_inbox_enedis.phase_extract(
            inbox_dir   = INBOX_DIR_SCRAP,
            archive_dir = INBOX_ARCHIVE_SCRAP,
            output_dir  = RAW_DIR_SCRAP,
        )

        status = result.get("status", "?")
        if status == "no_files":
            print(f"[scrap_extract] Inbox scrap vide : {result.get('message')}",
                  flush=True)
        elif status == "no_data":
            print("[scrap_extract] XLSX trouves mais aucune donnee lisible",
                  flush=True)
        else:
            print(f"[scrap_extract] {result['fichiers']} fichier(s) "
                  f"| {result['lignes']} lignes "
                  f"| -> {Path(str(result['output'])).name}", flush=True)
            for r in result.get("rejetes", []):
                print(f"[scrap_extract]   KO {r}", flush=True)

        out = result.get("output")
        context["ti"].xcom_push(key=_XCOM_SCRAP_XLSX,
                                value=str(out) if out else None)
        return result

    except Exception as e:
        print(f"[ERROR] scrap_extract -> {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


def task_scrap_transform(**context):
    """
    CANAL C / 3 -- TRANSFORM.

    Equivalent de task_transform_inbox MAIS sur les fichiers issus
    de scrap_extract. Output dans RAW_DIR_SCRAP.
    """
    try:
        import etl_inbox_enedis
        importlib.reload(etl_inbox_enedis)

        xlsx_path_str = context["ti"].xcom_pull(
            task_ids="scrap_extract", key=_XCOM_SCRAP_XLSX
        )
        if not xlsx_path_str:
            print("[scrap_transform] Aucun XLSX scrap recu (inbox vide) -- skip",
                  flush=True)
            context["ti"].xcom_push(key=_XCOM_SCRAP_CSV, value=None)
            return {"status": "skipped", "reason": "no xlsx from scrap_extract"}

        result = etl_inbox_enedis.phase_transform(
            new_data_path = Path(xlsx_path_str),
            output_dir    = RAW_DIR_SCRAP,
            dst_table     = DST_TABLE,
        )

        status = result.get("status", "?")
        if status == "ok":
            print(f"[scrap_transform] {result['lignes_input']} lignes in "
                  f"-> {result['lignes_output']} lignes out "
                  f"| DST traite "
                  f"| -> {Path(str(result['output'])).name}", flush=True)
        else:
            print(f"[scrap_transform] Statut : {status}", flush=True)

        out = result.get("output")
        context["ti"].xcom_push(key=_XCOM_SCRAP_CSV,
                                value=str(out) if out else None)
        return result

    except Exception as e:
        print(f"[ERROR] scrap_transform -> {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


def task_scrap_load(**context):
    """
    CANAL C -- Phase 4 LOAD (source="scrap", priorite basse).

    Ecrit dans la MEME DATABASE que le canal manuel (Database_Enedis_30_min.csv).
    Source "scrap" -> keep="first" : en cas de divergence sur (Date,Time)
    deja presents dans la DB (typiquement venant d'un load_inbox manuel
    qui vient de passer juste avant), la valeur en place est CONSERVEE.
    Le scrap RESPECTE donc systematiquement le manuel.

    Audit divergence : log WARNING si valeurs differentes sur cles communes.

    Snapshot : archive/Database_Enedis_30_min_YYYYMMDD.csv (rotation, 30 max).
    Cohabitation OK avec load_inbox grace a l'ordre sequentiel impose dans
    le graphe : t_load >> t_scrap_load (trigger_rule="all_done").
    """
    try:
        import etl_inbox_enedis
        importlib.reload(etl_inbox_enedis)

        csv_path_str = context["ti"].xcom_pull(
            task_ids="scrap_transform", key=_XCOM_SCRAP_CSV
        )
        if not csv_path_str:
            print("[scrap_load] Aucun CSV scrap recu (etapes precedentes vides) -- skip",
                  flush=True)
            return {"status": "skipped", "reason": "no csv from scrap_transform"}

        DB_ARCHIVE.mkdir(parents=True, exist_ok=True)

        result = etl_inbox_enedis.phase_load(
            new_csv_path  = Path(csv_path_str),
            database_path = DATABASE_CSV,        # MEME DB que le canal manuel
            archive_dir   = DB_ARCHIVE,          # MEME archive
            source        = "scrap",              # priorite basse : keep="first"
            keep_versioned = 30,
        )

        if result.get("status") == "ok":
            print(f"[scrap_load] +{result['lignes_ajoutees']} lignes ajoutees "
                  f"| total {result['lignes_apres']} lignes "
                  f"| dernier jour {result['last_date']}", flush=True)
            vp = result.get("versioned")
            if vp:
                print(f"[scrap_load] Copie versionnee : {Path(str(vp)).name}",
                      flush=True)

        return result

    except Exception as e:
        print(f"[ERROR] scrap_load -> {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


def task_agregation_journalier(**context):
    """
    CANAL C / 4b -- Agrégation 30 min → journalier.

    Lit Database_Enedis_30_min.csv (mis à jour par scrap_load) et
    recalcule database_enedis_journalier.csv par sommation des tranches.
    Seuls les jours ayant 48 tranches complètes sont retenus.
    """
    try:
        import agregation_journalier_enedis
        importlib.reload(agregation_journalier_enedis)

        result = agregation_journalier_enedis.run(
            database_30min_path      = DATABASE_CSV,
            database_journalier_path = CURATED_DIR / "database_enedis_journalier.csv",
            archive_dir              = DB_ARCHIVE,
            dst_table                = DST_TABLE,
        )

        if result.get("status") == "ok":
            print(
                f"[agregation_journalier] {result['jours_complets']} jours écrits "
                f"({result['first_date']} → {result['last_date']}) "
                f"| {result['jours_partiels']} jour(s) partiel(s) exclus "
                f"| {result['dst_anomalies']} anomalie(s) DST",
                flush=True,
            )
        else:
            print(f"[agregation_journalier] Statut : {result.get('status')} "
                  f"(aucune donnée journalière complète)", flush=True)

        return result

    except Exception as e:
        print(f"[ERROR] agregation_journalier -> {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


def task_agregation_horaire(**context):
    """
    CANAL C / 4c -- Agrégation 30 min → horaire.

    Lit Database_Enedis_30_min.csv et produit database_enedis_horaire.csv
    en sommant les 2 tranches de 30 min par heure.
    Seules les heures ayant 2 tranches complètes sont retenues.
    """
    try:
        import agregation_horaire_enedis
        importlib.reload(agregation_horaire_enedis)

        result = agregation_horaire_enedis.run(
            database_30min_path   = DATABASE_CSV,
            database_horaire_path = CURATED_DIR / "database_enedis_horaire.csv",
            archive_dir           = DB_ARCHIVE,
        )

        if result.get("status") == "ok":
            print(
                f"[agregation_horaire] {result['heures_completes']} heures écrites "
                f"({result['first_date']} → {result['last_date']}) "
                f"| {result['heures_partielles']} heure(s) partielle(s) exclue(s)",
                flush=True,
            )
        else:
            print(f"[agregation_horaire] Statut : {result.get('status')} "
                  f"(aucune heure complète disponible)", flush=True)

        return result

    except Exception as e:
        print(f"[ERROR] agregation_horaire -> {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


def task_scrap_summary(**context):
    """CANAL C / 5 -- Resume ETL scrap."""
    ti  = context["ti"]
    dl  = ti.xcom_pull(task_ids="scrap_download")  or {}
    ext = ti.xcom_pull(task_ids="scrap_extract")   or {}
    trn = ti.xcom_pull(task_ids="scrap_transform") or {}
    ld  = ti.xcom_pull(task_ids="scrap_load")      or {}

    print("=" * 70, flush=True)
    print("CANAL C -- ETL SCRAPPING -- RESUME", flush=True)
    print(f"   Execution : {context.get('ds', 'N/A')}", flush=True)
    print("", flush=True)

    # -- Download --------------------------------------------------------------
    print("   -- 1. DOWNLOAD (Playwright) --", flush=True)
    dl_status = dl.get("status", "?")
    if dl_status == "ok":
        print(f"   Fichier   : {Path(str(dl.get('file', ''))).name}", flush=True)
        print(f"   Periode   : {dl.get('d_start')} -> {dl.get('d_end')}",
              flush=True)
        print(f"   Duree     : {dl.get('duree_ms')} ms", flush=True)
    else:
        print(f"   Statut    : {dl_status}", flush=True)
    print("", flush=True)

    # -- Extract ---------------------------------------------------------------
    print("   -- 2. EXTRACT --", flush=True)
    ext_status = ext.get("status", "?")
    if ext_status in ("no_files", "no_data"):
        print(f"   {ext_status} : {ext.get('message', ext.get('reason', ''))}",
              flush=True)
    elif ext_status == "ok":
        print(f"   XLSX traites  : {ext.get('fichiers', '?')}", flush=True)
        print(f"   Lignes brutes : {ext.get('lignes', '?')}", flush=True)
    print("", flush=True)

    # -- Transform -------------------------------------------------------------
    print("   -- 3. TRANSFORM (DST) --", flush=True)
    trn_status = trn.get("status", "?")
    if trn_status == "skipped":
        print(f"   Skipped : {trn.get('reason', '')}", flush=True)
    elif trn_status == "ok":
        print(f"   Lignes input  : {trn.get('lignes_input', '?')}", flush=True)
        print(f"   Lignes output : {trn.get('lignes_output', '?')}", flush=True)
        print(f"   Dernier jour  : {trn.get('last_date', '?')}", flush=True)
    print("", flush=True)

    # -- Load ------------------------------------------------------------------
    print("   -- 4. LOAD --", flush=True)
    ld_status = ld.get("status", "?")
    if ld_status == "skipped":
        print(f"   Skipped : {ld.get('reason', '')}", flush=True)
    elif ld_status == "ok":
        print(f"   Lignes avant  : {ld.get('lignes_avant', '?')}", flush=True)
        print(f"   Lignes apres  : {ld.get('lignes_apres', '?')}", flush=True)
        print(f"   Lignes ajout. : +{ld.get('lignes_ajoutees', '?')}",
              flush=True)
        print(f"   Dernier jour  : {ld.get('last_date', '?')}", flush=True)
        vp = ld.get("versioned")
        if vp:
            print(f"   Version       : {Path(str(vp)).name}", flush=True)
    print("", flush=True)

    print(f"   INBOX SCRAP : {INBOX_DIR_SCRAP}", flush=True)
    print(f"   INTERMED.   : {RAW_DIR_SCRAP}", flush=True)
    print(f"   DATABASE    : {DATABASE_CSV}    (UNIQUE - partagee avec canal manuel)",
          flush=True)
    print("=" * 70, flush=True)


# ==============================================================================
# DEFINITION DU DAG
# ==============================================================================

with DAG(
    dag_id="dag_conso_elec_enedis",
    description=(
        "Conso electrique Enedis -- "
        "Canal B (manuel, ad-hoc) + Canal C (scrap, quotidien) "
        "alimentent une DB unique. Canal A (API sandbox) en pause."
    ),
    default_args=default_args,
    start_date=datetime(2026, 4, 20),
    schedule_interval="10 1 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["enedis", "conso_elec", "dataoz"],
) as dag:

    # ==========================================================================
    # CANAL A -- API DATA HUB  [EN PAUSE]
    # ==========================================================================
    # Les operateurs Python ci-dessous sont COMMENTES pour neutraliser le canal
    # API dans le graphe d'execution (decision : 2026-04-27).
    # Les CALLABLES (task_init_schema, task_ensure_prm, task_fetch_*, task_api_verify)
    # restent definis plus haut dans le module pour reactivation ulterieure.
    #
    # Pour reactiver : decommentez les operateurs et la ligne de dependance
    # tout en bas (`# t_init >> t_prm >> [...] >> t_api_verify`).
    # ==========================================================================
    #
    # t_init = PythonOperator(
    #     task_id="init_schema",
    #     python_callable=task_init_schema,
    #     execution_timeout=timedelta(minutes=2),
    # )
    #
    # t_prm = PythonOperator(
    #     task_id="ensure_prm",
    #     python_callable=task_ensure_prm,
    #     execution_timeout=timedelta(minutes=2),
    # )
    #
    # t_clc = PythonOperator(
    #     task_id="fetch_load_curve",
    #     python_callable=task_fetch_load_curve,
    #     execution_timeout=timedelta(minutes=5),
    # )
    #
    # t_daily = PythonOperator(
    #     task_id="fetch_daily_consumption",
    #     python_callable=task_fetch_daily_consumption,
    #     execution_timeout=timedelta(minutes=5),
    # )
    #
    # t_pmax = PythonOperator(
    #     task_id="fetch_daily_max_power",
    #     python_callable=task_fetch_daily_max_power,
    #     execution_timeout=timedelta(minutes=5),
    # )
    #
    # t_api_verify = PythonOperator(
    #     task_id="api_verify",
    #     python_callable=task_api_verify,
    #     trigger_rule="all_done",
    #     execution_timeout=timedelta(minutes=2),
    # )

    # -- CANAL B : ETL inbox MANUEL (file-only) -------------------------------
    t_extract = PythonOperator(
        task_id="extract_inbox",
        python_callable=task_extract_inbox,
        execution_timeout=timedelta(minutes=10),
    )

    t_transform = PythonOperator(
        task_id="transform_inbox",
        python_callable=task_transform_inbox,
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=5),
    )

    t_load = PythonOperator(
        task_id="load_inbox",
        python_callable=task_load_inbox,
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=5),
    )

    t_summary = PythonOperator(
        task_id="pipeline_summary",
        python_callable=task_pipeline_summary,
        trigger_rule="all_done",
    )

    # -- CANAL C : ETL scrapping Playwright (quotidien) -----------------------
    t_scrap_dl = PythonOperator(
        task_id="scrap_download",
        python_callable=task_scrap_download,
        execution_timeout=timedelta(minutes=15),
    )

    t_scrap_ext = PythonOperator(
        task_id="scrap_extract",
        python_callable=task_scrap_extract,
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=5),
    )

    t_scrap_trn = PythonOperator(
        task_id="scrap_transform",
        python_callable=task_scrap_transform,
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=5),
    )

    t_scrap_load = PythonOperator(
        task_id="scrap_load",
        python_callable=task_scrap_load,
        trigger_rule="all_done",   # s'execute meme si load_inbox ou scrap_transform a echoue
        execution_timeout=timedelta(minutes=5),
    )

    t_agr_jour = PythonOperator(
        task_id="agregation_journalier",
        python_callable=task_agregation_journalier,
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=5),
    )

    t_agr_hor = PythonOperator(
        task_id="agregation_horaire",
        python_callable=task_agregation_horaire,
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=5),
    )

    t_scrap_summary = PythonOperator(
        task_id="scrap_summary",
        python_callable=task_scrap_summary,
        trigger_rule="all_done",
    )

    # ── Chaine Canal B ────────────────────────────────────────────────────────
    # extract_inbox >> transform_inbox >> load_inbox >> pipeline_summary
    t_extract >> t_transform >> t_load >> t_summary

    # ── Chaine Canal C (telechargement, en parallele avec Canal B) ────────────
    # scrap_download >> scrap_extract >> scrap_transform
    t_scrap_dl >> t_scrap_ext >> t_scrap_trn

    # ── Point de synchronisation : scrap_load attend load_inbox ET scrap_transform
    # Garantit que le manuel passe avant le scrap (priorite Canal B sur Canal C).
    [t_load, t_scrap_trn] >> t_scrap_load

    # ── Post-load : agregations en parallele, puis resume Canal C ─────────────
    t_scrap_load >> [t_agr_jour, t_agr_hor] >> t_scrap_summary