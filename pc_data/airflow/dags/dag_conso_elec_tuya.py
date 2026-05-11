# -*- coding: utf-8 -*-
"""
dag_conso_elec_tuya.py
=======================
DAG Airflow — Consommation électrique Tuya / SmartLife

Récupère l'historique complet de consommation électrique de tous les
appareils SmartLife (API Beta Tuya, statistique `add_ele` en kWh) puis
exporte :

  ┌ data/raw/conso_elec/Tuya/   (exports bruts — un fichier par appareil)
  │     {nom}_{id8}_mois.csv     → consommation mensuelle depuis l'origine
  │     {nom}_{id8}_jours.csv    → consommation journalière depuis l'origine
  │     {nom}_{id8}_heures.csv   → consommation horaire (7 derniers jours)
  │     {nom}_{id8}_15min.csv    → consommation 15 min (7 derniers jours)
  │
  └ data/curated/conso_elec/tuya/   (synthèses pivots tous appareils)
        _SYNTHESE_MENSUELLE.csv    → tableau croisé mois × appareil + TOTAL
        _SYNTHESE_JOURNALIERE.csv  → tableau croisé jour × appareil + TOTAL
        _SYNTHESE_HORAIRE.csv      → tableau croisé heure × appareil (30 derniers j)
        _SYNTHESE_15MIN.csv        → tableau croisé 15min × appareil (7 derniers j)

Architecture :
    · mois + jours   → CSV uniquement (curated pivots)
    · heures + 15min → CSV (RAW) + Postgres (schéma tuya)
                       + synthèses pivots CSV tirées de la DB (curated)

Chaîne :

                     ┌─► extract_monthly ──► synthese_mensuelle ──────────┐
                     │                                                    │
  init_schema ─► list_devices ─┼─► extract_daily ─► synthese_journaliere ─┤
                     │                                                    │
                     ├─► extract_hourly   ─┐                               ├─► pipeline_summary
                     │                      ├─► load_postgres_hf ─┬─► synthese_horaire ─┤
                     └─► extract_quarters ─┘                     └─► synthese_15min   ──┘

Planification : tous les jours à 02:00 (heure de Paris)

Variables d'environnement attendues (docker-compose.yml) :
    TUYA_API_ID            Identifiant API Tuya
    TUYA_API_SECRET        Secret API Tuya
    TUYA_API_REGION        Région API (eu par défaut)
    TUYA_HISTORIQUE_DEBUT  Date de début de l'historique (YYYY-MM-DD)
    TUYA_DOSSIER_EXPORT    Dossier RAW des exports
    CONSO_ELEC_DB_URL      URL Postgres (granularités fines uniquement)
"""

import importlib
import os
import sys
import traceback
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


# ── PYTHONPATH : scripts Tuya ────────────────────────────────────────────────
_TUYA_ROOT = Path("/opt/airflow/scripts/conso_elec/tuya")
for _sub in ["", "extract", "transform", "load"]:
    _p = str(_TUYA_ROOT / _sub) if _sub else str(_TUYA_ROOT)
    if _p not in sys.path:
        sys.path.insert(0, _p)


# ── Chemins (dans le container Docker) ───────────────────────────────────────
RAW_DIR     = Path(os.environ.get(
    "TUYA_DOSSIER_EXPORT",
    "/opt/airflow/data/raw/conso_elec/Tuya",
))
CURATED_DIR = Path("/opt/airflow/data/curated/conso_elec/tuya")
HIST_DIR    = RAW_DIR / "_historique"       # exports Tuya antérieurs (rar)
_SQL_SCHEMA = _TUYA_ROOT / "sql" / "01_schema_tuya.sql"


# ── Paramètres du DAG ────────────────────────────────────────────────────────
def _annee_debut() -> int:
    """Extrait l'année de TUYA_HISTORIQUE_DEBUT (YYYY-MM-DD)."""
    val = os.environ.get("TUYA_HISTORIQUE_DEBUT", "2020-01-01")
    try:
        return int(val.split("-")[0])
    except (ValueError, IndexError):
        return 2020


default_args = {
    "owner":            "dataoz",
    "retries":          2,
    "retry_delay":      timedelta(minutes=10),
    "email_on_failure": False,
}


# ─────────────────────────────────────────────────────────────────────────────
# TÂCHES
# ─────────────────────────────────────────────────────────────────────────────

def task_init_schema(**context):
    """Crée (si besoin) le schéma tuya + tables granularités fines dans Postgres."""
    try:
        import db
        importlib.reload(db)

        if not _SQL_SCHEMA.exists():
            raise FileNotFoundError(f"Fichier SQL introuvable : {_SQL_SCHEMA}")
        db.execute_sql_file(_SQL_SCHEMA)
        print(f"[init_schema] ✓ schéma tuya initialisé via {_SQL_SCHEMA}",
              flush=True)
        return {"sql_file": str(_SQL_SCHEMA)}
    except Exception as e:
        print(f"[ERROR] init_schema -> {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


def task_list_devices(**context):
    """Authentifie et liste tous les appareils SmartLife du compte."""
    try:
        import extract_tuya
        importlib.reload(extract_tuya)

        # Import de l'erreur d'abonnement expiré depuis le client Tuya
        from tuya_client import TuyaSubscriptionExpiredError  # noqa: PLC0415

        RAW_DIR.mkdir(parents=True, exist_ok=True)
        CURATED_DIR.mkdir(parents=True, exist_ok=True)

        try:
            appareils = extract_tuya.lister_appareils()
        except TuyaSubscriptionExpiredError as sub_err:
            # ── Abonnement Tuya IoT Core expiré (code 28841002) ────────────
            print("", flush=True)
            print("🚨 " + "=" * 65, flush=True)
            print("🚨  ABONNEMENT TUYA EXPIRÉ — CODE 28841002", flush=True)
            print("🚨 " + "=" * 65, flush=True)
            print("🚨  Le pipeline est BLOQUÉ. Aucune donnée ne sera collectée.", flush=True)
            print("🚨", flush=True)
            print("🚨  RENOUVELLEMENT (gratuit, 5 minutes) :", flush=True)
            print("🚨    1. https://iot.tuya.com → connectez-vous", flush=True)
            print("🚨    2. Cloud → Mes abonnements", flush=True)
            print("🚨    3. Cliquer sur « IoT Core » → Renouveler", flush=True)
            print("🚨    4. Choisir Trial Edition (gratuit, 6 mois)", flush=True)
            print("🚨    5. Relancer manuellement le DAG après renouvellement", flush=True)
            print("🚨 " + "=" * 65, flush=True)
            print("", flush=True)
            raise RuntimeError(
                "🚨 Abonnement Tuya IoT Core expiré (code 28841002). "
                "Renouveler sur https://iot.tuya.com → Cloud → Mes abonnements "
                "→ IoT Core (Trial Edition, gratuit, 6 mois), "
                "puis relancer le DAG."
            ) from sub_err

        print(f"[list_devices] {len(appareils)} appareil(s) trouvé(s)",
              flush=True)
        for i, a in enumerate(appareils, 1):
            print(f"   {i:>2}. {a.get('name','?'):<30}  "
                  f"{a.get('id','?'):<25}  {a.get('model','-')}",
                  flush=True)
        context["ti"].xcom_push(key="appareils", value=appareils)
        return {"nb_appareils": len(appareils)}
    except Exception as e:
        print(f"[ERROR] list_devices -> {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


def _appareils_xcom(context) -> list[dict]:
    return context["ti"].xcom_pull(task_ids="list_devices",
                                    key="appareils") or []


def task_extract_monthly(**context):
    """Consommation mensuelle (depuis TUYA_HISTORIQUE_DEBUT)."""
    try:
        import extract_tuya
        importlib.reload(extract_tuya)

        result = extract_tuya.extraire_mois(
            dossier_raw=str(RAW_DIR),
            annee_debut=_annee_debut(),
            appareils=_appareils_xcom(context),
        )
        # On ne pousse les lignes détaillées qu'à cette étape (pour la synthèse)
        context["ti"].xcom_push(key="lignes_mois", value=result.get("lignes", []))
        print(f"[extract_monthly] {result['nb_appareils']} appareil(s)"
              f"  |  {result['total_kwh']} kWh total", flush=True)
        # On ne met pas les lignes dans le return → XCom principal plus léger
        return {k: v for k, v in result.items() if k != "lignes"}
    except Exception as e:
        print(f"[ERROR] extract_monthly -> {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


def task_extract_daily(**context):
    """Consommation journalière (depuis TUYA_HISTORIQUE_DEBUT)."""
    try:
        import extract_tuya
        importlib.reload(extract_tuya)

        result = extract_tuya.extraire_jours(
            dossier_raw=str(RAW_DIR),
            annee_debut=_annee_debut(),
            appareils=_appareils_xcom(context),
        )
        context["ti"].xcom_push(key="lignes_jours", value=result.get("lignes", []))
        print(f"[extract_daily] {result['nb_appareils']} appareil(s)"
              f"  |  {result['total_kwh']} kWh total", flush=True)
        return {k: v for k, v in result.items() if k != "lignes"}
    except Exception as e:
        print(f"[ERROR] extract_daily -> {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


def task_extract_hourly(**context):
    """Consommation horaire (7 derniers jours, heures > 0)."""
    try:
        import extract_tuya
        importlib.reload(extract_tuya)

        result = extract_tuya.extraire_heures(
            dossier_raw=str(RAW_DIR),
            jours=7,
            appareils=_appareils_xcom(context),
        )
        print(f"[extract_hourly] {result['nb_appareils']} appareil(s)"
              f"  |  {result['total_kwh']} kWh total", flush=True)
        return result
    except Exception as e:
        print(f"[ERROR] extract_hourly -> {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


def task_extract_quarters(**context):
    """Consommation par 15 min (7 derniers jours)."""
    try:
        import extract_tuya
        importlib.reload(extract_tuya)

        result = extract_tuya.extraire_15min(
            dossier_raw=str(RAW_DIR),
            jours=7,
            appareils=_appareils_xcom(context),
        )
        print(f"[extract_quarters] {result['nb_appareils']} appareil(s)"
              f"  |  {result['total_kwh']} kWh total", flush=True)
        return result
    except Exception as e:
        print(f"[ERROR] extract_quarters -> {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


def task_synthese_mensuelle(**context):
    """Synthèse pivot mois × appareil → _SYNTHESE_MENSUELLE.csv."""
    try:
        import synthese_tuya
        importlib.reload(synthese_tuya)

        lignes = context["ti"].xcom_pull(task_ids="extract_monthly",
                                           key="lignes_mois") or []
        result = synthese_tuya.synthese_mensuelle(
            dossier_curated=str(CURATED_DIR),
            lignes_mois=lignes,
        )
        print(f"[synthese_mensuelle] {result}", flush=True)
        return result
    except Exception as e:
        print(f"[ERROR] synthese_mensuelle -> {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


def task_synthese_journaliere(**context):
    """Synthèse pivot jour × appareil → _SYNTHESE_JOURNALIERE.csv."""
    try:
        import synthese_tuya
        importlib.reload(synthese_tuya)

        lignes = context["ti"].xcom_pull(task_ids="extract_daily",
                                           key="lignes_jours") or []
        result = synthese_tuya.synthese_journaliere(
            dossier_curated=str(CURATED_DIR),
            lignes_jours=lignes,
        )
        print(f"[synthese_journaliere] {result}", flush=True)
        return result
    except Exception as e:
        print(f"[ERROR] synthese_journaliere -> {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


def task_load_postgres_hf(**context):
    """
    Charge dans Postgres les CSV de granularité FINE :
        · heures (_heures.csv)
        · 15 min (_15min.csv)

    Scanne à la fois RAW_DIR (exports courants, 7j glissants) et
    HIST_DIR (exports historiques importés manuellement — fenêtres
    7j distinctes). Les mois/jours restent en CSV uniquement.
    """
    try:
        import load_tuya
        importlib.reload(load_tuya)

        # 1) Mise à jour dim_appareil
        appareils = _appareils_xcom(context)
        nb_appareils = load_tuya.upsert_appareils(appareils)

        # 2) Chargement des CSV fins (RAW courants + _historique)
        dossiers = [RAW_DIR, HIST_DIR]
        stats = load_tuya.scan_and_load_hf(dossiers)

        print(f"[load_postgres_hf] dim_appareil : {nb_appareils} upsert(s)",
              flush=True)
        print(f"[load_postgres_hf] fichiers heure : {stats['fichiers_heure']} "
              f"→ {stats['total_heure']} lignes", flush=True)
        print(f"[load_postgres_hf] fichiers 15min : {stats['fichiers_15min']} "
              f"→ {stats['total_15min']} lignes", flush=True)
        return {
            "appareils_upsert":  nb_appareils,
            "fichiers_heure":    stats["fichiers_heure"],
            "fichiers_15min":    stats["fichiers_15min"],
            "total_heure":       stats["total_heure"],
            "total_15min":       stats["total_15min"],
            "total_upsert":      stats["total_upsert"],
        }
    except Exception as e:
        print(f"[ERROR] load_postgres_hf -> {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


def task_synthese_horaire(**context):
    """
    Synthèse pivot heure × appareil depuis Postgres
    → curated/conso_elec/tuya/_SYNTHESE_HORAIRE.csv
    """
    try:
        import synthese_tuya
        importlib.reload(synthese_tuya)

        # Fenêtre par défaut : 30 jours (paramétrable via env)
        jours = int(os.environ.get("TUYA_SYNTHESE_HORAIRE_JOURS", "30"))
        result = synthese_tuya.synthese_horaire_db(
            dossier_curated=str(CURATED_DIR),
            jours=jours,
        )
        print(f"[synthese_horaire] {result}", flush=True)
        return result
    except Exception as e:
        print(f"[ERROR] synthese_horaire -> {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


def task_synthese_15min(**context):
    """
    Synthèse pivot 15min × appareil depuis Postgres
    → curated/conso_elec/tuya/_SYNTHESE_15MIN.csv
    """
    try:
        import synthese_tuya
        importlib.reload(synthese_tuya)

        # Fenêtre par défaut : 7 jours (le 15min est volumineux)
        jours = int(os.environ.get("TUYA_SYNTHESE_15MIN_JOURS", "7"))
        result = synthese_tuya.synthese_15min_db(
            dossier_curated=str(CURATED_DIR),
            jours=jours,
        )
        print(f"[synthese_15min] {result}", flush=True)
        return result
    except Exception as e:
        print(f"[ERROR] synthese_15min -> {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


def task_test_sql_last_day(**context):
    """
    Test d'intégration DB : requête SQL combinée heure + 15min sur la
    dernière journée en base → curated/conso_elec/tuya/last_day_sql_test.csv
    """
    try:
        import synthese_tuya
        importlib.reload(synthese_tuya)

        result = synthese_tuya.test_sql_last_day(
            dossier_curated=str(CURATED_DIR),
        )
        print(f"[test_sql_last_day] {result}", flush=True)
        return result
    except Exception as e:
        print(f"[ERROR] test_sql_last_day -> {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise


def task_pipeline_summary(**context):
    """Résumé global du pipeline."""
    ti      = context["ti"]
    dev     = ti.xcom_pull(task_ids="list_devices")        or {}
    mois    = ti.xcom_pull(task_ids="extract_monthly")     or {}
    jours   = ti.xcom_pull(task_ids="extract_daily")       or {}
    heures  = ti.xcom_pull(task_ids="extract_hourly")      or {}
    quarts  = ti.xcom_pull(task_ids="extract_quarters")    or {}
    syn_m   = ti.xcom_pull(task_ids="synthese_mensuelle")  or {}
    syn_j   = ti.xcom_pull(task_ids="synthese_journaliere") or {}
    syn_h   = ti.xcom_pull(task_ids="synthese_horaire")    or {}
    syn_q   = ti.xcom_pull(task_ids="synthese_15min")      or {}
    sql_t   = ti.xcom_pull(task_ids="test_sql_last_day")   or {}
    load_pg = ti.xcom_pull(task_ids="load_postgres_hf")    or {}

    print("=" * 70, flush=True)
    print("✅ PIPELINE CONSO ÉLEC TUYA — RÉSUMÉ", flush=True)
    print(f"   Exécution          : {context.get('ds', 'N/A')}", flush=True)
    print(f"   Historique début   : {os.environ.get('TUYA_HISTORIQUE_DEBUT', '2020-01-01')}",
          flush=True)
    print(f"   Région API         : {os.environ.get('TUYA_API_REGION', 'eu')}", flush=True)
    print("", flush=True)
    print(f"   Appareils détectés : {dev.get('nb_appareils', '?')}", flush=True)
    print("", flush=True)
    print("   ══ EXPORTS RAW ══", flush=True)
    print(f"   [Mois]   {mois.get('nb_appareils','?')} appareils — "
          f"{mois.get('total_kwh','?')} kWh", flush=True)
    print(f"   [Jours]  {jours.get('nb_appareils','?')} appareils — "
          f"{jours.get('total_kwh','?')} kWh", flush=True)
    print(f"   [Heures] {heures.get('nb_appareils','?')} appareils — "
          f"{heures.get('total_kwh','?')} kWh (7j)", flush=True)
    print(f"   [15min]  {quarts.get('nb_appareils','?')} appareils — "
          f"{quarts.get('total_kwh','?')} kWh (7j)", flush=True)
    print("", flush=True)
    print("   ══ SYNTHÈSES CURATED ══", flush=True)
    print(f"   [Mois]   {syn_m.get('fichier', '—')}"
          f"  ({syn_m.get('periodes','?')} périodes)", flush=True)
    print(f"   [Jour]   {syn_j.get('fichier', '—')}"
          f"  ({syn_j.get('periodes','?')} périodes)", flush=True)
    print(f"   [Heure]  {syn_h.get('fichier', '—')}"
          f"  ({syn_h.get('periodes','?')} périodes, "
          f"{syn_h.get('jours','?')}j)", flush=True)
    print(f"   [15min]  {syn_q.get('fichier', '—')}"
          f"  ({syn_q.get('periodes','?')} périodes, "
          f"{syn_q.get('jours','?')}j)", flush=True)
    print("", flush=True)
    print("   ══ POSTGRES — tuya.* (heures + 15min) ══", flush=True)
    print(f"   dim_appareil   : {load_pg.get('appareils_upsert','?')} upsert(s)",
          flush=True)
    print(f"   f_conso_heure  : {load_pg.get('fichiers_heure','?')} fichier(s) "
          f"→ {load_pg.get('total_heure','?')} ligne(s)", flush=True)
    print(f"   f_conso_15min  : {load_pg.get('fichiers_15min','?')} fichier(s) "
          f"→ {load_pg.get('total_15min','?')} ligne(s)", flush=True)
    print("", flush=True)
    print("   ══ TEST SQL (preuve d'intégration DB) ══", flush=True)
    print(f"   last_day_sql_test.csv :", flush=True)
    print(f"      heure({sql_t.get('jour_heure','—')}) "
          f"→ {sql_t.get('nb_heure','?')} lignes, "
          f"{sql_t.get('total_kwh','?')} kWh", flush=True)
    print(f"      15min({sql_t.get('jour_15min','—')}) "
          f"→ {sql_t.get('nb_15min','?')} lignes", flush=True)
    print("", flush=True)
    print(f"   📁 RAW     : {RAW_DIR}",     flush=True)
    print(f"   📁 HIST    : {HIST_DIR}",    flush=True)
    print(f"   📁 CURATED : {CURATED_DIR}", flush=True)
    print("=" * 70, flush=True)


# ═════════════════════════════════════════════════════════════════════════════
# DÉFINITION DU DAG
# ═════════════════════════════════════════════════════════════════════════════

with DAG(
    dag_id="dag_conso_elec_tuya",
    description=(
        "Conso électrique Tuya / SmartLife : "
        "exports mois/jours/heures/15min + synthèses pivots tous appareils"
    ),
    default_args=default_args,
    start_date=datetime(2026, 4, 15),
    schedule_interval="0 2 * * *",     # tous les jours à 02:00
    catchup=False,
    max_active_runs=1,
    tags=["tuya", "smartlife", "conso_elec", "dataoz"],
) as dag:

    # ── 0a. Initialisation du schéma Postgres (idempotent) ──────────────────
    t_init = PythonOperator(
        task_id="init_schema",
        python_callable=task_init_schema,
        execution_timeout=timedelta(minutes=2),
    )

    # ── 0b. Liste des appareils (source commune des étapes suivantes) ───────
    t_list = PythonOperator(
        task_id="list_devices",
        python_callable=task_list_devices,
        execution_timeout=timedelta(minutes=5),
    )

    # ── 1. Extractions (en parallèle) ───────────────────────────────────────
    t_mois = PythonOperator(
        task_id="extract_monthly",
        python_callable=task_extract_monthly,
        execution_timeout=timedelta(minutes=30),
    )

    t_jours = PythonOperator(
        task_id="extract_daily",
        python_callable=task_extract_daily,
        execution_timeout=timedelta(hours=1),
    )

    t_heures = PythonOperator(
        task_id="extract_hourly",
        python_callable=task_extract_hourly,
        execution_timeout=timedelta(minutes=15),
    )

    t_quarts = PythonOperator(
        task_id="extract_quarters",
        python_callable=task_extract_quarters,
        execution_timeout=timedelta(minutes=15),
    )

    # ── 2. Synthèses ────────────────────────────────────────────────────────
    t_syn_m = PythonOperator(
        task_id="synthese_mensuelle",
        python_callable=task_synthese_mensuelle,
        execution_timeout=timedelta(minutes=5),
    )

    t_syn_j = PythonOperator(
        task_id="synthese_journaliere",
        python_callable=task_synthese_journaliere,
        execution_timeout=timedelta(minutes=5),
    )

    # ── 3. Chargement Postgres (heures + 15min uniquement) ──────────────────
    t_load = PythonOperator(
        task_id="load_postgres_hf",
        python_callable=task_load_postgres_hf,
        execution_timeout=timedelta(minutes=15),
    )

    # ── 4. Synthèses depuis Postgres (heures + 15min) ───────────────────────
    t_syn_h = PythonOperator(
        task_id="synthese_horaire",
        python_callable=task_synthese_horaire,
        execution_timeout=timedelta(minutes=5),
    )

    t_syn_q = PythonOperator(
        task_id="synthese_15min",
        python_callable=task_synthese_15min,
        execution_timeout=timedelta(minutes=5),
    )

    # ── 4b. Test SQL : preuve d'intégration DB (last_day_sql_test.csv) ──────
    t_sql_test = PythonOperator(
        task_id="test_sql_last_day",
        python_callable=task_test_sql_last_day,
        execution_timeout=timedelta(minutes=2),
    )

    # ── 5. Résumé final ─────────────────────────────────────────────────────
    t_summary = PythonOperator(
        task_id="pipeline_summary",
        python_callable=task_pipeline_summary,
        trigger_rule="all_done",
    )

    # ── Dépendances ─────────────────────────────────────────────────────────
    # Init du schéma → puis liste appareils → puis extractions en parallèle
    t_init >> t_list >> [t_mois, t_jours, t_heures, t_quarts]
    # Synthèses CSV (mois / jours) à partir des XCom
    t_mois  >> t_syn_m
    t_jours >> t_syn_j
    # Chargement DB (heures + 15min) une fois les extractions fines terminées
    [t_heures, t_quarts] >> t_load
    # Synthèses horaire / 15min + test SQL à partir de la DB (après le load)
    t_load >> [t_syn_h, t_syn_q, t_sql_test]
    # Résumé final après toutes les synthèses + test DB