# -*- coding: utf-8 -*-
"""
dag_check_pipeline.py
======================
DAG Airflow — Vérification intégrale de la chaîne DataOZ

Vérifie les 6 étapes de la chaîne complète :
  1. DAGs de collecte (états des derniers runs Airflow)
  2. Fraîcheur des CSV curated locaux
  3. Fichiers présents dans le bucket OCI
  4. Jobs Oracle SUCCEEDED + row counts + fraîcheur des données
  5. Streamlit accessible et requêtable

Déclenchement : manuel (ou après dag_oracle_load via ExternalTaskSensor)
Planification  : aucune (on_demand) — déclencher manuellement après un run complet

Résumé final   : task `pipeline_summary` toujours exécutée (trigger_rule=all_done)
"""

import os
import time
import smtplib
import ssl
import requests
import oracledb
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import DagRun
from airflow.utils.state import DagRunState
from airflow.utils.email import send_email

ALERT_EMAIL   = "licorne2lc@msn.com"
SMTP_HOST     = os.getenv("AIRFLOW__SMTP__SMTP_HOST",     "smtp.gmail.com")
SMTP_PORT     = int(os.getenv("AIRFLOW__SMTP__SMTP_PORT", "587"))
SMTP_USER     = os.getenv("AIRFLOW__SMTP__SMTP_USER",     "")
SMTP_PASSWORD = os.getenv("AIRFLOW__SMTP__SMTP_PASSWORD", "")

# ── Constantes ─────────────────────────────────────────────────────────────────

STREAMLIT_URL   = "https://sql-database.dataoz.fr/"
ORACLE_USER     = "ADMIN"
ORACLE_DSN      = os.getenv("ORACLE_DSN",        "dataozdb_tp")
ORACLE_PASS     = os.getenv("ORACLE_PASSWORD",    "")
WALLET_DIR      = os.getenv("ORACLE_WALLET_DIR",  "/opt/airflow/wallet")
WALLET_PASS     = os.getenv("WALLET_PASSWORD",    "")
BASE_CURATED    = "/opt/airflow/data/curated"

# ── DAGs de collecte à surveiller ─────────────────────────────────────────────
# max_age_h : âge maximal acceptable du dernier run (en heures)
COLLECTION_DAGS = {
    "dag_meteo_station":       {"max_age_h": 26},    # quotidien matin — check valide toute la journée
    "dag_conso_elec_tuya":     {"max_age_h": 36},    # quotidien 02h00 (marge DST/retard)
    "dag_conso_elec_enedis":   {"max_age_h": 36},    # quotidien (marge retard)
    "dag_boursorama_cotation":  {"max_age_h": 120},   # jours ouvrés (5 j)
    "dag_calendaire":          {"max_age_h": 720},   # mensuel (~30 j)
    "dag_oracle_load":         {"max_age_h": 28},    # quotidien 06h00 UTC
}

# ── Fichiers CSV curated à vérifier ───────────────────────────────────────────
# max_age_h : âge maximal acceptable du fichier (en heures)
CSV_FILES = {
    "Météo Bresser":         {"path": f"{BASE_CURATED}/météo/bresser/common_weather_database.csv",                  "max_age_h": 26},   # quotidien matin — check valide toute la journée
    "Enedis 30 min":         {"path": f"{BASE_CURATED}/conso_elec/enedis/Database_Enedis_30_min.csv",               "max_age_h": 36},   # aligné sur COLLECTION_DAGS (dag à 01:10 UTC, check à 09:00 UTC = 32h si un run manqué)
    "Enedis journalier":     {"path": f"{BASE_CURATED}/conso_elec/enedis/database_enedis_journalier.csv",           "max_age_h": 336},   # manuel → 14 j
    "Tuya 15 min":           {"path": f"{BASE_CURATED}/conso_elec/tuya/_SYNTHESE_15MIN.csv",                        "max_age_h": 28},
    "Tuya horaire":          {"path": f"{BASE_CURATED}/conso_elec/tuya/_SYNTHESE_HORAIRE.csv",                      "max_age_h": 28},
    "Tuya journalier":       {"path": f"{BASE_CURATED}/conso_elec/tuya/_SYNTHESE_JOURNALIERE.csv",                  "max_age_h": 36},   # quotidien + marge
    "Tuya mensuel":          {"path": f"{BASE_CURATED}/conso_elec/tuya/_SYNTHESE_MENSUELLE.csv",                    "max_age_h": 720},
    "Calendrier":            {"path": f"{BASE_CURATED}/calendaire/socle_calendrier.csv",                            "max_age_h": 720},
    "Enedis horaire":        {"path": f"{BASE_CURATED}/conso_elec/enedis/database_enedis_horaire.csv",             "max_age_h": 36},   # aligné sur COLLECTION_DAGS
    "Finance cotations":     {"path": "/opt/airflow/data/raw/finance/cotations/boursorama_cotations.csv",         "max_age_h": 120},  # quotidien (jours ouvrés) — enriched cassé depuis mars, check sur le raw
    # "Finance enriched":    {"path": f"{BASE_CURATED}/finance/valeurs/boursorama_cotations_enriched.csv",        "max_age_h": 120},  # TODO: réactiver quand update_master est corrigé
}

# ── Fichiers bucket OCI attendus ──────────────────────────────────────────────
OCI_BUCKET_FILES = [
    "calendrier.csv", "meteo_bresser.csv",
    "enedis_30min.csv", "enedis_journalier.csv", "enedis_horaire.csv",
    "tuya_15min.csv", "tuya_horaire.csv", "tuya_journalier.csv", "tuya_mensuel.csv",
    "finance_cotations.csv",
]

# ── Tables Oracle à vérifier ──────────────────────────────────────────────────
# min_rows     : nombre minimum de lignes attendu
# freshness_sql: renvoie une STRING 'YYYY-MM-DD' (TO_CHAR ou MAX SUBSTR)
#                → évite les bugs de conversion oracledb sur les types DATE
# max_age_h    : âge maximal acceptable du dernier enregistrement (None = pas de check)
ORACLE_TABLES = {
    # COPY_DATA stocke les ts VARCHAR2 au format Oracle NLS : "DD-MON-RR HH24:MI:SS[.FF]"
    # La partie date fait toujours exactement 9 chars : "DD-MON-RR"
    # → SUBSTR(TRIM(ts),1,9) extrait uniquement la date, indépendamment de la partie heure
    #   et des éventuelles fractions de secondes (.FF) qui causent ORA-01830 avec un format fixe
    # → TO_DATE(...,'DD-MON-RR') puis TO_CHAR retourne 'YYYY-MM-DD' parseable en Python
    "METEO_BRESSER":     {"min_rows": 1000,   "freshness_sql": "SELECT TO_CHAR(MAX(TO_DATE(SUBSTR(TRIM(ts),1,9),'DD-MON-RR')),'YYYY-MM-DD') FROM meteo_bresser  WHERE LENGTH(TRIM(ts))>=9",  "max_age_h": 48},
    "ENEDIS_30MIN":      {"min_rows": 50000,  "freshness_sql": "SELECT TO_CHAR(MAX(ts),'YYYY-MM-DD') FROM enedis_30min",                                         "max_age_h": 120},
    "ENEDIS_JOURNALIER": {"min_rows": 100,    "freshness_sql": "SELECT TO_CHAR(MAX(date_jour),'YYYY-MM-DD') FROM enedis_journalier",                             "max_age_h": 120},  # même latence que 30min (Enedis J-2 à J-5)
    "ENEDIS_HORAIRE":    {"min_rows": 20000,  "freshness_sql": "SELECT TO_CHAR(MAX(ts),'YYYY-MM-DD') FROM enedis_horaire",                                       "max_age_h": 120},  # agrégat dérivé du 30min, même latence
    "TUYA_15MIN":        {"min_rows": 100,    "freshness_sql": "SELECT TO_CHAR(MAX(TO_DATE(SUBSTR(TRIM(ts),1,9),'DD-MON-RR')),'YYYY-MM-DD') FROM tuya_15min     WHERE LENGTH(TRIM(ts))>=9",  "max_age_h": 48},
    "TUYA_HORAIRE":      {"min_rows": 50,     "freshness_sql": "SELECT TO_CHAR(MAX(TO_DATE(SUBSTR(TRIM(ts),1,9),'DD-MON-RR')),'YYYY-MM-DD') FROM tuya_horaire   WHERE LENGTH(TRIM(ts))>=9",  "max_age_h": 48},
    "TUYA_JOURNALIER":   {"min_rows": 10,     "freshness_sql": "SELECT TO_CHAR(MAX(date_jour),'YYYY-MM-DD') FROM tuya_journalier",                               "max_age_h": 28},
    "TUYA_MENSUEL":      {"min_rows": 1,      "freshness_sql": None,                                                                                             "max_age_h": None},
    "CALENDRIER":        {"min_rows": 9000,   "freshness_sql": None,                                                                                             "max_age_h": None},
    "FINANCE_COTATIONS": {"min_rows": 100000, "freshness_sql": "SELECT TO_CHAR(MAX(date_import),'YYYY-MM-DD') FROM finance_cotations",                          "max_age_h": 120},
}

# ── Helpers ────────────────────────────────────────────────────────────────────

def _ok(msg):  print(f"  ✅  {msg}")
def _warn(msg): print(f"  ⚠️  {msg}")
def _ko(msg):  print(f"  ❌  {msg}")

def _get_oracle_conn(max_attempts: int = 8, wait_s: int = 45):
    """
    Tente de se connecter à Oracle avec retry.
    Utile quand l'Autonomous Database (Always Free) sort d'auto-suspend.
    Le redémarrage ADB prend 1 à 5 minutes — 8 tentatives x 45s = 6 min max.
    """
    last_err = None
    for attempt in range(1, max_attempts + 1):
        try:
            conn = oracledb.connect(
                user=ORACLE_USER,
                password=ORACLE_PASS,
                dsn=ORACLE_DSN,
                config_dir=WALLET_DIR,
                wallet_location=WALLET_DIR,
                wallet_password=WALLET_PASS,
            )
            if attempt > 1:
                print(f"  ✅  Oracle connecté après {attempt} tentative(s).")
            return conn
        except Exception as e:
            last_err = e
            if attempt < max_attempts:
                print(f"  ⚠️  Oracle connexion échouée (tentative {attempt}/{max_attempts}) : {e}")
                print(f"      Nouvelle tentative dans {wait_s}s (ADB en cours de wake-up ?)…")
                time.sleep(wait_s)
            else:
                print(f"  ❌  Oracle inaccessible après {max_attempts} tentatives : {e}")
    raise last_err

# ══════════════════════════════════════════════════════════════════════════════
# ÉTAPE 1 — DAGs de collecte
# ══════════════════════════════════════════════════════════════════════════════

def check_collection_dags(**kwargs):
    """Vérifie l'état du dernier run de chaque DAG de collecte."""
    print("=" * 60)
    print("ÉTAPE 1 — ÉTAT DES DAGs DE COLLECTE")
    print("=" * 60)

    errors = []
    now = datetime.now(timezone.utc)

    for dag_id, cfg in COLLECTION_DAGS.items():
        runs = DagRun.find(dag_id=dag_id)
        if not runs:
            msg = f"{dag_id} : aucun run trouvé"
            _ko(msg)
            errors.append(msg)
            continue

        last = sorted(runs, key=lambda r: r.execution_date)[-1]
        state  = last.state
        # Utiliser start_date (heure réelle du run) et non execution_date
        # qui est la date logique Airflow (toujours une période en retard)
        run_ts = last.start_date or last.execution_date
        if run_ts.tzinfo is None:
            run_ts = run_ts.replace(tzinfo=timezone.utc)
        age_h  = (now - run_ts).total_seconds() / 3600
        max_h  = cfg["max_age_h"]

        if state == DagRunState.SUCCESS and age_h <= max_h:
            _ok(f"{dag_id} → {state} il y a {age_h:.1f}h")
        elif state == DagRunState.SUCCESS and age_h > max_h:
            msg = f"{dag_id} → success mais trop ancien ({age_h:.1f}h > {max_h}h)"
            _warn(msg)
            errors.append(msg)
        else:
            msg = f"{dag_id} → état={state} il y a {age_h:.1f}h"
            _ko(msg)
            errors.append(msg)

    if errors:
        raise ValueError(f"Étape 1 — {len(errors)} problème(s) détecté(s):\n" + "\n".join(errors))

    _ok("Tous les DAGs de collecte sont en succès.")
    kwargs["ti"].xcom_push(key="step1", value="OK")


# ══════════════════════════════════════════════════════════════════════════════
# ÉTAPE 2 — Fraîcheur des CSV curated locaux
# ══════════════════════════════════════════════════════════════════════════════

def check_csv_freshness(**kwargs):
    """Vérifie que les fichiers CSV curated existent et sont récents."""
    print("=" * 60)
    print("ÉTAPE 2 — FRAÎCHEUR DES CSV CURATED LOCAUX")
    print("=" * 60)

    errors = []
    now = time.time()

    for label, cfg in CSV_FILES.items():
        path    = cfg["path"]
        max_h   = cfg["max_age_h"]

        if not os.path.exists(path):
            msg = f"{label} : fichier introuvable → {path}"
            _ko(msg)
            errors.append(msg)
            continue

        mtime   = os.path.getmtime(path)
        age_h   = (now - mtime) / 3600
        size_kb = os.path.getsize(path) / 1024

        if age_h <= max_h:
            _ok(f"{label} → {age_h:.1f}h  ({size_kb:.0f} Ko)")
        else:
            msg = f"{label} → trop ancien ({age_h:.1f}h > {max_h}h)"
            _warn(msg)
            errors.append(msg)

    if errors:
        raise ValueError(f"Étape 2 — {len(errors)} problème(s):\n" + "\n".join(errors))

    _ok("Tous les fichiers CSV curated sont frais.")
    kwargs["ti"].xcom_push(key="step2", value="OK")


# ══════════════════════════════════════════════════════════════════════════════
# ÉTAPE 3 — Bucket OCI
# ══════════════════════════════════════════════════════════════════════════════

def check_oci_bucket(**kwargs):
    """Vérifie que les 9 fichiers CSV sont présents dans le bucket OCI."""
    print("=" * 60)
    print("ÉTAPE 3 — FICHIERS DANS LE BUCKET OCI")
    print("=" * 60)

    import oci as oci_sdk

    oci_config  = os.getenv("OCI_CONFIG_FILE", "/opt/airflow/oci_key/config")
    oci_ns      = os.getenv("OCI_NAMESPACE",   "axdo67cv3ayo")
    oci_bucket  = os.getenv("OCI_BUCKET",      "dataoz-curated")

    config  = oci_sdk.config.from_file(oci_config)
    client  = oci_sdk.object_storage.ObjectStorageClient(config)

    # fields=timeModified obligatoire sinon time_modified est None dans la réponse
    response = client.list_objects(oci_ns, oci_bucket, fields="name,size,timeModified,timeCreated")
    objects  = response.data.objects
    present  = {o.name for o in objects}

    errors  = []
    now_utc = datetime.now(timezone.utc)

    for fname in OCI_BUCKET_FILES:
        if fname not in present:
            msg = f"{fname} : absent du bucket !"
            _ko(msg)
            errors.append(msg)
        else:
            obj      = next(o for o in objects if o.name == fname)
            ts_obj   = obj.time_modified or obj.time_created
            size_kb  = (obj.size or 0) / 1024
            if ts_obj is None:
                _warn(f"{fname} → présent ({size_kb:.0f} Ko) — date de modif indisponible")
            else:
                if ts_obj.tzinfo is None:
                    ts_obj = ts_obj.replace(tzinfo=timezone.utc)
                age_h = (now_utc - ts_obj).total_seconds() / 3600
                if age_h <= 28:
                    _ok(f"{fname} → {age_h:.1f}h  ({size_kb:.0f} Ko)")
                else:
                    msg = f"{fname} → présent mais ancien ({age_h:.1f}h)"
                    _warn(msg)
                    errors.append(msg)

    if errors:
        raise ValueError(f"Étape 3 — {len(errors)} problème(s):\n" + "\n".join(errors))

    _ok("Tous les fichiers sont présents et récents dans le bucket OCI.")
    kwargs["ti"].xcom_push(key="step3", value="OK")


# ══════════════════════════════════════════════════════════════════════════════
# ÉTAPE 4 — Oracle : jobs + row counts + fraîcheur
# ══════════════════════════════════════════════════════════════════════════════

def check_oracle(**kwargs):
    """Vérifie les jobs DBMS_SCHEDULER, les row counts et la fraîcheur des tables Oracle."""
    print("=" * 60)
    print("ÉTAPE 4 — BASE ORACLE AUTONOMOUS DATABASE")
    print("=" * 60)

    errors = []
    conn   = _get_oracle_conn()

    try:
        with conn.cursor() as cur:

            # ── 4a. Dernier run des jobs DBMS_SCHEDULER ───────────────────────
            # USER_SCHEDULER_JOBS.STATE = 'SCHEDULED' entre deux runs → normal
            # Il faut interroger USER_SCHEDULER_JOB_RUN_DETAILS pour le statut réel
            print("\n  [4a] Dernier run des jobs DBMS_SCHEDULER :")
            cur.execute("""
                SELECT job_name, status, actual_start_date
                FROM (
                    SELECT job_name, status, actual_start_date,
                           ROW_NUMBER() OVER (PARTITION BY job_name ORDER BY actual_start_date DESC) AS rn
                    FROM user_scheduler_job_run_details
                    WHERE job_name LIKE 'JOB_LOAD_%'
                )
                WHERE rn = 1
                ORDER BY job_name
            """)
            rows = cur.fetchall()
            if not rows:
                _warn("Aucun historique JOB_LOAD_* trouvé dans job_run_details")
            for job_name, status, actual_start in rows:
                if status == "SUCCEEDED":
                    _ok(f"{job_name} → {status}  (dernier run : {actual_start})")
                else:
                    msg = f"{job_name} → statut={status} (dernier run : {actual_start})"
                    _ko(msg)
                    errors.append(msg)

            # ── 4b. Row counts + fraîcheur ─────────────────────────────────────
            print("\n  [4b] Row counts et fraîcheur des tables :")
            now = datetime.now(timezone.utc)

            for table, cfg in ORACLE_TABLES.items():
                # Row count
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]

                count_ok = count >= cfg["min_rows"]
                if count_ok:
                    _ok(f"{table} → {count:,} lignes (min={cfg['min_rows']:,})")
                else:
                    msg = f"{table} → seulement {count:,} lignes (min={cfg['min_rows']:,})"
                    _ko(msg)
                    errors.append(msg)

                # Fraîcheur
                if cfg["freshness_sql"] and cfg["max_age_h"]:
                    cur.execute(cfg["freshness_sql"])
                    date_str = cur.fetchone()[0]   # STRING 'YYYY-MM-DD' retournée par TO_CHAR / MAX SUBSTR
                    if date_str is None:
                        msg = f"{table} → aucune donnée datée"
                        _warn(msg)
                        errors.append(msg)
                    else:
                        try:
                            dt    = datetime.strptime(str(date_str)[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
                            age_h = (now - dt).total_seconds() / 3600
                            if age_h < 0:
                                _warn(f"{table} → MAX date dans le futur ({date_str}) — données suspectes")
                            elif age_h <= cfg["max_age_h"]:
                                _ok(f"{table} → données fraîches ({age_h:.1f}h  max={cfg['max_age_h']}h)")
                            else:
                                msg = f"{table} → données trop anciennes ({age_h:.1f}h > {cfg['max_age_h']}h)"
                                _warn(msg)
                                errors.append(msg)
                        except Exception as parse_err:
                            _warn(f"{table} → impossible de parser la date '{date_str}' : {parse_err}")

    finally:
        conn.close()

    if errors:
        raise ValueError(f"Étape 4 — {len(errors)} problème(s):\n" + "\n".join(errors))

    _ok("Oracle : tous les jobs OK, toutes les tables alimentées et fraîches.")
    kwargs["ti"].xcom_push(key="step4", value="OK")


# ══════════════════════════════════════════════════════════════════════════════
# ÉTAPE 5 — Streamlit
# ══════════════════════════════════════════════════════════════════════════════

def check_smtp(**kwargs):
    """Vérifie que la connexion SMTP est opérationnelle (connect + login, sans envoi)."""
    print("=" * 60)
    print("ÉTAPE 6 — CONNEXION SMTP (alerte email)")
    print("=" * 60)

    errors = []

    if not SMTP_USER or not SMTP_PASSWORD:
        msg = "SMTP_USER ou SMTP_PASSWORD non définis"
        _ko(msg)
        errors.append(msg)
    else:
        try:
            ctx = ssl.create_default_context()
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10) as server:
                server.ehlo()
                server.starttls(context=ctx)
                server.ehlo()
                server.login(SMTP_USER, SMTP_PASSWORD)
            _ok(f"SMTP {SMTP_HOST}:{SMTP_PORT} → connexion et auth OK ({SMTP_USER})")
        except smtplib.SMTPAuthenticationError:
            msg = f"SMTP → échec authentification ({SMTP_USER})"
            _ko(msg)
            errors.append(msg)
        except Exception as e:
            msg = f"SMTP → erreur connexion : {e}"
            _ko(msg)
            errors.append(msg)

    if errors:
        raise ValueError(f"Étape 6 — {len(errors)} problème(s):\n" + "\n".join(errors))

    _ok("SMTP opérationnel — les alertes email sont actives.")
    kwargs["ti"].xcom_push(key="step6", value="OK")


def check_streamlit(**kwargs):
    """Vérifie que le Streamlit répond (HTTP 200) en moins de 10 s."""
    print("=" * 60)
    print("ÉTAPE 5 — STREAMLIT (sql-database.dataoz.fr)")
    print("=" * 60)

    errors = []

    try:
        t0   = time.time()
        resp = requests.get(STREAMLIT_URL, timeout=15)
        ms   = int((time.time() - t0) * 1000)

        if resp.status_code == 200:
            _ok(f"HTTP {resp.status_code} en {ms} ms → {STREAMLIT_URL}")
        else:
            msg = f"HTTP {resp.status_code} (attendu 200) → {STREAMLIT_URL}"
            _ko(msg)
            errors.append(msg)

    except requests.exceptions.Timeout:
        msg = f"Timeout (>15s) → {STREAMLIT_URL}"
        _ko(msg)
        errors.append(msg)
    except Exception as e:
        msg = f"Erreur connexion → {e}"
        _ko(msg)
        errors.append(msg)

    if errors:
        raise ValueError(f"Étape 5 — {len(errors)} problème(s):\n" + "\n".join(errors))

    _ok("Streamlit accessible et opérationnel.")
    kwargs["ti"].xcom_push(key="step5", value="OK")


# ══════════════════════════════════════════════════════════════════════════════
# RÉSUMÉ FINAL
# ══════════════════════════════════════════════════════════════════════════════

def pipeline_summary(**kwargs):
    """Affiche un résumé global de tous les checks et envoie une alerte email si anomalie."""
    ti  = kwargs["ti"]
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    steps = {
        "Étape 1 — DAGs collecte":     ti.xcom_pull(key="step1", task_ids="check_collection_dags"),
        "Étape 2 — CSV curated locaux": ti.xcom_pull(key="step2", task_ids="check_csv_freshness"),
        "Étape 3 — Bucket OCI":         ti.xcom_pull(key="step3", task_ids="check_oci_bucket"),
        "Étape 4 — Oracle DB":          ti.xcom_pull(key="step4", task_ids="check_oracle"),
        "Étape 5 — Streamlit":          ti.xcom_pull(key="step5", task_ids="check_streamlit"),
        "Étape 6 — SMTP":               ti.xcom_pull(key="step6", task_ids="check_smtp"),
    }

    print()
    print("=" * 60)
    print("   RÉSUMÉ INTÉGRAL PIPELINE DATAOZ")
    print(f"   {now}")
    print("=" * 60)

    all_ok = True
    for label, result in steps.items():
        icon = "✅" if result == "OK" else "❌"
        if result != "OK":
            all_ok = False
        print(f"  {icon}  {label} → {result or 'ECHEC'}")

    print("=" * 60)
    if all_ok:
        print("  PIPELINE 100% OPERATIONNEL")
    else:
        print("  DES ANOMALIES ONT ETE DETECTEES -- voir logs ci-dessus")
    print("=" * 60)

    if not all_ok:
        rows_html = ""
        for label, result in steps.items():
            ok      = result == "OK"
            couleur = "#d4edda" if ok else "#f8d7da"
            icone   = "✅" if ok else "❌"
            statut  = result or "ECHEC"
            rows_html += (
                f'<tr style="background:{couleur};">'
                f'<td style="padding:8px 12px;">{icone} {label}</td>'
                f'<td style="padding:8px 12px;font-weight:bold;">{statut}</td>'
                f'</tr>'
            )

        html_body = f"""
        <html><body style="font-family:Arial,sans-serif;color:#333;">
          <h2 style="color:#c0392b;">DataOZ -- Anomalie pipeline détectée</h2>
          <p><strong>Date :</strong> {now} UTC</p>
          <table border="0" cellspacing="0" cellpadding="0"
                 style="border-collapse:collapse;width:100%;max-width:600px;">
            <thead>
              <tr style="background:#343a40;color:#fff;">
                <th style="padding:10px 12px;text-align:left;">Etape</th>
                <th style="padding:10px 12px;text-align:left;">Statut</th>
              </tr>
            </thead>
            <tbody>{rows_html}</tbody>
          </table>
          <p style="margin-top:16px;">
            Voir les logs dans Airflow : http://localhost:8080/dags/dag_check_pipeline/grid
          </p>
          <hr style="margin-top:24px;">
          <small style="color:#888;">DataOZ Monitoring -- dag_check_pipeline</small>
        </body></html>
        """

        nb_erreurs = sum(1 for v in steps.values() if v != "OK")
        subject    = f"DataOZ Pipeline -- {nb_erreurs} anomalie(s) détectée(s) [{now[:10]}]"

        try:
            send_email(to=ALERT_EMAIL, subject=subject, html_content=html_body)
            print(f"  Alerte email envoyée --> {ALERT_EMAIL}", flush=True)
        except Exception as e:
            print(f"  Impossible d'envoyer l'email d'alerte : {e}", flush=True)


# ============================================================
# DAG DEFINITION
# ============================================================
# Planning (heure Paris / CEST) :
#   02:30  dag_oracle_load    uploade les CSV vers le bucket OCI
#   04:00  DBMS_SCHEDULER Oracle charge les tables depuis OCI (02:00 UTC)
#   05:15  dag_check_pipeline verifie que tout est OK
#   10:45  Tache Windows reveille le PC si en veille (filet securite)
# ============================================================

default_args = {
    "owner":            "dataoz",
    "depends_on_past":  False,
    "retries":          1,                        # 1 retry Airflow au niveau tâche (filet de sécurité)
    "retry_delay":      timedelta(minutes=5),     # attendre 5 min avant retry (ADB wake-up ~1-3 min)
    "email_on_failure": False,
}

with DAG(
    dag_id="dag_check_pipeline",
    description="Verification integrale de la chaine DataOZ (6 etapes)",
    schedule_interval="15 5 * * *",    # tous les jours a 05:15 CEST (apres DBMS_SCHEDULER 04:00 CEST / 02:00 UTC)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["dataoz", "monitoring", "check"],
) as dag:

    t1 = PythonOperator(task_id="check_collection_dags",  python_callable=check_collection_dags)
    t2 = PythonOperator(task_id="check_csv_freshness",    python_callable=check_csv_freshness)
    t3 = PythonOperator(task_id="check_oci_bucket",       python_callable=check_oci_bucket)
    t4 = PythonOperator(task_id="check_oracle",           python_callable=check_oracle)
    t5 = PythonOperator(task_id="check_streamlit",        python_callable=check_streamlit)
    t6 = PythonOperator(task_id="check_smtp",             python_callable=check_smtp)

    t_summary = PythonOperator(
        task_id="pipeline_summary",
        python_callable=pipeline_summary,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    [t1, t2, t3, t4, t5, t6] >> t_summary
