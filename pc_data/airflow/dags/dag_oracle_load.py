"""
dag_oracle_load.py
==================
DAG Airflow -- Upload quotidien CSV curated -> Oracle Object Storage bucket
Le chargement Oracle est ensuite pris en charge automatiquement par
DBMS_SCHEDULER (COPY_DATA) planifie a 04:00 CEST (02:00 UTC).

Flow par canal :
  upload_X  ->  integrity_X  -+
                               +--> pipeline_summary
  upload_Y  ->  integrity_Y  -+

Variables d'environnement requises :
  OCI_CONFIG_FILE : chemin vers le fichier config OCI (cle API)
                    defaut : /opt/airflow/oci_key/config
  OCI_NAMESPACE   : namespace Object Storage (axdo67cv3ayo)
  OCI_BUCKET      : nom du bucket (dataoz-curated)
"""
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import upload_to_bucket as bkt


# Verification par concordance exacte local <-> bucket OCI
# (lignes, colonnes, octets doivent etre identiques)


# Fabrique de fonctions integrity_*
def _make_integrity_fn(object_name: str, label: str):
    """
    Retourne une fonction Airflow qui verifie la concordance exacte entre
    le fichier uploade localement et ce qui est arrive dans le bucket OCI :
      1. Presence dans le bucket (head_object)
      2. Concordance nombre de lignes  : local == bucket
      3. Concordance nombre de colonnes: local == bucket
      4. Concordance taille en octets  : local == bucket
    Les metriques locales sont lues depuis le XCom de la tache upload_*.
    Leve ValueError si l'un des checks echoue -> tache FAILED dans Airflow.
    """
    def integrity_check(**context):
        import csv
        import io
        import oci as oci_sdk

        oci_config = oci_sdk.config.from_file(
            os.getenv("OCI_CONFIG_FILE", "/opt/airflow/oci_key/config")
        )
        client     = oci_sdk.object_storage.ObjectStorageClient(oci_config)
        oci_ns     = os.getenv("OCI_NAMESPACE", "axdo67cv3ayo")
        oci_bucket = os.getenv("OCI_BUCKET",    "dataoz-curated")

        # 1. Metriques locales (XCom de la tache upload_*)
        upload_task_id = "upload_" + object_name.replace(".csv", "")
        local = context["ti"].xcom_pull(task_ids=upload_task_id) or {}
        local_rows  = local.get("rows")
        local_cols  = local.get("cols")
        local_bytes = local.get("bytes")

        print(f"[{label}] Concordance local -> bucket OCI : {object_name}")

        if not local or local_bytes is None:
            raise ValueError(
                f"[{label}] XCom manquant depuis '{upload_task_id}' -- "
                f"la tache upload n'a pas retourne ses metriques (rows/cols/bytes). "
                f"Verifiez que upload_to_bucket.py v2 est bien recharge dans le container."
            )

        print(f"  Local  : {local_rows:,} lignes | {local_cols} colonnes | {local_bytes:,} octets")

        # 2. Presence dans le bucket (head_object)
        try:
            head = client.head_object(oci_ns, oci_bucket, object_name)
        except oci_sdk.exceptions.ServiceError as e:
            if e.status == 404:
                raise ValueError(f"[{label}] Fichier absent du bucket : {object_name}")
            raise

        bucket_bytes = int(head.headers.get("content-length", 0))

        # 3. Telechargement + parsing CSV
        response  = client.get_object(oci_ns, oci_bucket, object_name)
        raw       = response.data.content
        text      = raw.decode("utf-8", errors="replace")
        reader    = csv.reader(io.StringIO(text), delimiter=";")
        all_rows  = list(reader)

        bucket_cols = len(all_rows[0]) if all_rows else 0
        bucket_rows = len(all_rows) - 1   # hors header

        print(f"  Bucket : {bucket_rows:,} lignes | {bucket_cols} colonnes | {bucket_bytes:,} octets")

        # 4. Controles de concordance
        errors = []
        if local_rows is not None and bucket_rows != local_rows:
            errors.append(f"lignes : local={local_rows:,} != bucket={bucket_rows:,}")
        if local_cols is not None and bucket_cols != local_cols:
            errors.append(f"colonnes : local={local_cols} != bucket={bucket_cols}")
        if local_bytes is not None and bucket_bytes != local_bytes:
            errors.append(f"octets : local={local_bytes:,} != bucket={bucket_bytes:,}")

        if errors:
            msg = f"[{label}] Ecart detecte : " + " | ".join(errors)
            print(f"  {msg}")
            raise ValueError(msg)

        kb = bucket_bytes / 1024
        print(f"  OK -- Concordance parfaite : {bucket_rows:,} lignes | {bucket_cols} col | {kb:.1f} Ko")

        # Pousse les metriques en XCom pour le pipeline_summary
        context["ti"].xcom_push(key="integrity_result", value={
            "label":  label,
            "object": object_name,
            "bytes":  bucket_bytes,
            "kb":     round(kb, 1),
            "rows":   bucket_rows,
            "cols":   bucket_cols,
            "status": "OK",
        })

    integrity_check.__name__ = f"integrity_{object_name.replace('.csv', '')}"
    return integrity_check


# Resume consolide
def pipeline_summary(**context):
    """
    Tache finale (trigger_rule=ALL_DONE) -- toujours executee.
    Collecte les resultats XCom des taches integrity_* et produit un tableau recap.
    """
    dag_run = context["dag_run"]
    ti      = context["ti"]
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Mapping integrity_task_id -> label (pour collecter les XCom)
    integrity_tasks = {
        "integrity_calendrier":        "Calendrier",
        "integrity_meteo_bresser":     "Meteo Bresser",
        "integrity_enedis_30min":      "Enedis 30min",
        "integrity_enedis_journalier": "Enedis journalier",
        "integrity_enedis_horaire":    "Enedis horaire",
        "integrity_tuya_15min":        "Tuya 15min",
        "integrity_tuya_horaire":      "Tuya horaire",
        "integrity_tuya_journalier":   "Tuya journalier",
        "integrity_tuya_mensuel":      "Tuya mensuel",
        "integrity_finance_cotations": "Finance cotations",
    }

    print("=" * 77)
    print("   RESUME UPLOAD + INTEGRITY OCI -- dag_oracle_load")
    print(f"   {now_str}")
    print("=" * 77)
    print(f"  {'Source':<22}  {'Upload':<8} {'Integrity':<10} {'Taille':>9}  {'Lignes':>9}  {'Cols':>5}")
    print("  " + "-" * 72)

    errors = []
    for integrity_task_id, label in integrity_tasks.items():
        upload_task_id = integrity_task_id.replace("integrity_", "upload_")

        # Etats Airflow
        ti_up  = dag_run.get_task_instance(upload_task_id)
        ti_int = dag_run.get_task_instance(integrity_task_id)
        st_up  = ti_up.state  if ti_up  else "inconnu"
        st_int = ti_int.state if ti_int else "inconnu"

        icon_up  = "OK" if st_up  == "success" else "KO"
        icon_int = "OK" if st_int == "success" else "KO"

        # Metriques OCI depuis XCom du integrity
        xcom = ti.xcom_pull(task_ids=integrity_task_id, key="integrity_result") or {}
        kb   = f"{xcom['kb']} Ko"         if xcom else "--"
        rows = f"{xcom['rows']:,}"         if xcom else "--"
        cols = str(xcom.get("cols", "--"))  if xcom else "--"

        print(f"  {label:<22}  {icon_up} {st_up:<6} {icon_int} {st_int:<8} "
              f"{kb:>9}  {rows:>9}  {cols:>5}")

        if st_up != "success":
            errors.append(f"{label} -- upload echoue ({st_up})")
        if st_int != "success":
            errors.append(f"{label} -- integrity OCI echouee ({st_int})")

    print("=" * 77)
    if not errors:
        print("  OK  TOUS LES CANAUX OK -- DBMS_SCHEDULER peut charger")
    else:
        print(f"  KO  {len(errors)} PROBLEME(S) DETECTE(S) :")
        for e in errors:
            print(f"      - {e}")
        print("  Les tables Oracle correspondantes ne seront pas a jour.")
    print("=" * 77)


# Parametres par defaut
default_args = {
    "owner":            "dataoz",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="dag_oracle_load",
    description="Upload quotidien CSV curated -> Oracle Object Storage (DBMS_SCHEDULER charge ensuite)",
    schedule_interval="30 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["dataoz", "oracle", "bucket", "load"],
) as dag:

    # Uploads (en parallele)
    t_cal = PythonOperator(task_id="upload_calendrier",        python_callable=bkt.upload_calendrier)
    t_met = PythonOperator(task_id="upload_meteo_bresser",     python_callable=bkt.upload_meteo_bresser)
    t_e30 = PythonOperator(task_id="upload_enedis_30min",      python_callable=bkt.upload_enedis_30min)
    t_ej  = PythonOperator(task_id="upload_enedis_journalier", python_callable=bkt.upload_enedis_journalier)
    t_eh  = PythonOperator(task_id="upload_enedis_horaire",    python_callable=bkt.upload_enedis_horaire)
    t_t15 = PythonOperator(task_id="upload_tuya_15min",        python_callable=bkt.upload_tuya_15min)
    t_th  = PythonOperator(task_id="upload_tuya_horaire",      python_callable=bkt.upload_tuya_horaire)
    t_tj  = PythonOperator(task_id="upload_tuya_journalier",   python_callable=bkt.upload_tuya_journalier)
    t_tm  = PythonOperator(task_id="upload_tuya_mensuel",      python_callable=bkt.upload_tuya_mensuel)
    t_fin = PythonOperator(task_id="upload_finance_cotations", python_callable=bkt.upload_finance_cotations)

    # Integrity OCI (une par canal, apres son upload)
    t_cal_i = PythonOperator(task_id="integrity_calendrier",        python_callable=_make_integrity_fn("calendrier.csv",         "Calendrier"))
    t_met_i = PythonOperator(task_id="integrity_meteo_bresser",     python_callable=_make_integrity_fn("meteo_bresser.csv",      "Meteo Bresser"))
    t_e30_i = PythonOperator(task_id="integrity_enedis_30min",      python_callable=_make_integrity_fn("enedis_30min.csv",       "Enedis 30min"))
    t_ej_i  = PythonOperator(task_id="integrity_enedis_journalier", python_callable=_make_integrity_fn("enedis_journalier.csv",  "Enedis journalier"))
    t_eh_i  = PythonOperator(task_id="integrity_enedis_horaire",    python_callable=_make_integrity_fn("enedis_horaire.csv",     "Enedis horaire"))
    t_t15_i = PythonOperator(task_id="integrity_tuya_15min",        python_callable=_make_integrity_fn("tuya_15min.csv",         "Tuya 15min"))
    t_th_i  = PythonOperator(task_id="integrity_tuya_horaire",      python_callable=_make_integrity_fn("tuya_horaire.csv",       "Tuya horaire"))
    t_tj_i  = PythonOperator(task_id="integrity_tuya_journalier",   python_callable=_make_integrity_fn("tuya_journalier.csv",    "Tuya journalier"))
    t_tm_i  = PythonOperator(task_id="integrity_tuya_mensuel",      python_callable=_make_integrity_fn("tuya_mensuel.csv",       "Tuya mensuel"))
    t_fin_i = PythonOperator(task_id="integrity_finance_cotations", python_callable=_make_integrity_fn("finance_cotations.csv",  "Finance cotations"))

    # Resume final (toujours execute)
    t_summary = PythonOperator(
        task_id="pipeline_summary",
        python_callable=pipeline_summary,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Dependances : upload -> integrity -> summary
    t_cal >> t_cal_i >> t_summary
    t_met >> t_met_i >> t_summary
    t_e30 >> t_e30_i >> t_summary
    t_ej  >> t_ej_i  >> t_summary
    t_eh  >> t_eh_i  >> t_summary
    t_t15 >> t_t15_i >> t_summary
    t_th  >> t_th_i  >> t_summary
    t_tj  >> t_tj_i  >> t_summary
    t_tm  >> t_tm_i  >> t_summary
    t_fin >> t_fin_i >> t_summary
