"""
dag_oracle_load.py
==================
DAG Airflow — Upload quotidien CSV curated → Oracle Object Storage bucket
Le chargement Oracle est ensuite pris en charge automatiquement par
DBMS_SCHEDULER (COPY_DATA) planifié à 07h30 UTC.

Variables d'environnement requises :
  OCI_CONFIG_FILE : chemin vers le fichier config OCI (clé API)
                    défaut : /opt/airflow/oci_key/config
  OCI_NAMESPACE   : namespace Object Storage (axdo67cv3ayo)
  OCI_BUCKET      : nom du bucket (dataoz-curated)
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import upload_to_bucket as bkt

default_args = {
    "owner":            "dataoz",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="dag_oracle_load",
    description="Upload quotidien CSV curated → Oracle Object Storage (DBMS_SCHEDULER charge ensuite)",
    schedule_interval="0 2 * * *",  # tous les jours à 02:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["dataoz", "oracle", "bucket", "load"],
) as dag:

    t_cal = PythonOperator(task_id="upload_calendrier",       python_callable=bkt.upload_calendrier)
    t_met = PythonOperator(task_id="upload_meteo_bresser",    python_callable=bkt.upload_meteo_bresser)
    t_e30 = PythonOperator(task_id="upload_enedis_30min",     python_callable=bkt.upload_enedis_30min)
    t_ej  = PythonOperator(task_id="upload_enedis_journalier",python_callable=bkt.upload_enedis_journalier)
    t_eh  = PythonOperator(task_id="upload_enedis_horaire",   python_callable=bkt.upload_enedis_horaire)
    t_t15 = PythonOperator(task_id="upload_tuya_15min",       python_callable=bkt.upload_tuya_15min)
    t_th  = PythonOperator(task_id="upload_tuya_horaire",     python_callable=bkt.upload_tuya_horaire)
    t_tj  = PythonOperator(task_id="upload_tuya_journalier",  python_callable=bkt.upload_tuya_journalier)
    t_tm  = PythonOperator(task_id="upload_tuya_mensuel",     python_callable=bkt.upload_tuya_mensuel)
    t_fin 