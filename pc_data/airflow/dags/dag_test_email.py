# -*- coding: utf-8 -*-
"""
dag_test_email.py
==================
DAG Airflow — Test d'envoi d'email SMTP

Déclenché manuellement pour valider la configuration SMTP.
Envoie un vrai email HTML de test à l'adresse configurée.

Déclenchement : manuel uniquement (schedule_interval=None)
"""

import os
import smtplib
import ssl
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from airflow import DAG
from airflow.operators.python import PythonOperator

ALERT_EMAIL   = "licorne2lc@msn.com"
SMTP_HOST     = os.getenv("AIRFLOW__SMTP__SMTP_HOST",     "smtp.gmail.com")
SMTP_PORT     = int(os.getenv("AIRFLOW__SMTP__SMTP_PORT", "587"))
SMTP_USER     = os.getenv("AIRFLOW__SMTP__SMTP_USER",     "")
SMTP_PASSWORD = os.getenv("AIRFLOW__SMTP__SMTP_PASSWORD", "")


# ══════════════════════════════════════════════════════════════════════════════
# TÂCHE 1 — Test de connexion SMTP (sans envoi)
# ══════════════════════════════════════════════════════════════════════════════

def test_smtp_connection(**kwargs):
    """Vérifie que la connexion et l'authentification SMTP fonctionnent."""
    print("=" * 60)
    print("ÉTAPE 1 — TEST DE CONNEXION SMTP")
    print("=" * 60)
    print(f"  Hôte     : {SMTP_HOST}:{SMTP_PORT}")
    print(f"  Compte   : {SMTP_USER}")

    if not SMTP_USER or not SMTP_PASSWORD:
        raise ValueError("SMTP_USER ou SMTP_PASSWORD non définis dans les variables d'environnement.")

    context = ssl.create_default_context()
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as server:
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()
        server.login(SMTP_USER, SMTP_PASSWORD)
        print("  ✅  Connexion et authentification SMTP réussies.")

    kwargs["ti"].xcom_push(key="smtp_conn", value="OK")


# ══════════════════════════════════════════════════════════════════════════════
# TÂCHE 2 — Envoi d'un email de test
# ══════════════════════════════════════════════════════════════════════════════

def send_test_email(**kwargs):
    """Envoie un email HTML de test pour valider l'envoi bout-en-bout."""
    print("=" * 60)
    print("ÉTAPE 2 — ENVOI DE L'EMAIL DE TEST")
    print("=" * 60)

    now      = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    subject  = f"✅ DataOZ — Test SMTP OK [{now[:10]}]"

    html_body = f"""
    <html><body style="font-family:Arial,sans-serif;color:#333;">
      <h2 style="color:#27ae60;">✅ DataOZ — Alerte email opérationnelle</h2>
      <p>Cet email confirme que la configuration SMTP Airflow fonctionne correctement.</p>
      <table border="0" cellspacing="0" cellpadding="0"
             style="border-collapse:collapse;width:100%;max-width:500px;">
        <tr style="background:#f0f0f0;">
          <td style="padding:8px 12px;"><strong>Hôte SMTP</strong></td>
          <td style="padding:8px 12px;">{SMTP_HOST}:{SMTP_PORT}</td>
        </tr>
        <tr>
          <td style="padding:8px 12px;"><strong>Expéditeur</strong></td>
          <td style="padding:8px 12px;">{SMTP_USER}</td>
        </tr>
        <tr style="background:#f0f0f0;">
          <td style="padding:8px 12px;"><strong>Destinataire</strong></td>
          <td style="padding:8px 12px;">{ALERT_EMAIL}</td>
        </tr>
        <tr>
          <td style="padding:8px 12px;"><strong>Date</strong></td>
          <td style="padding:8px 12px;">{now} UTC</td>
        </tr>
      </table>
      <p style="margin-top:16px;color:#555;">
        Les alertes automatiques seront envoyées à cette adresse lorsque
        <strong>dag_check_pipeline</strong> détectera une anomalie.
      </p>
      <hr style="margin-top:24px;">
      <small style="color:#888;">DataOZ Monitoring — dag_test_email</small>
    </body></html>
    """

    context = ssl.create_default_context()
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = SMTP_USER
    msg["To"]      = ALERT_EMAIL
    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as server:
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(SMTP_USER, ALERT_EMAIL, msg.as_string())

    print(f"  ✅  Email de test envoyé → {ALERT_EMAIL}")
    kwargs["ti"].xcom_push(key="email_sent", value="OK")


# ══════════════════════════════════════════════════════════════════════════════
# DAG DEFINITION
# ══════════════════════════════════════════════════════════════════════════════

default_args = {
    "owner":            "dataoz",
    "depends_on_past":  False,
    "retries":          0,
    "email_on_failure": False,
}

with DAG(
    dag_id="dag_test_email",
    description="Test manuel de la configuration SMTP (connexion + envoi email)",
    schedule_interval=None,   # manuel uniquement
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["dataoz", "monitoring", "smtp", "test"],
) as dag:

    t1 = PythonOperator(
        task_id="test_smtp_connection",
        python_callable=test_smtp_connection,
    )

    t2 = PythonOperator(
        task_id="send_test_email",
        python_callable=send_test_email,
    )

    t1 >> t2
