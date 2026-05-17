# -*- coding: utf-8 -*-
"""
db.py
=====
Helpers de connexion Postgres pour le pipeline Tuya (granularités fines).

Variable d'environnement lue :
    CONSO_ELEC_DB_URL   URL Postgres (ex. postgresql://user:pwd@postgres:5432/airflow)

Usage :
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
"""
from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import psycopg2
import psycopg2.extras

log = logging.getLogger(__name__)


def _dsn() -> str:
    dsn = os.environ.get("CONSO_ELEC_DB_URL", "").strip()
    if not dsn:
        raise RuntimeError(
            "CONSO_ELEC_DB_URL manquante — "
            "à définir dans docker-compose.yml sous x-airflow-common / environment"
        )
    return dsn


@contextmanager
def get_conn() -> Iterator[psycopg2.extensions.connection]:
    """Connexion Postgres en context manager, commit automatique si pas d'erreur."""
    conn = psycopg2.connect(_dsn())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def execute_sql_file(path: str | Path) -> None:
    """Exécute un fichier SQL entier (DDL idempotent, par exemple)."""
    sql = Path(path).read_text(encoding="utf-8")
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
    log.info("SQL exécuté : %s", path)


def execute_values(sql: str, rows: list[tuple], page_size: int = 1000) -> int:
    """Exécute un INSERT ... VALUES %s via psycopg2.extras.execute_values."""
    if not rows:
        return 0
    with get_conn() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=page_size)
        return cur.rowcount
