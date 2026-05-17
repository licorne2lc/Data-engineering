# -*- coding: utf-8 -*-
"""
load_enedis.py
==============
Fonctions d'upsert Postgres pour les 4 tables du schéma `enedis`.

Toutes les opérations sont IDEMPOTENTES (INSERT ... ON CONFLICT DO UPDATE) :
on peut relancer le DAG autant de fois qu'on veut sans doublons ni doublures.

Tables cibles :
  · enedis.dim_prm
  · enedis.f_conso_30min
  · enedis.f_conso_jour
  · enedis.f_pmax_jour
  · enedis.api_call_log
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

import psycopg2.extras

from load.db import get_conn

log = logging.getLogger(__name__)


# ═════════════════════════════════════════════════════════════════════════════
# DIMENSION PRM
# ═════════════════════════════════════════════════════════════════════════════

def upsert_prm(prm: str, libelle: str | None = None) -> int:
    """
    Insère ou met à jour le PRM dans enedis.dim_prm.
    Retourne 1 si ligne insérée/modifiée.
    """
    if not prm:
        return 0
    sql = """
        INSERT INTO enedis.dim_prm (prm, libelle, actif, cree_le, maj_le)
        VALUES (%s, %s, TRUE, now(), now())
        ON CONFLICT (prm) DO UPDATE
           SET libelle = COALESCE(EXCLUDED.libelle, enedis.dim_prm.libelle),
               actif   = TRUE,
               maj_le  = now();
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (prm, libelle))
        return cur.rowcount


# ═════════════════════════════════════════════════════════════════════════════
# FACTS — upserts
# ═════════════════════════════════════════════════════════════════════════════

_SQL_UPSERT_30MIN = """
    INSERT INTO enedis.f_conso_30min (prm, ts_debut, wh, source_file)
    VALUES %s
    ON CONFLICT (prm, ts_debut) DO UPDATE
       SET wh          = EXCLUDED.wh,
           source_file = EXCLUDED.source_file,
           loaded_at   = now();
"""

_SQL_UPSERT_DAILY = """
    INSERT INTO enedis.f_conso_jour (prm, jour, wh, source_file)
    VALUES %s
    ON CONFLICT (prm, jour) DO UPDATE
       SET wh          = EXCLUDED.wh,
           source_file = EXCLUDED.source_file,
           loaded_at   = now();
"""

_SQL_UPSERT_PMAX = """
    INSERT INTO enedis.f_pmax_jour (prm, jour, pmax_va, ts_pmax, source_file)
    VALUES %s
    ON CONFLICT (prm, jour) DO UPDATE
       SET pmax_va     = EXCLUDED.pmax_va,
           ts_pmax     = EXCLUDED.ts_pmax,
           source_file = EXCLUDED.source_file,
           loaded_at   = now();
"""


def _execute_values(sql: str, rows: list[tuple], page_size: int = 500) -> int:
    if not rows:
        return 0
    with get_conn() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=page_size)
        return cur.rowcount


def upsert_conso_30min(rows: list[tuple]) -> int:
    """rows : [(prm, ts_debut, wh, source_file)]"""
    return _execute_values(_SQL_UPSERT_30MIN, rows)


def upsert_conso_jour(rows: list[tuple]) -> int:
    """rows : [(prm, jour, wh, source_file)]"""
    return _execute_values(_SQL_UPSERT_DAILY, rows)


def upsert_pmax_jour(rows: list[tuple]) -> int:
    """rows : [(prm, jour, pmax_va, ts_pmax, source_file)]"""
    return _execute_values(_SQL_UPSERT_PMAX, rows)


# ═════════════════════════════════════════════════════════════════════════════
# API CALL LOG
# ═════════════════════════════════════════════════════════════════════════════

def log_api_call(
    endpoint:    str,
    prm:         str | None = None,
    date_debut:  date | str | None = None,
    date_fin:    date | str | None = None,
    http_status: int | None = None,
    n_points:    int | None = None,
    duree_ms:    int | None = None,
    erreur:      str | None = None,
) -> None:
    """Ajoute une ligne dans enedis.api_call_log (traçabilité des appels)."""
    sql = """
        INSERT INTO enedis.api_call_log
            (endpoint, prm, date_debut, date_fin,
             http_status, n_points, duree_ms, erreur)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (
                endpoint, prm, date_debut, date_fin,
                http_status, n_points, duree_ms, erreur,
            ))
    except Exception as e:
        # Le log ne doit jamais faire planter le DAG.
        log.warning("api_call_log insertion failed : %s", e)
