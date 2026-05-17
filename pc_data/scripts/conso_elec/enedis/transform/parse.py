# -*- coding: utf-8 -*-
"""
parse.py
========
Conversion des payloads JSON Enedis Data Hub en tuples prêts pour Postgres.

Format attendu des réponses Enedis (v5) — le JSON du type :

    {
        "meter_reading": {
            "usage_point_id": "22516914714270",
            "start": "2026-04-14",
            "end":   "2026-04-21",
            "quality": "BRUT",
            "reading_type": {...},
            "interval_reading": [
                {"value": "2249", "date": "2026-04-14 00:30:00",
                 "interval_length": "PT30M", "measure_type": "B"},
                ...
            ]
        }
    }

Les 3 endpoints (consumption_load_curve, daily_consumption,
daily_consumption_max_power) partagent cette structure mais diffèrent par :
  · `date` : horodatage 30-min (CLC), jour (daily), ou ts du pic (pmax)
  · `value` : Wh / 30 min (CLC), Wh / jour (daily), VA (pmax)
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any


def _mr(payload: dict[str, Any]) -> dict[str, Any]:
    """Déballe l'enveloppe `meter_reading` si présente."""
    if isinstance(payload, dict) and "meter_reading" in payload:
        return payload["meter_reading"] or {}
    return payload or {}


def parse_load_curve(
    payload: dict[str, Any],
    source_file: str | None = None,
) -> list[tuple]:
    """
    Parse la courbe de charge 30 min.

    Enedis renvoie le timestamp de FIN de chaque tranche (ex. 00:30 représente
    le pas 00:00 → 00:30). On normalise vers le timestamp de DEBUT pour que
    l'ordre temporel soit naturel et la clé primaire cohérente.

    Returns list of tuples for `enedis.f_conso_30min` :
        (prm, ts_debut, wh, source_file)
    """
    mr   = _mr(payload)
    prm  = mr.get("usage_point_id", "")
    rows: list[tuple] = []
    for r in mr.get("interval_reading", []) or []:
        ts_fin_str = r.get("date", "")
        val        = r.get("value", "")
        if not ts_fin_str or val in (None, ""):
            continue
        try:
            ts_fin   = datetime.strptime(ts_fin_str, "%Y-%m-%d %H:%M:%S")
            ts_debut = ts_fin - timedelta(minutes=30)
            wh       = int(val)
        except (ValueError, TypeError):
            continue
        rows.append((prm, ts_debut, wh, source_file))
    return rows


def parse_daily_consumption(
    payload: dict[str, Any],
    source_file: str | None = None,
) -> list[tuple]:
    """
    Parse la conso quotidienne (Wh par jour).

    Returns list of tuples for `enedis.f_conso_jour` :
        (prm, jour, wh, source_file)
    """
    mr   = _mr(payload)
    prm  = mr.get("usage_point_id", "")
    rows: list[tuple] = []
    for r in mr.get("interval_reading", []) or []:
        d   = r.get("date", "")
        val = r.get("value", "")
        if not d or val in (None, ""):
            continue
        try:
            # Format ISO 'YYYY-MM-DD'
            jour = date.fromisoformat(d[:10])
            wh   = int(val)
        except (ValueError, TypeError):
            continue
        rows.append((prm, jour, wh, source_file))
    return rows


def parse_daily_max_power(
    payload: dict[str, Any],
    source_file: str | None = None,
) -> list[tuple]:
    """
    Parse la puissance max quotidienne.

    Enedis renvoie la DATE avec l'HEURE PRÉCISE du pic (ex. "2026-04-14 11:37:15").
    On stocke à la fois le jour calendaire (PK) et le timestamp exact du pic.

    Returns list of tuples for `enedis.f_pmax_jour` :
        (prm, jour, pmax_va, ts_pmax, source_file)
    """
    mr   = _mr(payload)
    prm  = mr.get("usage_point_id", "")
    rows: list[tuple] = []
    for r in mr.get("interval_reading", []) or []:
        d   = r.get("date", "")
        val = r.get("value", "")
        if not d or val in (None, ""):
            continue
        try:
            # Le format peut être "YYYY-MM-DD HH:MM:SS" ou "YYYY-MM-DD"
            if len(d) > 10:
                ts_pmax = datetime.strptime(d, "%Y-%m-%d %H:%M:%S")
                jour    = ts_pmax.date()
            else:
                ts_pmax = None
                jour    = date.fromisoformat(d)
            pmax_va = int(val)
        except (ValueError, TypeError):
            continue
        rows.append((prm, jour, pmax_va, ts_pmax, source_file))
    return rows
