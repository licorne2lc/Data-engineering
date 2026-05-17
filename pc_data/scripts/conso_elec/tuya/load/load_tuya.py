# -*- coding: utf-8 -*-
"""
load_tuya.py
============
Charge les CSV Tuya de granularité FINE (heure + 15 min) dans Postgres.

Les mois/jours restent en CSV (pas de table Postgres pour eux).

Fonctions publiques (appelées par le DAG) :
    upsert_appareils(appareils)          → UPSERT dim_appareil
    scan_and_load_hf(dossiers)           → charge *.csv heure/15min des dossiers passés

Sémantique :
    · UPSERT idempotent via ON CONFLICT DO UPDATE
    · Les périodes futures (hypothétiquement retournées par Tuya) sont ignorées
    · Les timestamps sont interprétés en Europe/Paris, stockés TIMESTAMPTZ
    · Le loader tolère les NUL bytes parasites (cf. bug occasionnel export Tuya)
"""
from __future__ import annotations

import csv
import logging
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

from db import execute_values, get_conn

log = logging.getLogger(__name__)

try:
    from zoneinfo import ZoneInfo
    PARIS = ZoneInfo("Europe/Paris")
except ImportError:
    PARIS = timezone(timedelta(hours=1))

# Regex de détection : on ne s'intéresse qu'à heures + 15min
_RX_HEURES = re.compile(r"_heures\.csv$", re.I)
_RX_15MIN  = re.compile(r"_15min\.csv$",  re.I)

# Un appareil_id Tuya est une chaîne de 22 caractères alphanumériques
# (ex. bf28133c02cbbd0433cefp). On exige exactement cette longueur pour
# rejeter les lignes tronquées (ex. bf4374fce2062c = 14 car, bf4374fce2062c0e02dxu
# = 21 car) et éviter qu'une seule ligne corrompue fasse échouer tout le batch
# sur une violation de clé étrangère.
_RX_APPAREIL_ID = re.compile(r"^[0-9a-z]{22}$", re.I)


# =============================================================================
# Dimension appareils
# =============================================================================

def upsert_appareils(appareils: list[dict]) -> int:
    """
    Ajoute/met à jour les appareils dans dim_appareil.
    Les appareils présents en base mais absents de `appareils` sont passés à
    `actif = FALSE` (soft delete).
    """
    if not appareils:
        log.warning("upsert_appareils : liste vide")
        return 0

    rows = [
        (
            a["id"],
            a.get("name") or "sans_nom",
            a.get("model") or None,
            a.get("category") or None,
            True,
            datetime.now(tz=PARIS),
        )
        for a in appareils if a.get("id")
    ]
    sql = """
        INSERT INTO tuya.dim_appareil (appareil_id, nom, model, category, actif, maj_le)
        VALUES %s
        ON CONFLICT (appareil_id) DO UPDATE SET
            nom      = EXCLUDED.nom,
            model    = COALESCE(EXCLUDED.model, tuya.dim_appareil.model),
            category = COALESCE(EXCLUDED.category, tuya.dim_appareil.category),
            actif    = TRUE,
            maj_le   = now();
    """
    nb = execute_values(sql, rows)

    # Soft delete des appareils absents de la dernière sync
    ids_actifs = [r[0] for r in rows]
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE tuya.dim_appareil SET actif = FALSE, maj_le = now() "
            "WHERE actif = TRUE AND appareil_id <> ALL(%s);",
            (ids_actifs,),
        )
    log.info("✓ dim_appareil : %d upsert(s)", nb)
    return nb


# =============================================================================
# Parsing périodes fines
# =============================================================================

def _parse_heure(cle: str) -> datetime | None:
    """YYYYMMDDHH → datetime tz Europe/Paris."""
    cle = str(cle).strip()
    if len(cle) != 10 or not cle.isdigit():
        return None
    try:
        return datetime(
            int(cle[:4]), int(cle[4:6]), int(cle[6:8]),
            int(cle[8:10]), tzinfo=PARIS,
        )
    except ValueError:
        return None


def _parse_15min(cle: str) -> datetime | None:
    """YYYYMMDDHHMM → datetime tz Europe/Paris."""
    cle = str(cle).strip()
    if len(cle) != 12 or not cle.isdigit():
        return None
    try:
        return datetime(
            int(cle[:4]), int(cle[4:6]), int(cle[6:8]),
            int(cle[8:10]), int(cle[10:12]), tzinfo=PARIS,
        )
    except ValueError:
        return None


def _tronquer_futur_dt(ts: datetime) -> bool:
    return ts > datetime.now(tz=PARIS)


def _lire_csv(chemin: Path) -> list[dict]:
    """Lit un CSV Tuya, tolérant aux NUL parasites."""
    raw = chemin.read_bytes().replace(b"\x00", b"")
    text = raw.decode("utf-8-sig", errors="replace")
    return list(csv.DictReader(text.splitlines(), delimiter=";"))


# =============================================================================
# UPSERT par granularité
# =============================================================================

_SQL_UPSERT_HEURE = """
    INSERT INTO tuya.f_conso_heure (appareil_id, ts_debut, kwh, source_file)
    VALUES %s
    ON CONFLICT (appareil_id, ts_debut) DO UPDATE
       SET kwh         = EXCLUDED.kwh,
           source_file = EXCLUDED.source_file,
           loaded_at   = now();
"""

_SQL_UPSERT_15MIN = """
    INSERT INTO tuya.f_conso_15min (appareil_id, ts_debut, kwh, source_file)
    VALUES %s
    ON CONFLICT (appareil_id, ts_debut) DO UPDATE
       SET kwh         = EXCLUDED.kwh,
           source_file = EXCLUDED.source_file,
           loaded_at   = now();
"""


def _load_fichier_heure(chemin: Path) -> int:
    lignes = _lire_csv(chemin)
    rows = []
    rejetes = 0
    for l in lignes:
        aid = (l.get("appareil_id") or "").strip()
        if not aid or not _RX_APPAREIL_ID.match(aid):
            rejetes += 1
            continue
        try:
            kwh = float(l.get("kWh") or l.get("kwh") or 0)
        except (ValueError, TypeError):
            continue
        h = _parse_heure(l.get("heure", ""))
        if h is None or _tronquer_futur_dt(h) or kwh <= 0:
            continue
        rows.append((aid, h, kwh, chemin.name))
    nb = execute_values(_SQL_UPSERT_HEURE, rows)
    if rejetes:
        log.warning("   %s : %d ligne(s) rejetée(s) (appareil_id invalide)",
                    chemin.name, rejetes)
    log.info("   %-50s %5d lues → %5d upsert (heure)", chemin.name, len(lignes), nb)
    return nb


def _load_fichier_15min(chemin: Path) -> int:
    lignes = _lire_csv(chemin)
    rows = []
    rejetes = 0
    for l in lignes:
        aid = (l.get("appareil_id") or "").strip()
        if not aid or not _RX_APPAREIL_ID.match(aid):
            rejetes += 1
            continue
        try:
            kwh = float(l.get("kWh") or l.get("kwh") or 0)
        except (ValueError, TypeError):
            continue
        q = _parse_15min(l.get("periode_15min", ""))
        if q is None or _tronquer_futur_dt(q):
            continue
        rows.append((aid, q, kwh, chemin.name))
    nb = execute_values(_SQL_UPSERT_15MIN, rows)
    if rejetes:
        log.warning("   %s : %d ligne(s) rejetée(s) (appareil_id invalide)",
                    chemin.name, rejetes)
    log.info("   %-50s %5d lues → %5d upsert (15min)", chemin.name, len(lignes), nb)
    return nb


# =============================================================================
# Scan
# =============================================================================

def scan_and_load_hf(dossiers: Iterable[str | Path]) -> dict:
    """
    Parcourt chaque dossier, charge les CSV heure et 15min dans Postgres.
    `dossiers` : liste de dossiers à scanner (ex. [raw/Tuya, raw/Tuya/_historique]).
    Retourne les compteurs agrégés.
    """
    total_heure = 0
    total_15min = 0
    fichiers_heure = 0
    fichiers_15min = 0

    for d in dossiers:
        dossier = Path(d)
        if not dossier.exists():
            log.warning("Dossier introuvable, ignoré : %s", dossier)
            continue
        # Non récursif : on traite chaque dossier passé explicitement
        for f in sorted(dossier.glob("*.csv")):
            if f.name.startswith("_"):
                continue  # synthèses globales (préfixe _)
            if _RX_HEURES.search(f.name):
                try:
                    total_heure += _load_fichier_heure(f)
                    fichiers_heure += 1
                except Exception as e:
                    log.exception("Échec heures %s : %s", f.name, e)
            elif _RX_15MIN.search(f.name):
                try:
                    total_15min += _load_fichier_15min(f)
                    fichiers_15min += 1
                except Exception as e:
                    log.exception("Échec 15min %s : %s", f.name, e)

    return {
        "fichiers_heure":   fichiers_heure,
        "fichiers_15min":   fichiers_15min,
        "total_heure":      total_heure,
        "total_15min":      total_15min,
        "total_upsert":     total_heure + total_15min,
    }
