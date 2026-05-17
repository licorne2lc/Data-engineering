# -*- coding: utf-8 -*-
"""
import_historique_csv.py
========================
Importateur idempotent pour backfiller `enedis.f_conso_30min` depuis le CSV
historique Enedis (export manuel espace client).

Format CSV attendu (separateur ';', encodage UTF-8) :
    Date;Time;Conso (W)
    2022-08-09;00:30:00;194.0
    ...

CONVENTIONS
-----------
  . Time = FIN de la tranche 30 min (heure locale Europe/Paris)
  . Conso (W) = puissance moyenne sur la tranche (Watts)
  . Wh stocke = W * 0.5  (arrondi entier)
  . ts_debut  = FIN - 30 min, converti en UTC avant stockage

GESTION DST (passage heure ete / spring-forward)
-------------------------------------------------
  Lors du passage a l'heure d'ete, deux lignes du CSV representent le meme
  instant UTC (ex. 02:30 et 03:30 le jour du changement). Python ne detecte
  pas ces doublons quand les deux datetime partagent le meme objet tzinfo
  ZoneInfo (comparaison naive sans offset). PostgreSQL, lui, les rejette avec
  CardinalityViolation.

  Solution : ts_debut est normalise en UTC immediatement dans _parse_row(),
  et le fichier de reference `table_chgt_heure.csv` est charge pour identifier
  les dates spring-forward, valider les doublons detectes et les logger.

  Fichier de reference (monte dans le container) :
    /opt/airflow/data/curated/calendaire/chgt_heure/table_chgt_heure.csv
    Format : Date,ete/hivers  ('ete' = spring-forward, 'hivers' = fall-back)

USAGE
-----
  python import_historique_csv.py \
      --csv  /opt/airflow/data/raw/conso_elec/enedis/_historique/Database_Enedis_30_min.csv \
      --prm  22130390723840 \
      --libelle "Import historique 2022-2024"

IDEMPOTENT : relancable sans risque (INSERT ... ON CONFLICT DO UPDATE).
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
from datetime import datetime, timedelta, timezone, date
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from load.load_enedis import upsert_conso_30min, upsert_prm  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s : %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("import_historique_csv")

TZ_PARIS = ZoneInfo("Europe/Paris")
TZ_UTC   = timezone.utc
BATCH_SIZE = 1000

# Chemin de la table de reference DST (dans le container Airflow)
DST_TABLE_PATH = Path(
    "/opt/airflow/data/curated/calendaire/chgt_heure/table_chgt_heure.csv"
)


# ---------------------------------------------------------------------------
# Chargement de la table de reference des changements d'heure
# ---------------------------------------------------------------------------

def _load_spring_forward_dates(path: Path) -> set[str]:
    """
    Charge les dates de passage a l'heure d'ete (spring-forward) depuis
    `table_chgt_heure.csv`. Ces dates peuvent produire des doublons UTC dans
    le CSV Enedis (deux tranches locales -> meme instant UTC).

    Retourne un ensemble de dates ISO (str) : {"2023-03-26", "2024-03-31", ...}
    """
    spring_dates: set[str] = set()
    if not path.exists():
        log.warning("  [DST] Table de reference introuvable : %s", path)
        log.warning("  [DST] La deduplication DST reste active (UTC), sans validation.")
        return spring_dates

    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            type_chgt = (row.get("ete/hivers") or "").strip().lower()
            d = (row.get("Date") or "").strip()
            if type_chgt == "ete" and d:
                spring_dates.add(d)

    log.info("  [DST] Table reference chargee : %d dates spring-forward", len(spring_dates))
    return spring_dates


# ---------------------------------------------------------------------------
# Parsing d'une ligne CSV -> tuple (prm, ts_debut_utc, wh, source_file)
# ---------------------------------------------------------------------------

def _parse_row(row: dict[str, str], prm: str, source_file: str) -> tuple | None:
    date_str = (row.get("Date") or "").strip()
    time_str = (row.get("Time") or "").strip()
    val_str  = (row.get("Conso (W)") or "").strip()

    if not date_str or not time_str or not val_str:
        return None

    # "23:59:59" est un artefact CSV pour "24:00:00" (derniere tranche du jour)
    if time_str == "23:59:59":
        ts_end_naive = datetime.fromisoformat(date_str) + timedelta(days=1)
    else:
        ts_end_naive = datetime.fromisoformat(date_str + "T" + time_str)

    ts_debut_naive = ts_end_naive - timedelta(minutes=30)

    # Normalisation UTC immediate.
    # Deux datetime Europe/Paris partageant le meme objet ZoneInfo sont compares
    # de facon naive par Python (sans appliquer l'offset), ce qui rend invisibles
    # les doublons spring-forward pour les dicts Python alors que PostgreSQL les
    # voit comme le meme TIMESTAMPTZ. La conversion en UTC resout les deux.
    ts_debut = ts_debut_naive.replace(tzinfo=TZ_PARIS).astimezone(TZ_UTC)

    try:
        watts = float(val_str.replace(",", "."))
    except ValueError:
        return None

    wh = max(0, int(round(watts * 0.5)))
    return (prm, ts_debut, wh, source_file)


# ---------------------------------------------------------------------------
# Importateur principal
# ---------------------------------------------------------------------------

def import_csv(csv_path: Path, prm: str, libelle: str | None = None) -> dict:
    if not csv_path.exists():
        raise FileNotFoundError("CSV introuvable : " + str(csv_path))

    log.info("=" * 70)
    log.info("IMPORT HISTORIQUE CSV  ->  enedis.f_conso_30min")
    log.info("=" * 70)
    log.info("  CSV     : %s", csv_path)
    log.info("  PRM     : %s", prm)
    log.info("  Libelle : %s", libelle or "(aucun)")
    log.info("  Batch   : %d lignes / upsert", BATCH_SIZE)

    # Charger la table de reference DST
    spring_forward_dates = _load_spring_forward_dates(DST_TABLE_PATH)

    upsert_prm(prm, libelle)
    log.info("  dim_prm : PRM enregistre OK")

    source_file = csv_path.name
    total_skip  = 0
    total_dup   = 0
    ts_min: datetime | None = None
    ts_max: datetime | None = None

    # Lecture + deduplication globale par (prm, ts_debut_utc).
    # Les doublons DST (spring-forward) sont detectes grace a la normalisation
    # UTC et valides contre la table de reference.
    seen: dict[tuple, tuple] = {}
    dup_details: list[tuple[str, str]] = []  # (date_str, ts_utc_str)

    with csv_path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            parsed = _parse_row(row, prm, source_file)
            if parsed is None:
                total_skip += 1
                continue
            key = (parsed[0], parsed[1])   # (prm, ts_debut_utc)
            if key in seen:
                total_dup += 1
                date_str = (row.get("Date") or "").strip()
                ts_utc   = parsed[1].isoformat()
                dup_details.append((date_str, ts_utc))
            else:
                seen[key] = parsed

    # Validation des doublons contre la table de reference
    if total_dup:
        log.warning("  [DST] %d doublon(s) spring-forward detecte(s) :", total_dup)
        for d_str, ts_str in dup_details:
            in_ref = d_str in spring_forward_dates
            ref_label = "OK dans table ref" if in_ref else "ABSENT de la table ref !"
            log.warning("        date=%s  ts_utc=%s  [%s]", d_str, ts_str, ref_label)
    else:
        log.info("  [DST] Aucun doublon spring-forward detecte.")

    # Controle inverse : dates spring-forward du tableau sans doublon dans le CSV
    if spring_forward_dates:
        dup_dates_found = {d for d, _ in dup_details}
        # Filtrer aux dates dans la periode couverte par le CSV
        if ts_min and ts_max:
            min_date = ts_min.date().isoformat()
            max_date = ts_max.date().isoformat()
            expected_in_range = {
                d for d in spring_forward_dates
                if min_date <= d <= max_date
            }
            missing = expected_in_range - dup_dates_found
            if missing:
                for m in sorted(missing):
                    log.info("  [DST] Date spring-forward %s : pas de doublon (normal si hors periode CSV)", m)

    # Upsert par batchs
    rows_dedup = list(seen.values())

    for row_tuple in rows_dedup:
        ts = row_tuple[1]
        if ts_min is None or ts < ts_min:
            ts_min = ts
        if ts_max is None or ts > ts_max:
            ts_max = ts

    total_ok = 0
    for i in range(0, len(rows_dedup), BATCH_SIZE):
        batch = rows_dedup[i : i + BATCH_SIZE]
        upsert_conso_30min(batch)
        total_ok += len(batch)
        log.info("  ... %6d upserts cumules", total_ok)

    log.info("-" * 70)
    log.info("  Lignes importees       : %d", total_ok)
    log.info("  Lignes ignorees        : %d (vides / parse error)", total_skip)
    log.info("  Doublons DST elimines  : %d", total_dup)
    if ts_min and ts_max:
        log.info("  Periode couverte : %s  ->  %s",
                 ts_min.isoformat(), ts_max.isoformat())
    log.info("=" * 70)

    return {
        "fichier":    source_file,
        "prm":        prm,
        "lignes_ok":  total_ok,
        "ignorees":   total_skip,
        "doublons":   total_dup,
        "ts_min":     ts_min.isoformat() if ts_min else None,
        "ts_max":     ts_max.isoformat() if ts_max else None,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _default_csv_path() -> Path:
    candidates = [
        Path("/opt/airflow/data/raw/conso_elec/enedis/_historique/Database_Enedis_30_min.csv"),
        Path(__file__).resolve().parents[3]
            / "data" / "raw" / "conso_elec" / "enedis" / "_historique"
            / "Database_Enedis_30_min.csv",
    ]
    for c in candidates:
        if c.exists():
            return c
    return candidates[0]


def main() -> int:
    p = argparse.ArgumentParser(description="Import CSV historique Enedis 30 min")
    p.add_argument("--csv",     type=Path, default=_default_csv_path())
    p.add_argument("--prm",     type=str,
                   default=os.getenv("PRM_ID") or os.getenv("ENEDIS_TEST_PRM"))
    p.add_argument("--libelle", type=str, default="Import historique CSV")
    args = p.parse_args()

    if not args.prm:
        log.error("Aucun PRM fourni (ni --prm, ni env PRM_ID / ENEDIS_TEST_PRM).")
        return 2

    try:
        import_csv(args.csv, args.prm, args.libelle)
    except Exception as e:
        log.exception("Echec import : %s", e)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
