# -*- coding: utf-8 -*-
"""
migration_add_source_column.py
==============================
ONE-SHOT (2026-04-28) : ajoute colonne 'source' a Database_Enedis_30_min.csv
  - 'manuel' partout par defaut
  - 'auto' pour 2026-04-22 -> 2026-04-26 (240 lignes integrees via T19)
"""
from __future__ import annotations

import argparse, logging, shutil, sys
from datetime import datetime
from pathlib import Path
import pandas as pd

_REPO = Path(__file__).resolve().parents[3]
CURATED_DIR  = _REPO / "data" / "curated" / "conso_elec" / "enedis"
DATABASE_CSV = CURATED_DIR / "Database_Enedis_30_min.csv"
ARCHIVE_DIR  = CURATED_DIR / "archive"
SCRAP_DATE_START = "2026-04-22"
SCRAP_DATE_END   = "2026-04-26"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)-7s] %(message)s",
                    datefmt="%H:%M:%S", stream=sys.stdout)
log = logging.getLogger("migration_source")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    log.info("=" * 70)
    log.info("MIGRATION ONE-SHOT : ajout colonne source")
    log.info("=" * 70)
    log.info("DB        : %s", DATABASE_CSV)
    log.info("Plage auto: %s -> %s", SCRAP_DATE_START, SCRAP_DATE_END)
    log.info("Mode dry-run: %s", args.dry_run)
    log.info("")

    if not DATABASE_CSV.exists():
        log.error("DB introuvable")
        sys.exit(2)

    log.info("[1] Lecture DB...")
    df = pd.read_csv(DATABASE_CSV, sep=";", dtype={"Date": str, "Time": str})
    log.info("    %d lignes / colonnes : %s", len(df), list(df.columns))

    if "source" in df.columns:
        log.warning("Colonne 'source' deja presente — migration deja appliquee.")
        log.info("    Repartition : %s", df["source"].value_counts().to_dict())
        sys.exit(0)

    expected = ["Date", "Time", "Conso (W)"]
    if list(df.columns) != expected:
        log.error("Schema inattendu. Attendu %s, trouve %s", expected, list(df.columns))
        sys.exit(2)

    log.info("")
    log.info("[2] Backfill colonne source...")
    df["source"] = "manuel"
    mask_auto = (df["Date"] >= SCRAP_DATE_START) & (df["Date"] <= SCRAP_DATE_END)
    n_auto    = int(mask_auto.sum())
    n_manuel  = int((~mask_auto).sum())
    df.loc[mask_auto, "source"] = "auto"
    log.info("    'manuel' : %d lignes", n_manuel)
    log.info("    'auto'   : %d lignes", n_auto)
    log.info("    total    : %d lignes", len(df))

    log.info("")
    log.info("[3] Reordering : Date ; Time ; source ; Conso (W)")
    df = df[["Date", "Time", "source", "Conso (W)"]]

    log.info("")
    log.info("[4] Apercu...")
    for _, row in df[df["source"] == "manuel"].head(2).iterrows():
        log.info("    %s %s | %s | %s", row["Date"], row["Time"], row["source"], row["Conso (W)"])
    log.info("    ...")
    for _, row in df[df["source"] == "auto"].head(2).iterrows():
        log.info("    %s %s | %s | %s", row["Date"], row["Time"], row["source"], row["Conso (W)"])

    log.info("")
    if args.dry_run:
        log.info("[5] DRY-RUN : pas d'ecriture.")
        return

    log.info("[5] Sauvegarde + ecriture...")
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = ARCHIVE_DIR / f"pre_add_source_Database_Enedis_30_min_{ts}.csv"
    shutil.copy2(DATABASE_CSV, backup)
    log.info("    backup ecrit : %s", backup.name)

    df.to_csv(DATABASE_CSV, sep=";", index=False, encoding="utf-8")
    log.info("    DB ecrite : %s (%d lignes)", DATABASE_CSV.name, len(df))

    log.info("")
    log.info("=" * 70)
    log.info("MIGRATION TERMINEE")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
