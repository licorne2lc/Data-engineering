# -*- coding: utf-8 -*-
"""
migration_unify_db.py
=====================
Script de migration ONE-SHOT (apres passage au DAG unifie 2026-04-27).

Fusionne Database_Enedis_30_min_scrap.csv -> Database_Enedis_30_min.csv,
priorite manuel sur conflits (keep="first" sur (Date,Time) communs).

Usage:
    python3 scripts/conso_elec/enedis/migration_unify_db.py [--dry-run]
"""
from __future__ import annotations

import argparse
import logging
import shutil
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[3]
CURATED_DIR        = _REPO / "data" / "curated" / "conso_elec" / "enedis"
DATABASE_CSV       = CURATED_DIR / "Database_Enedis_30_min.csv"
DATABASE_CSV_SCRAP = CURATED_DIR / "Database_Enedis_30_min_scrap.csv"
ARCHIVE_DIR        = CURATED_DIR / "archive"

logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s [%(levelname)-7s] %(message)s",
    datefmt = "%H:%M:%S",
    stream  = sys.stdout,
)
log = logging.getLogger("migration_unify")


def load_csv(p: Path) -> pd.DataFrame:
    if not p.exists():
        log.warning("CSV introuvable : %s", p)
        return pd.DataFrame(columns=["Date", "Time", "Conso (W)"])
    df = pd.read_csv(p, sep=";", dtype={"Date": str, "Time": str})
    log.info("  lu %s : %d lignes", p.name, len(df))
    return df


def audit_divergences(df_db: pd.DataFrame, df_scrap: pd.DataFrame) -> int:
    if df_db.empty or df_scrap.empty:
        return 0
    db_idx    = df_db.set_index(["Date", "Time"])["Conso (W)"]
    scrap_idx = df_scrap.set_index(["Date", "Time"])["Conso (W)"]
    common    = db_idx.index.intersection(scrap_idx.index)
    if not len(common):
        log.info("  aucun (Date,Time) commun entre db et scrap")
        return 0
    cmp = pd.DataFrame({
        "db":    db_idx.loc[common].astype(float),
        "scrap": scrap_idx.loc[common].astype(float),
    })
    cmp["delta_abs"] = (cmp["scrap"] - cmp["db"]).abs()
    n_diverge = int((cmp["delta_abs"] >= 0.01).sum())
    log.info("  cles communes : %d  | divergences (delta>=0.01): %d",
             len(common), n_diverge)
    if n_diverge > 0:
        diverging = cmp[cmp["delta_abs"] >= 0.01].head(10)
        log.warning("  echantillon divergences (db conservee, priorite manuel) :")
        for (d, t), row in diverging.iterrows():
            log.warning("    %s %s  db=%.3f  scrap=%.3f  delta=%.3f",
                        d, t, row["db"], row["scrap"], row["delta_abs"])
    return n_diverge


def merge_priority_manuel(df_db: pd.DataFrame,
                          df_scrap: pd.DataFrame) -> pd.DataFrame:
    if df_scrap.empty:
        log.info("  base scrap vide, rien a fusionner")
        return df_db.copy()
    merged = (
        pd.concat([df_db, df_scrap], ignore_index=True)
        .drop_duplicates(subset=["Date", "Time"], keep="first")
        .reset_index(drop=True)
    )
    return merged


def sort_by_datetime(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df["_dt"] = pd.to_datetime(
        df["Date"] + " " + df["Time"],
        format="%Y-%m-%d %H:%M:%S",
        errors="coerce",
    )
    df = df.sort_values("_dt", kind="mergesort").drop(columns=["_dt"])
    return df.reset_index(drop=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true",
                    help="Affiche le diff sans ecrire")
    args = ap.parse_args()

    log.info("=" * 70)
    log.info("MIGRATION ONE-SHOT : DB scrap -> DB unifiee")
    log.info("=" * 70)
    log.info("DB manuelle : %s", DATABASE_CSV)
    log.info("DB scrap    : %s", DATABASE_CSV_SCRAP)
    log.info("Archive     : %s", ARCHIVE_DIR)
    log.info("Mode dry-run: %s", args.dry_run)
    log.info("")

    log.info("[1] Lecture des bases...")
    df_db    = load_csv(DATABASE_CSV)
    df_scrap = load_csv(DATABASE_CSV_SCRAP)

    if df_db.empty and df_scrap.empty:
        log.error("Les 2 bases sont vides ou introuvables. Abandon.")
        sys.exit(2)

    log.info("")
    log.info("[2] Audit des divergences (priorite : manuel)...")
    audit_divergences(df_db, df_scrap)

    log.info("")
    log.info("[3] Fusion (keep='first' sur (Date,Time) communs)...")
    df_merged = merge_priority_manuel(df_db, df_scrap)
    df_merged = sort_by_datetime(df_merged)

    n_before = len(df_db)
    n_after  = len(df_merged)
    n_added  = n_after - n_before
    log.info("  db avant      : %d lignes", n_before)
    log.info("  db apres      : %d lignes", n_after)
    log.info("  ajout net     : +%d lignes (issues du scrap)", n_added)

    log.info("")
    log.info("[4] Plage temporelle couverte...")
    if not df_merged.empty:
        log.info("  debut : %s %s", df_merged.iloc[0]["Date"],
                 df_merged.iloc[0]["Time"])
        log.info("  fin   : %s %s", df_merged.iloc[-1]["Date"],
                 df_merged.iloc[-1]["Time"])

    log.info("")
    if args.dry_run:
        log.info("[5] DRY-RUN : pas d'ecriture sur disque.")
        log.info("    (Pour appliquer : reexecuter sans --dry-run)")
        return

    log.info("[5] Sauvegarde + ecriture...")
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    if DATABASE_CSV.exists():
        ts     = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup = ARCHIVE_DIR / f"pre_unify_Database_Enedis_30_min_{ts}.csv"
        shutil.copy2(DATABASE_CSV, backup)
        log.info("  backup ecrit : %s", backup.name)

    df_merged.to_csv(DATABASE_CSV, sep=";", index=False, encoding="utf-8")
    log.info("  DB unifiee ecrite : %s (%d lignes)", DATABASE_CSV.name, n_after)

    log.info("")
    log.info("=" * 70)
    log.info("MIGRATION TERMINEE AVEC SUCCES")
    log.info("=" * 70)
    log.info("Etapes suivantes (manuelles) :")
    log.info("  1. Verifier la DB unifiee : %s", DATABASE_CSV)
    log.info("  2. Si OK, supprimer la DB scrap obsolete :")
    log.info("       %s", DATABASE_CSV_SCRAP)
    log.info("  3. Et le dossier archive_scrap obsolete :")
    log.info("       %s", CURATED_DIR / "archive_scrap")


if __name__ == "__main__":
    main()
