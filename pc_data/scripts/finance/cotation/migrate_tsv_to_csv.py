# -*- coding: utf-8 -*-
"""
migrate_tsv_to_csv.py
=====================
Migration ponctuelle : convertit tous les fichiers .tsv de intraday_db
vers le format .csv avec séparateur ';'.

À lancer UNE SEULE FOIS depuis le conteneur Airflow ou en local,
puis supprimer ce script.

Usage :
    python migrate_tsv_to_csv.py [--dry-run] [--intraday-dir <chemin>]

    --dry-run      Affiche ce qui serait fait sans rien écrire ni supprimer
    --intraday-dir Chemin vers intraday_db/ (défaut : variable d'env DATAOZ_COTATION_BASE
                   ou D:\\projet_dataoz\\pc_data\\data\\curated\\finance\\cotations)
"""

import argparse
import os
import sys
from pathlib import Path

import pandas as pd


def log(msg: str) -> None:
    print(msg, flush=True)


def migrate(intraday_dir: Path, dry_run: bool) -> None:
    if not intraday_dir.exists():
        log(f"[ERREUR] Répertoire introuvable : {intraday_dir}")
        sys.exit(1)

    tsv_files = sorted(intraday_dir.rglob("*.tsv"))

    if not tsv_files:
        log("Aucun fichier .tsv trouvé — rien à migrer.")
        return

    log(f"{'[DRY-RUN] ' if dry_run else ''}Fichiers .tsv à migrer : {len(tsv_files)}")
    log("")

    converted = 0
    skipped   = 0
    errors    = 0

    for tsv_path in tsv_files:
        csv_path = tsv_path.with_suffix(".csv")

        # Si le .csv existe déjà → fichier déjà migré, supprimer le .tsv orphelin
        if csv_path.exists():
            log(f"  [SKIP] {tsv_path.relative_to(intraday_dir)} → .csv déjà présent")
            if not dry_run:
                tsv_path.unlink()
            skipped += 1
            continue

        try:
            df = pd.read_csv(tsv_path, sep="\t", dtype=str, engine="python")

            # Ajouter row_sig si absent (fichiers très anciens)
            if "row_sig" not in df.columns and all(
                c in df.columns for c in ["ts", "ouv", "haut", "bas", "clot", "vol", "devise"]
            ):
                df["row_sig"] = (
                    df["ts"].astype(str) + "|" + df["ouv"].astype(str) + "|"
                    + df["haut"].astype(str) + "|" + df["bas"].astype(str) + "|"
                    + df["clot"].astype(str) + "|" + df["vol"].astype(str) + "|"
                    + df["devise"].astype(str)
                )

            rel = tsv_path.relative_to(intraday_dir)
            log(f"  [OK]   {rel}  →  {rel.with_suffix('.csv')}  ({len(df)} lignes)")

            if not dry_run:
                df.to_csv(csv_path, sep=";", index=False, encoding="utf-8-sig")
                tsv_path.unlink()   # supprimer le .tsv après conversion réussie

            converted += 1

        except Exception as e:
            log(f"  [ERR]  {tsv_path.relative_to(intraday_dir)} → {e}")
            errors += 1

    log("")
    log("=" * 55)
    log(f"{'[DRY-RUN] ' if dry_run else ''}Résultat de la migration :")
    log(f"  Convertis  : {converted}")
    log(f"  Ignorés    : {skipped}  (csv déjà présent)")
    log(f"  Erreurs    : {errors}")
    log("=" * 55)

    if dry_run:
        log("")
        log("Mode dry-run : aucun fichier écrit ni supprimé.")
        log("Relancer sans --dry-run pour appliquer la migration.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Migration intraday_db .tsv → .csv")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Affiche les actions sans rien modifier"
    )
    parser.add_argument(
        "--intraday-dir", default=None,
        help="Chemin vers le répertoire intraday_db/"
    )
    args = parser.parse_args()

    if args.intraday_dir:
        intraday_dir = Path(args.intraday_dir)
    else:
        base = Path(
            os.environ.get(
                "DATAOZ_COTATION_BASE",
                r"D:\projet_dataoz\pc_data\data\curated\finance\cotations",
            )
        )
        intraday_dir = base / "cotation" / "intraday_db"

    log(f"Répertoire intraday_db : {intraday_dir}")
    log("")
    migrate(intraday_dir, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
