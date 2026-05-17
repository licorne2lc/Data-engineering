# -*- coding: utf-8 -*-
"""
agregation_horaire_enedis.py
============================
Agrégation locale 30 min → horaire pour la consommation Enedis.

Principe
--------
L'API Enedis retourne des tranches de 30 min en puissance moyenne (W).
Chaque heure H contient 2 tranches :
    Tranche 1 : Début H:00 → Fin H:30  (Time = "H:30:00")
    Tranche 2 : Début H:30 → Fin H+1:00  (Time = "(H+1):00:00")
    Exception : Tranche 2 de l'heure 23 → Fin = "23:59:59" (convention Enedis)

Conversion : énergie kWh = somme(W) × 0.5 h / 1000 = somme(W) / 2000

Schéma de la database horaire produite
---------------------------------------
    fichier  : data/curated/conso_elec/enedis/database_enedis_horaire.csv
    colonnes : Date ; Heure ; source ; Conso (kWh)
        Date          date locale Europe/Paris (YYYY-MM-DD)
        Heure         heure de DÉBUT de la plage (0-23, entier)
        source        'agregat_30min'
        Conso (kWh)   somme des 2 tranches ÷ 2000, arrondi 4 décimales

Seules les heures ayant exactement 2 tranches complètes sont retenues.
Les heures partielles (J-1 en cours d'alimentation) sont exclues et listées.

Versionnement
-------------
À chaque exécution :
    • database_enedis_horaire.csv (courant) est écrit
    • snapshot database_enedis_horaire_YYYYMMDD.csv déposé dans
      archive/ (rotation : 30 fichiers max)
"""
from __future__ import annotations

import io
import logging
import shutil
from datetime import datetime
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    raise ImportError("pandas est requis : pip install pandas --break-system-packages")

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Constantes
# ─────────────────────────────────────────────────────────────────────────────

W_TO_KWH_PER_30MIN     = 1.0 / 2000.0
EXPECTED_TRANCHES_HEURE = 2           # 2 tranches de 30 min par heure
SOURCE_LABEL            = "agregat_30min"
DB_HORAIRE_COLUMNS      = ["Date", "Heure", "source", "Conso (kWh)"]
_VER_GLOB               = "database_enedis_horaire_????????.csv"
KEEP_VERSIONED_DEFAULT  = 30


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _read_csv_clean(path: Path) -> pd.DataFrame:
    """Lit un CSV en purgeant les octets NUL (montage Windows ↔ Linux)."""
    raw = path.read_bytes().replace(b"\x00", b"")
    return pd.read_csv(io.BytesIO(raw), sep=";", dtype=str, encoding="utf-8",
                       on_bad_lines="skip")


def _time_to_start_hour(time_str: str) -> int | None:
    """
    Convertit la colonne Time (fin de tranche) en heure de DÉBUT (0-23).

    Exemples :
        "00:30:00" → 0   (tranche 00:00-00:30)
        "01:00:00" → 0   (tranche 00:30-01:00)
        "01:30:00" → 1
        "23:30:00" → 23  (tranche 23:00-23:30)
        "23:59:59" → 23  (tranche 23:30-24:00, convention Enedis)
    """
    t = str(time_str).strip()
    if t == "23:59:59":
        return 23
    parts = t.split(":")
    if len(parts) < 2:
        return None
    try:
        h, m = int(parts[0]), int(parts[1])
    except ValueError:
        return None
    total_minutes = h * 60 + m - 30
    if total_minutes < 0:
        return None          # heure invalide
    return total_minutes // 60


def _load_db_30min(path: Path) -> pd.DataFrame:
    """
    Charge Database_Enedis_30_min.csv.
    Retourne un DataFrame avec colonnes : Date (str), Heure (int), Conso_W (float).
    """
    if not path.exists():
        raise FileNotFoundError(f"Database 30 min introuvable : {path}")

    df = _read_csv_clean(path)
    df.columns = [c.strip() for c in df.columns]

    for col in ("Date", "Time", "Conso (W)"):
        if col not in df.columns:
            raise ValueError(f"Colonne '{col}' manquante dans {path.name}")
        df[col] = df[col].astype(str).str.strip()

    df["Conso_W"] = (
        df["Conso (W)"].str.replace(",", ".", regex=False)
    )
    df["Conso_W"] = pd.to_numeric(df["Conso_W"], errors="coerce")
    df = df.dropna(subset=["Conso_W"])

    df["Heure"] = df["Time"].apply(_time_to_start_hour)
    df = df.dropna(subset=["Heure"])
    df["Heure"] = df["Heure"].astype(int)

    log.info("[horaire] DB 30 min chargée : %d lignes (%s … %s)",
             len(df),
             df["Date"].min() if not df.empty else "N/A",
             df["Date"].max() if not df.empty else "N/A")
    return df[["Date", "Heure", "Conso_W"]].copy()


# ─────────────────────────────────────────────────────────────────────────────
# Agrégation
# ─────────────────────────────────────────────────────────────────────────────

def _aggregate_to_hourly(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Agrège par (Date, Heure). Retourne (df_complets, df_partiels).
    df_complets : heures ayant exactement 2 tranches → colonnes DB_HORAIRE_COLUMNS
    df_partiels : heures avec ≠ 2 tranches (audit)
    """
    if df.empty:
        return (
            pd.DataFrame(columns=DB_HORAIRE_COLUMNS),
            pd.DataFrame(columns=["Date", "Heure", "nb_tranches", "Conso (kWh)"]),
        )

    grp = df.groupby(["Date", "Heure"], as_index=False).agg(
        nb_tranches=("Conso_W", "size"),
        somme_W=("Conso_W", "sum"),
    )
    grp["Conso (kWh)"] = (grp["somme_W"] * W_TO_KWH_PER_30MIN).round(4)

    mask = grp["nb_tranches"] == EXPECTED_TRANCHES_HEURE
    df_ok = grp.loc[mask, ["Date", "Heure", "Conso (kWh)"]].copy()
    df_ok["source"] = SOURCE_LABEL
    df_ok = df_ok[DB_HORAIRE_COLUMNS].sort_values(["Date", "Heure"]).reset_index(drop=True)

    df_ko = grp.loc[~mask, ["Date", "Heure", "nb_tranches", "Conso (kWh)"]].copy()
    df_ko = df_ko.sort_values(["Date", "Heure"]).reset_index(drop=True)

    return df_ok, df_ko


# ─────────────────────────────────────────────────────────────────────────────
# Versionnement
# ─────────────────────────────────────────────────────────────────────────────

def _archive_snapshot(path: Path, archive_dir: Path, keep: int) -> Path:
    archive_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d")
    versioned = archive_dir / f"database_enedis_horaire_{stamp}.csv"
    shutil.copy2(str(path), str(versioned))
    log.info("[horaire] Snapshot → archive/%s", versioned.name)
    old = sorted(archive_dir.glob(_VER_GLOB), key=lambda p: p.name)
    for p in old[:-keep]:
        p.unlink()
        log.info("[horaire] Rotation — supprimé : %s", p.name)
    return versioned


# ─────────────────────────────────────────────────────────────────────────────
# API publique
# ─────────────────────────────────────────────────────────────────────────────

def run(
    database_30min_path:   Path,
    database_horaire_path: Path,
    archive_dir:           Path,
    keep_versioned:        int = KEEP_VERSIONED_DEFAULT,
) -> dict:
    """
    Pipeline : Database_Enedis_30_min.csv → database_enedis_horaire.csv.

    Retourne un dict de stats :
        status              'ok' | 'no_data'
        lignes_30min        lignes lues dans la DB 30 min
        heures_completes    heures retenues (2 tranches)
        heures_partielles   heures exclues
        first_date/last_date bornes de la DB horaire produite
        database/versioned  chemins des fichiers écrits
    """
    database_30min_path   = Path(database_30min_path)
    database_horaire_path = Path(database_horaire_path)
    archive_dir           = Path(archive_dir)

    # 1. Lecture
    df = _load_db_30min(database_30min_path)
    n_30min = len(df)

    if df.empty:
        log.warning("[horaire] DB 30 min vide — pas de sortie")
        return {"status": "no_data", "lignes_30min": 0,
                "heures_completes": 0, "heures_partielles": 0}

    # 2. Agrégation
    df_ok, df_ko = _aggregate_to_hourly(df)
    n_ok = len(df_ok)
    n_ko = len(df_ko)

    log.info("[horaire] %d heures complètes, %d partielles exclues", n_ok, n_ko)
    for _, row in df_ko.head(20).iterrows():
        log.info("[horaire]   ⤷ partiel %s H%02d : %d/2 tranches",
                 row["Date"], int(row["Heure"]), int(row["nb_tranches"]))

    if n_ok == 0:
        log.warning("[horaire] Aucune heure complète — pas d'écriture")
        return {"status": "no_data", "lignes_30min": n_30min,
                "heures_completes": 0, "heures_partielles": n_ko}

    # 3. Écriture
    database_horaire_path.parent.mkdir(parents=True, exist_ok=True)
    df_out = df_ok.copy()
    df_out["Conso (kWh)"] = df_out["Conso (kWh)"].map(lambda x: f"{x:.4f}")
    df_out.to_csv(database_horaire_path, sep=";", index=False, encoding="utf-8")

    first_date = df_ok["Date"].min()
    last_date  = df_ok["Date"].max()
    log.info("[horaire] database_enedis_horaire.csv écrit : %d heures (%s … %s)",
             n_ok, first_date, last_date)

    # 4. Snapshot
    versioned = _archive_snapshot(database_horaire_path, archive_dir, keep_versioned)

    return {
        "status":            "ok",
        "lignes_30min":      n_30min,
        "heures_completes":  n_ok,
        "heures_partielles": n_ko,
        "first_date":        first_date,
        "last_date":         last_date,
        "database":          str(database_horaire_path),
        "versioned":         str(versioned),
    }


# ─────────────────────────────────────────────────────────────────────────────
# CLI (test local)
# ─────────────────────────────────────────────────────────────────────────────

def main() -> int:
    import argparse
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s : %(message)s",
        datefmt="%H:%M:%S",
    )
    _CURATED = Path("/opt/airflow/data/curated/conso_elec/enedis")
    p = argparse.ArgumentParser(
        description="Agrégation Database_Enedis_30_min.csv → database_enedis_horaire.csv"
    )
    p.add_argument("--db-30min",   type=Path, default=_CURATED / "Database_Enedis_30_min.csv")
    p.add_argument("--db-horaire", type=Path, default=_CURATED / "database_enedis_horaire.csv")
    p.add_argument("--archive",    type=Path, default=_CURATED / "archive")
    p.add_argument("--keep",       type=int,  default=KEEP_VERSIONED_DEFAULT)
    args = p.parse_args()

    result = run(
        database_30min_path   = args.db_30min,
        database_horaire_path = args.db_horaire,
        archive_dir           = args.archive,
        keep_versioned        = args.keep,
    )
    if result["status"] == "ok":
        print(f"OK : {result['heures_completes']} heures écrites "
              f"({result['first_date']} → {result['last_date']}) "
              f"| {result['heures_partielles']} heure(s) partielle(s) exclue(s)")
        return 0
    print("Aucune donnée à agréger.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
