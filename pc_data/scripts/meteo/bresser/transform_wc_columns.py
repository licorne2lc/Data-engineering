# -*- coding: utf-8 -*-
"""
transform_wc_columns.py
========================
Processus TRANSFORM — alignement des colonnes Weathercloud
sur le format de référence common_weather_database.csv.

Flux :
  weathercloud_bresser_YYYY-MM-DD_30min.csv   (colonnes FR, UTF-8)
        ──► catalog.json (mapping FR → EN)
        ──► météo_bresser_transformed.csv      (colonnes EN, UTF-8)

Opérations :
  1. Lecture du fichier 30 min (sortie de transform_30min)
  2. Nettoyage des noms de colonnes (BOM, NUL)
  3. Séparation Date (Europe/Paris) → Date (yyyy-mm-dd) + Time (HH:MM)
  4. Renommage des colonnes via catalog.json  (FR → EN)
  5. Suppression des colonnes "non utilisée"
  6. Ajout des colonnes absentes dans Weathercloud (NaN) pour
     cohérence parfaite avec common_weather_database.csv
  7. Tri chronologique + suppression lignes entièrement vides
  8. Écriture : météo_bresser_transformed.csv

Importé depuis le DAG :
    from transform_wc_columns import run_transform_columns
    result = run_transform_columns(wc_30min_file, catalog_file, output_dir)

Standalone :
    python transform_wc_columns.py
"""

import json
import logging
import os
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

# Colonne date dans le fichier 30 min (format yyyy-mm-dd HH:MM)
DATE_COL_WC = "Date (Europe/Paris)"

# Colonnes présentes dans common_weather_database mais ABSENTES de Weathercloud
# → ajoutées avec NaN pour cohérence de schéma
COLS_ABSENT_WC = [
    "Baro Pressure Abs",
    "Wind Chill",
    "Etage Temperature", "Etage Humidity",
    "Cave Temperature",  "Cave Humidity",
    "CH3 Temperature", "CH3 Humidity",
    "CH4 Temperature", "CH4 Humidity",
    "CH5 Temperature", "CH5 Humidity",
    "CH6 Temperature", "CH6 Humidity",
    "CH7 Temperature", "CH7 Humidity",
]

# Ordre final des colonnes — identique à common_weather_database.csv
COLS_ORDRE_FINAL = [
    "Date", "Time",
    "IN Temperature", "IN Humidity",
    "Baro Pressure Abs", "Baro Pressure Rel",
    "Out Temperature", "Out Humidity",
    "Feels Like", "Dew Point", "Heat Index", "Wind Chill",
    "Wind speed", "Wind Gust", "Wind Direction",
    "Rain Rate", "Hourly Rain",
    "UVI", "Light intensity",
    "Etage Temperature", "Etage Humidity",
    "Cave Temperature",  "Cave Humidity",
    "CH3 Temperature", "CH3 Humidity",
    "CH4 Temperature", "CH4 Humidity",
    "CH5 Temperature", "CH5 Humidity",
    "CH6 Temperature", "CH6 Humidity",
    "CH7 Temperature", "CH7 Humidity",
]

# Nom de sortie : bresser_wc_YYYY-MM-DD.csv (date extraite du fichier 30min source)


# ─────────────────────────────────────────────────────────────────────────────
# Chargement du catalog
# ─────────────────────────────────────────────────────────────────────────────

def _charger_catalog(catalog_file: Path) -> dict:
    """
    Lit catalog.json et retourne un dict {col_wc: col_db}.
    Les valeurs "non utilisée" sont conservées pour filtrage ultérieur.
    Les valeurs liste (["Date", "Time"]) indiquent un split de colonne.
    """
    with open(catalog_file, "r", encoding="utf-8") as f:
        catalog = json.load(f)
    log.info("Catalog chargé : %d entrées", len(catalog))
    return catalog


# ─────────────────────────────────────────────────────────────────────────────
# Fonction principale
# ─────────────────────────────────────────────────────────────────────────────

def run_transform_columns(wc_30min_file: Path,
                          catalog_file: Path,
                          output_dir: Path) -> dict:
    """
    Transforme le fichier 30 min Weathercloud en appliquant le mapping
    du catalog.json pour aligner les colonnes sur common_weather_database.csv.

    Args:
        wc_30min_file : fichier 30 min Weathercloud
                        ex: .../weathercloud/weathercloud_bresser_2026-04-17_30min.csv
        catalog_file  : catalog.json (mapping FR → EN)
                        ex: .../curated/météo/bresser/catalog.json
        output_dir    : répertoire de sortie
                        ex: .../raw/météo_bresser/weathercloud/

    Returns:
        dict : out_file, lignes, colonnes, debut, fin, colonnes_mappees,
               colonnes_absentes_ajoutees, colonnes_ignorees, taille_ko
    """
    wc_30min_file = Path(wc_30min_file)
    catalog_file  = Path(catalog_file)
    output_dir    = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    log.info("=" * 60)
    log.info("TRANSFORM WC COLUMNS — %s", wc_30min_file.name)
    log.info("Catalog : %s", catalog_file)
    log.info("=" * 60)

    # ── 1. Lecture du fichier 30 min ─────────────────────────────────────────
    df = pd.read_csv(wc_30min_file, dtype=str, encoding="utf-8")
    log.info("Fichier lu : %d lignes, %d colonnes", len(df), len(df.columns))

    # ── 2. Nettoyage noms de colonnes (BOM, NUL, espaces) ────────────────────
    df.columns = [
        str(c).replace("\x00", "").replace("\ufeff", "").strip()
        for c in df.columns
    ]
    log.info("Colonnes source : %s", df.columns.tolist())

    # ── 3. Vérification colonne date ─────────────────────────────────────────
    if DATE_COL_WC not in df.columns:
        raise RuntimeError(
            f"Colonne date '{DATE_COL_WC}' introuvable.\n"
            f"Colonnes présentes : {df.columns.tolist()}"
        )

    # ── 4. Chargement du catalog ──────────────────────────────────────────────
    catalog = _charger_catalog(catalog_file)

    # ── 5. Séparation Date (Europe/Paris) → Date + Time ──────────────────────
    # Format attendu depuis transform_30min : "yyyy-mm-dd HH:MM"
    date_series = (
        df[DATE_COL_WC]
        .astype(str)
        .str.replace("\x00", "", regex=False)
        .str.replace("\ufeff", "", regex=False)
        .str.strip()
    )
    log.info("Exemple horodatage[0] : %r",
             date_series.iloc[0] if len(date_series) > 0 else "vide")

    # Parsing
    # Format depuis transform_30min : "yyyy-mm-dd HH:MM" → pas besoin de dayfirst
    dt_parsed = pd.to_datetime(date_series, dayfirst=False, errors="coerce")
    nb_invalides = dt_parsed.isna().sum()
    if nb_invalides > 0:
        log.warning("%d horodatage(s) invalide(s) sur %d",
                    nb_invalides, len(df))

    df["Date"] = dt_parsed.dt.strftime("%Y-%m-%d")
    df["Time"] = dt_parsed.dt.strftime("%H:%M")

    # Suppression de la colonne source Date (Europe/Paris)
    df.drop(columns=[DATE_COL_WC], inplace=True)

    # ── 6. Renommage des colonnes via catalog ─────────────────────────────────
    cols_mappees        = []
    cols_ignorees       = []   # colonnes source non reconnues dans le catalog
    cols_non_utilisees  = []   # colonnes "non utilisée" → à supprimer

    rename_map = {}
    for col_wc, col_db in catalog.items():
        # La colonne date est déjà traitée (étape 5)
        if col_wc == DATE_COL_WC:
            continue

        if col_wc not in df.columns:
            # Colonne absente dans le fichier source (ex: Unnamed: 19)
            continue

        if col_db == "non utilisée" or (isinstance(col_db, list) and "non utilisée" in col_db):
            cols_non_utilisees.append(col_wc)
            continue

        if isinstance(col_db, str):
            # Correction : catalog dit "Wind Speed" mais database a "Wind speed"
            col_db_corr = col_db if col_db != "Wind Speed" else "Wind speed"
            rename_map[col_wc] = col_db_corr
            cols_mappees.append(f"{col_wc} → {col_db_corr}")

    # Colonnes présentes dans le fichier mais non dans le catalog
    cols_dans_catalog = set(catalog.keys()) | {DATE_COL_WC}
    for col in df.columns:
        if col not in cols_dans_catalog and col not in ("Date", "Time"):
            cols_ignorees.append(col)

    # Application du renommage
    df.rename(columns=rename_map, inplace=True)

    # Suppression des colonnes "non utilisée"
    df.drop(columns=[c for c in cols_non_utilisees if c in df.columns],
            inplace=True)

    # Suppression des colonnes non reconnues (non dans catalog)
    df.drop(columns=[c for c in cols_ignorees if c in df.columns],
            inplace=True)

    log.info("Colonnes mappées (%d) :", len(cols_mappees))
    for m in cols_mappees:
        log.info("  ✔ %s", m)
    if cols_non_utilisees:
        log.info("Colonnes supprimées 'non utilisée' : %s", cols_non_utilisees)
    if cols_ignorees:
        log.warning("Colonnes non reconnues (ignorées) : %s", cols_ignorees)

    # ── 7. Ajout des colonnes absentes (NaN) pour cohérence de schéma ────────
    cols_absentes_ajoutees = []
    for col in COLS_ABSENT_WC:
        if col not in df.columns:
            df[col] = pd.NA
            cols_absentes_ajoutees.append(col)

    log.info("Colonnes absentes ajoutées (NaN) : %s", cols_absentes_ajoutees)

    # ── 8. Suppression des lignes sans date valide ────────────────────────────
    avant = len(df)
    df = df.dropna(subset=["Date", "Time"])
    if avant > len(df):
        log.warning("Lignes supprimées (date invalide) : %d", avant - len(df))

    # ── 9. Suppression des lignes entièrement vides (hors Date/Time) ──────────
    cols_mesure = [c for c in df.columns if c not in ("Date", "Time")]
    df = df.dropna(subset=cols_mesure, how="all")
    log.info("Lignes après nettoyage : %d", len(df))

    if len(df) == 0:
        raise RuntimeError("Aucune ligne de données après transformation.")

    # ── 10. Normalisation des valeurs numériques (format FR → point décimal) ───
    # Weathercloud exporte avec virgule décimale et espace milliers :
    #   "18,7"    → 18.7
    #   "1 013,9" → 1013.9
    cols_numeriques = [c for c in df.columns if c not in ("Date", "Time")]
    for col in cols_numeriques:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace("\xa0", "",  regex=False)   # espace insécable
            .str.replace("\u202f", "", regex=False)  # espace fine insécable
            .str.replace(" ",  "",    regex=False)   # espace ordinaire (milliers)
            .str.replace(",",  ".",   regex=False)   # virgule → point
            .replace("nan", pd.NA)
            .replace("",    pd.NA)
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")

    log.info("Normalisation numérique appliquée sur %d colonnes", len(cols_numeriques))

    # ── 11. Renommage capteurs additionnels : CH1 → Etage, CH2 → Cave ────────
    RENAME_CAPTEURS = {
        "CH1 Temperature": "Etage Temperature",
        "CH1 Humidity":    "Etage Humidity",
        "CH2 Temperature": "Cave Temperature",
        "CH2 Humidity":    "Cave Humidity",
    }
    rename_effectue = {src: dst for src, dst in RENAME_CAPTEURS.items()
                       if src in df.columns}
    if rename_effectue:
        df.rename(columns=rename_effectue, inplace=True)
        for src, dst in rename_effectue.items():
            log.info("  ✔ Renommage : %s → %s", src, dst)
    else:
        log.info("  ✔ Renommage capteurs : aucune colonne CH1/CH2 présente")

    # ── 12. Tri chronologique ─────────────────────────────────────────────────
    df.sort_values(["Date", "Time"], inplace=True, ignore_index=True)

    # ── 13. Réordonnancement des colonnes (ordre final = common_weather_database)
    cols_presentes_dans_ordre = [c for c in COLS_ORDRE_FINAL if c in df.columns]
    cols_extra                = [c for c in df.columns if c not in COLS_ORDRE_FINAL]
    df = df[cols_presentes_dans_ordre + cols_extra]

    log.info("Colonnes finales (%d) : %s",
             len(df.columns), df.columns.tolist())

    # ── 14. Ajout des colonnes de traçabilité ─────────────────────────────────
    df.insert(2, "source",  "wc")
    df.insert(3, "qualite", "approx_30min")
    log.info("  ✔ Colonnes source='wc' et qualite='approx_30min' ajoutées")

    # ── 15. Écriture ──────────────────────────────────────────────────────────
    # Nom : bresser_wc_YYYY-MM-DD.csv
    # La date est extraite du fichier _30min source :
    #   weathercloud_bresser_2026-04-18_30min.csv → 2026-04-18
    date_str = (
        wc_30min_file.stem              # weathercloud_bresser_2026-04-18_30min
        .replace("_30min", "")          # weathercloud_bresser_2026-04-18
        .replace("weathercloud_bresser_", "")  # 2026-04-18
    )
    out_file = output_dir / f"bresser_wc_{date_str}.csv"
    df.to_csv(out_file, index=False, encoding="utf-8")

    stats = {
        "status":                  "ok",
        "out_file":                str(out_file),
        "lignes":                  len(df),
        "colonnes":                len(df.columns),
        "debut":                   str(df["Date"].min()),
        "fin":                     str(df["Date"].max()),
        "colonnes_mappees":        len(cols_mappees),
        "colonnes_absentes_ajout": cols_absentes_ajoutees,
        "colonnes_ignorees":       cols_ignorees,
        "taille_ko":               round(out_file.stat().st_size / 1024, 1),
    }

    log.info("✅ Fichier transformé : %s", out_file)
    log.info("   Lignes            : %d", stats["lignes"])
    log.info("   Colonnes          : %d", stats["colonnes"])
    log.info("   Période           : %s → %s", stats["debut"], stats["fin"])
    log.info("   Taille            : %.1f Ko", stats["taille_ko"])

    return stats


# ─────────────────────────────────────────────────────────────────────────────
# STANDALONE
# ─────────────────────────────────────────────────────────────────────────────

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

    # Cherche automatiquement le dernier fichier _30min dans le répertoire WC
    wc_dir = Path(os.environ.get(
        "WC_RAW_DIR",
        r"D:\projet_dataoz\pc_data\data\raw\météo_bresser\weathercloud"
    ))
    catalog_file = Path(os.environ.get(
        "CATALOG_FILE",
        r"D:\projet_dataoz\pc_data\data\curated\météo\bresser\catalog.json"
    ))
    output_dir = wc_dir  # même répertoire que la source

    # Fichier 30 min le plus récent
    fichiers_30min = sorted(wc_dir.glob("*_30min.csv"), reverse=True)
    if not fichiers_30min:
        raise FileNotFoundError(f"Aucun fichier *_30min.csv dans {wc_dir}")

    wc_30min_file = fichiers_30min[0]
    print(f"Fichier source : {wc_30min_file.name}")

    result = run_transform_columns(wc_30min_file, catalog_file, output_dir)

    print(f"\n{'='*55}")
    print("RÉSULTAT TRANSFORM WC COLUMNS")
    print(f"{'='*55}")
    for k, v in result.items():
        print(f"  {k:28s}: {v}")


if __name__ == "__main__":
    main()
