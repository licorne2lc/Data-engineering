# -*- coding: utf-8 -*-
"""
etl_weathercloud.py
====================
Transforme le CSV brut Weathercloud (raw) en CSV propre (curated).

Importé depuis le DAG :
    from etl_weathercloud import run_etl
    stats = run_etl(raw_file, curated_dir)
"""
import logging
from datetime import date
from io import StringIO
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

COLUMN_MAP = {
    "Date (Europe/Paris)":               "horodatage",
    "Température intérieur (°C)":        "temp_interieur_c",
    "Température (°C)":                  "temperature_c",
    "Température ressentie (°C)":        "temp_ressentie_c",
    "Point de rosée intérieur (°C)":     "rosee_interieur_c",
    "Point de rosée (°C)":               "rosee_c",
    "Indice de chaleur intérieur (°C)":  "indice_chaleur_interieur_c",
    "Indice de chaleur (°C)":            "indice_chaleur_c",
    "Humidité intérieur (%)":            "humidite_interieur_pct",
    "Humidité (%)":                      "humidite_pct",
    "Rafale maximale de vent (m/s)":     "vent_rafale_ms",
    "Vitesse moyenne du vent (m/s)":     "vent_moyen_ms",
    "Direction moyenne du vent (°)":     "vent_direction_deg",
    "Pression atmosphérique (hPa)":      "pression_hpa",
    "Pluie (mm)":                        "pluie_mm",
    "Évapotranspiration (mm)":           "evapotranspiration_mm",
    "Intensité de pluie (mm/h)":         "intensite_pluie_mmh",
    "Rayonnement solaire (W/m²)":        "rayonnement_solaire_wm2",
    "Indice UV":                         "indice_uv",
}


def _detect_encoding(raw_file: Path) -> str:
    """
    Détecte l'encodage via le BOM et le pattern des octets nuls.

    Cas couverts :
      - UTF-16 LE/BE avec BOM  (\xff\xfe  ou  \xfe\xff)
      - UTF-8 avec BOM         (\xef\xbb\xbf)
      - UTF-16 LE sans BOM     (octet pair = \x00 : 'D\x00a\x00t\x00…')
      - UTF-16 BE sans BOM     (octet impair = \x00 : '\x00D\x00a\x00t…')
      - UTF-8 (défaut)
    """
    with open(raw_file, "rb") as f:
        raw = f.read(8)

    # BOM explicite
    if raw[:2] in (b"\xff\xfe", b"\xfe\xff"):
        return "utf-16"
    if raw[:3] == b"\xef\xbb\xbf":
        return "utf-8-sig"

    # UTF-16 sans BOM : détection par octets nuls intercalés
    # UTF-16 LE : octets impairs (index 1,3,5,7) sont \x00
    if len(raw) >= 4 and raw[1] == 0 and raw[3] == 0:
        return "utf-16-le"
    # UTF-16 BE : octets pairs (index 0,2,4,6) sont \x00
    if len(raw) >= 4 and raw[0] == 0 and raw[2] == 0:
        return "utf-16-be"

    return "utf-8"


def _read_raw(raw_file: Path) -> pd.DataFrame:
    encoding = _detect_encoding(raw_file)
    log.info("Encodage détecté : %s", encoding)

    # Décodage manuel + StringIO pour éviter le décalage de colonnes (index_col)
    # que pandas provoque quand les lignes de données ont N+1 champs vs N en en-tête.
    with open(raw_file, "rb") as fh:
        raw_bytes = fh.read()

    if encoding in ("utf-16", "utf-16-le", "utf-16-be"):
        bom_le = b"\xff\xfe"
        bom_be = b"\xfe\xff"
        if raw_bytes[:2] == bom_le:
            text = raw_bytes[2:].decode("utf-16-le")
        elif raw_bytes[:2] == bom_be:
            text = raw_bytes[2:].decode("utf-16-be")
        else:
            text = raw_bytes.decode(encoding)
    else:
        text = raw_bytes.decode(encoding.replace("-sig", ""), errors="replace")
        text = text.lstrip("\ufeff")

    df = pd.read_csv(
        StringIO(text),
        sep=";",
        dtype=str,
        skip_blank_lines=False,
        index_col=False,
    )
    log.info("Fichier lu — %d lignes, %d colonnes", len(df), len(df.columns))
    return df


def _clean_numeric(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.replace("\x00",   "", regex=False)
        .str.replace("\xa0",   "", regex=False)
        .str.replace("\u202f", "", regex=False)
        .str.replace(" ",      "", regex=False)
        .str.replace(",",      ".", regex=False)
        .replace("",    pd.NA)
        .replace("nan", pd.NA)
    )


def run_etl(raw_file: Path, curated_dir: Path) -> dict:
    raw_file    = Path(raw_file)
    curated_dir = Path(curated_dir)
    curated_dir.mkdir(parents=True, exist_ok=True)

    log.info("ETL démarré : %s", raw_file)

    # ── 1. Lecture ────────────────────────────────────────────────────────────
    df = _read_raw(raw_file)

    # Supprimer colonnes vides et Unnamed (artefact ';' final)
    df = df[[c for c in df.columns
             if pd.notna(c)
             and str(c).strip() != ""
             and not str(c).startswith("Unnamed:")]]

    # ── 2. Nettoyage noms de colonnes ─────────────────────────────────────────
    df.columns = [
        str(c).replace("\x00", "").replace("\ufeff", "").strip()
        for c in df.columns
    ]
    log.info("Colonnes : %s", df.columns.tolist())

    # ── 3. Renommage ──────────────────────────────────────────────────────────
    df = df.rename(columns=COLUMN_MAP)

    if "horodatage" not in df.columns:
        raise RuntimeError(
            f"Colonne date introuvable. Colonnes : {df.columns.tolist()}"
        )

    # ── 4. Parsing horodatage ─────────────────────────────────────────────────
    date_series = (
        df["horodatage"]
        .astype(str)
        .str.replace("\x00",   "", regex=False)
        .str.replace("\ufeff", "", regex=False)
        .str.strip()
    )
    log.info("Exemple date[0] : %r", date_series.iloc[0] if len(date_series) > 0 else "vide")

    df["horodatage"] = pd.to_datetime(date_series, dayfirst=True, errors="coerce")

    avant = len(df)
    df = df.dropna(subset=["horodatage"])
    log.info("Après dropna horodatage : %d/%d lignes", len(df), avant)

    # Supprimer lignes entièrement vides hors horodatage
    data_cols = [c for c in df.columns if c != "horodatage"]
    df = df.dropna(subset=data_cols, how="all")
    log.info("Lignes valides : %d", len(df))

    if len(df) == 0:
        raise RuntimeError("Aucune ligne de données après nettoyage.")

    # ── 5. Conversion numérique ───────────────────────────────────────────────
    for col in data_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(_clean_numeric(df[col]), errors="coerce")

    # ── 6. Vent m/s → km/h ───────────────────────────────────────────────────
    if "vent_rafale_ms" in df.columns:
        df["vent_rafale_kmh"] = (df["vent_rafale_ms"] * 3.6).round(2)
    if "vent_moyen_ms" in df.columns:
        df["vent_moyen_kmh"]  = (df["vent_moyen_ms"]  * 3.6).round(2)

    # ── 7. Partitionnement ────────────────────────────────────────────────────
    df["annee"]   = df["horodatage"].dt.year
    df["mois"]    = df["horodatage"].dt.month
    df["semaine"] = df["horodatage"].dt.isocalendar().week.astype(int)

    # ── 8. Ordonnancement colonnes ────────────────────────────────────────────
    cols_ordre = [
        "horodatage", "annee", "mois", "semaine",
        "temperature_c", "temp_ressentie_c", "temp_interieur_c",
        "indice_chaleur_c", "indice_chaleur_interieur_c",
        "humidite_pct", "humidite_interieur_pct",
        "rosee_c", "rosee_interieur_c",
        "vent_rafale_ms", "vent_rafale_kmh",
        "vent_moyen_ms",  "vent_moyen_kmh",
        "vent_direction_deg",
        "pression_hpa",
        "pluie_mm", "intensite_pluie_mmh", "evapotranspiration_mm",
        "rayonnement_solaire_wm2", "indice_uv",
    ]
    df = df[[c for c in cols_ordre if c in df.columns]]

    # ── 9. Écriture curated ───────────────────────────────────────────────────
    fname    = raw_file.stem.replace("weathercloud_bresser_", "meteo_bresser_curated_")
    out_file = curated_dir / f"{fname}.csv"
    df.to_csv(out_file, index=False, sep=",", encoding="utf-8")

    stats = {
        "raw_file":  str(raw_file),
        "out_file":  str(out_file),
        "lignes":    len(df),
        "colonnes":  len(df.columns),
        "debut":     str(df["horodatage"].min()),
        "fin":       str(df["horodatage"].max()),
        "taille_ko": round(out_file.stat().st_size / 1024, 1),
    }
    log.info("✅ Curated : %s (%d lignes, %.1f Ko)",
             out_file, stats["lignes"], stats["taille_ko"])
    return stats
