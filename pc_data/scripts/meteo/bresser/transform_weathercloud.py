# -*- coding: utf-8 -*-
"""
transform_weathercloud.py
==========================
Transforme le CSV brut Weathercloud (raw) en CSV 30 minutes.

Règles appliquées :
  1. Conversion date  dd/mm/yyyy HH:MM:SS  →  yyyy-mm-dd HH:MM  (sans secondes)
  2. Conservation uniquement des créneaux HH:00 et HH:30
  3. Suppression des lignes sans aucune valeur de mesure
  4. Sortie dans le répertoire dédié weathercloud/

Importé depuis le DAG :
    from transform_weathercloud import run_transform
    result = run_transform(raw_file, out_dir)
"""

import logging
from io import StringIO
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

# Colonnes de mesure (toutes sauf horodatage)
MEASURE_COLS = [
    "Température intérieur (°C)",
    "Température (°C)",
    "Température ressentie (°C)",
    "Point de rosée intérieur (°C)",
    "Point de rosée (°C)",
    "Indice de chaleur intérieur (°C)",
    "Indice de chaleur (°C)",
    "Humidité intérieur (%)",
    "Humidité (%)",
    "Rafale maximale de vent (m/s)",
    "Vitesse moyenne du vent (m/s)",
    "Direction moyenne du vent (°)",
    "Pression atmosphérique (hPa)",
    "Pluie (mm)",
    "Évapotranspiration (mm)",
    "Intensité de pluie (mm/h)",
    "Rayonnement solaire (W/m²)",
    "Indice UV",
]

DATE_COL_RAW = "Date (Europe/Paris)"


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


def run_transform(raw_file: Path, out_dir: Path) -> dict:
    """
    Lit le CSV brut Weathercloud et génère un CSV filtré aux créneaux 30 min.

    Args:
        raw_file : chemin vers le fichier CSV brut (ex: weathercloud_bresser_2026-04.csv)
        out_dir  : répertoire de sortie (ex: /opt/airflow/data/raw/météo_bresser/weathercloud)

    Returns:
        dict avec les clés : raw_file, out_file, lignes_brutes, lignes_30min, debut, fin, taille_ko
    """
    raw_file = Path(raw_file)
    out_dir  = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    log.info("Transform démarré : %s", raw_file)

    # ── 1. Lecture ────────────────────────────────────────────────────────────
    encoding = _detect_encoding(raw_file)
    log.info("Encodage détecté : %s", encoding)

    # ── Lecture robuste : décodage manuel + StringIO ──────────────────────────
    # Nécessaire pour UTF-16 sans BOM (pandas + index_col décalé sinon).
    # Le fichier Weathercloud produit N+1 champs par ligne vs N dans le header
    # (point-virgule final) : index_col=False empêche pandas d'utiliser la
    # première colonne comme index.
    with open(raw_file, "rb") as fh:
        raw_bytes = fh.read()

    if encoding in ("utf-16", "utf-16-le", "utf-16-be"):
        # Retirer le BOM s'il est présent avant de décoder
        bom_utf16_le = b"\xff\xfe"
        bom_utf16_be = b"\xfe\xff"
        if raw_bytes[:2] == bom_utf16_le:
            text = raw_bytes[2:].decode("utf-16-le")
        elif raw_bytes[:2] == bom_utf16_be:
            text = raw_bytes[2:].decode("utf-16-be")
        else:
            text = raw_bytes.decode(encoding)
    else:
        # UTF-8 / UTF-8-SIG
        text = raw_bytes.decode(encoding.replace("-sig", ""), errors="replace")
        text = text.lstrip("\ufeff")   # retirer BOM UTF-8 s'il reste

    df = pd.read_csv(
        StringIO(text),
        sep=";",
        dtype=str,
        skip_blank_lines=False,
        index_col=False,
        on_bad_lines="warn",   # logue les lignes avec trop de champs et continue
    )
    log.info("Fichier lu — %d lignes, %d colonnes", len(df), len(df.columns))
    lignes_brutes = len(df)

    # ── 2. Nettoyage noms de colonnes (BOM, NUL) ──────────────────────────────
    df.columns = [
        str(c).replace("\x00", "").replace("\ufeff", "").strip()
        for c in df.columns
    ]

    # Supprimer colonnes vides ou Unnamed
    df = df[[c for c in df.columns
             if pd.notna(c)
             and str(c).strip() != ""
             and not str(c).startswith("Unnamed:")]]

    log.info("Colonnes présentes : %s", df.columns.tolist())

    # ── 3. Vérification colonne date ──────────────────────────────────────────
    if DATE_COL_RAW not in df.columns:
        raise RuntimeError(
            f"Colonne date introuvable ('{DATE_COL_RAW}'). "
            f"Colonnes : {df.columns.tolist()}"
        )

    # ── 4. Parsing horodatage ─────────────────────────────────────────────────
    date_series = (
        df[DATE_COL_RAW]
        .astype(str)
        .str.replace("\x00",   "", regex=False)
        .str.replace("\ufeff", "", regex=False)
        .str.strip()
    )
    log.info("Exemple date[0] : %r",
             date_series.iloc[0] if len(date_series) > 0 else "vide")

    df["_horodatage"] = pd.to_datetime(date_series, dayfirst=True, errors="coerce")

    avant = len(df)
    df = df.dropna(subset=["_horodatage"])
    log.info("Après dropna horodatage : %d/%d lignes", len(df), avant)

    if len(df) == 0:
        raise RuntimeError("Aucune ligne avec date valide après parsing.")

    # ── 5. Filtrage créneaux HH:00 et HH:30 ──────────────────────────────────
    df = df[df["_horodatage"].dt.minute.isin([0, 30])]
    log.info("Après filtrage 30 min : %d lignes", len(df))

    # ── 6. Suppression lignes entièrement vides (hors date) ───────────────────
    mesure_cols_present = [c for c in MEASURE_COLS if c in df.columns]
    if mesure_cols_present:
        df = df.dropna(subset=mesure_cols_present, how="all")
        log.info("Après suppression lignes vides : %d lignes", len(df))

    if len(df) == 0:
        raise RuntimeError("Aucune ligne de données après filtrage.")

    # ── 7. Construction colonne date formatée (sans secondes, format ISO) ─────
    df["Date (Europe/Paris)"] = df["_horodatage"].dt.strftime("%Y-%m-%d %H:%M")
    df = df.drop(columns=["_horodatage"])

    # ── 8. Remise en ordre : date en premier ──────────────────────────────────
    autres_cols = [c for c in df.columns if c != "Date (Europe/Paris)"]
    df = df[["Date (Europe/Paris)"] + autres_cols]

    # ── 9. Écriture ───────────────────────────────────────────────────────────
    # Nom de fichier : weathercloud_bresser_YYYY-MM_30min.csv
    stem    = raw_file.stem  # ex: weathercloud_bresser_2026-04
    out_file = out_dir / f"{stem}_30min.csv"

    df.to_csv(out_file, index=False, sep=",", encoding="utf-8")
    lignes_30min = len(df)

    stats = {
        "raw_file":     str(raw_file),
        "out_file":     str(out_file),
        "lignes_brutes": lignes_brutes,
        "lignes_30min":  lignes_30min,
        "debut":         str(df["Date (Europe/Paris)"].iloc[0]),
        "fin":           str(df["Date (Europe/Paris)"].iloc[-1]),
        "taille_ko":     round(out_file.stat().st_size / 1024, 1),
    }
    log.info("✅ Transform 30 min : %s (%d lignes, %.1f Ko)",
             out_file, lignes_30min, stats["taille_ko"])
    return stats
