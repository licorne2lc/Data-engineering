"""
upload_to_bucket.py  —  Préparation CSV + upload vers Oracle Object Storage
# v2 — retour de métriques (rows/cols/bytes) via XCom pour concordance OCI
============================================================================
Remplace load_data_oracle.py (MERGE ligne par ligne via Python).
Nouvelle architecture :
  CSV curated local → nettoyage/renommage colonnes → upload bucket OCI
  → DBMS_SCHEDULER Oracle (COPY_DATA) charge automatiquement à 07h30 UTC

Prérequis :
  pip install oci pandas
  Clé API OCI dans le dossier défini par OCI_CONFIG_FILE
"""

import os
import re
import io
import oci
import pandas as pd
from datetime import datetime, date

# ── Chemins ────────────────────────────────────────────────────────────────────
BASE       = "/opt/airflow/data/curated"
OCI_CONFIG = os.getenv("OCI_CONFIG_FILE", "/opt/airflow/oci_key/config")
OCI_NS     = os.getenv("OCI_NAMESPACE",   "axdo67cv3ayo")
OCI_BUCKET = os.getenv("OCI_BUCKET",      "dataoz-curated")

FILES = {
    "meteo_bresser":     os.path.join(BASE, "météo",      "bresser",  "common_weather_database.csv"),
    "enedis_30min":      os.path.join(BASE, "conso_elec", "enedis",   "Database_Enedis_30_min.csv"),
    "enedis_journalier": os.path.join(BASE, "conso_elec", "enedis",   "database_enedis_journalier.csv"),
    "enedis_horaire":    os.path.join(BASE, "conso_elec", "enedis",   "database_enedis_horaire.csv"),
    "tuya_15min":        os.path.join(BASE, "conso_elec", "tuya",     "_SYNTHESE_15MIN.csv"),
    "tuya_horaire":      os.path.join(BASE, "conso_elec", "tuya",     "_SYNTHESE_HORAIRE.csv"),
    "tuya_journalier":   os.path.join(BASE, "conso_elec", "tuya",     "_SYNTHESE_JOURNALIERE.csv"),
    "tuya_mensuel":      os.path.join(BASE, "conso_elec", "tuya",     "_SYNTHESE_MENSUELLE.csv"),
    "calendrier":        os.path.join(BASE, "calendaire", "calendrier.csv"),
    "finance_cotations": os.path.join(BASE, "finance",    "valeurs",  "boursorama_cotations_enriched.csv"),
}

# ── Helpers ────────────────────────────────────────────────────────────────────
def clean_col(name: str) -> str:
    n = name.strip().lstrip("﻿").lower()
    n = re.sub(r"['\s\(\)°%/]+", "_", n)
    n = re.sub(r"_+", "_", n).strip("_")
    replacements = {
        "ballon_d_eau_chaude": "ballon_eau_chaude",
        "prise_generale_pc":   "prise_pc",
        "n_semaine_iso":       "num_semaine_iso",
        "prise_parfum_ch":     "prise_parfum_ch_parents",
    }
    for old, new in replacements.items():
        if n == old or n.startswith(old):
            return new
    return n

def safe_float(val):
    try:
        f = float(str(val).replace(",", "."))
        return None if pd.isna(f) else f
    except Exception:
        return None

def safe_int(val):
    try:
        return int(float(str(val)))
    except Exception:
        return None

# ── Upload OCI ─────────────────────────────────────────────────────────────────
def _get_oci_client():
    config = oci.config.from_file(OCI_CONFIG)
    return oci.object_storage.ObjectStorageClient(config)

def _upload_df(df: pd.DataFrame, object_name: str) -> dict:
    """
    Sérialise le DataFrame en CSV (sep=;) et l'uploade dans le bucket OCI.
    Retourne un dict {object, rows, cols, bytes} pour concordance (XCom).
    """
    client = _get_oci_client()
    buf = io.BytesIO()
    df.to_csv(buf, index=False, sep=";", encoding="utf-8", na_rep="")
    nb_bytes = buf.tell()
    buf.seek(0)
    client.put_object(
        namespace_name=OCI_NS,
        bucket_name=OCI_BUCKET,
        object_name=object_name,
        put_object_body=buf,
    )
    kb = nb_bytes / 1024
    nb_cols = len(df.columns)
    print(f"[OK] bucket/{object_name} — {len(df)} lignes — {nb_cols} col — {kb:.1f} Ko ({nb_bytes} octets)")
    return {"object": object_name, "rows": len(df), "cols": nb_cols, "bytes": nb_bytes}

# ── Fonctions d'upload par table ───────────────────────────────────────────────

def upload_calendrier(**kwargs):
    df = pd.read_csv(FILES["calendrier"], sep=";", low_memory=False)
    rows = []
    for _, r in df.iterrows():
        try:
            d = datetime.strptime(str(r["Date"]), "%Y-%m-%d").date()
        except Exception:
            continue
        rows.append({
            "date_jour":       str(d),
            "jour_semaine":    str(r.get("Jour de la semaine", ""))[:20],
            "jour_sem":        str(r.get("jour Sem", ""))[:20],
            "num_semaine_iso": safe_int(r.get("N° semaine ISO")),
            "sem_impaire":     safe_int(r.get("Sem. Impaire")),
            "utc":             str(r.get("UTC", ""))[:15],
            "nom_jour_ferie":  str(r.get("nom_jour_ferie", ""))[:60],
            "vac_scol_a":      str(r.get("vac_scol_A", ""))[:60],
            "vac_scol_b":      str(r.get("vac_scol_B", ""))[:60],
            "vac_scol_c":      str(r.get("vac_scol_C", ""))[:60],
        })
    return _upload_df(pd.DataFrame(rows), "calendrier.csv")


def upload_meteo_bresser(**kwargs):
    df = pd.read_csv(FILES["meteo_bresser"], sep=",", low_memory=False)
    df.columns = [clean_col(c) for c in df.columns]
    col_map = {
        "in_temperature":   "temp_interieure",   "in_humidity":          "hum_interieure",
        "baro_pressure_abs":"pression_abs",       "baro_pressure_rel":    "pression_rel",
        "out_temperature":  "temp_exterieure",    "out_humidity":         "hum_exterieure",
        "feels_like":       "ressenti",           "dew_point":            "point_rosee",
        "heat_index":       "indice_chaleur",     "wind_chill":           "refroidissement_eolien",
        "wind_speed":       "vent_vitesse",       "wind_gust":            "vent_rafale",
        "wind_direction":   "vent_direction",     "rain_rate":            "pluie_taux",
        "hourly_rain":      "pluie_horaire",      "light_intensity":      "luminosite",
        "etage_temperature":"temp_etage",         "etage_humidity":       "hum_etage",
        "cave_temperature": "temp_cave",          "cave_humidity":        "hum_cave",
    }
    df.rename(columns=col_map, inplace=True)
    rows = []
    for _, r in df.iterrows():
        try:
            ts = datetime.strptime(f"{r['date']} {r['time']}", "%Y-%m-%d %H:%M")
        except Exception:
            continue
        rows.append({
            "ts":                     ts.strftime("%Y-%m-%d %H:%M:%S"),
            "source":                 str(r.get("source", ""))[:20],
            "qualite":                str(r.get("qualite", ""))[:20],
            "temp_interieure":        safe_float(r.get("temp_interieure")),
            "hum_interieure":         safe_int(r.get("hum_interieure")),
            "temp_exterieure":        safe_float(r.get("temp_exterieure")),
            "hum_exterieure":         safe_int(r.get("hum_exterieure")),
            "ressenti":               safe_float(r.get("ressenti")),
            "point_rosee":            safe_float(r.get("point_rosee")),
            "indice_chaleur":         safe_float(r.get("indice_chaleur")),
            "refroidissement_eolien": safe_float(r.get("refroidissement_eolien")),
            "pression_abs":           safe_float(r.get("pression_abs")),
            "pression_rel":           safe_float(r.get("pression_rel")),
            "vent_vitesse":           safe_float(r.get("vent_vitesse")),
            "vent_rafale":            safe_float(r.get("vent_rafale")),
            "vent_direction":         safe_int(r.get("vent_direction")),
            "pluie_taux":             safe_float(r.get("pluie_taux")),
            "pluie_horaire":          safe_float(r.get("pluie_horaire")),
            "uvi":                    safe_float(r.get("uvi")),
            "luminosite":             safe_float(r.get("luminosite")),
            "temp_etage":             safe_float(r.get("temp_etage")),
            "hum_etage":              safe_int(r.get("hum_etage")),
            "temp_cave":              safe_float(r.get("temp_cave")),
            "hum_cave":               safe_int(r.get("hum_cave")),
        })
    return _upload_df(pd.DataFrame(rows), "meteo_bresser.csv")


def upload_enedis_30min(**kwargs):
    df = pd.read_csv(FILES["enedis_30min"], sep=";", low_memory=False)
    rows = []
    for _, r in df.iterrows():
        try:
            ts = datetime.strptime(f"{r['Date']} {r['Time']}", "%Y-%m-%d %H:%M:%S")
        except Exception:
            continue
        rows.append({
            "ts":      ts.strftime("%Y-%m-%d %H:%M:%S"),
            "source":  str(r.get("source", ""))[:30],
            "conso_w": safe_float(r.get("Conso (W)")),
        })
    return _upload_df(pd.DataFrame(rows), "enedis_30min.csv")


def upload_enedis_journalier(**kwargs):
    df = pd.read_csv(FILES["enedis_journalier"], sep=";", low_memory=False)
    rows = []
    for _, r in df.iterrows():
        try:
            d = datetime.strptime(str(r["Date"]), "%Y-%m-%d").date()
        except Exception:
            continue
        rows.append({
            "date_jour": str(d),
            "source":    str(r.get("source", ""))[:30],
            "conso_kwh": safe_float(r.get("Conso (kWh)")),
        })
    return _upload_df(pd.DataFrame(rows), "enedis_journalier.csv")


def upload_enedis_horaire(**kwargs):
    df = pd.read_csv(FILES["enedis_horaire"], sep=";", low_memory=False)
    rows = []
    for _, r in df.iterrows():
        try:
            d   = datetime.strptime(str(r["Date"]), "%Y-%m-%d").date()
            h   = int(float(str(r["Heure"])))
            ts  = f"{d} {h:02d}:00:00"
        except Exception:
            continue
        rows.append({
            "ts":        ts,
            "source":    str(r.get("source", ""))[:30],
            "conso_kwh": safe_float(r.get("Conso (kWh)")),
        })
    return _upload_df(pd.DataFrame(rows), "enedis_horaire.csv")


def _upload_tuya_granulaire(file_key, object_name, pk_col, ts_format):
    """
    Prépare et uploade un CSV de granularité fine (horaire ou 15min) vers OCI.

    IMPORTANT — ordre des colonnes dans le CSV uploadé :
    Oracle DBMS_SCHEDULER / COPY_DATA charge les fichiers de façon POSITIONNELLE.
    Il faut donc que l'ordre des colonnes dans le CSV corresponde exactement
    à l'ordre des colonnes de la table Oracle cible.
    Ordre imposé : pk_col | ts | appareils (kWh) | total_kwh (toujours en dernier)

    ⚠ Ne jamais mettre total_kwh avant les colonnes appareils dans le row dict,
    sinon Oracle écrit le total dans ballon_eau_chaude et décale tout d'une colonne.
    """
    df = pd.read_csv(FILES[file_key], sep=";", low_memory=False, encoding="utf-8-sig")
    df.columns = [clean_col(c) for c in df.columns]
    # Colonnes à exclure : clé primaire, libellé textuel, colonne ts Python, total calculé
    _exclure = {pk_col, "date_lisible", "total_kwh", "ts"}
    appareils = [c for c in df.columns if c not in _exclure]
    rows = []
    for _, r in df.iterrows():
        pk_val = str(r[pk_col]).strip()
        try:
            ts = datetime.strptime(str(r["date_lisible"]).strip(), ts_format)
            ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            ts_str = None
        # Ordre fixe : clé → timestamp → appareils → total (jamais total avant les devices)
        row = {pk_col: pk_val, "ts": ts_str}
        for app in appareils:
            row[app] = safe_float(r.get(app, 0))
        row["total_kwh"] = safe_float(r.get("total_kwh"))   # toujours en dernier
        rows.append(row)
    return _upload_df(pd.DataFrame(rows), object_name)


def upload_tuya_15min(**kwargs):
    return _upload_tuya_granulaire("tuya_15min", "tuya_15min.csv", "periode_15min", "%Y-%m-%d %H:%M")


def upload_tuya_horaire(**kwargs):
    return _upload_tuya_granulaire("tuya_horaire", "tuya_horaire.csv", "heure", "%Y-%m-%d %H:%M")


def upload_tuya_journalier(**kwargs):
    df = pd.read_csv(FILES["tuya_journalier"], sep=";", low_memory=False, encoding="utf-8-sig")
    df.columns = [clean_col(c) for c in df.columns]
    appareils = [c for c in df.columns if c not in ("jour", "date_lisible", "total_kwh")]
    rows = []
    for _, r in df.iterrows():
        try:
            d = datetime.strptime(str(r["date_lisible"]).strip(), "%Y-%m-%d").date()
            d_str = str(d)
        except Exception:
            d_str = None
        # Ordre : clé → date → appareils → total (jamais total avant les devices)
        row = {"jour": str(r["jour"]).strip(), "date_jour": d_str}
        for app in appareils:
            row[app] = safe_float(r.get(app, 0))
        row["total_kwh"] = safe_float(r.get("total_kwh"))
        rows.append(row)
    return _upload_df(pd.DataFrame(rows), "tuya_journalier.csv")


def upload_tuya_mensuel(**kwargs):
    df = pd.read_csv(FILES["tuya_mensuel"], sep=";", low_memory=False, encoding="utf-8-sig")
    df.columns = [clean_col(c) for c in df.columns]
    appareils = [c for c in df.columns if c not in ("mois", "date_lisible", "total_kwh")]
    rows = []
    for _, r in df.iterrows():
        # Ordre : clé → date → appareils → total (jamais total avant les devices)
        row = {"mois": str(r["mois"]).strip(), "date_lisible": str(r.get("date_lisible", ""))[:10]}
        for app in appareils:
            row[app] = safe_float(r.get(app, 0))
        row["total_kwh"] = safe_float(r.get("total_kwh"))
        rows.append(row)
    return _upload_df(pd.DataFrame(rows), "tuya_mensuel.csv")


def upload_finance_cotations(**kwargs):
    """
    Consolide les fichiers OHLC historiques (ohlc_10a/ — un dossier par symbole)
    et les enrichit avec les metadonnees de boursorama_cotations_enriched.csv,
    puis uploade le CSV consolide dans le bucket OCI.
    full_load=True dans kwargs pour tout l historique (defaut), False pour 40 jours.
    """
    from pathlib import Path
    from datetime import timedelta

    full_load = kwargs.get("full_load", True)
    ohlc_base = Path(BASE) / "finance" / "cotations" / "ohlc_10a"

    # Referentiel metadonnees
    ref = pd.read_csv(FILES["finance_cotations"], sep=";", low_memory=False)
    ref = ref.rename(columns={"sector": "secteur", "exchangeCode": "exchange_code", "category": "categorie"})
    ref_by_symbol = {str(r["symbol"]): r for _, r in ref.iterrows()}

    # Fenetre temporelle
    if full_load:
        cutoff_date = None
        print("finance_cotations : CHARGEMENT COMPLET")
    else:
        cutoff_date = date.today() - timedelta(days=40)
        print(f"finance_cotations : incremental depuis {cutoff_date}")

    rows = []
    symbols_ok = 0

    for sym_dir in sorted(ohlc_base.iterdir()):
        if not sym_dir.is_dir():
            continue
        symbol = sym_dir.name
        csv_files = list(sym_dir.glob("*.csv"))
        if not csv_files:
            continue
        try:
            # D\u00e9tection automatique du s\u00e9parateur (tab=Boursorama brut, ;=trait\u00e9)
            with open(csv_files[0], "r", encoding="utf-8", errors="replace") as _f:
                _sample = _f.read(2048)
            _sep = "\t" if _sample.count("\t") >= _sample.count(";") else ";"
            ohlc = pd.read_csv(csv_files[0], sep=_sep, low_memory=False)
            ohlc.columns = [c.lstrip("\ufeff").strip() for c in ohlc.columns]
            # Renommer colonnes format Boursorama \u2192 format standard
            ohlc = ohlc.rename(columns={
                "ouv": "open", "haut": "high", "bas": "low",
                "clot": "close", "vol": "volume",
            })
            ohlc["date"] = pd.to_datetime(ohlc["date"], errors="coerce", dayfirst=True)
            ohlc = ohlc.dropna(subset=["date"]).sort_values("date")
            if cutoff_date:
                ohlc = ohlc[ohlc["date"].dt.date >= cutoff_date]
            if ohlc.empty:
                continue
            for col in ["open", "high", "low", "close", "volume"]:
                if col not in ohlc.columns:
                    ohlc[col] = None
                else:
                    ohlc[col] = pd.to_numeric(
                        ohlc[col].astype(str).str.replace("\u202f", "", regex=False)
                        .str.replace(" ", "", regex=False).str.replace(",", ".", regex=False),
                        errors="coerce",
                    )
            ohlc["close_prev"] = ohlc["close"].shift(1)
            ohlc["variation"]  = ((ohlc["close"] - ohlc["close_prev"]) / ohlc["close_prev"]).where(ohlc["close_prev"].notna())
            meta = ref_by_symbol.get(symbol, {})
            for _, row in ohlc.iterrows():
                rows.append({
                    "date_import":   str(row["date"].date()),
                    "label":         str(meta.get("label",        symbol))[:100],
                    "symbol":        symbol[:30],
                    "isin":          str(meta.get("isin",          ""))[:20],
                    "mnemonic":      str(meta.get("mnemonic",      ""))[:20],
                    "dernier":       safe_float(row.get("close")),
                    "precedent":     safe_float(row.get("close_prev")),
                    "haut":          safe_float(row.get("high")),
                    "bas":           safe_float(row.get("low")),
                    "open":          safe_float(row.get("open")),
                    "volume":        safe_float(row.get("volume")),
                    "variation":     safe_float(row.get("variation")),
                    "exchange_code": str(meta.get("exchange_code", ""))[:10],
                    "categorie":     str(meta.get("categorie",     ""))[:20],
                    "secteur":       str(meta.get("secteur",       ""))[:100],
                    "pays":          str(meta.get("Pays",          ""))[:50],
                })
            symbols_ok += 1
        except Exception as e:
            print(f"   \u26a0 {symbol} : {e}")
            continue

    if rows:
        result = _upload_df(pd.DataFrame(rows), "finance_cotations.csv")
        print(f"finance_cotations : {symbols_ok} symboles, {len(rows)} lignes")
        return result
    else:
        print("finance_cotations : aucune donnee")
        return {"object": "finance_cotations.csv", "rows": 0, "cols": 0, "bytes": 0}
