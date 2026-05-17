"""
load_data.py  -  Chargement CSV curated → PostgreSQL
=====================================================
Usage :
    python load_data.py --host <IP_VM> --db dataoz --user dataoz_user --password <mdp>

Depuis le PC local, pointe vers l'IP publique de la VM Oracle (port 5432 ouvert).
Pour un chargement initial complet :  python load_data.py --all
Pour une table précise            :  python load_data.py --table meteo_bresser
"""

import argparse
import os
import re
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

# ── Chemins des fichiers curated ──────────────────────────────────────────────
BASE = r"D:\projet_dataoz\pc_data\data\curated"

FILES = {
    "meteo_bresser":    os.path.join(BASE, "météo", "bresser", "common_weather_database.csv"),
    "enedis_30min":     os.path.join(BASE, "conso_elec", "enedis", "Database_Enedis_30_min.csv"),
    "enedis_journalier":os.path.join(BASE, "conso_elec", "enedis", "database_enedis_journalier.csv"),
    "tuya_15min":       os.path.join(BASE, "conso_elec", "tuya", "_SYNTHESE_15MIN.csv"),
    "tuya_horaire":     os.path.join(BASE, "conso_elec", "tuya", "_SYNTHESE_HORAIRE.csv"),
    "tuya_journalier":  os.path.join(BASE, "conso_elec", "tuya", "_SYNTHESE_JOURNALIERE.csv"),
    "tuya_mensuel":     os.path.join(BASE, "conso_elec", "tuya", "_SYNTHESE_MENSUELLE.csv"),
    "calendrier":       os.path.join(BASE, "calendaire", "socle_calendrier.csv"),
    "finance_cotations":os.path.join(BASE, "finance", "valeurs", "boursorama_cotations_enriched.csv"),
}

# ── Helpers ────────────────────────────────────────────────────────────────────
def clean_col(name: str) -> str:
    """Nettoie un nom de colonne pour PostgreSQL."""
    n = name.strip().lstrip('﻿')
    n = n.lower()
    n = re.sub(r"['\s\(\)°%/]+", "_", n)
    n = re.sub(r"_+", "_", n).strip("_")
    replacements = {
        "ballon_d_eau_chaude": "ballon_eau_chaude",
        "prise_generale_pc":   "prise_pc",
        "n_semaine_iso":       "num_semaine_iso",
        "sem_impaire":         "sem_impaire",
        "prise_parfum_ch":     "prise_parfum_ch_parents",
    }
    # Correspondance partielle pour les noms tronqués
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

def connect(args):
    return psycopg2.connect(
        host=args.host, port=args.port,
        dbname=args.db, user=args.user, password=args.password,
        connect_timeout=10
    )

def upsert(conn, table, rows, unique_cols):
    """
    Insère ou met à jour les lignes.
    unique_cols = colonnes de la contrainte UNIQUE (clé naturelle),
                  PAS la colonne id SERIAL qui est gérée automatiquement.
    """
    if not rows:
        return 0
    # Dédupliquer par clé naturelle (garder la dernière occurrence)
    seen = {}
    for row in rows:
        key = tuple(str(row.get(c)) for c in unique_cols)
        seen[key] = row
    rows = list(seen.values())
    # On exclut 'id' des colonnes à insérer (géré par SERIAL)
    cols = [c for c in rows[0].keys() if c != "id"]
    values = [tuple(r[c] for c in cols) for r in rows]
    set_clause = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in cols if c not in unique_cols
    )
    sql = (
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s "
        f"ON CONFLICT ({', '.join(unique_cols)}) DO UPDATE SET {set_clause}"
    )
    with conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=500)
    conn.commit()
    return len(rows)

# ── Chargeurs par table ────────────────────────────────────────────────────────

def load_meteo_bresser(conn):
    print("→ meteo_bresser...")
    df = pd.read_csv(FILES["meteo_bresser"], sep=",", low_memory=False)
    df.columns = [clean_col(c) for c in df.columns]

    col_map = {
        "in_temperature":   "temp_interieure",
        "in_humidity":      "hum_interieure",
        "baro_pressure_abs":"pression_abs",
        "baro_pressure_rel":"pression_rel",
        "out_temperature":  "temp_exterieure",
        "out_humidity":     "hum_exterieure",
        "feels_like":       "ressenti",
        "dew_point":        "point_rosee",
        "heat_index":       "indice_chaleur",
        "wind_chill":       "refroidissement_eolien",
        "wind_speed":       "vent_vitesse",
        "wind_gust":        "vent_rafale",
        "wind_direction":   "vent_direction",
        "rain_rate":        "pluie_taux",
        "hourly_rain":      "pluie_horaire",
        "light_intensity":  "luminosite",
        "etage_temperature":"temp_etage",
        "etage_humidity":   "hum_etage",
        "cave_temperature": "temp_cave",
        "cave_humidity":    "hum_cave",
    }
    df.rename(columns=col_map, inplace=True)

    rows = []
    for _, r in df.iterrows():
        try:
            ts = datetime.strptime(f"{r['date']} {r['time']}", "%Y-%m-%d %H:%M")
        except Exception:
            continue
        rows.append({
            "timestamp":               ts,
            "source":                  str(r.get("source", ""))[:20],
            "qualite":                 str(r.get("qualite", ""))[:20],
            "temp_interieure":         safe_float(r.get("temp_interieure")),
            "hum_interieure":          safe_int(r.get("hum_interieure")),
            "temp_exterieure":         safe_float(r.get("temp_exterieure")),
            "hum_exterieure":          safe_int(r.get("hum_exterieure")),
            "ressenti":                safe_float(r.get("ressenti")),
            "point_rosee":             safe_float(r.get("point_rosee")),
            "indice_chaleur":          safe_float(r.get("indice_chaleur")),
            "refroidissement_eolien":  safe_float(r.get("refroidissement_eolien")),
            "pression_abs":            safe_float(r.get("pression_abs")),
            "pression_rel":            safe_float(r.get("pression_rel")),
            "vent_vitesse":            safe_float(r.get("vent_vitesse")),
            "vent_rafale":             safe_float(r.get("vent_rafale")),
            "vent_direction":          safe_int(r.get("vent_direction")),
            "pluie_taux":              safe_float(r.get("pluie_taux")),
            "pluie_horaire":           safe_float(r.get("pluie_horaire")),
            "uvi":                     safe_float(r.get("uvi")),
            "luminosite":              safe_float(r.get("luminosite")),
            "temp_etage":              safe_float(r.get("temp_etage")),
            "hum_etage":               safe_int(r.get("hum_etage")),
            "temp_cave":               safe_float(r.get("temp_cave")),
            "hum_cave":                safe_int(r.get("hum_cave")),
        })
    n = upsert(conn, "meteo_bresser", rows, ["timestamp"])
    print(f"   ✓ {n} lignes chargées")

def load_enedis_30min(conn):
    print("→ enedis_30min...")
    df = pd.read_csv(FILES["enedis_30min"], sep=";", low_memory=False)
    rows = []
    for _, r in df.iterrows():
        try:
            ts = datetime.strptime(f"{r['Date']} {r['Time']}", "%Y-%m-%d %H:%M:%S")
        except Exception:
            continue
        rows.append({
            "timestamp": ts,
            "source":    str(r.get("source", ""))[:30],
            "conso_w":   safe_float(r.get("Conso (W)")),
        })
    n = upsert(conn, "enedis_30min", rows, ["timestamp"])
    print(f"   ✓ {n} lignes chargées")

def load_enedis_journalier(conn):
    print("→ enedis_journalier...")
    df = pd.read_csv(FILES["enedis_journalier"], sep=";", low_memory=False)
    rows = []
    for _, r in df.iterrows():
        try:
            d = datetime.strptime(str(r["Date"]), "%Y-%m-%d").date()
        except Exception:
            continue
        rows.append({
            "date":      d,
            "source":    str(r.get("source", ""))[:30],
            "conso_kwh": safe_float(r.get("Conso (kWh)")),
        })
    n = upsert(conn, "enedis_journalier", rows, ["date"])
    print(f"   ✓ {n} lignes chargées")

def _load_tuya(conn, table, pk_col, file_key, ts_format, date_col="timestamp"):
    """
    Charge un CSV de synthèse Tuya vers PostgreSQL.

    IMPORTANT — ordre des colonnes dans le row dict :
    Les colonnes appareils sont écrites APRÈS les colonnes de clé/date,
    et AVANT total_kwh, pour correspondre à l'ordre de la table PostgreSQL.
    L'UPSERT utilise des noms de colonnes explicites (pas positionnel),
    ce qui garantit l'absence de décalage même si la table a un ordre différent.

    Colonnes exclues de `appareils` (ne sont PAS des devices) :
        · pk_col        : clé primaire (ex. "heure", "jour", "mois")
        · "date_lisible": colonne de label textuel
        · "total_kwh"   : somme calculée par la synthèse (≠ appareil)
        · date_col      : colonne timestamp PostgreSQL (ex. "timestamp", "date")
    """
    print(f"→ {table}...")
    df = pd.read_csv(FILES[file_key], sep=";", low_memory=False, encoding="utf-8-sig")

    # Normaliser les noms de colonnes (supprime BOM, espaces, apostrophes → snake_case)
    df.columns = [clean_col(c) for c in df.columns]

    # Colonnes à exclure : clés + libellés + total calculé
    # On exclut aussi date_col si différent de pk_col (ex. "timestamp", "date")
    _exclure = {pk_col, "date_lisible", "total_kwh", date_col}
    appareils = [c for c in df.columns if c not in _exclure]

    # Vérification : signaler si total_kwh est absent (CSV mal formé)
    if "total_kwh" not in df.columns:
        print(f"   ⚠ Colonne 'total_kwh' absente dans {FILES[file_key]} — total mis à NULL")

    rows = []
    for _, r in df.iterrows():
        pk_val = str(r[pk_col]).strip()
        try:
            ts = datetime.strptime(str(r["date_lisible"]).strip(), ts_format)
        except Exception:
            ts = None

        # Pour tuya_mensuel (date_col="date_lisible"), stocker la chaîne formatée
        if date_col == "date_lisible":
            date_val = ts.strftime(ts_format) if ts else str(r.get("date_lisible", ""))
        else:
            date_val = ts

        # ── Construction du row dict (ordre fixe pour lisibilité) ──────────
        # 1. Clé primaire
        # 2. Colonne timestamp/date PostgreSQL
        # 3. Colonnes appareils (valeurs kWh par device)  ← cœur du pivot
        # 4. Total calculé par la synthèse
        # L'upsert utilise des noms de colonnes explicites → pas de risque de décalage
        row: dict = {pk_col: pk_val, date_col: date_val}
        for app in appareils:
            row[app] = safe_float(r.get(app, 0))
        row["total_kwh"] = safe_float(r.get("total_kwh"))   # toujours en dernier
        rows.append(row)

    n = upsert(conn, table, rows, [pk_col])
    print(f"   ✓ {n} lignes chargées  ({len(appareils)} appareils : {', '.join(appareils)})")

def load_tuya_15min(conn):
    _load_tuya(conn, "tuya_15min",     "periode_15min", "tuya_15min",     "%Y-%m-%d %H:%M", date_col="timestamp")

def load_tuya_horaire(conn):
    _load_tuya(conn, "tuya_horaire",   "heure",          "tuya_horaire",   "%Y-%m-%d %H:%M", date_col="timestamp")

def load_tuya_journalier(conn):
    _load_tuya(conn, "tuya_journalier","jour",           "tuya_journalier","%Y-%m-%d",        date_col="date")

def load_tuya_mensuel(conn):
    _load_tuya(conn, "tuya_mensuel",   "mois",           "tuya_mensuel",   "%Y-%m",           date_col="date_lisible")

def load_calendrier(conn):
    print("→ calendrier...")
    df = pd.read_csv(FILES["calendrier"], sep=";", low_memory=False)
    rows = []
    for _, r in df.iterrows():
        try:
            d = datetime.strptime(str(r["Date"]), "%Y-%m-%d").date()
        except Exception:
            continue
        rows.append({
            "date":             d,
            "jour_semaine":     str(r.get("Jour de la semaine",""))[:20],
            "jour_sem":         str(r.get("jour Sem",""))[:20],
            "num_semaine_iso":  safe_int(r.get("N° semaine ISO")),
            "sem_impaire":      safe_int(r.get("Sem. Impaire")),
            "utc":              str(r.get("UTC",""))[:15],
            "nom_jour_ferie":   str(r.get("nom_jour_ferie",""))[:60],
            "vac_scol_a":       str(r.get("vac_scol_A",""))[:60],
            "vac_scol_b":       str(r.get("vac_scol_B",""))[:60],
            "vac_scol_c":       str(r.get("vac_scol_C",""))[:60],
        })
    n = upsert(conn, "calendrier", rows, ["date"])
    print(f"   ✓ {n} lignes chargées")

def load_finance_cotations(conn):
    print("→ finance_cotations...")
    df = pd.read_csv(FILES["finance_cotations"], sep=";", low_memory=False)
    rows = []
    for _, r in df.iterrows():
        rows.append({
            "date_import":  datetime.today().date(),
            "label":        str(r.get("label",""))[:100],
            "symbol":       str(r.get("symbol",""))[:30],
            "isin":         str(r.get("isin",""))[:20],
            "mnemonic":     str(r.get("mnemonic",""))[:20],
            "dernier":      safe_float(r.get("last")),
            "precedent":    safe_float(r.get("previousClose")),
            "haut":         safe_float(r.get("high")),
            "bas":          safe_float(r.get("low")),
            "variation":    safe_float(r.get("variation")),
            "volume":       safe_float(r.get("volume")),
            "exchange_code":str(r.get("exchangeCode",""))[:10],
            "categorie":    str(r.get("category",""))[:20],
            "secteur":      str(r.get("sector",""))[:100],
            "pays":         str(r.get("Pays",""))[:50],
        })
    # Pour les cotations on insère sans upsert (snapshot journalier)
    if not rows:
        return
    cols = list(rows[0].keys())
    values = [tuple(r[c] for c in cols) for r in rows]
    sql = f"INSERT INTO finance_cotations ({', '.join(cols)}) VALUES %s ON CONFLICT (date_import, symbol) DO NOTHING"
    with conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=500)
    conn.commit()
    print(f"   ✓ {len(rows)} lignes chargées")

# ── Main ────────────────────────────────────────────────────────────────────────
LOADERS = {
    "meteo_bresser":     load_meteo_bresser,
    "enedis_30min":      load_enedis_30min,
    "enedis_journalier": load_enedis_journalier,
    "tuya_15min":        load_tuya_15min,
    "tuya_horaire":      load_tuya_horaire,
    "tuya_journalier":   load_tuya_journalier,
    "tuya_mensuel":      load_tuya_mensuel,
    "calendrier":        load_calendrier,
    "finance_cotations": load_finance_cotations,
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host",     default="localhost")
    parser.add_argument("--port",     default=5432, type=int)
    parser.add_argument("--db",       default="dataoz")
    parser.add_argument("--user",     default="dataoz_user")
    parser.add_argument("--password", required=True)
    parser.add_argument("--all",      action="store_true", help="Charger toutes les tables")
    parser.add_argument("--table",    help="Charger une table spécifique")
    args = parser.parse_args()

    conn = connect(args)
    print(f"Connecté à {args.host}:{args.port}/{args.db}\n")

    if args.all:
        for name, fn in LOADERS.items():
            try:
                fn(conn)
            except Exception as e:
                conn.rollback()
                print(f"   ✗ Erreur {name}: {e}")
    elif args.table:
        if args.table in LOADERS:
            LOADERS[args.table](conn)
        else:
            print(f"Table inconnue. Disponibles : {list(LOADERS.keys())}")
    else:
        parser.print_help()

    conn.close()
    print("\nChargement terminé.")
