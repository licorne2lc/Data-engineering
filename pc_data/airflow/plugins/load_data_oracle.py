"""
load_data_oracle.py  -  Chargeur CSV → Oracle Autonomous Database
==================================================================
Remplace load_data_pg.py (PostgreSQL) par Oracle Autonomous DB.
Utilisé par dag_oracle_load.py via PythonOperator.

Prérequis :
  pip install oracledb pandas
  Wallet Oracle extrait dans le dossier défini par WALLET_DIR
"""

import os
import re
import oracledb
import pandas as pd
from datetime import datetime

# ── Chemins Linux (montés dans Docker) ────────────────────────────────────────
BASE       = "/opt/airflow/data/curated"
WALLET_DIR = "/opt/airflow/wallet"          # dossier du wallet Oracle extrait

FILES = {
    "meteo_bresser":     os.path.join(BASE, "météo",      "bresser",  "common_weather_database.csv"),
    "enedis_30min":      os.path.join(BASE, "conso_elec", "enedis",   "Database_Enedis_30_min.csv"),
    "enedis_journalier": os.path.join(BASE, "conso_elec", "enedis",   "database_enedis_journalier.csv"),
    "tuya_15min":        os.path.join(BASE, "conso_elec", "tuya",     "_SYNTHESE_15MIN.csv"),
    "tuya_horaire":      os.path.join(BASE, "conso_elec", "tuya",     "_SYNTHESE_HORAIRE.csv"),
    "tuya_journalier":   os.path.join(BASE, "conso_elec", "tuya",     "_SYNTHESE_JOURNALIERE.csv"),
    "tuya_mensuel":      os.path.join(BASE, "conso_elec", "tuya",     "_SYNTHESE_MENSUELLE.csv"),
    "calendrier":        os.path.join(BASE, "calendaire", "calendrier.csv"),
    "finance_cotations": os.path.join(BASE, "finance",    "valeurs",  "boursorama_cotations_enriched.csv"),
}

# ── Connexion Oracle Autonomous Database ──────────────────────────────────────
ORA_USER    = "ADMIN"
ORA_PASS    = os.environ.get("ORACLE_PASSWORD", "")   # via variable d'environnement
ORA_DSN     = "dataozdb_tp"                            # service dans tnsnames.ora du wallet

def get_conn(max_attempts: int = 8, wait_s: int = 45):
    """
    Connexion Oracle avec retry — couvre le redémarrage ADB Always Free (1-5 min).
    8 tentatives x 45s = 6 minutes max.
    """
    import time
    last_err = None
    for attempt in range(1, max_attempts + 1):
        try:
            conn = oracledb.connect(
                user=ORA_USER,
                password=ORA_PASS,
                dsn=ORA_DSN,
                config_dir=WALLET_DIR,
                wallet_location=WALLET_DIR,
                wallet_password=os.environ.get("WALLET_PASSWORD", ""),
            )
            if attempt > 1:
                print(f"  ✅  Oracle connecté après {attempt} tentative(s).")
            return conn
        except Exception as e:
            last_err = e
            if attempt < max_attempts:
                print(f"  ⚠️  Oracle connexion échouée (tentative {attempt}/{max_attempts}) : {e}")
                print(f"      Nouvelle tentative dans {wait_s}s (ADB Always Free en wake-up ?)…")
                time.sleep(wait_s)
            else:
                print(f"  ❌  Oracle inaccessible après {max_attempts} tentatives : {e}")
    raise last_err

# ── Helpers ───────────────────────────────────────────────────────────────────
def clean_col(name: str) -> str:
    n = name.strip().lstrip('﻿').lower()
    n = re.sub(r"['\s\(\)°%/]+", "_", n)
    n = re.sub(r"_+", "_", n).strip("_")
    replacements = {
        "ballon_d_eau_chaude": "ballon_eau_chaude",
        "prise_generale_pc":   "prise_pc",
        "n_semaine_iso":       "num_semaine_iso",
        "prise_parfum_ch":     "prise_parfum_ch",
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

def merge_rows(conn, table, rows, unique_cols):
    """UPSERT Oracle via MERGE INTO ... USING DUAL"""
    if not rows:
        return 0
    # Dédoublonnage
    seen = {}
    for row in rows:
        key = tuple(str(row.get(c)) for c in unique_cols)
        seen[key] = row
    rows = list(seen.values())

    cols        = [c for c in rows[0].keys() if c != "id"]
    upd_cols    = [c for c in cols if c not in unique_cols]
    on_clause   = " AND ".join(f"t.{c} = :{c}" for c in unique_cols)
    set_clause  = ", ".join(f"t.{c} = :{c}" for c in upd_cols)
    ins_cols    = ", ".join(cols)
    ins_vals    = ", ".join(f":{c}" for c in cols)
    select_part = ", ".join(f":{c} {c}" for c in cols)

    sql = f"""
        MERGE INTO {table} t
        USING (SELECT {select_part} FROM DUAL) s
        ON ({on_clause.replace(':',  's.')})
        WHEN MATCHED THEN
            UPDATE SET {set_clause.replace(':', 's.')}
        WHEN NOT MATCHED THEN
            INSERT ({ins_cols}) VALUES ({ins_vals.replace(':', 's.')})
    """
    # Réécriture simplifiée avec paramètres nommés
    on_c  = " AND ".join(f"t.{c} = s.{c}" for c in unique_cols)
    set_c = ", ".join(f"t.{c} = s.{c}" for c in upd_cols)
    ins_v = ", ".join(f"s.{c}" for c in cols)
    sel_c = ", ".join(f":{c} AS {c}" for c in cols)

    sql = f"""MERGE INTO {table} t
USING (SELECT {sel_c} FROM DUAL) s
ON ({on_c})
WHEN MATCHED THEN UPDATE SET {set_c}
WHEN NOT MATCHED THEN INSERT ({ins_cols}) VALUES ({ins_v})"""

    with conn.cursor() as cur:
        cur.executemany(sql, rows, batcherrors=True)
        errs = cur.getbatcherrors()
        if errs:
            for e in errs[:5]:
                print(f"  Batch error ligne {e.offset}: {e.message}")
    conn.commit()
    return len(rows)

# ── Chargeurs ─────────────────────────────────────────────────────────────────
def load_calendrier(**kwargs):
    conn = get_conn()
    try:
        df = pd.read_csv(FILES["calendrier"], sep=";", low_memory=False)
        rows = []
        for _, r in df.iterrows():
            try:
                d = datetime.strptime(str(r["Date"]), "%Y-%m-%d").date()
            except Exception:
                continue
            rows.append({
                "date_jour":       d,
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
        n = merge_rows(conn, "calendrier", rows, ["date_jour"])
        print(f"calendrier : {n} lignes")
    finally:
        conn.close()


def load_meteo_bresser(**kwargs):
    conn = get_conn()
    try:
        df = pd.read_csv(FILES["meteo_bresser"], sep=",", low_memory=False)
        df.columns = [clean_col(c) for c in df.columns]
        col_map = {
            "in_temperature": "temp_interieure", "in_humidity": "hum_interieure",
            "baro_pressure_abs": "pression_abs",  "baro_pressure_rel": "pression_rel",
            "out_temperature": "temp_exterieure", "out_humidity": "hum_exterieure",
            "feels_like": "ressenti",             "dew_point": "point_rosee",
            "heat_index": "indice_chaleur",       "wind_chill": "refroidissement_eolien",
            "wind_speed": "vent_vitesse",         "wind_gust": "vent_rafale",
            "wind_direction": "vent_direction",   "rain_rate": "pluie_taux",
            "hourly_rain": "pluie_horaire",       "light_intensity": "luminosite",
            "etage_temperature": "temp_etage",    "etage_humidity": "hum_etage",
            "cave_temperature": "temp_cave",      "cave_humidity": "hum_cave",
        }
        df.rename(columns=col_map, inplace=True)
        rows = []
        for _, r in df.iterrows():
            try:
                ts = datetime.strptime(f"{r['date']} {r['time']}", "%Y-%m-%d %H:%M")
            except Exception:
                continue
            rows.append({
                "ts": ts,
                "source": str(r.get("source", ""))[:20],
                "qualite": str(r.get("qualite", ""))[:20],
                "temp_interieure": safe_float(r.get("temp_interieure")),
                "hum_interieure": safe_int(r.get("hum_interieure")),
                "temp_exterieure": safe_float(r.get("temp_exterieure")),
                "hum_exterieure": safe_int(r.get("hum_exterieure")),
                "ressenti": safe_float(r.get("ressenti")),
                "point_rosee": safe_float(r.get("point_rosee")),
                "indice_chaleur": safe_float(r.get("indice_chaleur")),
                "refroidissement_eolien": safe_float(r.get("refroidissement_eolien")),
                "pression_abs": safe_float(r.get("pression_abs")),
                "pression_rel": safe_float(r.get("pression_rel")),
                "vent_vitesse": safe_float(r.get("vent_vitesse")),
                "vent_rafale": safe_float(r.get("vent_rafale")),
                "vent_direction": safe_int(r.get("vent_direction")),
                "pluie_taux": safe_float(r.get("pluie_taux")),
                "pluie_horaire": safe_float(r.get("pluie_horaire")),
                "uvi": safe_float(r.get("uvi")),
                "luminosite": safe_float(r.get("luminosite")),
                "temp_etage": safe_float(r.get("temp_etage")),
                "hum_etage": safe_int(r.get("hum_etage")),
                "temp_cave": safe_float(r.get("temp_cave")),
                "hum_cave": safe_int(r.get("hum_cave")),
            })
        n = merge_rows(conn, "meteo_bresser", rows, ["ts"])
        print(f"meteo_bresser : {n} lignes")
    finally:
        conn.close()


def load_enedis_30min(**kwargs):
    conn = get_conn()
    try:
        df = pd.read_csv(FILES["enedis_30min"], sep=";", low_memory=False)
        rows = []
        for _, r in df.iterrows():
            try:
                ts = datetime.strptime(f"{r['Date']} {r['Time']}", "%Y-%m-%d %H:%M:%S")
            except Exception:
                continue
            rows.append({
                "ts":      ts,
                "source":  str(r.get("source", ""))[:30],
                "conso_w": safe_float(r.get("Conso (W)")),
            })
        n = merge_rows(conn, "enedis_30min", rows, ["ts"])
        print(f"enedis_30min : {n} lignes")
    finally:
        conn.close()


def load_enedis_journalier(**kwargs):
    conn = get_conn()
    try:
        df = pd.read_csv(FILES["enedis_journalier"], sep=";", low_memory=False)
        rows = []
        for _, r in df.iterrows():
            try:
                d = datetime.strptime(str(r["Date"]), "%Y-%m-%d").date()
            except Exception:
                continue
            rows.append({
                "date_jour": d,
                "source":    str(r.get("source", ""))[:30],
                "conso_kwh": safe_float(r.get("Conso (kWh)")),
            })
        n = merge_rows(conn, "enedis_journalier", rows, ["date_jour"])
        print(f"enedis_journalier : {n} lignes")
    finally:
        conn.close()


def _load_tuya(table, pk_col, file_key, ts_format):
    conn = get_conn()
    try:
        df = pd.read_csv(FILES[file_key], sep=";", low_memory=False, encoding="utf-8-sig")
        df.columns = [clean_col(c) for c in df.columns]
        appareils = [c for c in df.columns if c not in (pk_col, "date_lisible", "total_kwh")]
        rows = []
        for _, r in df.iterrows():
            pk_val = str(r[pk_col]).strip()
            try:
                ts = datetime.strptime(str(r["date_lisible"]).strip(), ts_format)
            except Exception:
                ts = None
            row = {pk_col: pk_val, "ts": ts, "total_kwh": safe_float(r.get("total_kwh"))}
            for app in appareils:
                row[app] = safe_float(r.get(app, 0))
            rows.append(row)
        n = merge_rows(conn, table, rows, [pk_col])
        print(f"{table} : {n} lignes")
    finally:
        conn.close()

def _load_tuya_mensuel():
    conn = get_conn()
    try:
        df = pd.read_csv(FILES["tuya_mensuel"], sep=";", low_memory=False, encoding="utf-8-sig")
        df.columns = [clean_col(c) for c in df.columns]
        appareils = [c for c in df.columns if c not in ("mois", "date_lisible", "total_kwh")]
        rows = []
        for _, r in df.iterrows():
            row = {
                "mois": str(r["mois"]).strip(),
                "date_lisible": str(r.get("date_lisible", ""))[:10],
                "total_kwh": safe_float(r.get("total_kwh")),
            }
            for app in appareils:
                row[app] = safe_float(r.get(app, 0))
            rows.append(row)
        n = merge_rows(conn, "tuya_mensuel", rows, ["mois"])
        print(f"tuya_mensuel : {n} lignes")
    finally:
        conn.close()

def load_tuya_15min(**kwargs):
    _load_tuya("tuya_15min",   "periode_15min", "tuya_15min",    "%Y-%m-%d %H:%M")

def load_tuya_horaire(**kwargs):
    _load_tuya("tuya_horaire", "heure",         "tuya_horaire",  "%Y-%m-%d %H:%M")

def load_tuya_journalier(**kwargs):
    conn = get_conn()
    try:
        df = pd.read_csv(FILES["tuya_journalier"], sep=";", low_memory=False, encoding="utf-8-sig")
        df.columns = [clean_col(c) for c in df.columns]
        appareils = [c for c in df.columns if c not in ("jour", "date_lisible", "total_kwh")]
        rows = []
        for _, r in df.iterrows():
            try:
                d = datetime.strptime(str(r["date_lisible"]).strip(), "%Y-%m-%d").date()
            except Exception:
                d = None
            row = {"jour": str(r["jour"]).strip(), "date_jour": d,
                   "total_kwh": safe_float(r.get("total_kwh"))}
            for app in appareils:
                row[app] = safe_float(r.get(app, 0))
            rows.append(row)
        n = merge_rows(conn, "tuya_journalier", rows, ["jour"])
        print(f"tuya_journalier : {n} lignes")
    finally:
        conn.close()

def load_tuya_mensuel(**kwargs):
    _load_tuya_mensuel()


def load_finance_cotations(**kwargs):
    """
    Charge les données historiques OHLC depuis ohlc_10a/ (un dossier par symbole)
    et les enrichit avec les métadonnées de boursorama_cotations_enriched.csv.

    Stratégie :
      - Chargement incrémental par défaut : seulement les 40 derniers jours
        (pour les runs Airflow quotidiens, couvre week-ends + jours fériés)
      - Chargement complet si full_load=True dans kwargs (premier chargement)
      - MERGE Oracle sur (date_import, symbol) → idempotent, sans doublons

    Mapping colonnes OHLC → finance_cotations :
      date   → date_import
      close  → dernier
      close[t-1] → precedent  (clôture veille calculée par shift)
      high   → haut
      low    → bas
      (close - close[t-1]) / close[t-1] → variation
      volume → volume
    """
    import os
    from pathlib import Path

    full_load   = kwargs.get("full_load", True)
    ohlc_base   = Path(BASE) / "finance" / "cotations" / "ohlc_10a"
    enriched_csv = FILES["finance_cotations"]

    # ── Référentiel métadonnées ───────────────────────────────────────────────
    ref = pd.read_csv(enriched_csv, sep=";", low_memory=False)
    ref = ref.rename(columns={
        "sector":       "secteur",
        "exchangeCode": "exchange_code",
        "category":     "categorie",
    })
    ref_by_symbol = {
        str(r["symbol"]): r
        for _, r in ref.iterrows()
    }

    # Fenetre temporelle (chargement incremental vs complet)
    if full_load:
        cutoff_date = None
        print("finance_cotations : CHARGEMENT COMPLET (tous les historiques OHLC)")
    else:
        from datetime import timedelta
        cutoff_date = datetime.today().date() - timedelta(days=40)
        print(f"finance_cotations : chargement incremental depuis {cutoff_date}")

    # Lecture et consolidation des fichiers OHLC
    all_rows = []
    symbols_ok = 0
    symbols_ko = 0

    for sym_dir in sorted(ohlc_base.iterdir()):
        if not sym_dir.is_dir():
            continue
        symbol = sym_dir.name
        csv_files = list(sym_dir.glob("*.csv"))
        if not csv_files:
            continue
        try:
            ohlc = pd.read_csv(csv_files[0], sep=";", low_memory=False)
            ohlc.columns = [c.lstrip("﻿") for c in ohlc.columns]
            ohlc["date"] = pd.to_datetime(ohlc["date"], errors="coerce")
            ohlc = ohlc.dropna(subset=["date"]).sort_values("date")

            if cutoff_date:
                ohlc = ohlc[ohlc["date"].dt.date >= cutoff_date]
            if ohlc.empty:
                continue

            # Calcul du cours precedent et de la variation
            ohlc["close_prev"] = ohlc["close"].shift(1)
            ohlc["variation"]  = (
                (ohlc["close"] - ohlc["close_prev"]) / ohlc["close_prev"]
            ).where(ohlc["close_prev"].notna())

            meta = ref_by_symbol.get(symbol, {})

            for _, row in ohlc.iterrows():
                all_rows.append({
                    "date_import":   row["date"].date(),
                    "label":         str(meta.get("label",        symbol))[:100],
                    "symbol":        symbol[:30],
                    "isin":          str(meta.get("isin",          ""))[:20],
                    "mnemonic":      str(meta.get("mnemonic",      ""))[:20],
                    "dernier":       safe_float(row.get("close")),
                    "precedent":     safe_float(row.get("close_prev")),
                    "haut":          safe_float(row.get("high")),
                    "bas":           safe_float(row.get("low")),
                    "variation":     safe_float(row.get("variation")),
                    "volume":        safe_float(row.get("volume")),
                    "exchange_code": str(meta.get("exchange_code", ""))[:10],
                    "categorie":     str(meta.get("categorie",     ""))[:20],
                    "secteur":       str(meta.get("secteur",       ""))[:100],
                    "pays":          str(meta.get("Pays",          ""))[:50],
                    "risk_level":    str(meta.get("risk_level",    ""))[:100],
                    "eligibility":   str(meta.get("eligibility",   ""))[:50],
                    "elig_pea":      str(meta.get("elig_pea",      ""))[:10],
                })
            symbols_ok += 1

        except Exception as e:
            print(f"  [WARN] {symbol} ignore : {e}")
            symbols_ko += 1

    print(f"finance_cotations : {symbols_ok} symboles OK, {symbols_ko} en erreur, {len(all_rows)} lignes a charger")

    if not all_rows:
        print("finance_cotations : aucune ligne a charger.")
        return

    conn = get_conn()
    try:
        n = merge_rows(conn, "finance_cotations", all_rows, ["date_import", "symbol"])
        print(f"finance_cotations : {n} lignes chargees/mises a jour dans Oracle")
    finally:
        conn.close()
