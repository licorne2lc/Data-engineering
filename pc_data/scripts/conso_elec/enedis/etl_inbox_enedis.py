# -*- coding: utf-8 -*-
"""
etl_inbox_enedis.py
===================
ETL manuel — XLSX Enedis (export espace client) → Database_Enedis_30_min.csv

╔══════════════════════════════════════════════════════════════════════════════╗
║  PHASE 1 — EXTRACT                                                          ║
║    · Scan inbox_enedis/ pour les XLSX :                                     ║
║      "PRM_Export_courbe_de_charge_Consommation_DDMMYYYY-DDMMYYYY.xlsx"      ║
║    · Tri calendaire par date début (extraite du nom de fichier)             ║
║    · Concaténation en un DataFrame (Date, Debut_HM, Conso_W)               ║
║    · Sauvegarde  → new_data_enedis_YYYYMMDD.xlsx                            ║
║    · Archivage sources (max 10 fichiers XLSX conservés en archive)          ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  PHASE 2 — TRANSFORM                                                        ║
║    · Chargement new_data xlsx                                               ║
║    · Traitement DST via table_chgt_heure.csv :                              ║
║        - été→hiver  (fall-back)    : 'add'  → somme Début 02:00 et 02:30   ║
║        - hiver→été  (spring-fwd)   : 'low'  → 0.1 W pour Début 02:00/02:30 ║
║    · Conversion Début → Fin (+ 30 min), convention "23:59:59" pour minuit   ║
║    · Export → new_data_enedis_YYYYMMDD.csv                                  ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  PHASE 3 — LOAD                                                             ║
║    · Archivage Database_Enedis_30_min.csv dans archive/                     ║
║    · Fusion + déduplication sur (Date, Time=Fin)                            ║
║    · Tri chronologique                                                      ║
║    · Export Database_Enedis_30_min.csv (courant)                            ║
║           + Database_Enedis_30_min_YYYYMMDD.csv (versionné)                 ║
╚══════════════════════════════════════════════════════════════════════════════╝

Convention Time dans la DATABASE finale :
    Date = date locale Europe/Paris du DÉBUT de tranche
    Time = heure de FIN de tranche (Début + 30 min), Europe/Paris
           "23:59:59" si la fin tombe à minuit (convention Enedis)
    Conso (W) = puissance moyenne en Watts
"""
from __future__ import annotations

import csv
import logging
import re
import shutil
from datetime import datetime, timedelta
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    raise ImportError("pandas est requis : pip install pandas --break-system-packages")

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Constantes
# ─────────────────────────────────────────────────────────────────────────────

# Pattern nom de fichier XLSX Enedis (export espace client)
# ex : 22130390723840_Export_courbe_de_charge_Consommation_26102025-31102025.xlsx
_XLSX_RE = re.compile(
    r"^(?P<prm>\d+)_Export_courbe_de_charge_Consommation_"
    r"(?P<dd>\d{2})(?P<mm>\d{2})(?P<yyyy>\d{4})"
    r"-\d{2}\d{2}\d{4}\.xlsx$",
    re.IGNORECASE,
)

# Pattern nom de fichier XLSX Scrapping (Playwright dag_conso_elec_enedis)
# ex : scrap_enedis_22042026-25042026__20260427_053421.xlsx
_SCRAP_RE = re.compile(
    r"^scrap_enedis_"
    r"(?P<dd>\d{2})(?P<mm>\d{2})(?P<yyyy>\d{4})"
    r"-\d{2}\d{2}\d{4}"
    r"__\d{8}_\d{6}\.xlsx$",
    re.IGNORECASE,
)

DST_TABLE_PATH = Path(
    "/opt/airflow/data/curated/calendaire/chgt_heure/table_chgt_heure.csv"
)

MAX_ARCHIVE_XLSX = 10   # nombre de XLSX sources conservés en archive


# ─────────────────────────────────────────────────────────────────────────────
# Utilitaires partagés
# ─────────────────────────────────────────────────────────────────────────────

def _filename_start_date(name: str) -> datetime | None:
    """
    Extrait la date de début depuis le nom d'un XLSX Enedis.
    Supporte deux conventions de nommage :
      • API/manuel  : <PRM>_Export_courbe_de_charge_Consommation_DDMMYYYY-DDMMYYYY.xlsx
      • Scrapping   : scrap_enedis_DDMMYYYY-DDMMYYYY__YYYYMMDD_HHMMSS.xlsx
    """
    m = _XLSX_RE.match(name) or _SCRAP_RE.match(name)
    if not m:
        return None
    return datetime(int(m.group("yyyy")), int(m.group("mm")), int(m.group("dd")))


def _load_dst_dates(path: Path) -> tuple[set[str], set[str]]:
    """
    Lit table_chgt_heure.csv.
    Retourne (spring_forward_dates, fall_back_dates) — ensembles de str ISO.
    """
    spring: set[str] = set()
    fall:   set[str] = set()
    if not path.exists():
        log.warning("[DST] Table introuvable : %s — traitement DST désactivé", path)
        return spring, fall

    with path.open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            t = (row.get("ete/hivers") or "").strip().lower()
            d = (row.get("Date") or "").strip()
            if not d:
                continue
            if t == "ete":
                spring.add(d)
            elif t in ("hivers", "hiver"):
                fall.add(d)

    log.info("[DST] %d spring-forward, %d fall-back chargés", len(spring), len(fall))
    return spring, fall


def _archive_trim(archive_dir: Path, pattern: str, keep: int) -> None:
    """Supprime les fichiers les plus anciens au-delà de `keep` dans archive_dir."""
    files = sorted(archive_dir.glob(pattern), key=lambda p: p.stat().st_mtime)
    for old in files[:-keep] if len(files) > keep else []:
        old.unlink()
        log.info("[archive] Supprimé (limite %d) : %s", keep, old.name)


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 1 — EXTRACT
# ─────────────────────────────────────────────────────────────────────────────

def _read_one_xlsx(xlsx_path: Path) -> pd.DataFrame:
    """
    Lit un XLSX Enedis (export espace client).

    Colonnes retournées :
        Date     (str YYYY-MM-DD)  — date locale du DÉBUT de tranche
        Debut    (str HH:MM:SS)    — heure de début (locale Europe/Paris)
        Conso_W  (float)           — puissance moyenne Watts = kW × 1000

    Les tranches 'NA' (gap spring-forward) sont ignorées ici ;
    elles seront recréées avec Conso=0.1 dans la phase Transform.
    """
    try:
        import openpyxl
    except ImportError:
        raise ImportError("openpyxl est requis : pip install openpyxl --break-system-packages")

    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb.active
    all_rows = list(ws.iter_rows(values_only=True))
    wb.close()

    # Localise la ligne d'en-tête "Début"
    data_start = None
    for idx, row in enumerate(all_rows):
        lowers = [str(c).strip().lower() if c else "" for c in row]
        if "début" in lowers or "debut" in lowers:
            data_start = idx + 1
            break

    if data_start is None:
        log.warning("[extract] %s — en-tête 'Début' introuvable", xlsx_path.name)
        return pd.DataFrame(columns=["Date", "Debut", "Conso_W"])

    records = []
    for row in all_rows[data_start:]:
        debut_raw  = None
        valeur_raw = None

        # Deux formats possibles :
        #   [None, None, Début, Fin, Valeur(kW), ...]
        #   [Début, Fin, Valeur(kW), ...]
        if len(row) >= 5 and row[2] is not None:
            debut_raw, valeur_raw = row[2], row[4]
        elif len(row) >= 3 and row[0] is not None:
            debut_raw, valeur_raw = row[0], row[2]
        else:
            continue

        if debut_raw is None or valeur_raw is None:
            continue

        # Parse timestamp Début (openpyxl renvoie souvent un datetime natif)
        if isinstance(debut_raw, datetime):
            ts = debut_raw
        else:
            ts = None
            for fmt in ("%d/%m/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S",
                        "%d/%m/%Y %H:%M",    "%Y-%m-%d %H:%M"):
                try:
                    ts = datetime.strptime(str(debut_raw).strip(), fmt)
                    break
                except ValueError:
                    continue
            if ts is None:
                continue

        # Parse Valeur (kW) — NA = tranche gap spring-forward → ignorée
        if isinstance(valeur_raw, (int, float)):
            kw = float(valeur_raw)
        else:
            s = str(valeur_raw).strip()
            if s.upper() in ("NA", "N/A", "", "-"):
                continue
            try:
                kw = float(s.replace(",", "."))
            except ValueError:
                continue

        records.append({
            "Date":    ts.date().isoformat(),           # YYYY-MM-DD
            "Debut":   ts.strftime("%H:%M:%S"),          # HH:MM:SS (début local)
            "Conso_W": round(kw * 1000, 2),             # W = kW × 1000
        })

    return pd.DataFrame(records)


def phase_extract(
    inbox_dir:   Path,
    archive_dir: Path,
    output_dir:  Path,
) -> dict:
    """
    Scanne inbox_dir, trie les XLSX par date début (nommage fichier),
    concatène en un DataFrame, exporte new_data_enedis_YYYYMMDD.xlsx,
    archive les XLSX sources (max MAX_ARCHIVE_XLSX conservés).

    Retourne un dict de stats + chemin du fichier produit.
    """
    inbox_dir.mkdir(parents=True, exist_ok=True)
    archive_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Scan + tri calendaire
    xlsx_files = []
    rejected   = []
    for p in inbox_dir.iterdir():
        if p.suffix.lower() != ".xlsx" or not p.is_file():
            continue
        if p.parent == archive_dir:
            continue
        d = _filename_start_date(p.name)
        if d is None:
            rejected.append(p.name)
            log.warning("[extract] Nom non reconnu, ignoré : %s", p.name)
        else:
            xlsx_files.append((d, p))

    if not xlsx_files:
        return {
            "status":   "no_files",
            "message":  f"Aucun XLSX Enedis reconnu dans {inbox_dir}",
            "fichiers": 0,
            "lignes":   0,
            "output":   None,
            "rejetes":  rejected,
        }

    xlsx_files.sort(key=lambda x: x[0])
    log.info("[extract] %d fichier(s) XLSX trouvé(s), tri calendaire OK", len(xlsx_files))

    # Concaténation
    frames = []
    for dt, path in xlsx_files:
        df = _read_one_xlsx(path)
        if df.empty:
            rejected.append(path.name)
            log.warning("[extract] ✗ %s — aucune donnée lisible", path.name)
        else:
            frames.append(df)
            log.info("[extract] ✓ %s — %d lignes", path.name, len(df))

    if not frames:
        return {
            "status":   "no_data",
            "message":  "Fichiers trouvés mais aucune donnée lisible",
            "fichiers": len(xlsx_files),
            "lignes":   0,
            "output":   None,
            "rejetes":  rejected,
        }

    combined = pd.concat(frames, ignore_index=True)

    # Date du dernier jour présent dans les données → nom de fichier
    last_date = combined["Date"].max().replace("-", "")   # YYYYMMDD
    out_name  = f"new_data_enedis_{last_date}.xlsx"
    out_path  = output_dir / out_name

    # Export XLSX (feuille unique)
    combined.to_excel(out_path, index=False, sheet_name="new_data")
    log.info("[extract] new_data → %s (%d lignes)", out_name, len(combined))

    # Archivage des XLSX sources
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    for _, p in xlsx_files:
        dest = archive_dir / f"{p.stem}__{stamp}{p.suffix}"
        shutil.move(str(p), str(dest))
        log.info("[extract] Archivé : %s → %s", p.name, dest.name)

    # Nettoyage archive (max MAX_ARCHIVE_XLSX)
    _archive_trim(archive_dir, "*.xlsx", MAX_ARCHIVE_XLSX)

    return {
        "status":    "ok",
        "fichiers":  len(xlsx_files),
        "lignes":    len(combined),
        "last_date": last_date,
        "output":    str(out_path),
        "rejetes":   rejected,
    }


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 2 — TRANSFORM
# ─────────────────────────────────────────────────────────────────────────────

def _apply_dst(df: pd.DataFrame, spring_dates: set[str], fall_dates: set[str]) -> pd.DataFrame:
    """
    Traitement des changements d'heure sur le DataFrame (colonnes Date, Debut, Conso_W).

    Conventions Enedis observées sur les exports XLSX espace client :

    fall-back (été→hiver), opération 'add' :
        Sur les dates fall-back, Debut=01:30:00 et Debut=02:00:00 apparaissent
        chacun DEUX fois (une fois en heure d'été CEST, une fois en heure d'hiver CET).
        → On ADDITIONNE les deux valeurs (consommation totale de la tranche).

        Correspondance Debut → Time (Fin=Debut+30min) :
            Debut 01:30 × 2  →  Fin 02:00:00  (identique à traitement_chgt_heure 'add')
            Debut 02:00 × 2  →  Fin 02:30:00  (identique à traitement_chgt_heure 'add')

    spring-forward (hiver→été), opération 'low' :
        Sur les dates spring-forward, Debut=01:30:00 et Debut=02:00:00 sont
        absentes (NA dans l'XLSX Enedis ; l'horloge saute de 02:00 à 03:00 CET).
        → On INSÈRE des lignes avec Conso_W=0.1 W (tranche non mesurée).

        Correspondance Debut → Time (Fin) :
            Debut 01:30 manquant  →  Fin 02:00:00  (identique à traitement_chgt_heure 'low')
            Debut 02:00 manquant  →  Fin 02:30:00  (identique à traitement_chgt_heure 'low')

    IMPORTANT : appeler AVANT drop_duplicates — les doublons fall-back sont légitimes
                et doivent être sommés, pas supprimés.
    """
    if df.empty:
        return df

    # Heures Enedis concernées par le changement d'heure (colonne Debut)
    DST_DEBUT_TIMES = {"01:30:00", "02:00:00"}

    # ── Fall-back : ADD ──────────────────────────────────────────────────────
    if fall_dates:
        mask_fb = (df["Date"].isin(fall_dates)) & (df["Debut"].isin(DST_DEBUT_TIMES))

        if mask_fb.any():
            df_keep = df[~mask_fb].copy()

            # Groupby (Date, Debut) → SUM des deux occurrences
            df_fb = (
                df[mask_fb]
                .groupby(["Date", "Debut"], as_index=False)["Conso_W"]
                .sum()
            )
            n_dup = mask_fb.sum() - len(df_fb)
            log.info("[transform] Fall-back : %d doublon(s) sommé(s) sur date(s) %s",
                     n_dup,
                     sorted(df["Date"][mask_fb].unique()))

            df = pd.concat([df_keep, df_fb], ignore_index=True)
            df = df.sort_values(["Date", "Debut"]).reset_index(drop=True)

    # ── Spring-forward : LOW ─────────────────────────────────────────────────
    if spring_dates:
        new_rows = []
        dates_in_data = set(df["Date"].unique())

        for date_iso in sorted(spring_dates):
            if date_iso not in dates_in_data:
                continue   # date hors période couverte par les données

            for debut_time in ("01:30:00", "02:00:00"):
                exists = ((df["Date"] == date_iso) & (df["Debut"] == debut_time)).any()
                if not exists:
                    new_rows.append({
                        "Date":    date_iso,
                        "Debut":   debut_time,
                        "Conso_W": 0.1,
                    })
                    log.info("[transform] Spring-forward %s : ajout tranche manquante"
                             " Debut=%s → 0.1 W", date_iso, debut_time)

        if new_rows:
            df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
            df = df.sort_values(["Date", "Debut"]).reset_index(drop=True)

    return df


def _debut_to_fin(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convertit la colonne Debut (HH:MM:SS) en Fin (Debut + 30 min).

    Convention Enedis / base de données :
        Date = date locale du DÉBUT de tranche
        Time = heure de FIN de tranche (Debut + 30 min)
               "23:59:59" si la fin tombe à minuit (dernière tranche du jour)

    Retourne un DataFrame avec colonnes : Date, Time, Conso (W)
    """
    # Reconstitue un datetime "naïf" pour le calcul
    df = df.copy()
    df["_dt_debut"] = pd.to_datetime(df["Date"] + " " + df["Debut"],
                                     format="%Y-%m-%d %H:%M:%S")
    df["_dt_fin"]   = df["_dt_debut"] + pd.Timedelta(minutes=30)

    # Heure de fin au format HH:MM:SS
    df["Time"] = df["_dt_fin"].dt.strftime("%H:%M:%S")

    # Convention 23:59:59 pour minuit
    is_midnight = (df["_dt_fin"].dt.hour == 0) & (df["_dt_fin"].dt.minute == 0)
    df.loc[is_midnight, "Time"] = "23:59:59"
    # Date reste celle du DÉBUT (pas la date du lendemain)

    df = df.rename(columns={"Conso_W": "Conso (W)"})
    df = df[["Date", "Time", "Conso (W)"]].copy()
    return df


def phase_transform(
    new_data_path: Path,
    output_dir:    Path,
    dst_table:     Path = DST_TABLE_PATH,
) -> dict:
    """
    Charge le new_data XLSX, applique le traitement DST, convertit Début→Fin,
    exporte en CSV au format Enedis (Date;Time;Conso (W)).

    Ordre des opérations :
        1. Traitement DST (AVANT tout drop_duplicates)
           - fall-back  : somme les doublons Debut 01:30 et 02:00
           - spring-fwd : insère Debut 01:30 et 02:00 à 0.1 W
        2. Déduplication des chevauchements résiduels (overlaps XLSX)
        3. Conversion Début → Fin (+30 min, convention 23:59:59 pour minuit)
        4. Export CSV (Date;Time;Conso (W))

    Retourne un dict de stats + chemin du CSV produit.
    """
    if not new_data_path.exists():
        raise FileNotFoundError(f"new_data introuvable : {new_data_path}")

    log.info("[transform] Chargement : %s", new_data_path.name)
    df = pd.read_excel(new_data_path, dtype={"Date": str, "Debut": str})
    df["Conso_W"] = pd.to_numeric(df["Conso_W"], errors="coerce").fillna(0.0)

    if df.empty:
        return {"status": "empty", "lignes_input": 0, "lignes_output": 0, "output": None}

    n_input = len(df)
    log.info("[transform] %d lignes lues", n_input)

    # Chargement des dates DST
    spring_dates, fall_dates = _load_dst_dates(dst_table)

    # ── Étape 1 : traitement DST (AVANT dedup pour conserver les doublons fall-back)
    df = _apply_dst(df, spring_dates, fall_dates)

    # ── Étape 2 : déduplication des chevauchements résiduels (XLSX qui se recoupent)
    #    Les doublons fall-back ont déjà été sommés → seuls restent les vrais doublons
    before_dedup = len(df)
    df = df.drop_duplicates(subset=["Date", "Debut"], keep="last").copy()
    if len(df) < before_dedup:
        log.info("[transform] %d doublon(s) résiduel(s) (chevauchement XLSX) supprimé(s)",
                 before_dedup - len(df))

    # ── Étape 3 : conversion Début → Fin
    df_out = _debut_to_fin(df)

    # Arrondi Conso (W) à 1 décimale
    df_out["Conso (W)"] = df_out["Conso (W)"].round(1)

    # ── Étape 4 : export CSV
    last_date = df_out["Date"].max().replace("-", "")
    csv_name  = f"new_data_enedis_{last_date}.csv"
    csv_path  = output_dir / csv_name

    output_dir.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(csv_path, sep=";", index=False, encoding="utf-8")
    log.info("[transform] → %s (%d lignes)", csv_name, len(df_out))

    return {
        "status":        "ok",
        "lignes_input":  n_input,
        "lignes_output": len(df_out),
        "last_date":     last_date,
        "output":        str(csv_path),
    }


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 3 — LOAD
# ─────────────────────────────────────────────────────────────────────────────

def phase_load(
    new_csv_path:   Path,
    database_path:  Path,
    archive_dir:    Path,
    source:         str = "manuel",   # "manuel" ou "scrap"
    keep_versioned: int = 30,          # nombre de snapshots datés conservés
) -> dict:
    """
    Archive la database existante, fusionne les nouvelles données,
    exporte Database_Enedis_30_min.csv (courant) + un snapshot daté dans archive_dir/.

    Stratégie de fusion selon `source` :
        • 'manuel' (défaut) : les nouvelles données ÉCRASENT les valeurs existantes
                              sur les (Date, Time) en collision  →  keep='last'
        • 'scrap'           : les nouvelles données NE REMPLACENT PAS les valeurs
                              déjà présentes (priorité au canal manuel)  →  keep='first'

    Dans les deux cas, les divergences entre nouvelles et anciennes valeurs sur les
    (Date, Time) communs sont LOGGÉES (jusqu'à 10 lignes) pour audit.

    Versionnement : Database_Enedis_30_min_YYYYMMDD.csv est créé dans `archive_dir/`
    (PAS à la racine de curated/) avec rotation `keep_versioned`.

    Schéma DB :
        Date ; Time ; source ; Conso (W)
    Colonne `source` :
        • 'manuel' : donnée venant d'un XLSX déposé manuellement dans inbox_enedis/
        • 'auto'   : donnée venant du canal scrap (download Playwright)

    Retourne un dict de stats.
    """
    if source not in ("manuel", "scrap"):
        raise ValueError(f"source doit être 'manuel' ou 'scrap', reçu : {source!r}")

    # Mapping vers le libellé écrit dans la DB (colonne source)
    SOURCE_LABEL = {"manuel": "manuel", "scrap": "auto"}
    db_source_label = SOURCE_LABEL[source]
    DB_COLUMNS = ["Date", "Time", "source", "Conso (W)"]

    archive_dir.mkdir(parents=True, exist_ok=True)

    if not new_csv_path.exists():
        raise FileNotFoundError(f"CSV new_data introuvable : {new_csv_path}")

    # ── Helpers de lecture robuste ───────────────────────────────────────────
    def _read_csv_clean(path: Path) -> pd.DataFrame:
        """Lit un CSV en purgeant les octets NUL (corruption fréquente sur mounts Windows)."""
        raw = path.read_bytes().replace(b"\x00", b"")
        import io
        return pd.read_csv(io.BytesIO(raw), sep=";", dtype=str, encoding="utf-8",
                           on_bad_lines="skip")

    # ── Lecture du CSV new_data ──────────────────────────────────────────────
    df_new = _read_csv_clean(new_csv_path)
    df_new.columns = [c.strip() for c in df_new.columns]

    # Nettoyage des valeurs (espaces parasites, virgule → point pour les floats)
    for col in ["Date", "Time"]:
        if col in df_new.columns:
            df_new[col] = df_new[col].str.strip()
    if "Conso (W)" in df_new.columns:
        df_new["Conso (W)"] = (
            df_new["Conso (W)"]
            .str.strip()
            .str.replace(",", ".", regex=False)
        )
        df_new["Conso (W)"] = pd.to_numeric(df_new["Conso (W)"], errors="coerce")

    df_new = df_new.dropna(subset=["Date", "Time", "Conso (W)"])
    # Tag le canal d'origine : 'manuel' ou 'auto' (= ex 'scrap')
    df_new["source"] = db_source_label
    log.info("[load] %d lignes new_data lues (%s … %s)  source=%s",
             len(df_new),
             df_new["Date"].min() if not df_new.empty else "N/A",
             df_new["Date"].max() if not df_new.empty else "N/A",
             db_source_label)

    if df_new.empty:
        raise ValueError(
            f"new_data CSV vide après parsing : {new_csv_path}. "
            "Vérifier le format (séparateur ';', colonnes Date;Time;Conso (W))."
        )

    # ── Lecture de la database existante ────────────────────────────────────
    if database_path.exists():
        df_db = _read_csv_clean(database_path)
        df_db.columns = [c.strip() for c in df_db.columns]
        for col in ["Date", "Time"]:
            if col in df_db.columns:
                df_db[col] = df_db[col].str.strip()
        if "Conso (W)" in df_db.columns:
            df_db["Conso (W)"] = (
                df_db["Conso (W)"]
                .str.strip()
                .str.replace(",", ".", regex=False)
            )
            df_db["Conso (W)"] = pd.to_numeric(df_db["Conso (W)"], errors="coerce")
        # Backward compat : si la DB n'a pas encore la colonne source (ancien schéma),
        # on la crée en backfillant 'manuel' (l'historique vient des XLSX manuels).
        if "source" not in df_db.columns:
            log.warning("[load] DB sans colonne 'source' — backfill 'manuel' (compat)")
            df_db["source"] = "manuel"
        else:
            df_db["source"] = df_db["source"].astype(str).str.strip()
        df_db = df_db.dropna(subset=["Date", "Time", "Conso (W)"])
        log.info("[load] Database existante : %d lignes (%s … %s)",
                 len(df_db), df_db["Date"].min(), df_db["Date"].max())

        # Archivage de la database avant modification
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_name = f"Database_Enedis_30_min__{stamp}.csv"
        shutil.copy2(str(database_path), str(archive_dir / archive_name))
        log.info("[load] Database archivée → %s", archive_name)
    else:
        df_db = pd.DataFrame(columns=DB_COLUMNS)
        log.info("[load] Aucune database existante — création ex nihilo")

    n_db_before = len(df_db)
    n_new       = len(df_new)

    log.info("[load] Fusion (source=%s) : %d lignes DB + %d lignes new_data",
             source, n_db_before, n_new)

    # ── Audit des divergences sur les (Date, Time) communs ───────────────────
    # Compare les valeurs Conso(W) entre DB existante et new_data avant fusion.
    if not df_db.empty and not df_new.empty:
        db_idx  = df_db.set_index(["Date", "Time"])["Conso (W)"]
        new_idx = df_new.set_index(["Date", "Time"])["Conso (W)"]
        common  = db_idx.index.intersection(new_idx.index)
        if len(common):
            cmp = pd.DataFrame({
                "db":  db_idx.loc[common].astype(float),
                "new": new_idx.loc[common].astype(float),
            })
            cmp["delta_abs"] = (cmp["new"] - cmp["db"]).abs()
            n_diverge = (cmp["delta_abs"] >= 0.01).sum()
            log.info("[load] Audit chevauchement : %d (Date,Time) communs — "
                     "%d identiques, %d divergents",
                     len(common), len(common) - n_diverge, n_diverge)
            if n_diverge > 0:
                top = cmp[cmp["delta_abs"] >= 0.01].nlargest(10, "delta_abs")
                log.warning("[load] DIVERGENCES (top 10) — source=%s :", source)
                for (d, t), row in top.iterrows():
                    winner = "new" if source == "manuel" else "db"
                    log.warning("  %s %s : db=%.1f vs new=%.1f (Δ=%.1f) → %s gagne",
                                d, t, row["db"], row["new"], row["new"] - row["db"], winner)

    # ── Fusion + déduplication selon priorité ────────────────────────────────
    # source='manuel' → keep='last'  : new_data écrase la DB existante
    # source='scrap'  → keep='first' : DB existante (peut-être manuelle) gagne
    keep_strategy = "last" if source == "manuel" else "first"
    df_merged = (
        pd.concat([df_db, df_new], ignore_index=True)
        .drop_duplicates(subset=["Date", "Time"], keep=keep_strategy)
    )
    log.info("[load] Après fusion+dedup : %d lignes (attendu ≥ %d)",
             len(df_merged), max(n_db_before, n_new))

    if len(df_merged) < n_db_before:
        log.error("[load] ANOMALIE : fusion produit MOINS de lignes que la database "
                  "(%d < %d) — fusion annulée pour protéger les données",
                  len(df_merged), n_db_before)
        raise RuntimeError(
            f"Fusion anormale : {len(df_merged)} lignes < {n_db_before} lignes existantes."
        )

    # ── Tri chronologique ────────────────────────────────────────────────────
    # "23:59:59" doit se trier APRÈS "23:30:00" → remplacement temporaire
    def _sort_key(row):
        t = row["Time"]
        return row["Date"] + ("235960" if t == "23:59:59" else t.replace(":", ""))

    df_merged["_sort"] = df_merged.apply(_sort_key, axis=1)
    df_merged = df_merged.sort_values("_sort").drop(columns=["_sort"]).reset_index(drop=True)

    n_merged  = len(df_merged)
    n_added   = n_merged - n_db_before
    last_date = df_merged["Date"].max().replace("-", "")
    log.info("[load] Résultat fusion : %d lignes (+%d nouvelles) jusqu'au %s",
             n_merged, n_added, last_date)

    # ── Export courant ───────────────────────────────────────────────────────
    df_merged["Conso (W)"] = df_merged["Conso (W)"].map(lambda x: f"{x:.1f}")
    # Garantit l'ordre canonique : Date ; Time ; source ; Conso (W)
    df_merged = df_merged[DB_COLUMNS]
    df_merged.to_csv(database_path, sep=";", index=False, encoding="utf-8")
    log.info("[load] Database mise à jour → %s (%d lignes, schéma %s)",
             database_path.name, n_merged, ";".join(DB_COLUMNS))

    # ── Export versionné (dans archive_dir/, PAS à la racine curated/) ───────
    versioned_name = f"Database_Enedis_30_min_{last_date}.csv"
    versioned_path = archive_dir / versioned_name
    shutil.copy2(str(database_path), str(versioned_path))
    log.info("[load] Snapshot daté → %s/%s", archive_dir.name, versioned_name)

    # ── Rotation des snapshots datés (conserve les `keep_versioned` plus récents) ─
    _VER_GLOB = "Database_Enedis_30_min_????????.csv"
    versions = sorted(archive_dir.glob(_VER_GLOB), key=lambda p: p.name)
    if len(versions) > keep_versioned:
        for old_ver in versions[:-keep_versioned]:
            old_ver.unlink()
            log.info("[load] Snapshot rotation (>%d) — supprimé : %s",
                     keep_versioned, old_ver.name)

    # ── Nettoyage défensif : supprime tout snapshot daté qui traînerait
    # à la racine curated/ (héritage de l'ancienne logique).
    legacy_root = database_path.parent
    for stray in legacy_root.glob(_VER_GLOB):
        if stray.resolve() != database_path.resolve():
            stray.unlink()
            log.info("[load] Nettoyage legacy — supprimé à la racine : %s", stray.name)

    # Répartition par source pour audit
    source_breakdown = df_merged["source"].value_counts().to_dict()
    log.info("[load] Répartition DB par source : %s", source_breakdown)

    return {
        "status":           "ok",
        "source":           source,
        "source_label":     db_source_label,
        "lignes_new_data":  n_new,
        "lignes_avant":     n_db_before,
        "lignes_apres":     n_merged,
        "lignes_ajoutees":  n_added,
        "source_breakdown": source_breakdown,
        "last_date":        last_date,
        "database":         str(database_path),
        "versioned":        str(versioned_path),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Orchestrateur principal
# ─────────────────────────────────────────────────────────────────────────────

def run_etl(
    inbox_dir:         str | Path,
    inbox_archive_dir: str | Path,
    raw_dir:           str | Path,
    curated_dir:       str | Path,
    db_archive_dir:    str | Path,
    database_path:     str | Path,
    dst_table:         str | Path = DST_TABLE_PATH,
    source:            str = "manuel",
) -> dict:
    """
    Orchestre les trois phases ETL :
        1. EXTRACT   inbox XLSX → new_data_enedis_YYYYMMDD.xlsx  (dans raw_dir)
        2. TRANSFORM new_data xlsx → new_data_enedis_YYYYMMDD.csv (dans raw_dir)
        3. LOAD      new_data csv → Database_Enedis_30_min.csv    (dans curated_dir)

    Séparation des répertoires :
        raw_dir     : fichiers intermédiaires (xlsx concaténé + csv transformé)
        curated_dir : database finale (Database_Enedis_30_min.csv)

    `source` : 'manuel' ou 'scrap' — détermine la priorité en cas de conflit
    sur (Date, Time) lors de la phase LOAD (manuel écrase, scrap respecte).

    Retourne un dict de synthèse avec les stats des trois phases.
    """
    inbox_dir         = Path(inbox_dir)
    inbox_archive_dir = Path(inbox_archive_dir)
    raw_dir           = Path(raw_dir)
    curated_dir       = Path(curated_dir)
    db_archive_dir    = Path(db_archive_dir)
    database_path     = Path(database_path)
    dst_table         = Path(dst_table)

    log.info("=" * 70)
    log.info("ETL INBOX ENEDIS — DÉMARRAGE (source=%s)", source)
    log.info("=" * 70)

    # ── Phase 1 : Extract ────────────────────────────────────────────────────
    log.info("[1/3] EXTRACT")
    r_extract = phase_extract(inbox_dir, inbox_archive_dir, raw_dir)

    if r_extract["status"] != "ok":
        log.warning("[1/3] EXTRACT : %s", r_extract.get("message", r_extract["status"]))
        return {"status": r_extract["status"], "extract": r_extract,
                "transform": None, "load": None}

    # ── Phase 2 : Transform ──────────────────────────────────────────────────
    log.info("[2/3] TRANSFORM")
    r_transform = phase_transform(Path(r_extract["output"]), raw_dir, dst_table)

    if r_transform["status"] != "ok" or r_transform["output"] is None:
        log.error("[2/3] TRANSFORM : échec ou aucune donnée")
        return {"status": "transform_failed", "extract": r_extract,
                "transform": r_transform, "load": None}

    # ── Phase 3 : Load ───────────────────────────────────────────────────────
    log.info("[3/3] LOAD")
    r_load = phase_load(
        new_csv_path  = Path(r_transform["output"]),
        database_path = database_path,
        archive_dir   = db_archive_dir,
        source        = source,
    )

    log.info("=" * 70)
    log.info("ETL TERMINÉ — %d lignes ajoutées / %d lignes total",
             r_load["lignes_ajoutees"], r_load["lignes_apres"])
    log.info("=" * 70)

    return {
        "status":    "ok",
        "extract":   r_extract,
        "transform": r_transform,
        "load":      r_load,
    }


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main() -> int:
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s : %(message)s",
        datefmt="%H:%M:%S",
    )

    _RAW     = Path("/opt/airflow/data/raw/conso_elec/enedis")
    _CURATED = Path("/opt/airflow/data/curated/conso_elec/enedis")

    p = argparse.ArgumentParser(description="ETL Enedis (manuel ou scrap) → Database CSV")
    p.add_argument("--inbox",    type=Path, default=_RAW / "inbox_enedis")
    p.add_argument("--raw",      type=Path, default=_RAW / "_manuel",
                   help="Répertoire RAW pour les fichiers intermédiaires (xlsx/csv new_data)")
    p.add_argument("--curated",  type=Path, default=_CURATED,
                   help="Répertoire CURATED pour la database finale")
    p.add_argument("--database", type=Path,
                   default=_CURATED / "Database_Enedis_30_min.csv")
    p.add_argument("--dst-table", type=Path, default=DST_TABLE_PATH)
    p.add_argument("--source",   choices=["manuel", "scrap"], default="manuel",
                   help="Canal source — détermine la priorité en cas de conflit "
                        "(manuel écrase, scrap respecte)")
    args = p.parse_args()

    result = run_etl(
        inbox_dir         = args.inbox,
        inbox_archive_dir = args.inbox / "archive",
        raw_dir           = args.raw,
        curated_dir       = args.curated,
        db_archive_dir    = args.curated / "archive",
        database_path     = args.database,
        dst_table         = args.dst_table,
        source            = args.source,
    )

    if result["status"] == "ok":
        return 0
    elif result["status"] in ("no_files", "no_data"):
        log.info("Rien à traiter : %s", result["status"])
        return 0
    else:
        log.error("ETL échoué : %s", result["status"])
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
