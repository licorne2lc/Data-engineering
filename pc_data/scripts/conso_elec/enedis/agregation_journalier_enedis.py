# -*- coding: utf-8 -*-
"""
agregation_journalier_enedis.py
================================
Agrégation locale 30 min → journalier pour la consommation Enedis.

Pourquoi pas de scraping séparé ?
---------------------------------
Comparaison empirique sur 27 jours (export Enedis "Energie (kWh)" du
2026-03-28 au 2026-04-27 vs somme(Conso_W)/2000 sur Database_Enedis_30_min.csv) :

    Δ absolu moyen     : 0.000 kWh
    Δ absolu max       : 0.000 kWh
    Jours identiques   : 27 / 27

L'export "Energie (kWh)" Enedis est strictement la somme des 48 tranches
de la courbe de charge 30 min (puissance moyenne en W, durée 0.5 h →
énergie kWh = W × 0.0005). On reconstitue donc localement, sans Playwright.

Schéma de la database journalière produite
-------------------------------------------
    fichier  : data/curated/conso_elec/enedis/database_enedis_journalier.csv
    colonnes : Date ; source ; Conso (kWh)
        Date          date locale Europe/Paris (YYYY-MM-DD)
        source        'agregat_30min'
        Conso (kWh)   somme des tranches du jour ÷ 2000, arrondi 3 décimales

Gestion des changements d'heure (DST) — IMPORTANT
--------------------------------------------------
Les jours de changement d'heure (cf. data/curated/calendaire/chgt_heure/
table_chgt_heure.csv) sont déjà NORMALISÉS à 48 tranches par la phase
TRANSFORM de l'ETL (`etl_inbox_enedis._apply_dst`). Donc, à l'entrée de
ce module :

  • SPRING-FORWARD (passage hiver→été, fin mars) :
        L'horloge saute de 02:00 CET à 03:00 CEST (le jour ne fait
        que 23h). L'ETL Transform INSÈRE 2 tranches synthétiques à
        0.1 W (placeholder Time=02:00:00 et Time=02:30:00) → 48 tranches.
        Conséquence sur l'agrégation journalière : un sur-comptage
        artificiel de (0.1 + 0.1) × 0.5 / 1000 = 0.0001 kWh par jour
        spring-forward. Ce biais étant inférieur à la résolution
        d'arrondi (3 décimales), il est ignoré ici.

  • FALL-BACK (passage été→hiver, fin octobre) :
        L'horloge recule de 03:00 CEST à 02:00 CET (le jour fait 25h).
        L'export Enedis brut comporte 50 tranches (01:30 et 02:00 deux
        fois). L'ETL Transform SOMME les doublons (Time=02:00:00 et
        02:30:00 portent la conso totale des deux passages) → 48
        tranches. Conséquence : la somme des Conso_W ÷ 2000 reflète
        bien la consommation réelle du jour de 25h.

Ce module se contente donc d'exiger 48 tranches par jour. Les jours
qui n'ont pas 48 tranches (la plupart du temps : J-1 ou J-2 en cours
d'alimentation) sont EXCLUS de la DB journalière. Le module loggue
EXPLICITEMENT chaque jour DST traversé (audit), et FLAGGE comme
anomalie tout jour DST qui n'aurait pas 48 tranches (ETL défaillant).

Versionnement
-------------
À chaque exécution :
    • database_enedis_journalier.csv (courant) est écrit
    • snapshot database_enedis_journalier_YYYYMMDD.csv déposé dans
      archive/ (rotation : 30 fichiers max)
"""
from __future__ import annotations

import csv
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

# Conversion W (puissance moyenne sur 30 min) → kWh par tranche :
#   énergie = puissance (kW) × durée (h) = (W / 1000) × 0.5 = W / 2000
W_TO_KWH_PER_30MIN = 1.0 / 2000.0

# Nombre de tranches attendues par jour APRÈS le passage par etl_inbox_enedis
# (cf. docstring : la phase Transform normalise spring-forward et fall-back à 48)
EXPECTED_TRANCHES_PAR_JOUR = 48

# Libellé écrit dans la colonne 'source' (constant : valeur calculée localement)
SOURCE_LABEL = "agregat_30min"

# Ordre canonique des colonnes de la DB journalière
DB_JOURNALIER_COLUMNS = ["Date", "source", "Conso (kWh)"]

# Pattern et rotation des snapshots datés
_VER_GLOB = "database_enedis_journalier_????????.csv"
KEEP_VERSIONED_DEFAULT = 30

# Chemin par défaut de la table des changements d'heure (Europe/Paris)
DST_TABLE_DEFAULT = Path(
    "/opt/airflow/data/curated/calendaire/chgt_heure/table_chgt_heure.csv"
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _read_csv_clean(path: Path) -> pd.DataFrame:
    """
    Lit un CSV en purgeant les octets NUL (corruption fréquente sur les
    montages Windows ↔ Linux). Retourne toutes les colonnes en str.
    """
    raw = path.read_bytes().replace(b"\x00", b"")
    return pd.read_csv(io.BytesIO(raw), sep=";", dtype=str, encoding="utf-8",
                       on_bad_lines="skip")


def _load_dst_dates(path: Path) -> tuple[set[str], set[str]]:
    """
    Lit la table_chgt_heure.csv (séparateur virgule, en-tête 'Date,ete/hivers').
    Retourne (spring_forward_dates, fall_back_dates) — ensembles de str ISO.

    Si la table est absente, retourne deux ensembles vides + log warning.
    Le module continuera à fonctionner mais la VALIDATION DST sera neutralisée.
    """
    spring: set[str] = set()
    fall:   set[str] = set()
    if not path.exists():
        log.warning("[journalier/DST] Table introuvable : %s — "
                    "validation DST désactivée", path)
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
    log.info("[journalier/DST] Table chargée : %d spring-forward, %d fall-back",
             len(spring), len(fall))
    return spring, fall


def _load_db_30min(database_30min_path: Path) -> pd.DataFrame:
    """
    Charge Database_Enedis_30_min.csv et retourne un DataFrame propre :
        Date (str YYYY-MM-DD), Time (str HH:MM:SS), Conso_W (float)
    """
    if not database_30min_path.exists():
        raise FileNotFoundError(
            f"Database 30 min introuvable : {database_30min_path}"
        )
    df = _read_csv_clean(database_30min_path)
    df.columns = [c.strip() for c in df.columns]

    for col in ("Date", "Time"):
        if col not in df.columns:
            raise ValueError(
                f"Colonne '{col}' manquante dans {database_30min_path.name} "
                f"(colonnes lues : {list(df.columns)})"
            )
        df[col] = df[col].astype(str).str.strip()

    if "Conso (W)" not in df.columns:
        raise ValueError(
            f"Colonne 'Conso (W)' manquante dans {database_30min_path.name}"
        )

    df["Conso_W"] = (
        df["Conso (W)"].astype(str).str.strip().str.replace(",", ".", regex=False)
    )
    df["Conso_W"] = pd.to_numeric(df["Conso_W"], errors="coerce")
    df = df.dropna(subset=["Date", "Time", "Conso_W"])

    log.info("[journalier] DB 30 min chargée : %d lignes (%s … %s)",
             len(df),
             df["Date"].min() if not df.empty else "N/A",
             df["Date"].max() if not df.empty else "N/A")
    return df[["Date", "Time", "Conso_W"]].copy()


# ─────────────────────────────────────────────────────────────────────────────
# Audit DST
# ─────────────────────────────────────────────────────────────────────────────

def _audit_dst_days(df_30min: pd.DataFrame,
                    spring_dates: set[str],
                    fall_dates: set[str],
                    expected_tranches: int = EXPECTED_TRANCHES_PAR_JOUR
                    ) -> dict:
    """
    Vérifie la cohérence des jours DST dans la DB 30 min APRÈS ETL.

    Pour chaque date DST présente dans la DB :
        • compte les tranches → doit valoir `expected_tranches` (= 48)
        • spring-forward : vérifie la présence du placeholder 0.1 W
                           sur Time=02:00:00 et Time=02:30:00
        • fall-back     : vérifie que les tranches 02:00:00 et 02:30:00
                           sont présentes (elles portent la SOMME des
                           doublons après ETL)
    Tout écart est loggué en WARNING (anomalie ETL probable).

    Retourne un dict d'audit :
        spring_days   : liste [{date, nb_tranches, kwh, ok, placeholder_ok}]
        fall_days     : liste [{date, nb_tranches, kwh, ok}]
        anomalies     : liste de chaînes lisibles
    """
    spring_audit: list[dict] = []
    fall_audit:   list[dict] = []
    anomalies:    list[str]  = []

    if df_30min.empty:
        return {"spring_days": [], "fall_days": [], "anomalies": []}

    dates_in_db = set(df_30min["Date"].unique())

    # ── Spring-forward audit ─────────────────────────────────────────────────
    for d in sorted(spring_dates & dates_in_db):
        sub = df_30min[df_30min["Date"] == d]
        nb = len(sub)
        kwh = float(sub["Conso_W"].sum()) * W_TO_KWH_PER_30MIN
        # Le placeholder est inséré par etl_inbox_enedis._apply_dst sur Debut
        # 01:30 et 02:00 → après _debut_to_fin, Time devient 02:00:00 et 02:30:00
        ph_0200 = (
            (sub["Time"] == "02:00:00") &
            (sub["Conso_W"].between(0.05, 0.15))
        ).any()
        ph_0230 = (
            (sub["Time"] == "02:30:00") &
            (sub["Conso_W"].between(0.05, 0.15))
        ).any()
        ok = (nb == expected_tranches)
        ph_ok = bool(ph_0200 and ph_0230)
        spring_audit.append({
            "date":           d,
            "nb_tranches":    nb,
            "kwh":            round(kwh, 3),
            "ok":             ok,
            "placeholder_ok": ph_ok,
        })
        if not ok:
            anomalies.append(
                f"Spring-forward {d} : {nb} tranches au lieu de "
                f"{expected_tranches} (ETL Transform défaillant ?)"
            )
        elif not ph_ok:
            anomalies.append(
                f"Spring-forward {d} : 48 tranches OK mais "
                f"placeholder 0.1 W absent sur 02:00/02:30 "
                f"(ph_0200={ph_0200}, ph_0230={ph_0230})"
            )
        log.info("[journalier/DST] spring-fwd %s : %d tr | %.3f kWh | "
                 "placeholder=%s",
                 d, nb, kwh, "OK" if ph_ok else "ABSENT")

    # ── Fall-back audit ──────────────────────────────────────────────────────
    for d in sorted(fall_dates & dates_in_db):
        sub = df_30min[df_30min["Date"] == d]
        nb = len(sub)
        kwh = float(sub["Conso_W"].sum()) * W_TO_KWH_PER_30MIN
        # Les tranches 02:00:00 et 02:30:00 (Fin) doivent être présentes —
        # elles portent la SOMME des deux passages (CEST+CET).
        has_0200 = (sub["Time"] == "02:00:00").any()
        has_0230 = (sub["Time"] == "02:30:00").any()
        ok = (nb == expected_tranches)
        fall_audit.append({
            "date":         d,
            "nb_tranches":  nb,
            "kwh":          round(kwh, 3),
            "ok":           ok,
            "has_0200":     bool(has_0200),
            "has_0230":     bool(has_0230),
        })
        if not ok:
            anomalies.append(
                f"Fall-back {d} : {nb} tranches au lieu de "
                f"{expected_tranches} (ETL Transform défaillant ? "
                f"Doublons CEST/CET non sommés ?)"
            )
        elif not (has_0200 and has_0230):
            anomalies.append(
                f"Fall-back {d} : 48 tranches mais Time 02:00:00/02:30:00 "
                f"absentes (has_0200={has_0200}, has_0230={has_0230})"
            )
        log.info("[journalier/DST] fall-back %s : %d tr | %.3f kWh | "
                 "Time(02:00)=%s | Time(02:30)=%s",
                 d, nb, kwh,
                 "OK" if has_0200 else "ABSENT",
                 "OK" if has_0230 else "ABSENT")

    if anomalies:
        log.warning("[journalier/DST] %d anomalie(s) détectée(s) :",
                    len(anomalies))
        for a in anomalies:
            log.warning("   ⚠ %s", a)

    return {
        "spring_days": spring_audit,
        "fall_days":   fall_audit,
        "anomalies":   anomalies,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Cœur d'agrégation
# ─────────────────────────────────────────────────────────────────────────────

def _aggregate_to_daily(df_30min: pd.DataFrame,
                        expected_tranches: int = EXPECTED_TRANCHES_PAR_JOUR
                        ) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Agrège la DB 30 min en données journalières.

    Retourne un tuple (df_complets, df_partiels) :
        df_complets : jours avec exactement `expected_tranches` tranches.
                      Colonnes : Date, source, Conso (kWh)
        df_partiels : jours avec un nombre différent (audit).
                      Colonnes : Date, nb_tranches, Conso (kWh)
    """
    if df_30min.empty:
        empty = pd.DataFrame(columns=DB_JOURNALIER_COLUMNS)
        empty_part = pd.DataFrame(columns=["Date", "nb_tranches", "Conso (kWh)"])
        return empty, empty_part

    grp = df_30min.groupby("Date", as_index=False).agg(
        nb_tranches=("Conso_W", "size"),
        somme_W=("Conso_W", "sum"),
    )
    grp["Conso (kWh)"] = (grp["somme_W"] * W_TO_KWH_PER_30MIN).round(3)

    mask_complet = grp["nb_tranches"] == expected_tranches
    df_complets = grp.loc[mask_complet, ["Date", "Conso (kWh)"]].copy()
    df_complets["source"] = SOURCE_LABEL
    df_complets = df_complets[DB_JOURNALIER_COLUMNS]
    df_complets = df_complets.sort_values("Date").reset_index(drop=True)

    df_partiels = grp.loc[~mask_complet, ["Date", "nb_tranches", "Conso (kWh)"]].copy()
    df_partiels = df_partiels.sort_values("Date").reset_index(drop=True)

    return df_complets, df_partiels


# ─────────────────────────────────────────────────────────────────────────────
# Versionnement / archive
# ─────────────────────────────────────────────────────────────────────────────

def _archive_snapshot(database_journalier_path: Path,
                      archive_dir: Path,
                      keep_versioned: int) -> Path:
    """
    Copie la DB courante en database_enedis_journalier_YYYYMMDD.csv dans
    archive_dir/, puis applique la rotation (keep_versioned plus récents).
    """
    archive_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d")
    versioned_name = f"database_enedis_journalier_{stamp}.csv"
    versioned_path = archive_dir / versioned_name
    shutil.copy2(str(database_journalier_path), str(versioned_path))
    log.info("[journalier] Snapshot daté → archive/%s", versioned_name)

    versions = sorted(archive_dir.glob(_VER_GLOB), key=lambda p: p.name)
    if len(versions) > keep_versioned:
        for old_ver in versions[:-keep_versioned]:
            old_ver.unlink()
            log.info("[journalier] Rotation (>%d) — supprimé : %s",
                     keep_versioned, old_ver.name)
    return versioned_path


# ─────────────────────────────────────────────────────────────────────────────
# API publique
# ─────────────────────────────────────────────────────────────────────────────

def run(
    database_30min_path:      Path,
    database_journalier_path: Path,
    archive_dir:              Path,
    dst_table:                Path = DST_TABLE_DEFAULT,
    keep_versioned:           int  = KEEP_VERSIONED_DEFAULT,
    expected_tranches:        int  = EXPECTED_TRANCHES_PAR_JOUR,
) -> dict:
    """
    Pipeline complet : DB 30 min → DB journalière.

    Étapes :
        0. Chargement des dates DST (table_chgt_heure.csv)
        1. Lecture de Database_Enedis_30_min.csv
        2. AUDIT DST : pour chaque jour spring-forward / fall-back présent,
           vérifier que l'ETL Transform a bien normalisé à 48 tranches.
           Toute anomalie est loggée en WARNING.
        3. Agrégation par Date (sum(Conso_W) / 2000 = kWh)
        4. Filtrage : on ne conserve que les jours avec 48 tranches
           (jours partiels listés pour audit dans le retour)
        5. Écriture de database_enedis_journalier.csv (courant)
        6. Snapshot daté + rotation dans archive_dir/

    Retourne un dict de stats :
        status                'ok' | 'no_data'
        lignes_30min          lignes lues dans la DB 30 min
        jours_complets        jours retenus = lignes écrites
        jours_partiels        nombre de jours rejetés (< 48 tranches)
        partiels_details      max 10 [Date, nb_tranches]
        first_date / last_date  bornes de la DB journalière produite
        dst_audit             dict audit_dst_days (spring + fall + anomalies)
        dst_anomalies         nombre d'anomalies DST détectées
        database / versioned  chemins des fichiers écrits
    """
    database_30min_path      = Path(database_30min_path)
    database_journalier_path = Path(database_journalier_path)
    archive_dir              = Path(archive_dir)
    dst_table                = Path(dst_table)

    # ── 0. Chargement des dates DST ──────────────────────────────────────────
    spring_dates, fall_dates = _load_dst_dates(dst_table)

    # ── 1. Lecture DB 30 min ─────────────────────────────────────────────────
    df_30min = _load_db_30min(database_30min_path)
    n_30min = len(df_30min)

    if df_30min.empty:
        log.warning("[journalier] DB 30 min vide — aucune sortie")
        return {
            "status":         "no_data",
            "lignes_30min":   0,
            "jours_complets": 0,
            "jours_partiels": 0,
            "dst_audit":      {"spring_days": [], "fall_days": [], "anomalies": []},
            "dst_anomalies":  0,
        }

    # ── 2. Audit DST ─────────────────────────────────────────────────────────
    dst_audit = _audit_dst_days(df_30min, spring_dates, fall_dates,
                                expected_tranches)
    n_dst_anomalies = len(dst_audit["anomalies"])

    # ── 3 & 4. Agrégation + filtrage jours complets ──────────────────────────
    df_complets, df_partiels = _aggregate_to_daily(df_30min, expected_tranches)
    n_complets = len(df_complets)
    n_partiels = len(df_partiels)

    log.info("[journalier] Agrégation : %d jours complets (=%d tranches), "
             "%d jours partiels exclus, %d anomalie(s) DST",
             n_complets, expected_tranches, n_partiels, n_dst_anomalies)

    if n_partiels and n_partiels <= 20:
        for _, row in df_partiels.head(20).iterrows():
            log.info("[journalier]   ⤷ partiel %s : %d/%d tranches (%.3f kWh)",
                     row["Date"], int(row["nb_tranches"]),
                     expected_tranches, row["Conso (kWh)"])
    elif n_partiels > 20:
        log.info("[journalier]   ⤷ %d jours partiels (top 5) :", n_partiels)
        for _, row in df_partiels.head(5).iterrows():
            log.info("       %s : %d/%d tranches",
                     row["Date"], int(row["nb_tranches"]), expected_tranches)

    if n_complets == 0:
        log.warning("[journalier] Aucun jour complet — pas d'écriture")
        return {
            "status":           "no_data",
            "lignes_30min":     n_30min,
            "jours_complets":   0,
            "jours_partiels":   n_partiels,
            "dst_audit":        dst_audit,
            "dst_anomalies":    n_dst_anomalies,
            "partiels_details": [
                [row["Date"], int(row["nb_tranches"])]
                for _, row in df_partiels.head(10).iterrows()
            ],
        }

    # ── 5. Écriture DB journalière (3 décimales pour matcher l'export Enedis)
    database_journalier_path.parent.mkdir(parents=True, exist_ok=True)
    df_out = df_complets.copy()
    df_out["Conso (kWh)"] = df_out["Conso (kWh)"].map(lambda x: f"{x:.3f}")
    df_out = df_out[DB_JOURNALIER_COLUMNS]
    df_out.to_csv(database_journalier_path, sep=";", index=False, encoding="utf-8")

    first_date = df_complets["Date"].min()
    last_date  = df_complets["Date"].max()
    log.info("[journalier] DB journalière écrite : %s (%d jours, %s … %s)",
             database_journalier_path.name, n_complets, first_date, last_date)

    # ── 6. Snapshot daté + rotation
    versioned_path = _archive_snapshot(database_journalier_path, archive_dir,
                                       keep_versioned)

    return {
        "status":           "ok",
        "lignes_30min":     n_30min,
        "jours_complets":   n_complets,
        "jours_partiels":   n_partiels,
        "partiels_details": [
            [row["Date"], int(row["nb_tranches"])]
            for _, row in df_partiels.head(10).iterrows()
        ],
        "first_date":       first_date,
        "last_date":        last_date,
        "dst_audit":        dst_audit,
        "dst_anomalies":    n_dst_anomalies,
        "database":         str(database_journalier_path),
        "versioned":        str(versioned_path),
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
        description="Agrégation Database_Enedis_30_min.csv → "
                    "database_enedis_journalier.csv"
    )
    p.add_argument("--db-30min", type=Path,
                   default=_CURATED / "Database_Enedis_30_min.csv")
    p.add_argument("--db-journalier", type=Path,
                   default=_CURATED / "database_enedis_journalier.csv")
    p.add_argument("--archive", type=Path,
                   default=_CURATED / "archive")
    p.add_argument("--dst-table", type=Path, default=DST_TABLE_DEFAULT)
    p.add_argument("--keep", type=int, default=KEEP_VERSIONED_DEFAULT,
                   help="Nombre de snapshots datés conservés")
    args = p.parse_args()

    result = run(
        database_30min_path      = args.db_30min,
        database_journalier_path = args.db_journalier,
        archive_dir              = args.archive,
        dst_table                = args.dst_table,
        keep_versioned           = args.keep,
    )

    if result["status"] == "ok":
        print(f"OK : {result['jours_complets']} jours écrits "
              f"({result['first_date']} → {result['last_date']}) "
              f"| {result['dst_anomalies']} anomalie(s) DST")
        return 0
    elif result["status"] == "no_data":
        print("Aucune donnée à agréger.")
        return 0
    else:
        print(f"Échec : {result['status']}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
