# -*- coding: utf-8 -*-
"""
enrichir_vacances_scolaires.py
==============================
Enrichit le socle calendaire avec les colonnes vac_scol_A / vac_scol_B / vac_scol_C
à partir du dataset des vacances scolaires France métropolitaine
(data.education.gouv.fr).

Entrées :
    socle_calendrier.csv          (généré par socle_calendrier.py)
    vacances_scolaires.csv        (téléchargé par download_vacances_scolaires.py)

Sortie :
    calendrier.csv                (database calendaire enrichie)

Convention de remplissage (alignée sur 'exemple calendrier.csv') :
    vac_scol_X = 'Vacances d'Hiver' / 'Vacances de Noël' / ...   si jour vacant
    vac_scol_X = '--'                                            sinon

Logique métier (héritée de v3.4.py lignes 920-973, mais corrigée) :
  1. Filtrer les vacances aux Zones A/B/C uniquement (les zones DOM-TOM,
     Corse, Polynésie, etc. ne sont pas pertinentes pour la métropole).
  2. Dédoublonner par (description, start_date, end_date, zones) -- le
     dataset duplique chaque période par académie ; une seule ligne suffit.
  3. Convertir start_date / end_date (ISO 8601 UTC) en dates locales
     Europe/Paris -- les bornes du dataset sont à minuit Paris exprimées
     en UTC (ex. 2017-10-20T22:00:00+00:00 = 2017-10-21 00:00 Paris).
  4. Convention d'inclusion : [start_paris, end_paris)
     end_date dans le dataset = jour de RENTRÉE (premier jour d'école),
     donc EXCLUSIF.
  5. Pour chaque date du socle, remplir avec la description de la période
     correspondante par zone (s'il y en a plusieurs, on prend la première
     -- en pratique elles ne se chevauchent pas).
"""
from __future__ import annotations

import argparse
import logging
from datetime import date, datetime, timedelta
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    raise ImportError("pandas est requis : pip install pandas --break-system-packages")


log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Constantes
# ─────────────────────────────────────────────────────────────────────────────

PARIS_TZ_NAME = "Europe/Paris"

# Zones France métropolitaine (les seules qui peuplent vac_scol_A/B/C)
ZONES_METROPOLE = {"Zone A": "A", "Zone B": "B", "Zone C": "C"}

# Placeholder pour les jours hors vacances
PLACEHOLDER = "--"

# Colonnes attendues dans le socle (cohérent avec socle_calendrier.py)
SOCLE_COLUMNS = [
    "Date", "Jour de la semaine", "jour Sem", "N° semaine ISO",
    "Sem. Impaire", "UTC",
    "nom_jour_ferie", "vac_scol_A", "vac_scol_B", "vac_scol_C",
]

# Chemins par défaut (container Docker / Airflow).
# RAW utilise 'calendrier' (orthographe française, convention historique du projet).
# CURATED utilise 'calendaire' (typo conservée pour ne pas casser les autres scripts).
DEFAULT_SOCLE     = Path("/opt/airflow/data/curated/calendaire/socle_calendrier.csv")
DEFAULT_VACANCES  = Path("/opt/airflow/data/raw/calendrier/vacances/vacances_scolaires.csv")
DEFAULT_OUTPUT    = Path("/opt/airflow/data/curated/calendaire/calendrier.csv")


# ─────────────────────────────────────────────────────────────────────────────
# Lecture / nettoyage du dataset vacances
# ─────────────────────────────────────────────────────────────────────────────

def _load_vacances(path: Path) -> pd.DataFrame:
    """
    Lit le CSV des vacances scolaires data.education.gouv.fr et le ramène
    à un DataFrame minimal et propre :
        description        str   (ex. "Vacances d'Hiver")
        zone_lettre        str   ('A', 'B' ou 'C')
        start_date_paris   date  (jour inclusif)
        end_date_paris     date  (jour exclusif = jour de rentrée)

    Filtre :
      - zones in {Zone A, Zone B, Zone C}  (métropole)
      - dédoublonnage par (description, start, end, zone) car le dataset
        duplique chaque période par académie.
    """
    if not path.exists():
        raise FileNotFoundError(f"Fichier vacances introuvable : {path}")

    # encoding='utf-8-sig' pour avaler le BOM éventuel
    df = pd.read_csv(path, sep=";", dtype=str, encoding="utf-8-sig")
    df.columns = [c.strip().lower() for c in df.columns]

    # Filtre zones métropole
    df = df[df["zones"].isin(ZONES_METROPOLE.keys())].copy()
    log.info("[enrich] %d lignes vacances après filtre Zone A/B/C", len(df))

    # Conversion timezone : ISO UTC -> Europe/Paris -> date locale
    for col in ("start_date", "end_date"):
        ts = pd.to_datetime(df[col], utc=True, errors="coerce")
        df[f"{col}_paris"] = ts.dt.tz_convert(PARIS_TZ_NAME).dt.date

    df["zone_lettre"] = df["zones"].map(ZONES_METROPOLE)

    df = df[["description", "zone_lettre", "start_date_paris", "end_date_paris"]]
    df = df.drop_duplicates().reset_index(drop=True)

    # Sanity : aucune ligne avec date invalide
    n_bad = df[["start_date_paris", "end_date_paris"]].isna().any(axis=1).sum()
    if n_bad:
        log.warning("[enrich] %d ligne(s) avec date invalide -- supprimées",
                    n_bad)
        df = df.dropna(subset=["start_date_paris", "end_date_paris"]).reset_index(drop=True)

    log.info("[enrich] %d périodes de vacances uniques après dédoublonnage",
             len(df))
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Construction des index date -> description, par zone
# ─────────────────────────────────────────────────────────────────────────────

def _build_zone_index(vac_df: pd.DataFrame) -> dict[str, dict[date, str]]:
    """
    Pour chaque zone A/B/C, expanse les périodes de vacances en un dict
    { date_iso (date) : description }.

    Convention : end_date est EXCLUSIVE (jour de rentrée).
    Si plusieurs périodes se chevauchent sur un même jour pour une zone
    (cas rare), la première rencontrée gagne -- log un warning.
    """
    index: dict[str, dict[date, str]] = {z: {} for z in ZONES_METROPOLE.values()}
    n_overlap = 0

    for _, row in vac_df.iterrows():
        zone   = row["zone_lettre"]
        descr  = row["description"]
        start  = row["start_date_paris"]
        end    = row["end_date_paris"]   # exclusif

        if end <= start:
            log.warning("[enrich] Période invalide (end <= start) ignorée : "
                        "%s zone=%s [%s, %s)", descr, zone, start, end)
            continue

        d = start
        while d < end:
            existing = index[zone].get(d)
            if existing is not None and existing != descr:
                n_overlap += 1
                # Premier arrivé gagne (log discret pour ne pas spammer)
                if n_overlap <= 5:
                    log.warning("[enrich] Chevauchement zone=%s %s : %r vs %r "
                                "-- on garde %r", zone, d, existing, descr, existing)
            else:
                index[zone][d] = descr
            d += timedelta(days=1)

    if n_overlap:
        log.warning("[enrich] Total chevauchements résolus : %d", n_overlap)

    for z, idx in index.items():
        log.info("[enrich] Zone %s : %d jours de vacances indexés", z, len(idx))

    return index


# ─────────────────────────────────────────────────────────────────────────────
# Enrichissement du socle
# ─────────────────────────────────────────────────────────────────────────────

def enrichir(socle_path:    Path = DEFAULT_SOCLE,
             vacances_path: Path = DEFAULT_VACANCES,
             output_path:   Path = DEFAULT_OUTPUT) -> dict:
    """
    Fusionne le socle calendaire avec les périodes de vacances pour produire
    calendrier.csv enrichi.

    Retourne un dict de stats :
        lignes              : nb de lignes du calendrier produit
        jours_vac_A/B/C     : nb de jours marqués vacants par zone
        couverture          : Date min / max
        output              : chemin du CSV produit
    """
    if not socle_path.exists():
        raise FileNotFoundError(f"Socle introuvable : {socle_path}")

    log.info("[enrich] Chargement socle    : %s", socle_path)
    socle = pd.read_csv(socle_path, sep=";", dtype=str, encoding="utf-8")
    socle.columns = [c.strip() for c in socle.columns]

    if list(socle.columns) != SOCLE_COLUMNS:
        raise ValueError(
            f"Socle : colonnes inattendues.\n  reçu   : {list(socle.columns)}"
            f"\n  attendu: {SOCLE_COLUMNS}"
        )

    log.info("[enrich] Chargement vacances : %s", vacances_path)
    vac_df = _load_vacances(vacances_path)

    # Construction des index par zone
    zone_index = _build_zone_index(vac_df)

    # Conversion Date (str ISO) -> objet date pour le mapping
    socle["_date_obj"] = pd.to_datetime(socle["Date"], format="%Y-%m-%d").dt.date

    # Application des index par zone -- vectorisé via .map()
    for zone_lettre, col_name in [("A", "vac_scol_A"),
                                  ("B", "vac_scol_B"),
                                  ("C", "vac_scol_C")]:
        idx = zone_index[zone_lettre]
        socle[col_name] = socle["_date_obj"].map(idx).fillna(PLACEHOLDER)

    socle = socle.drop(columns=["_date_obj"])

    # Garantit l'ordre des colonnes
    socle = socle[SOCLE_COLUMNS]

    # Export
    output_path.parent.mkdir(parents=True, exist_ok=True)
    socle.to_csv(output_path, sep=";", index=False, encoding="utf-8")
    log.info("[enrich] Export -> %s (%d lignes)", output_path, len(socle))

    # Stats
    n_a = (socle["vac_scol_A"] != PLACEHOLDER).sum()
    n_b = (socle["vac_scol_B"] != PLACEHOLDER).sum()
    n_c = (socle["vac_scol_C"] != PLACEHOLDER).sum()
    couv_min = socle["Date"].min()
    couv_max = socle["Date"].max()

    log.info("[enrich] Jours vacances : Zone A=%d  Zone B=%d  Zone C=%d",
             n_a, n_b, n_c)

    return {
        "status":         "ok",
        "lignes":         len(socle),
        "jours_vac_A":    int(n_a),
        "jours_vac_B":    int(n_b),
        "jours_vac_C":    int(n_c),
        "couverture_min": couv_min,
        "couverture_max": couv_max,
        "output":         str(output_path),
    }


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s : %(message)s",
        datefmt="%H:%M:%S",
    )

    p = argparse.ArgumentParser(
        description="Enrichit le socle calendaire avec les vacances scolaires"
    )
    p.add_argument("--socle",    type=Path, default=DEFAULT_SOCLE)
    p.add_argument("--vacances", type=Path, default=DEFAULT_VACANCES)
    p.add_argument("--output",   type=Path, default=DEFAULT_OUTPUT)
    args = p.parse_args()

    try:
        result = enrichir(args.socle, args.vacances, args.output)
    except Exception as e:
        log.error("[enrich] ÉCHEC : %s", e)
        return 1

    print("=" * 70)
    print("Enrichissement vacances scolaires")
    print(f"  Socle       : {args.socle}")
    print(f"  Vacances    : {args.vacances}")
    print(f"  Lignes out  : {result['lignes']}")
    print(f"  Couverture  : {result['couverture_min']} → {result['couverture_max']}")
    print(f"  Jours Zone A: {result['jours_vac_A']}")
    print(f"  Jours Zone B: {result['jours_vac_B']}")
    print(f"  Jours Zone C: {result['jours_vac_C']}")
    print(f"  Sortie      : {result['output']}")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
