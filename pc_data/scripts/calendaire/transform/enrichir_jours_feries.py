# -*- coding: utf-8 -*-
"""
enrichir_jours_feries.py
========================
Enrichit le calendrier (déjà enrichi avec les vacances scolaires) en remplissant
la colonne nom_jour_ferie à partir du dataset jours fériés métropole
(téléchargé par download_jours_feries.py).

Entrées :
    calendrier.csv               (sortie de enrichir_vacances_scolaires.py)
    jours_feries_metropole.csv   (sortie de download_jours_feries.py)

Sortie :
    calendrier.csv               (écrasement en place)

Convention de remplissage (alignée sur les fichiers métier existants) :
    nom_jour_ferie = "1er janvier" / "Lundi de Pâques" / ...   si jour férié
    nom_jour_ferie = "--"                                       sinon

Logique métier :
    Le dataset Etalab jours_feries_metropole.csv liste un jour férié par ligne :
        date,annee,zone,nom_jour_ferie
        2025-01-01,2025,Métropole,1er janvier
    Pour chaque date du calendrier, on cherche une correspondance et on remplit
    la colonne nom_jour_ferie (jointure simple par la colonne 'date').

Le calendrier en sortie conserve toutes ses autres colonnes intactes (Date,
Jour de la semaine, jour Sem, N° semaine ISO, Sem. Impaire, UTC,
vac_scol_A/B/C). Seule la colonne nom_jour_ferie est mise à jour.
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    raise ImportError("pandas est requis : pip install pandas --break-system-packages")


log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Constantes
# ─────────────────────────────────────────────────────────────────────────────

PLACEHOLDER = "--"

# Colonnes attendues dans le calendrier (alignées sur enrichir_vacances)
CAL_COLUMNS = [
    "Date", "Jour de la semaine", "jour Sem", "N° semaine ISO",
    "Sem. Impaire", "UTC",
    "nom_jour_ferie", "vac_scol_A", "vac_scol_B", "vac_scol_C",
]

# Colonnes attendues dans le CSV jours fériés (Etalab)
EXPECTED_FERIES_COLS = {"date", "annee", "zone", "nom_jour_ferie"}

# Chemins par défaut (container Docker / Airflow).
# RAW utilise 'calendrier' / CURATED utilise 'calendaire' (typo historique).
DEFAULT_CALENDRIER = Path("/opt/airflow/data/curated/calendaire/calendrier.csv")
DEFAULT_FERIES     = Path("/opt/airflow/data/raw/calendrier/jours_feries/"
                          "jours_feries_metropole.csv")
DEFAULT_OUTPUT     = Path("/opt/airflow/data/curated/calendaire/calendrier.csv")


# ─────────────────────────────────────────────────────────────────────────────
# Lecture / nettoyage du dataset jours fériés
# ─────────────────────────────────────────────────────────────────────────────

def _load_feries(path: Path) -> dict[str, str]:
    """
    Lit le CSV des jours fériés Etalab et retourne un dict
    { date_iso (str YYYY-MM-DD) : nom_jour_ferie (str) }.

    Filtre :
      - on garde uniquement zone == 'Métropole' (par sécurité, même si
        le fichier downloadé ne contient déjà que ça)
    """
    if not path.exists():
        raise FileNotFoundError(f"Fichier jours fériés introuvable : {path}")

    # encoding='utf-8-sig' pour avaler le BOM éventuel
    df = pd.read_csv(path, dtype=str, encoding="utf-8-sig")
    df.columns = [c.strip().lower() for c in df.columns]

    missing = EXPECTED_FERIES_COLS - set(df.columns)
    if missing:
        raise ValueError(
            f"CSV jours fériés -- colonnes manquantes : {sorted(missing)}"
            f"  reçu : {list(df.columns)}"
        )

    # Filtre zone Métropole (par sécurité)
    df = df[df["zone"].str.strip().str.lower().str.startswith("métropole")
            | df["zone"].str.strip().str.lower().str.startswith("metropole")]

    # Normalisation : strip date + nom
    df["date"]           = df["date"].str.strip()
    df["nom_jour_ferie"] = df["nom_jour_ferie"].str.strip()

    # Validation format de date (YYYY-MM-DD)
    df = df.dropna(subset=["date", "nom_jour_ferie"])

    log.info("[enrich_feries] %d jours fériés chargés (zone Métropole)",
             len(df))

    # Construction du dict (en cas de doublon improbable, on garde le dernier)
    feries_idx = dict(zip(df["date"], df["nom_jour_ferie"]))
    return feries_idx


# ─────────────────────────────────────────────────────────────────────────────
# Enrichissement
# ─────────────────────────────────────────────────────────────────────────────

def enrichir(calendrier_path: Path = DEFAULT_CALENDRIER,
             feries_path:     Path = DEFAULT_FERIES,
             output_path:     Path = DEFAULT_OUTPUT) -> dict:
    """
    Met à jour la colonne nom_jour_ferie du calendrier à partir du CSV
    des jours fériés métropole.

    Retourne un dict de stats :
        lignes              : nb de lignes du calendrier
        feries_indexes      : nb de jours fériés dans le source
        feries_appliques    : nb de jours du calendrier marqués fériés
        feries_hors_periode : nb de fériés du source absents de la couverture
        couverture          : Date min / max
        output              : chemin du CSV produit
    """
    if not calendrier_path.exists():
        raise FileNotFoundError(f"Calendrier introuvable : {calendrier_path}")

    log.info("[enrich_feries] Chargement calendrier : %s", calendrier_path)
    cal = pd.read_csv(calendrier_path, sep=";", dtype=str, encoding="utf-8")
    cal.columns = [c.strip() for c in cal.columns]

    if list(cal.columns) != CAL_COLUMNS:
        raise ValueError(
            f"Calendrier : colonnes inattendues."
            f"\n  reçu   : {list(cal.columns)}"
            f"\n  attendu: {CAL_COLUMNS}"
        )

    log.info("[enrich_feries] Chargement jours fériés : %s", feries_path)
    feries_idx = _load_feries(feries_path)

    # Application du mapping date -> nom_jour_ferie
    # (on n'écrase QUE quand le source a une valeur, sinon on remet '--')
    cal["nom_jour_ferie"] = cal["Date"].map(feries_idx).fillna(PLACEHOLDER)

    # Garantit l'ordre des colonnes
    cal = cal[CAL_COLUMNS]

    # Export
    output_path.parent.mkdir(parents=True, exist_ok=True)
    cal.to_csv(output_path, sep=";", index=False, encoding="utf-8")
    log.info("[enrich_feries] Export -> %s (%d lignes)", output_path, len(cal))

    # Stats
    n_appliques = (cal["nom_jour_ferie"] != PLACEHOLDER).sum()
    couv_min    = cal["Date"].min()
    couv_max    = cal["Date"].max()
    n_hors_per  = sum(
        1 for d in feries_idx
        if not (couv_min <= d <= couv_max)
    )

    log.info("[enrich_feries] %d jours fériés appliqués sur %d disponibles "
             "(hors période couverte : %d)",
             n_appliques, len(feries_idx), n_hors_per)

    return {
        "status":              "ok",
        "lignes":              len(cal),
        "feries_indexes":      len(feries_idx),
        "feries_appliques":    int(n_appliques),
        "feries_hors_periode": n_hors_per,
        "couverture_min":      couv_min,
        "couverture_max":      couv_max,
        "output":              str(output_path),
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
        description="Enrichit le calendrier avec les jours fériés métropole"
    )
    p.add_argument("--calendrier", type=Path, default=DEFAULT_CALENDRIER)
    p.add_argument("--feries",     type=Path, default=DEFAULT_FERIES)
    p.add_argument("--output",     type=Path, default=DEFAULT_OUTPUT)
    args = p.parse_args()

    try:
        result = enrichir(args.calendrier, args.feries, args.output)
    except Exception as e:
        log.error("[enrich_feries] ÉCHEC : %s", e)
        return 1

    print("=" * 70)
    print("Enrichissement jours fériés")
    print(f"  Calendrier  : {args.calendrier}")
    print(f"  Jours fériés: {args.feries}")
    print(f"  Lignes out  : {result['lignes']}")
    print(f"  Couverture  : {result['couverture_min']} → "
          f"{result['couverture_max']}")
    print(f"  Source      : {result['feries_indexes']} jours fériés")
    print(f"  Appliqués   : {result['feries_appliques']}")
    print(f"  Hors période: {result['feries_hors_periode']}")
    print(f"  Sortie      : {result['output']}")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
