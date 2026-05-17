# -*- coding: utf-8 -*-
"""
socle_calendrier.py
===================
Génère le socle calendaire de référence (2010-01-01 -> 2035-12-31).

Colonnes produites (alignées sur l'exemple fourni par le métier) :
    Date                YYYY-MM-DD
    Jour de la semaine  Monday / Tuesday / ...
    jour Sem            (idem -- conservé pour compat ascendante)
    N° semaine ISO      Numéro de semaine ISO 8601 (1-53)
    Sem. Impaire        1 si numéro ISO impair, 0 sinon
    UTC                 "UTC +01:00" en heure d'hiver, "UTC +02:00" en heure d'été
    nom_jour_ferie      "--"   (rempli plus tard par enrichissement)
    vac_scol_A          "--"   (idem)
    vac_scol_B          "--"   (idem)
    vac_scol_C          "--"   (idem)

Le décalage UTC est calculé programmatiquement à partir de la zone IANA
'Europe/Paris' (zoneinfo) -- pas de dépendance à un fichier statique pour
les changements d'heure ; ainsi le socle est valide pour toute année future
sans maintenance.

Sortie :
    data/curated/calendaire/socle_calendrier.csv
    sep=';' encoding=utf-8

Usage CLI :
    python socle_calendrier.py
    python socle_calendrier.py --start 2010-01-01 --end 2035-12-31 \
                               --output /chemin/socle_calendrier.csv
"""
from __future__ import annotations

import argparse
import logging
from datetime import date, datetime, timedelta
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
except ImportError:                              # Python < 3.9
    from backports.zoneinfo import ZoneInfo      # type: ignore

try:
    import pandas as pd
except ImportError:
    raise ImportError("pandas est requis : pip install pandas --break-system-packages")


log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Constantes
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_START = date(2010, 1, 1)
DEFAULT_END   = date(2035, 12, 31)

PARIS_TZ = ZoneInfo("Europe/Paris")

# Chemin par défaut (container Docker / Airflow). En local hors container,
# passer --output explicitement.
DEFAULT_OUTPUT = Path(
    "/opt/airflow/data/curated/calendaire/socle_calendrier.csv"
)

# Valeurs sentinelles pour les colonnes enrichies plus tard
PLACEHOLDER = "--"

COLUMNS = [
    "Date",
    "Jour de la semaine",
    "jour Sem",
    "N° semaine ISO",
    "Sem. Impaire",
    "UTC",
    "nom_jour_ferie",
    "vac_scol_A",
    "vac_scol_B",
    "vac_scol_C",
]


# ─────────────────────────────────────────────────────────────────────────────
# Calcul UTC offset (Europe/Paris)
# ─────────────────────────────────────────────────────────────────────────────

def _utc_offset_label(d: date) -> str:
    """
    Retourne 'UTC +01:00' (CET, hiver) ou 'UTC +02:00' (CEST, été)
    pour la date donnée, à 12:00 locale (évite l'ambiguïté minuit/DST).

    Format strict aligné sur l'exemple métier : un espace entre 'UTC' et le
    signe, deux digits heures + ':00'.
    """
    dt    = datetime(d.year, d.month, d.day, 12, 0, tzinfo=PARIS_TZ)
    delta = dt.utcoffset()
    if delta is None:                            # jamais le cas pour Europe/Paris
        return "UTC +00:00"
    total_minutes = int(delta.total_seconds() // 60)
    sign  = "+" if total_minutes >= 0 else "-"
    hours = abs(total_minutes) // 60
    mins  = abs(total_minutes) % 60
    return f"UTC {sign}{hours:02d}:{mins:02d}"


# ─────────────────────────────────────────────────────────────────────────────
# Génération du socle
# ─────────────────────────────────────────────────────────────────────────────

def build_socle(start: date = DEFAULT_START,
                end:   date = DEFAULT_END) -> pd.DataFrame:
    """
    Construit le DataFrame socle entre `start` (inclus) et `end` (inclus).

    Le tri est ANTI-CHRONOLOGIQUE (date la plus récente en tête) pour
    rester aligné sur l'exemple métier fourni.
    """
    if end < start:
        raise ValueError(f"end ({end}) est antérieur à start ({start})")

    n_days = (end - start).days + 1
    log.info("[socle] Génération %s -> %s  (%d jours)", start, end, n_days)

    # date_range pandas, fréquence quotidienne
    dates = pd.date_range(start=start, end=end, freq="D")

    # Numéro de semaine ISO + parité
    iso_weeks   = dates.isocalendar().week.astype(int)
    sem_impaire = (iso_weeks % 2 == 1).astype(int)

    # Nom français du jour (Lundi, Mardi, ...)
    _FR_DAYS = {
        "Monday": "Lundi", "Tuesday": "Mardi", "Wednesday": "Mercredi",
        "Thursday": "Jeudi", "Friday": "Vendredi", "Saturday": "Samedi", "Sunday": "Dimanche"
    }
    jour_semaine = dates.day_name(locale="C").map(_FR_DAYS)

    # Numéro ISO du jour (1=Lundi … 7=Dimanche)
    jour_sem_num = dates.isocalendar().day.astype(int)

    # UTC offset par date
    utc_labels = [_utc_offset_label(d.date()) for d in dates]

    df = pd.DataFrame({
        "Date":               dates.strftime("%Y-%m-%d"),
        "Jour de la semaine": jour_semaine,
        "jour Sem":           jour_sem_num,
        "N° semaine ISO":     iso_weeks,
        "Sem. Impaire":       sem_impaire,
        "UTC":                utc_labels,
        "nom_jour_ferie":     PLACEHOLDER,
        "vac_scol_A":         PLACEHOLDER,
        "vac_scol_B":         PLACEHOLDER,
        "vac_scol_C":         PLACEHOLDER,
    })

    # Tri antichronologique
    df = df.sort_values("Date", ascending=False).reset_index(drop=True)

    # Garantit l'ordre canonique des colonnes
    df = df[COLUMNS]

    return df


def export_socle(df: pd.DataFrame, output: Path) -> Path:
    """Exporte le DataFrame en CSV (sep=';', utf-8) et retourne le chemin."""
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output, sep=";", index=False, encoding="utf-8")
    log.info("[socle] Export -> %s (%d lignes)", output, len(df))
    return output


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s : %(message)s",
        datefmt="%H:%M:%S",
    )

    p = argparse.ArgumentParser(
        description="Génère le socle calendaire (2010 -> 2035 par défaut)"
    )
    p.add_argument("--start",  type=_parse_date, default=DEFAULT_START,
                   help="Date de début incluse (YYYY-MM-DD)")
    p.add_argument("--end",    type=_parse_date, default=DEFAULT_END,
                   help="Date de fin incluse (YYYY-MM-DD)")
    p.add_argument("--output", type=Path,        default=DEFAULT_OUTPUT,
                   help="Chemin CSV de sortie")
    args = p.parse_args()

    df = build_socle(args.start, args.end)
    export_socle(df, args.output)

    # Résumé console
    print("=" * 70)
    print(f"Socle calendaire généré : {args.start} -> {args.end}")
    print(f"Lignes               : {len(df)}")
    print(f"Colonnes             : {list(df.columns)}")
    print(f"UTC unique           : {sorted(df['UTC'].unique())}")
    print(f"Premières lignes (tête) :")
    print(df.head(3).to_string(index=False))
    print(f"Dernières lignes (queue) :")
    print(df.tail(3).to_string(index=False))
    print(f"Sortie               : {args.output}")
    print("=" * 70)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
