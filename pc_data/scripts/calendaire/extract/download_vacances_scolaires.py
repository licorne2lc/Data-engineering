# -*- coding: utf-8 -*-
"""
download_vacances_scolaires.py
==============================
Télécharge le dataset des vacances scolaires françaises depuis
data.education.gouv.fr (open data, accès libre).

Source officielle :
    https://data.education.gouv.fr/explore/dataset/fr-en-calendrier-scolaire/

Endpoint export CSV (utilisé dans le script historique v3.4 lignes 906-908) :
    https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets/
        fr-en-calendrier-scolaire/exports/csv

Sortie :
    data/raw/calendrier/vacances/vacances_scolaires_YYYYMMDD.csv  (versionné)
    data/raw/calendrier/vacances/vacances_scolaires.csv           (latest)

Le script :
  1. crée le répertoire de destination si nécessaire
  2. télécharge le CSV (timeout 60s, retry 2x)
  3. valide en-tête minimal (description / start_date / end_date / zones)
     -- tolère aussi la variante FR (Description / Date de début / ...)
  4. écrit fichier daté + alias 'latest' (overwrite)
  5. retourne dict de stats pour usage Airflow / orchestration

Usage CLI :
    python download_vacances_scolaires.py
    python download_vacances_scolaires.py --output-dir /chemin/vacances

Inspiré de la fonction `telecharger_fichier(url, chemin_cible)` du
script historique v3.4.py (lignes 391-405).
"""
from __future__ import annotations

import argparse
import csv
import io
import logging
import shutil
import time
from datetime import date
from pathlib import Path

try:
    import requests
except ImportError:
    raise ImportError("requests est requis : pip install requests --break-system-packages")


log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Constantes
# ─────────────────────────────────────────────────────────────────────────────

URL_VACANCES = (
    "https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets/"
    "fr-en-calendrier-scolaire/exports/csv"
)

# Chemin par défaut (container Docker / Airflow)
DEFAULT_OUTPUT_DIR = Path("/opt/airflow/data/raw/calendrier/vacances")

# Nom canonique 'latest' (alias toujours à jour)
LATEST_NAME = "vacances_scolaires.csv"

# En-têtes acceptés (validation minimale du CSV téléchargé).
# data.education.gouv.fr expose deux variantes selon l'export :
#   - export API   : description / start_date / end_date / zones / ...
#   - export "vue" : Description / Date de début / Date de fin / Zones / ...
# La fonction _validate_header tolère les deux formats.
EXPECTED_COLUMNS_API = {"description", "start_date", "end_date", "zones"}
EXPECTED_COLUMNS_FR  = {"description", "date de début", "date de fin", "zones"}

REQUEST_TIMEOUT_S = 60
REQUEST_RETRIES   = 2
REQUEST_BACKOFF_S = 5

# Caractère BOM UTF-8 (présent en début de fichier dans certains exports CSV)
_BOM = "﻿"


# ─────────────────────────────────────────────────────────────────────────────
# Téléchargement
# ─────────────────────────────────────────────────────────────────────────────

def _download_bytes(url: str,
                    timeout: int = REQUEST_TIMEOUT_S,
                    retries: int = REQUEST_RETRIES) -> bytes:
    """
    Télécharge l'URL en mémoire avec retry exponentiel léger.
    Lève RuntimeError en cas d'échec définitif.
    """
    last_err = None
    for attempt in range(retries + 1):
        try:
            log.info("[download] GET %s  (essai %d/%d)",
                     url, attempt + 1, retries + 1)
            t0 = time.time()
            r  = requests.get(url, timeout=timeout)
            r.raise_for_status()
            dur = int((time.time() - t0) * 1000)
            log.info("[download] HTTP %d  %d bytes  %d ms",
                     r.status_code, len(r.content), dur)
            return r.content
        except requests.RequestException as e:
            last_err = e
            log.warning("[download] Échec essai %d : %s", attempt + 1, e)
            if attempt < retries:
                time.sleep(REQUEST_BACKOFF_S * (attempt + 1))

    raise RuntimeError(
        f"Téléchargement impossible après {retries + 1} essais : {last_err}"
    )


def _validate_header(content: bytes) -> tuple[str, list[str]]:
    """
    Détecte le séparateur (',' ou ';') et vérifie la présence des colonnes
    minimales attendues (variante API en anglais ou variante FR).
    Retourne (sep_détecté, colonnes_normalisées).
    """
    # Sniff sur les premiers Ko (header + 1 ligne)
    head_text = content[:8192].decode("utf-8-sig", errors="replace")
    try:
        dialect = csv.Sniffer().sniff(head_text, delimiters=",;\t|")
        sep     = dialect.delimiter
    except csv.Error:
        sep = ";"      # fallback au sep historique du dataset
        log.warning("[download] Sniffer csv : séparateur indéterminé, "
                    "fallback ';'")

    reader  = csv.reader(io.StringIO(head_text), delimiter=sep)
    columns = next(reader, [])
    # Normalisation : strip + retrait BOM éventuel + lower
    columns = [c.strip().lstrip(_BOM).lower() for c in columns]
    cols_set = set(columns)

    if EXPECTED_COLUMNS_API.issubset(cols_set):
        variant = "api"
    elif EXPECTED_COLUMNS_FR.issubset(cols_set):
        variant = "fr"
    else:
        raise ValueError(
            "En-tête CSV invalide -- aucune des variantes attendues détectée."
            f"\n  reçu        : {columns}"
            f"\n  attendu API : {sorted(EXPECTED_COLUMNS_API)}"
            f"\n  attendu FR  : {sorted(EXPECTED_COLUMNS_FR)}"
        )
    log.info("[download] Header OK  sep=%r  colonnes=%d  variant=%s",
             sep, len(columns), variant)
    return sep, columns


def download_vacances(output_dir: Path = DEFAULT_OUTPUT_DIR,
                      url:        str  = URL_VACANCES) -> dict:
    """
    Télécharge le CSV des vacances scolaires.

    Écrit deux fichiers :
        vacances_scolaires_YYYYMMDD.csv  (snapshot daté)
        vacances_scolaires.csv           (alias 'latest', overwrite)

    Retourne un dict :
        status         : 'ok' | 'error'
        url            : URL source
        bytes          : taille du payload
        sep            : séparateur détecté (',' ou ';')
        columns        : nb de colonnes du header
        snapshot_path  : chemin du fichier daté
        latest_path    : chemin de l'alias latest
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. Téléchargement en mémoire
    content = _download_bytes(url)

    # 2. Validation de l'en-tête (lève ValueError si format inattendu)
    sep, columns = _validate_header(content)

    # 3. Écriture snapshot daté
    stamp         = date.today().strftime("%Y%m%d")
    snapshot_path = output_dir / f"vacances_scolaires_{stamp}.csv"
    snapshot_path.write_bytes(content)
    log.info("[download] Snapshot -> %s", snapshot_path.name)

    # 4. Alias 'latest' (copie pour traçabilité, pas symlink)
    latest_path = output_dir / LATEST_NAME
    shutil.copy2(snapshot_path, latest_path)
    log.info("[download] Latest   -> %s", latest_path.name)

    return {
        "status":        "ok",
        "url":           url,
        "bytes":         len(content),
        "sep":           sep,
        "columns":       len(columns),
        "snapshot_path": str(snapshot_path),
        "latest_path":   str(latest_path),
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
        description="Télécharge le calendrier des vacances scolaires "
                    "(data.education.gouv.fr)"
    )
    p.add_argument("--url",        default=URL_VACANCES,
                   help="URL source (par défaut : endpoint export CSV)")
    p.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR,
                   help="Répertoire de destination des CSV")
    args = p.parse_args()

    try:
        result = download_vacances(args.output_dir, args.url)
    except Exception as e:
        log.error("[download] ÉCHEC : %s", e)
        return 1

    print("=" * 70)
    print("Vacances scolaires -- téléchargement")
    print(f"  URL       : {result['url']}")
    print(f"  Taille    : {result['bytes']:,} bytes")
    print(f"  Séparateur: {result['sep']!r}")
    print(f"  Colonnes  : {result['columns']}")
    print(f"  Snapshot  : {result['snapshot_path']}")
    print(f"  Latest    : {result['latest_path']}")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
