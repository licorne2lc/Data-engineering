# -*- coding: utf-8 -*-
"""
download_jours_feries.py
========================
Télécharge la liste des jours fériés France métropolitaine depuis le
dataset open data maintenu par Etalab :

    https://etalab.github.io/jours-feries-france-data/csv/jours_feries_metropole.csv

Format CSV identique à celui présent dans le projet
(`data/curated/calendaire/jours_feries/jours feries metropole.csv`) :

    date,annee,zone,nom_jour_ferie

Sortie :
    data/raw/calendrier/jours_feries/jours_feries_metropole_YYYYMMDD.csv  (versionné)
    data/raw/calendrier/jours_feries/jours_feries_metropole.csv           (latest)

Le script suit le même schéma que download_vacances_scolaires.py :
    1. crée le répertoire de destination si nécessaire
    2. télécharge le CSV (timeout 60s, retry 2x avec backoff)
    3. valide en-tête minimal (date / annee / zone / nom_jour_ferie)
    4. écrit fichier daté + alias 'latest' (overwrite)
    5. retourne dict de stats pour usage Airflow / orchestration

Usage CLI :
    python download_jours_feries.py
    python download_jours_feries.py --output-dir /chemin/jours_feries
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

URL_JOURS_FERIES = (
    "https://etalab.github.io/jours-feries-france-data/csv/"
    "jours_feries_metropole.csv"
)

# Chemin par défaut (container Docker / Airflow)
DEFAULT_OUTPUT_DIR = Path("/opt/airflow/data/raw/calendrier/jours_feries")

# Nom canonique 'latest' (alias toujours à jour)
LATEST_NAME = "jours_feries_metropole.csv"

# En-tête attendu (validation minimale du CSV téléchargé).
# Le dataset Etalab utilise l'en-tête français en minuscules.
EXPECTED_COLUMNS = {"date", "annee", "zone", "nom_jour_ferie"}

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
            log.info("[download_feries] GET %s  (essai %d/%d)",
                     url, attempt + 1, retries + 1)
            t0 = time.time()
            r  = requests.get(url, timeout=timeout)
            r.raise_for_status()
            dur = int((time.time() - t0) * 1000)
            log.info("[download_feries] HTTP %d  %d bytes  %d ms",
                     r.status_code, len(r.content), dur)
            return r.content
        except requests.RequestException as e:
            last_err = e
            log.warning("[download_feries] Échec essai %d : %s",
                        attempt + 1, e)
            if attempt < retries:
                time.sleep(REQUEST_BACKOFF_S * (attempt + 1))

    raise RuntimeError(
        f"Téléchargement impossible après {retries + 1} essais : {last_err}"
    )


def _validate_header(content: bytes) -> tuple[str, list[str]]:
    """
    Détecte le séparateur (',' ou ';') et vérifie la présence des colonnes
    minimales attendues.
    Retourne (sep_détecté, colonnes_normalisées).
    """
    head_text = content[:8192].decode("utf-8-sig", errors="replace")
    try:
        dialect = csv.Sniffer().sniff(head_text, delimiters=",;\t|")
        sep     = dialect.delimiter
    except csv.Error:
        sep = ","       # fallback au sep standard du dataset Etalab
        log.warning("[download_feries] Sniffer csv : séparateur indéterminé, "
                    "fallback ','")

    reader  = csv.reader(io.StringIO(head_text), delimiter=sep)
    columns = next(reader, [])
    columns = [c.strip().lstrip(_BOM).lower() for c in columns]
    cols_set = set(columns)

    missing = EXPECTED_COLUMNS - cols_set
    if missing:
        raise ValueError(
            "En-tête CSV invalide -- colonnes manquantes : "
            f"{sorted(missing)}\n  reçu   : {columns}"
            f"\n  attendu: {sorted(EXPECTED_COLUMNS)}"
        )
    log.info("[download_feries] Header OK  sep=%r  colonnes=%d",
             sep, len(columns))
    return sep, columns


def download_jours_feries(output_dir: Path = DEFAULT_OUTPUT_DIR,
                          url:        str  = URL_JOURS_FERIES) -> dict:
    """
    Télécharge le CSV des jours fériés France métropolitaine.

    Écrit deux fichiers :
        jours_feries_metropole_YYYYMMDD.csv  (snapshot daté)
        jours_feries_metropole.csv           (alias 'latest', overwrite)

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
    snapshot_path = output_dir / f"jours_feries_metropole_{stamp}.csv"
    snapshot_path.write_bytes(content)
    log.info("[download_feries] Snapshot -> %s", snapshot_path.name)

    # 4. Alias 'latest' (copie pour traçabilité, pas symlink)
    latest_path = output_dir / LATEST_NAME
    shutil.copy2(snapshot_path, latest_path)
    log.info("[download_feries] Latest   -> %s", latest_path.name)

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
        description="Télécharge les jours fériés métropole "
                    "(etalab.github.io/jours-feries-france-data)"
    )
    p.add_argument("--url",        default=URL_JOURS_FERIES,
                   help="URL source (par défaut : Etalab GitHub Pages)")
    p.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR,
                   help="Répertoire de destination des CSV")
    args = p.parse_args()

    try:
        result = download_jours_feries(args.output_dir, args.url)
    except Exception as e:
        log.error("[download_feries] ÉCHEC : %s", e)
        return 1

    print("=" * 70)
    print("Jours fériés métropole -- téléchargement")
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
