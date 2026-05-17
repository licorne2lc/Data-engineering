# -*- coding: utf-8 -*-
"""
update_valeurs.py  (version Docker/Airflow)
============================================
But :
  - Lire tous les CSV de référence présents dans les sous-dossiers de valeurs/ :
        valeurs/ETF/**           → composition_*.csv   (ETFs)
        valeurs/premieres/**     → *.csv               (premières matières premières)
        valeurs/premiere/**      → *.csv               (variante orthographique)
        valeurs/specifique/**    → *.csv               (valeurs spécifiques ajoutées manuellement)
  - Rechercher chaque nouvelle valeur sur Boursorama à partir de son ISIN
  - Enrichir avec les données marché (url, symbol, last, OHLC, volume, secteur, PEA…)
  - Fusionner dans boursorama_cotations_enriched.csv (source de vérité pour dag_boursorama_cotation)
  - Éviter les doublons (dédup par ISIN / mnémonique / label normalisé)
  - Mémoriser la structure des dossiers via manifest_imports.json
  - Arrêt immédiat si aucun changement détecté (structure + contenu identiques)

Format CSV attendu dans les dossiers source :
    label;ISIN;Pays;Poids

Chemins configurables via variables d'environnement Docker :
    DATAOZ_VALEURS_BASE  → dossier racine des valeurs (contient ETF/, premieres/, …)
    DATAOZ_FINANCE_ROOT  → dossier racine finance (contient valeurs/ et cotations/)
"""

import glob
import hashlib
import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import quote, urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup


# ============================================================
# CONFIG — env vars Docker ou fallback Windows
# ============================================================

SCRIPT_DIR = Path(__file__).resolve().parent

# Racine finance : curated/finance/
FINANCE_ROOT = Path(
    os.environ.get(
        "DATAOZ_FINANCE_ROOT",
        r"D:\projet_dataoz\pc_data\data\curated\finance",
    )
)

# Racine valeurs : curated/finance/valeurs/
# Contient les sous-dossiers ETF/, premieres/, premiere/, specifique/
# et le fichier boursorama_cotations_enriched.csv
BASE_VALUES_DIR = Path(
    os.environ.get(
        "DATAOZ_VALEURS_BASE",
        str(FINANCE_ROOT / "valeurs"),
    )
)

ETF_DIR       = BASE_VALUES_DIR / "ETF"
PREMIERES_DIR = BASE_VALUES_DIR / "premieres"
PREMIERE_DIR  = BASE_VALUES_DIR / "premiere"
SPECIFIQUE_DIR = BASE_VALUES_DIR / "specifique"

# Fichier source de vérité : alimenté par ce script, lu par dag_boursorama_cotation
ENRICHED_CSV   = BASE_VALUES_DIR / "boursorama_cotations_enriched.csv"
# Fichier intermédiaire (export des nouvelles valeurs avant merge)
OUTPUT_FILE    = BASE_VALUES_DIR / "valeurs_boursorama_importees.csv"
# Manifeste : hash de structure/contenu pour détecter les changements
MANIFEST_FILE  = BASE_VALUES_DIR / "manifest_imports.json"

ONLY_COMPOSITION_FILES = True   # Pour ETF : ne lire que les fichiers composition_*.csv
OVERWRITE_GENERAL_FILE = True   # Écraser enriched.csv (sinon crée un fichier _merged)

REQUEST_TIMEOUT         = 20
SLEEP_BETWEEN_REQUESTS  = 0.8

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}

session = requests.Session()
session.headers.update(HEADERS)


# ============================================================
# HELPERS — I/O & normalisation
# ============================================================

def log(message: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {message}", flush=True)


def safe_read_csv(path) -> pd.DataFrame:
    path = str(path)
    try:
        return pd.read_csv(path, sep=";", encoding="utf-8")
    except UnicodeDecodeError:
        return pd.read_csv(path, sep=";", encoding="latin-1")


def normalize_text(x) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip()
    s = re.sub(r"\s+", " ", s)
    return s


def normalize_label(x) -> str:
    s = normalize_text(x).upper()
    s = s.replace("\u2019", "'")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalize_label_visible_by_type(label: str, source_type: str) -> str:
    s = normalize_label(label)
    if source_type == "ETF":
        s = re.sub(r"^COURS\s+", "ETF ", s)
        if not s.startswith("ETF "):
            s = f"ETF {s}"
    return s


def normalize_label_for_match(x) -> str:
    s = normalize_label(x)
    s = re.sub(r"^(COURS|ETF)\s+", "", s)
    return s


def normalize_isin(x) -> str:
    s = normalize_text(x).upper()
    s = re.sub(r"\s+", "", s)
    return s


def normalize_mnemonic(x) -> str:
    s = normalize_text(x).upper()
    s = re.sub(r"\s+", "", s)
    return s


def normalize_weight(x):
    if pd.isna(x):
        return pd.NA
    s = str(x).strip().replace(",", ".")
    if not s:
        return pd.NA
    if s.endswith("%"):
        s = s[:-1].strip()
    try:
        return float(s)
    except ValueError:
        return pd.NA


def normalize_number(x):
    if x is None:
        return pd.NA
    if isinstance(x, (int, float)):
        return x
    s = str(x).strip()
    if s == "":
        return pd.NA
    s = s.replace("\xa0", " ").replace(" ", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return pd.NA


def is_valid_isin(isin: str) -> bool:
    return bool(re.fullmatch(r"[A-Z]{2}[A-Z0-9]{10}", isin))


def build_dedup_key(row) -> str:
    isin        = normalize_isin(row.get("isin", "") or row.get("ISIN", ""))
    label       = normalize_label(row.get("label", ""))
    mnemonic    = normalize_mnemonic(row.get("mnemonic", ""))
    source_type = normalize_text(row.get("source_type", ""))
    if isin:
        return f"{source_type}::ISIN::{isin}"
    if mnemonic:
        return f"{source_type}::MNEMO::{mnemonic}"
    return f"{source_type}::LABEL::{label}"


def extract_symbol_from_url(url: str) -> str:
    if not url:
        return ""
    m = re.search(r"/cours/([^/]+)/?", url)
    return m.group(1).strip() if m else ""


def fetch_html(url: str) -> str:
    resp = session.get(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.text


def align_columns(df_source: pd.DataFrame, target_columns: list) -> pd.DataFrame:
    df = df_source.copy()
    for col in target_columns:
        if col not in df.columns:
            df[col] = pd.NA
    return df[target_columns].copy()


# ============================================================
# HELPERS — Manifeste (hash de structure et contenu)
# ============================================================

def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def compute_structure_signature(structure_items: list) -> str:
    payload = json.dumps(structure_items, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def compute_content_signature(structure_items: list) -> str:
    minimal = [
        {
            "rel_path": item["rel_path"],
            "sha256":   item["sha256"],
            "size":     item["size"],
            "mtime_ns": item["mtime_ns"],
        }
        for item in structure_items
    ]
    payload = json.dumps(minimal, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def load_manifest(path: Path) -> dict:
    if not path.is_file():
        return {
            "created_at": "", "updated_at": "",
            "source_files": [], "value_keys": [], "value_count": 0,
            "structure_items": [],
            "structure_signature": "", "content_signature": "",
        }
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_manifest(
    path: Path,
    source_files: list,
    value_keys: list,
    structure_items: list,
    structure_signature: str,
    content_signature: str,
) -> None:
    old = load_manifest(path)
    payload = {
        "created_at":           old.get("created_at") or datetime.now().isoformat(timespec="seconds"),
        "updated_at":           datetime.now().isoformat(timespec="seconds"),
        "source_files":         sorted(source_files),
        "value_keys":           sorted(value_keys),
        "value_count":          len(value_keys),
        "structure_items":      structure_items,
        "structure_signature":  structure_signature,
        "content_signature":    content_signature,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def backup_file(path: Path) -> Path:
    stem   = path.stem
    ext    = path.suffix
    stamp  = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = path.parent / f"{stem}_backup_{stamp}{ext}"
    df = safe_read_csv(path)
    df.to_csv(backup, sep=";", index=False, encoding="utf-8")
    return backup


# ============================================================
# SOURCES — listage et chargement des CSV de référence
# ============================================================

def get_source_roots() -> list:
    """
    Retourne les 4 sources de valeurs.
    Chaque source pointe vers un sous-dossier de BASE_VALUES_DIR.
    """
    return [
        {"source_type": "ETF",       "base_dir": ETF_DIR,        "label": "ETF"},
        {"source_type": "premieres", "base_dir": PREMIERES_DIR,  "label": "premieres"},
        {"source_type": "premieres", "base_dir": PREMIERE_DIR,   "label": "premiere"},
        {"source_type": "specifique","base_dir": SPECIFIQUE_DIR, "label": "specifique"},
    ]


def list_source_files() -> tuple:
    """
    Liste tous les fichiers CSV source, calcule structure_signature et content_signature.
    Retourne (all_files, structure_items, structure_signature, content_signature).
    """
    all_files       = []
    structure_items = []

    for item in get_source_roots():
        source_type = item["source_type"]
        base_dir    = item["base_dir"]
        label       = item["label"]

        if not base_dir.is_dir():
            log(f"[SOURCE] Dossier absent ignoré : {base_dir}")
            continue

        if source_type == "ETF" and ONLY_COMPOSITION_FILES:
            pattern = str(base_dir / "**" / "composition_*.csv")
        else:
            pattern = str(base_dir / "**" / "*.csv")

        files = sorted(Path(x) for x in glob.glob(pattern, recursive=True))

        for file_path in files:
            # Exclure les fichiers générés par ce script lui-même
            if file_path.name in {OUTPUT_FILE.name, MANIFEST_FILE.name, ENRICHED_CSV.name}:
                continue

            rel_path = str(file_path.relative_to(BASE_VALUES_DIR)).replace("\\", "/")

            all_files.append({
                "path":        file_path,
                "source_type": source_type,
                "root_label":  label,
                "rel_path":    rel_path,
            })

            stat = file_path.stat()
            structure_items.append({
                "source_type": source_type,
                "root_label":  label,
                "rel_path":    rel_path,
                "filename":    file_path.name,
                "size":        stat.st_size,
                "mtime_ns":    stat.st_mtime_ns,
                "sha256":      file_sha256(file_path),
            })

    structure_items      = sorted(structure_items, key=lambda x: x["rel_path"])
    structure_signature  = compute_structure_signature(structure_items)
    content_signature    = compute_content_signature(structure_items)

    return all_files, structure_items, structure_signature, content_signature


def load_source_universe(files: list) -> pd.DataFrame:
    """
    Charge et concatène tous les CSV source.
    Déduplication par ISIN au niveau de chaque source_type.
    Colonnes attendues : label ; ISIN ; Pays ; Poids
    """
    all_dfs = []

    for item in files:
        file_path   = item["path"]
        source_type = item["source_type"]
        rel_path    = item["rel_path"]

        df = safe_read_csv(file_path)

        required_cols = ["label", "ISIN", "Pays", "Poids"]
        missing_cols  = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            raise ValueError(
                f"Colonnes manquantes dans {file_path} : {', '.join(missing_cols)}"
            )

        df = df[["label", "ISIN", "Pays", "Poids"]].copy()
        df["label"]       = df["label"].apply(normalize_label)
        df["ISIN"]        = df["ISIN"].apply(normalize_isin)
        df["Pays"]        = df["Pays"].apply(normalize_text)
        df["weight"]      = df["Poids"].apply(normalize_weight)
        df["source_file"] = file_path.name
        df["source_path"] = rel_path
        df["source_type"] = source_type

        df = df[df["label"] != ""].copy()
        df = df[df["ISIN"].apply(is_valid_isin)].copy()
        all_dfs.append(df)

    if not all_dfs:
        return pd.DataFrame(columns=[
            "label", "ISIN", "Pays", "Poids", "weight",
            "source_file", "source_path", "source_type",
        ])

    df_all = pd.concat(all_dfs, ignore_index=True)
    df_all["_dedup_key"] = df_all.apply(
        lambda row: f"{row['source_type']}::ISIN::{row['ISIN']}", axis=1
    )
    df_all = df_all.drop_duplicates(subset="_dedup_key", keep="first").drop(columns="_dedup_key")
    return df_all


def build_manifest_keys_from_source(df_source: pd.DataFrame) -> list:
    keys = []
    for _, row in df_source.iterrows():
        isin = normalize_isin(row.get("ISIN", ""))
        if isin:
            keys.append(f"{normalize_text(row.get('source_type', ''))}::ISIN::{isin}")
    return sorted(set(keys))


# ============================================================
# SCRAPING BOURSORAMA — recherche par ISIN
# ============================================================

def find_boursorama_url_by_isin(isin: str) -> str:
    candidate_urls = [
        f"https://www.boursorama.com/recherche/?query={quote(isin)}",
        f"https://www.boursorama.com/recherche/{quote(isin)}/",
        f"https://www.boursorama.com/bourse/recherche/?query={quote(isin)}",
    ]
    for search_url in candidate_urls:
        try:
            html  = fetch_html(search_url)
            found = parse_first_boursorama_course_url(html, isin)
            if found:
                return found
        except Exception:
            pass
        time.sleep(0.2)
    return ""


def parse_first_boursorama_course_url(html: str, isin: str = "") -> str:
    soup       = BeautifulSoup(html, "html.parser")
    best_match = ""

    for a in soup.find_all("a", href=True):
        href     = a.get("href", "").strip()
        full_url = urljoin("https://www.boursorama.com", href)

        if "boursorama.com" not in full_url:
            continue
        if "/cours/" not in full_url or "/actualites/" in full_url:
            continue

        text = a.get_text(" ", strip=True).upper()
        clean = full_url.split("?")[0].rstrip("/") + "/"

        if isin and isin in text:
            return clean          # correspondance exacte ISIN → priorité max
        if not best_match:
            best_match = clean    # premier résultat /cours/ trouvé → fallback

    return best_match


# ============================================================
# EXTRACTION — données fiche Boursorama
# ============================================================

def extract_heading_value_from_list_info(soup: BeautifulSoup, possible_headings: list) -> str:
    headings_lower = [h.lower() for h in possible_headings]
    for item in soup.select("li.c-list-info__item"):
        heading = item.select_one(".c-list-info__heading")
        value   = item.select_one(".c-list-info__value")
        if heading and heading.get_text(" ", strip=True).lower() in headings_lower:
            return normalize_text(value.get_text(" ", strip=True) if value else "")
    return ""


def extract_mnemonic_and_isin_from_faceplate(soup: BeautifulSoup) -> tuple:
    candidates = []
    h2 = soup.select_one("h2.c-faceplate__isin")
    if h2:
        candidates.append(h2.get_text(" ", strip=True))
    for tag in soup.find_all(["h1", "h2", "div", "span", "p"]):
        txt = tag.get_text(" ", strip=True)
        if re.search(r"\b[A-Z]{2}[A-Z0-9]{10}\b", txt.upper()):
            candidates.append(txt)

    for txt in candidates:
        txt = normalize_text(txt).upper()
        m = re.search(r"\b([A-Z]{2}[A-Z0-9]{10})\s+([A-Z0-9._-]{1,20})\b", txt)
        if m:
            return m.group(2).strip(), m.group(1).strip()
        m2 = re.search(r"\b([A-Z]{2}[A-Z0-9]{10})\b", txt)
        if m2:
            return "", m2.group(1).strip()
    return "", ""


def extract_elig_pea(soup: BeautifulSoup, page_text: str) -> bool:
    text_upper = page_text.upper()
    if "PEA-PME" in text_upper or "PEA" in text_upper:
        return True
    elig = extract_heading_value_from_list_info(
        soup, ["Éligibilité", "Eligibilité", "Éligibilité PEA", "Eligibilite PEA"]
    )
    return bool(elig and "PEA" in elig.upper())


def extract_sector_from_html(soup: BeautifulSoup, page_text: str) -> str:
    sector = extract_heading_value_from_list_info(
        soup, ["Secteur", "Secteur d'activité", "Industrie", "Industry"]
    )
    if sector:
        return sector
    for pattern in [
        r"Secteur d'activité\s*[:\-]?\s*([A-Za-zÀ-ÿ0-9&/,\- ']{3,100})",
        r"Secteur\s*[:\-]?\s*([A-Za-zÀ-ÿ0-9&/,\- ']{3,100})",
        r"Industrie\s*[:\-]?\s*([A-Za-zÀ-ÿ0-9&/,\- ']{3,100})",
    ]:
        m = re.search(pattern, page_text, flags=re.IGNORECASE)
        if m:
            v = normalize_text(m.group(1)).strip(" -:;,")
            if 2 < len(v) < 100:
                return v
    return ""


def extract_risk_level(soup: BeautifulSoup, page_text: str) -> str:
    risk = extract_heading_value_from_list_info(soup, ["Risque ESG", "Risque"])
    if risk:
        return risk
    for pattern in [
        r"Risque ESG\s*[:\-]?\s*([A-Za-zÀ-ÿ0-9&/,\- ']{1,60})",
        r"Risque\s*[:\-]?\s*([A-Za-zÀ-ÿ0-9&/,\- ']{1,60})",
    ]:
        m = re.search(pattern, page_text, flags=re.IGNORECASE)
        if m:
            v = normalize_text(m.group(1)).strip(" -:;,")
            if v:
                return v
    return ""


# ============================================================
# EXTRACTION — données marché (JSON embarqué ou fallback texte)
# ============================================================

def extract_market_data_from_json(html: str, expected_symbol: str = "") -> dict:
    result = {
        "symbol": expected_symbol,
        "last": pd.NA, "previousClose": pd.NA,
        "high": pd.NA, "low": pd.NA,
        "variation": pd.NA, "volume": pd.NA,
        "exchangeCode": "", "category": "",
    }
    soup = BeautifulSoup(html, "html.parser")
    for row in soup.select("tr[data-ist-init], div[data-ist-init], span[data-ist-init]"):
        raw = row.get("data-ist-init")
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        if not isinstance(data, dict):
            continue

        values = [data.get(k) for k in ["last", "previousClose", "high", "low", "variation", "totalVolume", "volume"]]
        if any(v is not None for v in values):
            result.update({
                "symbol":        data.get("symbol") or expected_symbol,
                "last":          normalize_number(data.get("last")),
                "previousClose": normalize_number(data.get("previousClose")),
                "high":          normalize_number(data.get("high")),
                "low":           normalize_number(data.get("low")),
                "variation":     normalize_number(data.get("variation")),
                "volume":        normalize_number(data.get("totalVolume") or data.get("volume")),
                "exchangeCode":  normalize_text(data.get("exchangeCode") or ""),
                "category":      normalize_text(data.get("category") or ""),
            })
            return result
    return result


def extract_market_data_from_page_text(page_text: str) -> dict:
    result = {
        "last": pd.NA, "previousClose": pd.NA,
        "high": pd.NA, "low": pd.NA,
        "variation": pd.NA, "volume": pd.NA,
        "exchangeCode": "", "category": "",
    }
    def search_number(pattern):
        m = re.search(pattern, page_text, flags=re.IGNORECASE)
        return normalize_number(m.group(1)) if m else pd.NA

    result["last"]          = search_number(r"\bDernier\s*[:\-]?\s*([0-9\s,.\-]+)")
    result["previousClose"] = search_number(r"\bClôture veille\s*[:\-]?\s*([0-9\s,.\-]+)")
    result["high"]          = search_number(r"\bPlus haut\s*[:\-]?\s*([0-9\s,.\-]+)")
    result["low"]           = search_number(r"\bPlus bas\s*[:\-]?\s*([0-9\s,.\-]+)")
    result["variation"]     = search_number(r"\bVariation\s*[:\-]?\s*([0-9\s,.\-%+]+)")
    result["volume"]        = search_number(r"\bVolume\s*[:\-]?\s*([0-9\s,.\-]+)")

    m = re.search(r"\bPlace\s*[:\-]?\s*([A-Za-z0-9À-ÿ&/,\- ']{2,60})", page_text, re.IGNORECASE)
    if m:
        result["exchangeCode"] = normalize_text(m.group(1))
    m = re.search(r"\bCatégorie\s*[:\-]?\s*([A-Za-z0-9À-ÿ&/,\- ']{2,60})", page_text, re.IGNORECASE)
    if m:
        result["category"] = normalize_text(m.group(1))
    return result


# ============================================================
# ENRICHISSEMENT — appel Boursorama par ISIN
# ============================================================

def enrich_from_boursorama(isin: str) -> dict:
    result = {
        "label": "", "url": "", "symbol": "",
        "last": pd.NA, "previousClose": pd.NA,
        "high": pd.NA, "low": pd.NA,
        "variation": pd.NA, "volume": pd.NA,
        "exchangeCode": "", "category": "",
        "sector": "", "risk_level": "", "elig_pea": False,
        "isin": "", "mnemonic": "",
    }
    if not isin or not is_valid_isin(isin):
        return result

    url = find_boursorama_url_by_isin(isin)
    if not url:
        return result

    result["url"]    = url
    result["symbol"] = extract_symbol_from_url(url)

    try:
        html = fetch_html(url)
    except Exception:
        return result

    soup      = BeautifulSoup(html, "html.parser")
    page_text = normalize_text(soup.get_text(" ", strip=True))

    h1 = soup.select_one("h1")
    if h1:
        result["label"] = normalize_text(h1.get_text(" ", strip=True)).upper()

    market = extract_market_data_from_json(html, expected_symbol=result["symbol"])
    if pd.isna(market["last"]):
        fallback = extract_market_data_from_page_text(page_text)
        for k in ["last", "previousClose", "high", "low", "variation", "volume", "exchangeCode", "category"]:
            if market[k] == "" or (isinstance(market[k], float) and pd.isna(market[k])):
                market[k] = fallback[k]

    result.update({
        "symbol":        market["symbol"] or result["symbol"],
        "last":          market["last"],
        "previousClose": market["previousClose"],
        "high":          market["high"],
        "low":           market["low"],
        "variation":     market["variation"],
        "volume":        market["volume"],
        "exchangeCode":  market["exchangeCode"],
        "category":      market["category"],
        "sector":        extract_sector_from_html(soup, page_text),
        "risk_level":    extract_risk_level(soup, page_text),
        "elig_pea":      extract_elig_pea(soup, page_text),
    })

    mnemonic, isin_found = extract_mnemonic_and_isin_from_faceplate(soup)
    result["mnemonic"] = mnemonic
    result["isin"]     = isin_found if isin_found else isin
    return result


# ============================================================
# BUILD IMPORT — enrichit les nouvelles valeurs
# ============================================================

def build_import_output(df_source: pd.DataFrame) -> pd.DataFrame:
    df = df_source[["label", "ISIN", "Pays", "weight", "source_file", "source_path", "source_type"]].copy()

    # Initialiser les colonnes enrichies
    for col in ["url", "symbol", "exchangeCode", "category", "sector", "risk_level", "isin", "mnemonic"]:
        df[col] = ""
    for col in ["last", "previousClose", "high", "low", "variation", "volume"]:
        df[col] = pd.NA
    df["elig_pea"] = False

    total = len(df)
    for i, idx in enumerate(df.index):
        isin        = df.at[idx, "ISIN"]
        source_type = df.at[idx, "source_type"]
        log(f"[ENRICH][{i+1}/{total}] [{source_type}] ISIN={isin}")

        info = enrich_from_boursorama(isin)

        if info["label"]:
            df.at[idx, "label"] = info["label"]

        for col in ["url", "symbol", "last", "previousClose", "high", "low",
                    "variation", "volume", "exchangeCode", "category",
                    "sector", "risk_level", "mnemonic"]:
            df.at[idx, col] = info[col]

        df.at[idx, "elig_pea"] = bool(info["elig_pea"])
        df.at[idx, "isin"]     = info["isin"] if info["isin"] else isin

        time.sleep(SLEEP_BETWEEN_REQUESTS)

    # Label visible : préfixe ETF pour les ETFs
    df["label"]    = df.apply(lambda r: normalize_label_visible_by_type(r["label"], r["source_type"]), axis=1)
    df["elig_pea"] = df["elig_pea"].astype(bool)

    FINAL_COLS = [
        "label", "url", "symbol",
        "last", "previousClose", "high", "low", "variation", "volume",
        "exchangeCode", "category", "sector", "risk_level", "elig_pea",
        "isin", "mnemonic",
        "Pays", "weight", "source_type", "source_file", "source_path",
    ]
    df = df[FINAL_COLS].copy()
    df["_dedup_key"] = df.apply(build_dedup_key, axis=1)
    df = df.drop_duplicates(subset="_dedup_key", keep="first").drop(columns="_dedup_key")
    df = df.sort_values(["source_type", "label", "isin"]).reset_index(drop=True)
    return df


# ============================================================
# MERGE — fusion dans boursorama_cotations_enriched.csv
# ============================================================

def prepare_general_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ["isin", "mnemonic"]:
        if col not in df.columns:
            df[col] = ""
    return df


def merge_imports_into_general(
    df_imports: pd.DataFrame,
    general_file: Path,
) -> tuple:
    """
    Fusionne df_imports dans general_file.
    Dédup par label normalisé, ISIN normalisé et mnémonique normalisé.
    Retourne (df_final, nb_avant, nb_import_total, nb_ajouté).
    """
    if not general_file.is_file():
        raise FileNotFoundError(f"Fichier général introuvable : {general_file}")

    df_general = prepare_general_columns(safe_read_csv(general_file))

    # Clés de dédup normalisées
    for df in [df_general, df_imports]:
        df["_label_norm"]    = df["label"].apply(normalize_label_for_match)
        df["_isin_norm"]     = df["isin"].apply(normalize_isin)
        df["_mnemonic_norm"] = df["mnemonic"].apply(normalize_mnemonic)

    df_imports = df_imports.drop_duplicates(
        subset=["_label_norm", "_isin_norm", "_mnemonic_norm"], keep="first"
    ).copy()

    general_labels    = set(x for x in df_general["_label_norm"]    if x)
    general_isins     = set(x for x in df_general["_isin_norm"]     if x)
    general_mnemonics = set(x for x in df_general["_mnemonic_norm"] if x)

    df_imports["_already_exists"] = (
        df_imports["_label_norm"].isin(general_labels)
        | df_imports["_isin_norm"].isin(general_isins)
        | df_imports["_mnemonic_norm"].isin(general_mnemonics)
    )

    df_to_add = df_imports[~df_imports["_already_exists"]].copy()

    nb_before       = len(df_general)
    nb_import_total = len(df_imports)
    nb_added        = len(df_to_add)

    cleanup_cols = ["_label_norm", "_isin_norm", "_mnemonic_norm", "_already_exists"]
    df_general   = df_general.drop(columns=[c for c in cleanup_cols if c in df_general.columns])

    if nb_added == 0:
        df_general = df_general.sort_values("label").reset_index(drop=True)
        return df_general, nb_before, nb_import_total, nb_added

    target_cols = list(df_general.columns)
    df_to_add   = df_to_add.drop(columns=[c for c in cleanup_cols if c in df_to_add.columns])
    df_to_add   = align_columns(df_to_add, target_cols)

    df_final = pd.concat([df_general, df_to_add], ignore_index=True)
    df_final = df_final.sort_values("label").reset_index(drop=True)
    return df_final, nb_before, nb_import_total, nb_added


# ============================================================
# MAIN
# ============================================================

def main() -> dict:
    """
    Point d'entrée principal.
    Retourne un dict résumé pour XCom Airflow.
    """
    log(f"[VALEURS] BASE_VALUES_DIR  : {BASE_VALUES_DIR}")
    log(f"[VALEURS] ENRICHED_CSV     : {ENRICHED_CSV}")

    # ── 1. Lister et hasher les fichiers source ──────────────────────────────
    files, structure_items, structure_sig, content_sig = list_source_files()

    if not files:
        log("[VALEURS] Aucun fichier CSV source trouvé — arrêt.")
        return {"status": "no_files", "added": 0}

    log(f"[VALEURS] {len(files)} fichier(s) source trouvé(s).")

    # ── 2. Comparer au manifeste précédent ───────────────────────────────────
    manifest          = load_manifest(MANIFEST_FILE)
    prev_struct_sig   = manifest.get("structure_signature", "")
    prev_content_sig  = manifest.get("content_signature", "")

    if prev_struct_sig == structure_sig and prev_content_sig == content_sig:
        log("[VALEURS] Aucun changement détecté (structure + contenu identiques) — arrêt.")
        return {"status": "no_change", "added": 0}

    if prev_struct_sig != structure_sig:
        log("[VALEURS] Changement d'architecture détecté.")
    if prev_content_sig != content_sig:
        log("[VALEURS] Changement de contenu détecté.")

    # ── 3. Charger l'univers source ──────────────────────────────────────────
    df_source = load_source_universe(files)
    if df_source.empty:
        log("[VALEURS] Aucune donnée exploitable — arrêt.")
        save_manifest(MANIFEST_FILE, [x["rel_path"] for x in files],
                      [], structure_items, structure_sig, content_sig)
        return {"status": "empty_source", "added": 0}

    # ── 4. Détecter les nouvelles valeurs (non présentes dans le manifeste) ──
    current_keys  = build_manifest_keys_from_source(df_source)
    previous_keys = sorted(set(manifest.get("value_keys", [])))
    new_keys      = sorted(set(current_keys) - set(previous_keys))

    if not new_keys:
        log("[VALEURS] Aucune nouvelle valeur — mise à jour du manifeste puis arrêt.")
        save_manifest(MANIFEST_FILE, [x["rel_path"] for x in files],
                      current_keys, structure_items, structure_sig, content_sig)
        return {"status": "no_new_values", "added": 0}

    log(f"[VALEURS] Nouvelles valeurs détectées : {len(new_keys)}")

    # ── 5. Filtrer df_source sur les nouvelles valeurs uniquement ────────────
    new_keys_set = set(new_keys)
    df_new_only  = df_source[
        df_source.apply(
            lambda r: f"{r['source_type']}::ISIN::{normalize_isin(r['ISIN'])}" in new_keys_set,
            axis=1,
        )
    ].copy()

    # ── 6. Enrichissement Boursorama (scraping par ISIN) ─────────────────────
    df_import = build_import_output(df_new_only)

    if df_import.empty:
        log("[VALEURS] Aucune donnée enrichie exploitable — arrêt.")
        save_manifest(MANIFEST_FILE, [x["rel_path"] for x in files],
                      current_keys, structure_items, structure_sig, content_sig)
        return {"status": "enrich_empty", "added": 0}

    # ── 7. Sauvegarde du fichier intermédiaire ────────────────────────────────
    BASE_VALUES_DIR.mkdir(parents=True, exist_ok=True)
    df_import.to_csv(OUTPUT_FILE, sep=";", index=False, encoding="utf-8")
    log(f"[VALEURS] Fichier intermédiaire : {OUTPUT_FILE} ({len(df_import)} lignes)")

    # ── 8. Merge dans enriched.csv ────────────────────────────────────────────
    if not ENRICHED_CSV.is_file():
        # Premier lancement : pas encore de fichier enriched → on le crée directement
        log(f"[VALEURS] Création initiale de {ENRICHED_CSV}")
        df_import.to_csv(ENRICHED_CSV, sep=";", index=False, encoding="utf-8")
        nb_before, nb_added = 0, len(df_import)
        df_final = df_import
    else:
        backup_path = backup_file(ENRICHED_CSV)
        log(f"[VALEURS] Sauvegarde créée : {backup_path}")
        df_final, nb_before, _, nb_added = merge_imports_into_general(df_import, ENRICHED_CSV)

        output_path = ENRICHED_CSV if OVERWRITE_GENERAL_FILE \
            else ENRICHED_CSV.with_name("boursorama_cotations_enriched_merged.csv")
        df_final.to_csv(output_path, sep=";", index=False, encoding="utf-8")
        log(f"[VALEURS] Fichier enrichi écrit : {output_path}")

    # ── 9. Mise à jour du manifeste ───────────────────────────────────────────
    updated_keys = sorted(set(previous_keys).union(set(new_keys)))
    save_manifest(
        MANIFEST_FILE,
        source_files=[x["rel_path"] for x in files],
        value_keys=updated_keys,
        structure_items=structure_items,
        structure_signature=structure_sig,
        content_signature=content_sig,
    )

    result = {
        "status":          "ok",
        "new_values":      len(new_keys),
        "added":           nb_added,
        "total_enriched":  len(df_final),
        "before":          nb_before,
        "enriched_csv":    str(ENRICHED_CSV),
    }
    log("")
    log("=" * 55)
    log("✅ PIPELINE VALEURS — RÉSUMÉ")
    log(f"   Nouvelles valeurs détectées : {len(new_keys)}")
    log(f"   Lignes avant fusion         : {nb_before}")
    log(f"   Lignes ajoutées             : {nb_added}")
    log(f"   Total fichier final         : {len(df_final)}")
    log(f"   Fichier enrichi             : {ENRICHED_CSV}")
    log("=" * 55)
    return result


if __name__ == "__main__":
    main()
