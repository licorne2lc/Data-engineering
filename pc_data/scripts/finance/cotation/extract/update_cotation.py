# -*- coding: utf-8 -*-
"""
update_cotation.py — v2 (Docker-compatible)
============================================
Chemins configurables via variables d'environnement :
  DATAOZ_COTATION_BASE  → dossier racine scrapping cotations
  DATAOZ_ETF_PATH       → dossier racine ETF (composition.csv)

Si les variables ne sont pas définies, les chemins Windows originaux sont utilisés
(compatibilité exécution standalone hors Docker).
"""

import argparse
import csv
import html
import json
import os
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# =========================================================
# CONFIG PAR DEFAUT — env vars Docker ou fallback Windows
# =========================================================
SCRIPT_DIR = Path(__file__).resolve().parent

DEFAULT_BASE_PATH = Path(
    os.environ.get(
        "DATAOZ_COTATION_BASE",
        r"D:\projet_dataoz\pc_data\data\curated\finance\cotations",
    )
)

# Chemin RAW : données brutes téléchargées (scraping + fichiers 5J)
# Séparé du chemin curated pour respecter l'architecture raw → curated
DEFAULT_RAW_PATH = Path(
    os.environ.get(
        "DATAOZ_COTATION_RAW",
        r"D:\projet_dataoz\pc_data\data\raw\finance\cotations",
    )
)

DEFAULT_ETF_PATH = os.environ.get(
    "DATAOZ_ETF_PATH",
    r"D:\projet_dataoz\pc_data\data\curated\finance\valeurs\ETF",
)

DEFAULT_BASE_URL = "https://www.boursorama.com/bourse/actions/cotations/"
DEFAULT_TOTAL_PAGES = 8

DEFAULT_MASTER_CSV = "boursorama_cotations.csv"
DEFAULT_ENRICHED_CSV = "boursorama_cotations_enriched.csv"
DEFAULT_REPORT_CSV = "update_cotation_report.csv"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
}

ISIN_CANDIDATE_REGEX = re.compile(r"\b[A-Z]{2}[A-Z0-9]{10}\b")
EXPECTED_COLS_5D = ["date", "ouv", "haut", "bas", "clot", "vol", "devise"]

SEL_LENGTH_5J = 'div.c-quote-chart__length[data-brs-quote-chart-duration-length="5"]'
SEL_LENGTH_10A = 'div.c-quote-chart__length[data-brs-quote-chart-duration-length="3650"]'
SEL_DOWNLOAD_ICON = "span.c-icon--download"


@dataclass
class AppConfig:
    # ── Chemins curated (bases consolidées) ──────────────────────────────
    base_path: Path         # curated/finance/cotations/
    # ── Chemin raw (données brutes téléchargées) ─────────────────────────
    raw_base_path: Path     # raw/finance/cotations/

    base_url: str
    total_pages: int
    etf_path: str

    # master_csv   → RAW  : scraping brut avant enrichissement
    # enriched_csv → CURATED : master enrichi avec ISIN (source pour update_history)
    master_csv: Path        # raw_base_path / boursorama_cotations.csv
    enriched_csv: Path      # base_path     / boursorama_cotations_enriched.csv
    report_csv: Path        # raw_base_path / update_cotation_report.csv
    archive_dir: Path       # raw_base_path / archives/
    archive_old: bool

    # cotation_dir   → CURATED : répertoire parent intraday_db (base consolidée)
    cotation_dir: Path      # base_path / cotation/
    # updates_5d_dir → RAW     : téléchargements 5J bruts par Playwright
    updates_5d_dir: Path    # raw_base_path / cotation/5d_updates/
    # intraday_db    → CURATED : série intraday consolidée (agrégation des 5J)
    intraday_db_dir: Path   # base_path / cotation/intraday_db/
    # ohlc_10a       → CURATED : OHLC 10 ans consolidé (incrémental)
    ohlc_10a_dir: Path      # base_path / ohlc_10a/

    headless: bool
    sleep_between: float
    retry_per_action: int

    pause_pages_s: float
    pause_quote_s: float
    jitter_s: float

    connect_timeout_s: int
    read_timeout_s: int
    max_retries: int
    backoff_factor: float

    skip_master: bool
    skip_history: bool


# =========================================================
# LOG / UTILS
# =========================================================
def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def sanitize(name: str) -> str:
    name = (name or "").strip()
    name = re.sub(r'[\\/:*?"<>|]+', "_", name)
    return name[:80] if name else "unknown"


def _normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _decode_data_ist_init(raw: str) -> str:
    return html.unescape(raw).replace("&quot;", '"')


def safe_symbol_in_url(symbol: str) -> str:
    return quote(symbol, safe="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-$")


def snooze(cfg: AppConfig, base: float) -> None:
    time.sleep(max(0.0, base + random.uniform(0, cfg.jitter_s)))


def sniff_delimiter(path: Path) -> str:
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        sample = f.read(4096)
    semicolons = sample.count(";")
    tabs = sample.count("\t")
    commas = sample.count(",")
    if tabs >= semicolons and tabs >= commas:
        return "\t"
    if semicolons >= commas:
        return ";"
    return ","


def normalize_numeric_series(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.astype(str)
        .str.replace("\u202f", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str.replace(",", ".", regex=False),
        errors="coerce",
    )


def write_report_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "timestamp", "symbol", "url", "status",
        "five_j_status", "five_j_attempts", "five_j_path",
        "intraday_status", "intraday_added",
        "ten_a_status", "ten_a_mode", "ten_a_attempts", "ten_a_path",
        "message",
    ]
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


# =========================================================
# ISIN
# =========================================================
def _isin_to_digits(isin: str) -> str:
    out = []
    for ch in isin:
        if ch.isdigit():
            out.append(ch)
        elif "A" <= ch <= "Z":
            out.append(str(ord(ch) - ord("A") + 10))
        else:
            return ""
    return "".join(out)


def _luhn_check(num_str: str) -> bool:
    if not num_str or not num_str.isdigit():
        return False
    total = 0
    rev = list(map(int, num_str[::-1]))
    for i, d in enumerate(rev):
        if i % 2 == 1:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


def is_valid_isin(isin: str) -> bool:
    isin = (isin or "").strip().upper()
    if not re.fullmatch(r"[A-Z]{2}[A-Z0-9]{10}", isin):
        return False
    return _luhn_check(_isin_to_digits(isin))


def normalize_isin(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip().upper()
    m = ISIN_CANDIDATE_REGEX.search(s)
    if not m:
        return None
    cand = m.group(0)
    return cand if is_valid_isin(cand) else None


# =========================================================
# HTTP
# =========================================================
def build_session(cfg: AppConfig) -> requests.Session:
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    retry = Retry(
        total=cfg.max_retries,
        connect=cfg.max_retries,
        read=cfg.max_retries,
        status=cfg.max_retries,
        backoff_factor=cfg.backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_html(session: requests.Session, url: str, cfg: AppConfig) -> str:
    timeout = (cfg.connect_timeout_s, cfg.read_timeout_s)
    r = session.get(url, timeout=timeout)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code} for {url}")
    return r.text


# =========================================================
# PHASE 1 - MASTER CSV
# =========================================================
def build_page_url(base_url: str, page: int) -> str:
    if page == 1:
        return base_url
    if not base_url.endswith("/"):
        base_url += "/"
    return f"{base_url}page-{page}/"


def extract_label_and_url(tr) -> Tuple[Optional[str], Optional[str]]:
    td0 = tr.find("td")
    if not td0:
        return None, None
    a = td0.find("a", href=True)
    if not a:
        txt = td0.get_text(" ", strip=True)
        return (txt or None), None
    label = a.get_text(" ", strip=True) or None
    href = a["href"]
    if href and href.startswith("/"):
        href = urljoin("https://www.boursorama.com", href)
    return label, href


def parse_list_page(html_text: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html_text, "html.parser")
    trs = soup.select("tr.c-table__row[data-ist-init]")
    if not trs:
        raise RuntimeError("Aucune ligne trouvée (structure changée ou blocage).")
    rows: List[Dict[str, Any]] = []
    for tr in trs:
        raw = _decode_data_ist_init(tr.get("data-ist-init", ""))
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        label, url = extract_label_and_url(tr)
        rows.append({
            "label": (label or "").strip().upper() if label else None,
            "url": url,
            "symbol": (data.get("symbol") or "").strip(),
            "isin": None,
            "last": data.get("last"),
            "previousClose": data.get("previousClose"),
            "high": data.get("high"),
            "low": data.get("low"),
            "variation": data.get("variation"),
            "volume": data.get("totalVolume") or data.get("volume"),
            "exchangeCode": data.get("exchangeCode"),
            "category": data.get("category"),
        })
    return rows


def scrape_all_list_pages(cfg: AppConfig, session: requests.Session) -> List[Dict[str, Any]]:
    all_rows: List[Dict[str, Any]] = []
    for page in range(1, cfg.total_pages + 1):
        url = build_page_url(cfg.base_url, page)
        try:
            html_text = fetch_html(session, url, cfg)
            page_rows = parse_list_page(html_text)
            log(f"[MASTER] page {page}: {len(page_rows)} lignes")
            all_rows.extend(page_rows)
        except Exception as e:
            log(f"[MASTER][WARN] page {page} impossible: {e}")
        snooze(cfg, cfg.pause_pages_s)
    seen = set()
    uniq = []
    for r in all_rows:
        s = (r.get("symbol") or "").strip()
        if not s or s in seen:
            continue
        uniq.append(r)
        seen.add(s)
    return uniq


def read_etf_components(etf_root_path: str) -> Dict[str, Dict[str, Any]]:
    root = Path(etf_root_path)
    if not root.exists():
        log(f"[MASTER][WARN] Répertoire ETF introuvable : {etf_root_path}")
        return {}
    by_symbol: Dict[str, Dict[str, Any]] = {}
    log(f"[MASTER][ETF] Scan: {root}")
    for csv_file in root.glob("**/composition.csv"):
        try:
            delim = sniff_delimiter(csv_file)
            with open(csv_file, "r", encoding="utf-8", errors="replace", newline="") as f:
                reader = csv.DictReader(f, delimiter=delim)
                for row in reader:
                    label = (
                        row.get("label") or row.get("Label") or
                        row.get("Nom") or row.get("name") or ""
                    ).strip()
                    symbol = (
                        row.get("Symbol") or row.get("symbol") or
                        row.get("Ticker") or row.get("ticker") or ""
                    ).strip()
                    isin = normalize_isin(
                        row.get("ISIN") or row.get("isin") or row.get("Code ISIN") or ""
                    )
                    if not symbol:
                        continue
                    label = label.upper() if label else None
                    if symbol not in by_symbol:
                        by_symbol[symbol] = {"label": label, "isin": isin}
                    else:
                        if not by_symbol[symbol].get("label") and label:
                            by_symbol[symbol]["label"] = label
                        if not by_symbol[symbol].get("isin") and isin:
                            by_symbol[symbol]["isin"] = isin
        except Exception as e:
            log(f"[MASTER][WARN] lecture ETF impossible {csv_file}: {e}")
    return by_symbol


def parse_isin_from_course_page(soup: BeautifulSoup, html_text: str) -> Optional[str]:
    h2 = soup.find("h2", class_="c-faceplate__isin")
    if h2:
        got = normalize_isin(h2.get_text(" ", strip=True))
        if got:
            return got
    txt = html_text.upper()
    for cand in ISIN_CANDIDATE_REGEX.findall(txt):
        if is_valid_isin(cand):
            return cand
    return None


def parse_faceplate_data(html_text: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html_text, "html.parser")
    faceplate = soup.find("div", class_="c-faceplate")
    data = {}
    if faceplate:
        raw = faceplate.get("data-ist-init", "")
        if raw:
            raw = _decode_data_ist_init(raw)
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                data = {}
    h1 = soup.find("h1")
    title = _normalize_spaces(h1.get_text(" ", strip=True)) if h1 else None
    isin = parse_isin_from_course_page(soup, html_text)
    return {"data": data, "page_title": title, "isin": isin}


def fetch_quote_for_symbol(session: requests.Session, symbol: str, cfg: AppConfig) -> Dict[str, Any]:
    url = f"https://www.boursorama.com/cours/{safe_symbol_in_url(symbol)}/"
    html_text = fetch_html(session, url, cfg)
    parsed = parse_faceplate_data(html_text)
    data = parsed.get("data") or {}
    page_title = parsed.get("page_title")
    if page_title:
        page_title = page_title.upper()
    return {
        "url": url,
        "symbol": data.get("symbol") or symbol,
        "isin": normalize_isin(parsed.get("isin")),
        "last": data.get("last"),
        "previousClose": data.get("previousClose"),
        "high": data.get("high"),
        "low": data.get("low"),
        "variation": data.get("variation"),
        "volume": data.get("totalVolume") or data.get("volume"),
        "exchangeCode": data.get("exchangeCode"),
        "category": data.get("category"),
        "page_title": page_title,
    }


def archive_file(src: Path, archive_dir: Path) -> Optional[Path]:
    if not src.exists():
        return None
    archive_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = archive_dir / f"{src.stem}_{stamp}{src.suffix}"
    src.replace(dst)
    return dst


def write_csv_semicolon(path: Path, rows: List[Dict[str, Any]]) -> None:
    fieldnames = [
        "label", "url", "symbol", "isin",
        "last", "previousClose", "high", "low", "variation", "volume",
        "exchangeCode", "category",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        w.writeheader()
        for r in rows:
            row = dict(r)
            row["isin"] = normalize_isin(row.get("isin"))
            row["label"] = (row.get("label") or "").strip().upper()
            w.writerow({k: row.get(k) for k in fieldnames})


def update_master_csv(cfg: AppConfig) -> Path:
    log("[MASTER] démarrage")
    session = build_session(cfg)

    log("[MASTER][1/3] Récupération des cotations standards")
    rows = scrape_all_list_pages(cfg, session)
    by_symbol = {r["symbol"]: r for r in rows if r.get("symbol")}
    existing = set(by_symbol.keys())

    log("[MASTER][2/3] Lecture des composition.csv ETF")
    etf_by_symbol = read_etf_components(cfg.etf_path)

    log("[MASTER][3/3] Ajout/enrichissement ETF")
    added = 0
    updated_isin = 0

    for sym, meta in etf_by_symbol.items():
        etf_label = (meta.get("label") or "").strip().upper() or None
        etf_isin = normalize_isin(meta.get("isin"))

        if sym in existing:
            r = by_symbol[sym]
            if etf_label and (not r.get("label") or r.get("label") == sym):
                r["label"] = etf_label
            if etf_isin and not normalize_isin(r.get("isin")):
                r["isin"] = etf_isin
                updated_isin += 1
            if not normalize_isin(r.get("isin")):
                try:
                    q = fetch_quote_for_symbol(session, sym, cfg)
                    if q.get("isin"):
                        r["isin"] = q["isin"]
                        updated_isin += 1
                except Exception as e:
                    log(f"[MASTER][WARN] ISIN via /cours/ impossible pour {sym}: {e}")
            continue

        new_row = {
            "label": etf_label if etf_label else sym.upper(),
            "url": f"https://www.boursorama.com/cours/{safe_symbol_in_url(sym)}/",
            "symbol": sym,
            "isin": etf_isin,
            "last": None, "previousClose": None, "high": None, "low": None,
            "variation": None, "volume": None, "exchangeCode": None, "category": None,
        }
        try:
            q = fetch_quote_for_symbol(session, sym, cfg)
            new_row["url"] = q.get("url") or new_row["url"]
            for k in ["last", "previousClose", "high", "low", "variation", "volume", "exchangeCode", "category"]:
                new_row[k] = q.get(k)
            if not new_row.get("isin") and q.get("isin"):
                new_row["isin"] = q["isin"]
                updated_isin += 1
            if (not etf_label) and q.get("page_title"):
                new_row["label"] = q.get("page_title")
        except Exception as e:
            log(f"[MASTER][WARN] /cours/ impossible pour {sym}: {e}")

        rows.append(new_row)
        by_symbol[sym] = new_row
        existing.add(sym)
        added += 1
        snooze(cfg, cfg.pause_quote_s)

    rows = sorted(rows, key=lambda r: ((r.get("label") or "").upper(), (r.get("symbol") or "")))
    tmp = cfg.master_csv.with_suffix(".tmp.csv")
    write_csv_semicolon(tmp, rows)

    if cfg.archive_old:
        archived = archive_file(cfg.master_csv, cfg.archive_dir)
        if archived:
            log(f"[MASTER] archive: {archived.name}")

    tmp.replace(cfg.master_csv)
    log(f"[MASTER] ETF ajoutés: {added} | ISIN complétés: {updated_isin}")
    log(f"[MASTER] CSV mis à jour: {cfg.master_csv}")
    return cfg.master_csv


# =========================================================
# LECTURE CSV SOURCE HISTORIQUE
# =========================================================
def get_history_source_csv(cfg: AppConfig) -> Path:
    candidates = []
    seen = set()

    def add_candidate(p: Path) -> None:
        p = Path(p).resolve()
        if str(p).lower() not in seen:
            seen.add(str(p).lower())
            candidates.append(p)

    add_candidate(cfg.enriched_csv)
    add_candidate(cfg.base_path / DEFAULT_ENRICHED_CSV)
    add_candidate(cfg.base_path.parent / DEFAULT_ENRICHED_CSV)
    add_candidate(SCRIPT_DIR / DEFAULT_ENRICHED_CSV)

    log("[HISTORY] recherche stricte de boursorama_cotations_enriched.csv")
    for cand in candidates:
        log(f"[HISTORY] candidat: {cand}")
        if cand.exists() and cand.is_file():
            log(f"[HISTORY] CSV enrichi trouvé: {cand}")
            return cand

    searched = "\n".join(f"- {p}" for p in candidates)
    raise FileNotFoundError(
        "Le fichier source prioritaire 'boursorama_cotations_enriched.csv' est introuvable.\n"
        f"Chemins testés :\n{searched}\n"
        "Utilise --enriched-csv pour imposer explicitement le bon chemin si nécessaire."
    )


def read_history_source_csv(path: Path) -> pd.DataFrame:
    sep = sniff_delimiter(path)
    df = pd.read_csv(path, sep=sep, dtype=str, engine="python").fillna("")
    df.columns = [c.strip() for c in df.columns]
    if "url" not in df.columns:
        raise ValueError(f"Colonne 'url' absente dans {path}")
    if "symbol" not in df.columns:
        raise ValueError(f"Colonne 'symbol' absente dans {path}")
    return df


# =========================================================
# PHASE 2 - PLAYWRIGHT / 5J / 10A
# =========================================================
def handle_didomi(page) -> None:
    page.wait_for_timeout(800)
    try:
        btn = page.get_by_role("button", name=re.compile("Tout accepter", re.IGNORECASE))
        if btn.count() > 0:
            btn.first.click(timeout=3000)
            page.wait_for_timeout(500)
    except Exception:
        pass
    try:
        page.evaluate("""
            const host = document.querySelector('#didomi-host');
            if (host) {
                host.style.pointerEvents = 'none';
                host.style.display = 'none';
            }
        """)
    except Exception:
        pass


def safe_click(locator) -> None:
    try:
        locator.click(timeout=20000)
    except Exception:
        locator.click(timeout=20000, force=True)


def download_chart_file(page, duration_selector: str, out_path: Path) -> None:
    loc = page.locator(duration_selector)
    if loc.count() == 0:
        raise RuntimeError(f"Sélecteur durée introuvable: {duration_selector}")
    safe_click(loc.first)
    page.wait_for_timeout(1200)
    handle_didomi(page)
    dl = page.locator(SEL_DOWNLOAD_ICON)
    if dl.count() == 0:
        raise RuntimeError("Icône de téléchargement introuvable.")
    with page.expect_download(timeout=60000) as dl_info:
        safe_click(dl.first)
    download = dl_info.value
    out_path.parent.mkdir(parents=True, exist_ok=True)
    download.save_as(str(out_path))


def read_5d_tsv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, sep="\t", dtype=str, engine="python")
    df.columns = [c.strip().lower() for c in df.columns]
    df = df.loc[:, [c for c in df.columns if c and not c.lower().startswith("unnamed")]]
    missing = [c for c in EXPECTED_COLS_5D if c not in df.columns]
    if missing:
        raise RuntimeError(f"Colonnes manquantes dans {path.name}: {missing} | présentes={df.columns.tolist()}")
    df = df[EXPECTED_COLS_5D].copy()
    df["ts"] = pd.to_datetime(df["date"], format="%d/%m/%Y %H:%M", errors="coerce")
    df = df[df["ts"].notna()].copy()
    for col in ["ouv", "haut", "bas", "clot"]:
        df[col] = normalize_numeric_series(df[col])
    df["vol"] = pd.to_numeric(
        df["vol"].astype(str)
        .str.replace("\u202f", "", regex=False)
        .str.replace(" ", "", regex=False),
        errors="coerce",
    )
    df["devise"] = df["devise"].astype(str).str.strip()
    df["day"] = df["ts"].dt.strftime("%Y-%m-%d")
    df["row_sig"] = (
        df["ts"].dt.strftime("%Y-%m-%d %H:%M") + "|"
        + df["ouv"].astype(str) + "|" + df["haut"].astype(str) + "|"
        + df["bas"].astype(str) + "|" + df["clot"].astype(str) + "|"
        + df["vol"].astype(str) + "|" + df["devise"].astype(str)
    )
    df = df.sort_values(["ts", "vol"], kind="mergesort").reset_index(drop=True)
    return df


def get_intraday_last_date(symbol: str, cfg: AppConfig) -> Optional[str]:
    """
    Retourne la date la plus récente présente dans intraday_db pour ce symbole
    (format 'YYYY-MM-DD'), ou None si le symbole n'a pas encore de données.
    Gère les deux extensions : .csv (format courant) et .tsv (format legacy).
    """
    sym_dir = cfg.intraday_db_dir / symbol
    if not sym_dir.exists():
        return None
    # Chercher .csv (nouveau format) et .tsv (legacy) — trier par stem (date)
    files = sorted(
        [p for p in sym_dir.iterdir() if p.suffix in (".csv", ".tsv")],
        key=lambda p: p.stem,
    )
    if not files:
        return None
    return files[-1].stem  # ex: "2026-04-11"


def update_intraday_db(symbol: str, df_new: pd.DataFrame, cfg: AppConfig) -> Tuple[int, List[str]]:
    """
    Merge les données intraday du fichier 5J dans intraday_db, jour par jour.

    Logique "jours manquants" :
      - Récupère la dernière date présente dans intraday_db
      - N'écrit QUE les jours du fichier 5J postérieurs à cette date
        (ou tous les jours si intraday_db est vide)
      - Pour le jour le plus récent (potentiellement en cours de journée),
        une déduplication fine par row_sig est appliquée pour n'ajouter
        que les nouvelles minutes.

    Retourne (nb_lignes_ajoutées, liste_des_jours_ajoutés).
    """
    sym_dir = cfg.intraday_db_dir / symbol
    sym_dir.mkdir(parents=True, exist_ok=True)

    last_date = get_intraday_last_date(symbol, cfg)  # ex: "2026-04-10" ou None
    added_total = 0
    days_added: List[str] = []

    for day, chunk in df_new.groupby("day"):
        # Ignorer les jours déjà complètement couverts
        # (sauf le dernier jour connu → peut être incomplet si run en cours de séance)
        if last_date is not None and day < last_date:
            continue  # jour plus ancien que la dernière date en base → skip

        # Format cible : CSV avec séparateur ';' — cohérent avec le reste du projet
        out_path_csv = sym_dir / f"{day}.csv"
        out_path_tsv = sym_dir / f"{day}.tsv"   # legacy (rétrocompatibilité)

        chunk_out = chunk[["ts", "ouv", "haut", "bas", "clot", "vol", "devise", "row_sig"]].copy()
        chunk_out["ts"] = chunk_out["ts"].dt.strftime("%Y-%m-%d %H:%M")

        # Déterminer quel fichier existe (préférer .csv, fallback .tsv legacy)
        if out_path_csv.exists():
            out_path = out_path_csv
            read_sep = ";"
        elif out_path_tsv.exists():
            out_path = out_path_tsv   # legacy : on lit en tsv mais on réécrit en csv
            read_sep = "\t"
        else:
            out_path = None
            read_sep = ";"

        if out_path is not None:
            # Jour déjà existant (typiquement le dernier jour, potentiellement incomplet)
            # → on fusionne uniquement les nouvelles lignes (dédup par row_sig)
            df_old = pd.read_csv(out_path, sep=read_sep, dtype=str, engine="python")
            if "row_sig" not in df_old.columns:
                df_old["row_sig"] = (
                    df_old["ts"].astype(str) + "|" + df_old["ouv"].astype(str) + "|"
                    + df_old["haut"].astype(str) + "|" + df_old["bas"].astype(str) + "|"
                    + df_old["clot"].astype(str) + "|" + df_old["vol"].astype(str) + "|"
                    + df_old["devise"].astype(str)
                )
            existing = set(df_old["row_sig"].astype(str).tolist())
            new_rows = chunk_out[~chunk_out["row_sig"].astype(str).isin(existing)].copy()
            if len(new_rows) > 0:
                df_all = pd.concat([df_old, new_rows], ignore_index=True)
                df_all = df_all.drop_duplicates(subset=["row_sig"], keep="last")
                df_all = df_all.sort_values("ts").reset_index(drop=True)
                # Toujours écrire en CSV ;  — supprime le .tsv legacy si nécessaire
                df_all.to_csv(out_path_csv, sep=";", index=False, encoding="utf-8-sig")
                if out_path_tsv.exists():
                    out_path_tsv.unlink()   # supprime le fichier legacy .tsv
                added_total += len(new_rows)
                days_added.append(day)
        else:
            # Nouveau jour → écriture directe en CSV ; trié par timestamp
            chunk_out = chunk_out.sort_values("ts").reset_index(drop=True)
            chunk_out.to_csv(out_path_csv, sep=";", index=False, encoding="utf-8-sig")
            added_total += len(chunk_out)
            days_added.append(day)

    return added_total, days_added


def load_intraday_days(symbol: str, days: List[str], cfg: AppConfig) -> pd.DataFrame:
    """
    Charge les fichiers CSV (ou TSV legacy) de intraday_db pour les jours indiqués
    et retourne un DataFrame consolidé avec les colonnes intraday standard.
    Utilisé pour recalculer le OHLC journalier depuis la source fiable.
    Rétrocompatible : lit .csv (nouveau, sep=';') ou .tsv (legacy, sep='\t').
    """
    frames = []
    sym_dir = cfg.intraday_db_dir / symbol
    for day in days:
        # Priorité .csv (nouveau format), fallback .tsv (legacy)
        p = sym_dir / f"{day}.csv"
        sep = ";"
        if not p.exists():
            p = sym_dir / f"{day}.tsv"
            sep = "\t"
        if p.exists():
            df = pd.read_csv(p, sep=sep, dtype=str, engine="python")
            df["day"] = day
            df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
            for col in ["ouv", "haut", "bas", "clot"]:
                df[col] = normalize_numeric_series(df[col])
            df["vol"] = pd.to_numeric(
                df["vol"].astype(str).str.replace(" ", "", regex=False),
                errors="coerce",
            )
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def daily_ohlc_from_intraday(df_intraday: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule le OHLC journalier correct à partir des données intraday minute.
    - open   : ouv  de la PREMIÈRE minute du jour
    - high   : MAX  de haut sur toutes les minutes du jour
    - low    : MIN  de bas  sur toutes les minutes du jour
    - close  : clot de la DERNIÈRE minute du jour
    - volume : SOMME des volumes de toutes les minutes du jour
    """
    df = df_intraday.sort_values("ts", kind="mergesort").copy()
    agg = df.groupby("day").agg(
        open   =("ouv",  "first"),
        high   =("haut", "max"),
        low    =("bas",  "min"),
        close  =("clot", "last"),
        volume =("vol",  "sum"),
    ).reset_index().rename(columns={"day": "date"})
    return agg.sort_values("date").reset_index(drop=True)


def read_ohlc_10a(path: Path) -> pd.DataFrame:
    sep = sniff_delimiter(path)
    df = pd.read_csv(path, sep=sep, dtype=str, engine="python")
    df.columns = [c.strip().lower() for c in df.columns]
    df = df.loc[:, [c for c in df.columns if c and not c.lower().startswith("unnamed")]]
    if len(df.columns) == 1 and "\t" in df.columns[0]:
        df = pd.read_csv(path, sep="\t", dtype=str, engine="python")
        df.columns = [c.strip().lower() for c in df.columns]
        df = df.loc[:, [c for c in df.columns if c and not c.lower().startswith("unnamed")]]
    rename_map = {"ouv": "open", "haut": "high", "bas": "low", "clot": "close", "vol": "volume"}
    df = df.rename(columns=rename_map)
    if "date" not in df.columns:
        raise RuntimeError(f"Colonne 'date' absente dans {path.name}. colonnes={df.columns.tolist()}")
    for col in ["open", "high", "low", "close", "volume"]:
        if col not in df.columns:
            df[col] = pd.NA
    df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True).dt.strftime("%Y-%m-%d")
    df = df[df["date"].notna()].copy()
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = normalize_numeric_series(df[col])
    df = df[["date", "open", "high", "low", "close", "volume"]].copy()
    df = df.drop_duplicates(subset=["date"], keep="last")
    df = df.sort_values("date").reset_index(drop=True)
    return df


def update_ohlc_10a_incremental(symbol: str, df_daily: pd.DataFrame, cfg: AppConfig) -> int:
    sym_dir = cfg.ohlc_10a_dir / symbol
    sym_dir.mkdir(parents=True, exist_ok=True)
    out_path = sym_dir / f"{symbol}_10a.csv"
    if out_path.exists():
        df10 = read_ohlc_10a(out_path)
        existing_dates = set(df10["date"].astype(str).tolist())
    else:
        df10 = pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
        existing_dates = set()
    df_new = df_daily[~df_daily["date"].astype(str).isin(existing_dates)].copy()
    if len(df_new) == 0:
        if not out_path.exists():
            df10.to_csv(out_path, sep=";", index=False, encoding="utf-8-sig")
        return 0
    df_final = pd.concat([df10, df_new], ignore_index=True)
    for c in ["date", "open", "high", "low", "close", "volume"]:
        if c not in df_final.columns:
            df_final[c] = pd.NA
    df_final = df_final[["date", "open", "high", "low", "close", "volume"]].copy()
    df_final = df_final.drop_duplicates(subset=["date"], keep="last")
    df_final = df_final.sort_values("date").reset_index(drop=True)
    df_final.to_csv(out_path, sep=";", index=False, encoding="utf-8-sig")
    return len(df_new)


def update_history_from_csv(cfg: AppConfig) -> None:
    source_csv = get_history_source_csv(cfg)
    df = read_history_source_csv(source_csv)

    cfg.updates_5d_dir.mkdir(parents=True, exist_ok=True)
    cfg.intraday_db_dir.mkdir(parents=True, exist_ok=True)
    cfg.ohlc_10a_dir.mkdir(parents=True, exist_ok=True)

    log(f"[HISTORY] source CSV: {source_csv}")
    log(f"[HISTORY] UPDATE cotations: {len(df)} actions")

    report_rows: List[Dict[str, Any]] = []
    count_success = 0
    count_partial = 0
    count_failed = 0
    count_5j_failed = 0
    count_10a_failed = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=cfg.headless,
            # Nécessaire dans Docker (pas de GPU / sandbox)
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
        )
        context = browser.new_context(
            accept_downloads=True,
            locale="fr-FR",
            viewport={"width": 1400, "height": 900},
        )
        page = context.new_page()

        for idx, row in df.iterrows():
            url = (row.get("url") or "").strip()
            symbol = sanitize(row.get("symbol", f"action_{idx}"))

            report = {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "symbol": symbol, "url": url, "status": "",
                "five_j_status": "", "five_j_attempts": 0, "five_j_path": "",
                "intraday_status": "", "intraday_added": "",
                "ten_a_status": "", "ten_a_mode": "", "ten_a_attempts": 0,
                "ten_a_path": "", "message": "",
            }

            if not url:
                log(f"[HISTORY][{idx + 1}/{len(df)}] {symbol} -> url vide, skip")
                report.update({"status": "failed", "five_j_status": "failed",
                               "intraday_status": "skipped", "ten_a_status": "skipped",
                               "message": "url vide"})
                report_rows.append(report)
                count_failed += 1
                count_5j_failed += 1
                continue

            sym_5d_dir = cfg.updates_5d_dir / symbol
            sym_5d_dir.mkdir(parents=True, exist_ok=True)
            path_5j = sym_5d_dir / f"{symbol}_5j.csv"
            path_10a = cfg.ohlc_10a_dir / symbol / f"{symbol}_10a.csv"
            report["five_j_path"] = str(path_5j)
            report["ten_a_path"] = str(path_10a)

            log(f"[HISTORY][{idx + 1}/{len(df)}] {symbol}")

            try:
                page.goto(url, timeout=60000)
                handle_didomi(page)
            except Exception as e:
                log(f"    ❌ ouverture page KO : {e}")
                report.update({"status": "failed", "five_j_status": "failed",
                               "intraday_status": "skipped", "ten_a_status": "skipped",
                               "message": f"ouverture page KO: {e}"})
                report_rows.append(report)
                count_failed += 1
                count_5j_failed += 1
                time.sleep(cfg.sleep_between)
                continue

            ok_5j = False
            five_j_attempts = 0
            last_5j_error = ""
            for attempt in range(cfg.retry_per_action):
                five_j_attempts = attempt + 1
                try:
                    download_chart_file(page, SEL_LENGTH_5J, path_5j)
                    log(f"    ✅ 5J -> {path_5j.name}")
                    ok_5j = True
                    break
                except Exception as e:
                    log(f"    ⚠ 5J tentative {attempt + 1} KO : {e}")
                    page.wait_for_timeout(1500)
                    last_5j_error = str(e)

            report["five_j_attempts"] = five_j_attempts

            if not ok_5j:
                log("    ❌ 5J échec définitif")
                report.update({"status": "failed", "five_j_status": "failed",
                               "intraday_status": "skipped", "ten_a_status": "skipped",
                               "message": f"5J échec définitif: {last_5j_error}"})
                report_rows.append(report)
                count_failed += 1
                count_5j_failed += 1
                time.sleep(cfg.sleep_between)
                continue

            report["five_j_status"] = "ok"

            # ── Détection nouveau symbole vs symbole existant ────────────────
            is_new_symbol = get_intraday_last_date(symbol, cfg) is None
            if is_new_symbol:
                log(f"    ℹ️  Nouveau symbole — initialisation complète")
            else:
                log(f"    ℹ️  Symbole existant — mise à jour incrémentale (jours manquants)")

            # ── Étape A : initialisation OHLC 10A (nouveaux symboles uniquement) ──
            # Pour un nouveau symbole : on télécharge l'historique 10A AVANT les 5J,
            # car il couvre la période antérieure que les 5J ne peuvent pas fournir.
            if not path_10a.exists():
                report["ten_a_mode"] = "init"
                ok_10a = False
                ten_a_attempts = 0
                last_10a_error = ""
                for attempt in range(cfg.retry_per_action):
                    ten_a_attempts = attempt + 1
                    try:
                        download_chart_file(page, SEL_LENGTH_10A, path_10a)
                        log(f"    ✅ 10A initialisé -> {path_10a.name}")
                        ok_10a = True
                        break
                    except Exception as e:
                        log(f"    ⚠ 10A tentative {attempt + 1} KO : {e}")
                        page.wait_for_timeout(1500)
                        last_10a_error = str(e)
                report["ten_a_attempts"] = ten_a_attempts
                if ok_10a:
                    report["ten_a_status"] = "ok"
                else:
                    log("    ❌ 10A initial non créé")
                    report["ten_a_status"] = "failed"
                    count_10a_failed += 1
                    if report["message"]:
                        report["message"] += " | "
                    report["message"] += f"10A initial non créé: {last_10a_error}"

            # ── Étape B : merge intraday 5J → intraday_db (jours manquants) ──
            days_written: List[str] = []
            try:
                df_5d = read_5d_tsv(path_5j)
                added_intraday, days_written = update_intraday_db(symbol, df_5d, cfg)
                report["intraday_status"] = "ok"
                report["intraday_added"] = int(added_intraday)
                log(f"    ✅ intraday +{added_intraday} lignes sur {len(days_written)} jour(s) : {days_written}")
            except Exception as e:
                report["intraday_status"] = "failed"
                report["intraday_added"] = ""
                report["message"] = f"post-traitement 5J KO: {e}"
                log(f"    ❌ post-traitement 5J KO : {e}")

            # ── Étape C : mise à jour OHLC 10A incrémentale depuis intraday_db ──
            # On calcule le OHLC journalier depuis la SOURCE FIABLE (intraday_db stocké),
            # et non depuis le fichier 5J en mémoire. Seuls les jours effectivement
            # écrits en base sont traités.
            if path_10a.exists() and report["ten_a_status"] != "ok":
                # OHLC existant → mise à jour incrémentale
                report["ten_a_mode"] = "incremental"
                report["ten_a_attempts"] = 0
                if days_written:
                    try:
                        # Charger uniquement les jours nouveaux depuis intraday_db
                        df_intraday_new = load_intraday_days(symbol, days_written, cfg)
                        if not df_intraday_new.empty:
                            df_daily_ohlc = daily_ohlc_from_intraday(df_intraday_new)
                            added_daily = update_ohlc_10a_incremental(symbol, df_daily_ohlc, cfg)
                            report["ten_a_status"] = "ok"
                            log(f"    ✅ 10A incrémental +{added_daily} jour(s) depuis intraday_db")
                        else:
                            report["ten_a_status"] = "skipped"
                            report["message"] = (report["message"] + " | " if report["message"] else "") + \
                                "10A incrémental : intraday_db vide pour les jours concernés"
                    except Exception as e:
                        report["ten_a_status"] = "failed"
                        count_10a_failed += 1
                        report["message"] = (report["message"] + " | " if report["message"] else "") + \
                            f"update 10A incrémental KO: {e}"
                        log(f"    ❌ update 10A incrémental KO : {e}")
                else:
                    report["ten_a_status"] = "skipped"
                    log(f"    ℹ️  10A : aucun nouveau jour à ajouter")

            five_ok = report["five_j_status"] == "ok"
            ten_ok_or_skipped = report["ten_a_status"] in ("ok", "skipped")
            intraday_ok_or_failed = report["intraday_status"] in ("ok", "failed")

            if five_ok and report["ten_a_status"] == "ok" and report["intraday_status"] == "ok":
                report["status"] = "success"
                count_success += 1
            elif five_ok and ten_ok_or_skipped and intraday_ok_or_failed:
                report["status"] = "partial"
                count_partial += 1
            else:
                report["status"] = "failed"
                count_failed += 1

            report_rows.append(report)
            time.sleep(cfg.sleep_between)

        context.close()
        browser.close()

    write_report_csv(cfg.report_csv, report_rows)
    log("[HISTORY] UPDATE terminé.")
    log(
        f"[HISTORY][SUMMARY] success={count_success} | partial={count_partial} | "
        f"failed={count_failed} | 5J_failed={count_5j_failed} | 10A_failed={count_10a_failed}"
    )
    log(f"[HISTORY][SUMMARY] rapport: {cfg.report_csv}")


# =========================================================
# ARGUMENTS / MAIN
# =========================================================
def parse_args():
    p = argparse.ArgumentParser(description="Mise à jour complète Boursorama : master + 5J + 10A")
    p.add_argument("--base-path", default=str(DEFAULT_BASE_PATH),
                   help="Chemin curated (bases consolidées)")
    p.add_argument("--raw-path", default=str(DEFAULT_RAW_PATH),
                   help="Chemin raw (données brutes : master CSV, 5J downloads, rapport)")
    p.add_argument("--base-url", default=DEFAULT_BASE_URL)
    p.add_argument("--total-pages", type=int, default=DEFAULT_TOTAL_PAGES)
    p.add_argument("--etf-path", default=DEFAULT_ETF_PATH)
    p.add_argument("--master-csv", default=None)
    p.add_argument("--enriched-csv", default=None)
    p.add_argument("--report-csv", default=None)
    p.add_argument("--no-archive", action="store_true")
    p.add_argument("--headless", type=int, default=1)
    p.add_argument("--sleep-between", type=float, default=2.0)
    p.add_argument("--retry-per-action", type=int, default=2)
    p.add_argument("--connect-timeout", type=int, default=20)
    p.add_argument("--read-timeout", type=int, default=60)
    p.add_argument("--retries", type=int, default=5)
    p.add_argument("--backoff", type=float, default=0.8)
    p.add_argument("--skip-master", action="store_true")
    p.add_argument("--skip-history", action="store_true")
    return p.parse_args()


def build_config(args) -> AppConfig:
    # ── Chemin curated (bases consolidées) ──────────────────────────────────
    base_path = Path(args.base_path).resolve()

    # ── Chemin raw (données brutes) ──────────────────────────────────────────
    # Priorité : argument --raw-path > valeur par défaut DEFAULT_RAW_PATH
    raw_path_arg = getattr(args, "raw_path", None)
    raw_base_path = Path(raw_path_arg).resolve() if raw_path_arg else DEFAULT_RAW_PATH.resolve()

    # ── Fichiers CSV ─────────────────────────────────────────────────────────
    # master_csv   → raw  (scraping brut avant enrichissement)
    master_csv   = Path(args.master_csv).resolve()   if args.master_csv   else (raw_base_path / DEFAULT_MASTER_CSV)
    # enriched_csv → curated/finance/valeurs/ (enrichi avec ISIN, source pour update_history)
    # base_path = curated/finance/cotations/ → parent = curated/finance/ → valeurs/ est un sibling de cotations/
    enriched_csv = Path(args.enriched_csv).resolve() if args.enriched_csv else (base_path.parent / "valeurs" / DEFAULT_ENRICHED_CSV)
    # report_csv   → raw  (rapport du téléchargement)
    report_csv   = Path(args.report_csv).resolve()   if args.report_csv   else (raw_base_path / DEFAULT_REPORT_CSV)

    # ── Répertoires ──────────────────────────────────────────────────────────
    curated_cotation_dir = base_path     / "cotation"          # curated : intraday_db
    raw_cotation_dir     = raw_base_path / "cotation"           # raw     : 5d_updates

    return AppConfig(
        base_path=base_path,
        raw_base_path=raw_base_path,
        base_url=args.base_url,
        total_pages=args.total_pages,
        etf_path=args.etf_path,
        master_csv=master_csv,
        enriched_csv=enriched_csv,
        report_csv=report_csv,
        archive_dir=(raw_base_path / "archives"),   # archives du master brut → raw
        archive_old=(not args.no_archive),
        cotation_dir=curated_cotation_dir,           # parent de intraday_db → curated
        updates_5d_dir=(raw_cotation_dir / "5d_updates"),  # téléchargements 5J → raw
        intraday_db_dir=(curated_cotation_dir / "intraday_db"),  # DB consolidée → curated
        ohlc_10a_dir=(base_path / "ohlc_10a"),      # OHLC consolidé → curated
        headless=bool(args.headless),
        sleep_between=args.sleep_between,
        retry_per_action=args.retry_per_action,
        pause_pages_s=0.8,
        pause_quote_s=0.6,
        jitter_s=0.6,
        connect_timeout_s=args.connect_timeout,
        read_timeout_s=args.read_timeout,
        max_retries=args.retries,
        backoff_factor=args.backoff,
        skip_master=args.skip_master,
        skip_history=args.skip_history,
    )


def main():
    args = parse_args()
    cfg = build_config(args)
    # Répertoires curated
    cfg.base_path.mkdir(parents=True, exist_ok=True)
    cfg.cotation_dir.mkdir(parents=True, exist_ok=True)
    cfg.ohlc_10a_dir.mkdir(parents=True, exist_ok=True)
    # Répertoires raw
    cfg.raw_base_path.mkdir(parents=True, exist_ok=True)
    cfg.updates_5d_dir.mkdir(parents=True, exist_ok=True)
    cfg.archive_dir.mkdir(parents=True, exist_ok=True)
    log("=== DEMARRAGE UPDATE BOURSORAMA ALL ===")
    if not cfg.skip_master:
        update_master_csv(cfg)
    else:
        log("[MASTER] étape ignorée (--skip-master)")
    if not cfg.skip_history:
        update_history_from_csv(cfg)
    else:
        log("[HISTORY] étape ignorée (--skip-history)")
    log("=== FIN UPDATE BOURSORAMA ALL ===")


if __name__ == "__main__":
    main()
