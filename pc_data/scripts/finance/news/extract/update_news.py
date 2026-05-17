# -*- coding: utf-8 -*-
"""
update_news.py — v2 (Docker-compatible)
========================================
Chemins configurables via variables d'environnement :
  DATAOZ_NEWS_ROOT     → dossier racine des news par symbole
  DATAOZ_FINANCE_ROOT  → dossier racine finance (contient le CSV)

Si les variables ne sont pas définies, les chemins Windows originaux sont utilisés
(compatibilité exécution standalone hors Docker).
"""

from __future__ import annotations  # Python 3.8 compatibility (list[str], tuple[...])

import json
import os
import random
import re
import time
import traceback
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.pdfgen import canvas


# =====================================================
# CONFIG — chemins via env vars (Docker) ou fallback Windows
# =====================================================

BASE_ROOT_DIR = Path(os.environ.get(
    "DATAOZ_NEWS_ROOT",
    r"D:\projet_dataoz\pc_data\data\curated\finance\news"
))

UPDATES_PARENT_DIR = Path(os.environ.get(
    "DATAOZ_NEWS_RAW",
    r"D:\projet_dataoz\pc_data\data\raw\finance"
))

_FINANCE_ROOT = Path(os.environ.get(
    "DATAOZ_FINANCE_ROOT",
    r"D:\projet_dataoz\pc_data\data\curated\finance"
))

# Chemin du script (pour retrouver des ressources relatives si besoin)
_SCRIPT_DIR = Path(__file__).resolve().parent  # .../scripts/finance/extract/

GENERAL_CSV_CANDIDATES = [
    _FINANCE_ROOT / "valeurs" / "boursorama_cotations_enriched.csv",
    _FINANCE_ROOT / "boursorama_cotations_enriched.csv",
    Path(__file__).resolve().parent / "boursorama_cotations_enriched.csv",
]

MAX_PAGES = 50
LIMIT = 10
BASE = "https://www.boursorama.com"

SLEEP_LIST_MIN, SLEEP_LIST_MAX = 2.5, 4.5
SLEEP_BETWEEN_ARTICLES_SECONDS = 2.5

MAX_RETRIES = 5
BACKOFF_SECONDS = [10, 30, 60, 120, 300]

MAX_WORKERS = 4

MIN_BODY_CHARS_DEFAULT = 200

MANIFEST_SEP = ";"
MANIFEST_DT_COL = "datetime"
MANIFEST_DT_FORMAT = "%Y-%m-%d %H:%M"
MANIFEST_COLUMNS = ["symbol", "datetime", "source", "title", "url", "pdf_path"]
OUT_ENCODING = "utf-8-sig"

UNICODE_BULLETS = [
    "•", "▪", "◦", "‣", "·", "■", "□", "●", "○", "♦", "◆", "\x7f", "\uf0b7"
]


# =====================================================
# STRUCTURE
# =====================================================

@dataclass
class NewsItem:
    symbol: str
    dt: datetime
    source: str
    title: str
    url: str
    url_kind: str
    body: str = ""


# =====================================================
# HELPERS
# =====================================================

def safe_mkdir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def sleep_rand(a, b):
    time.sleep(random.uniform(a, b))


def get_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept-Language": "fr-FR,fr;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "keep-alive",
    })
    return s


def get_with_retry(session, url, timeout=30):
    last_exc = None
    for attempt in range(MAX_RETRIES):
        try:
            return session.get(url, timeout=timeout)
        except requests.exceptions.RequestException as e:
            last_exc = e
            wait = BACKOFF_SECONDS[min(attempt, len(BACKOFF_SECONDS) - 1)]
            print(f"[RETRY] {wait}s -> {url}", flush=True)
            time.sleep(wait)
    raise last_exc


def unique_path_with_dup(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suf = path.suffix
    for i in range(1, 1000):
        cand = path.with_name(f"{stem}_dup{i}{suf}")
        if not cand.exists():
            return cand
    raise RuntimeError(f"Trop de collisions sur {path.name}")


def normalize_symbol(value) -> str:
    if pd.isna(value):
        return ""
    s = str(value).strip()
    s = re.sub(r"\s+", "", s)
    return s


def resolve_general_csv() -> Path:
    for path in GENERAL_CSV_CANDIDATES:
        if path.exists():
            return path
    raise FileNotFoundError(
        "boursorama_cotations_enriched.csv introuvable. Emplacements testés :\n- "
        + "\n- ".join(str(p) for p in GENERAL_CSV_CANDIDATES)
    )


def read_general_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, sep=";", dtype=str, encoding=OUT_ENCODING).fillna("")
    except Exception:
        try:
            return pd.read_csv(path, sep=";", dtype=str, encoding="utf-8").fillna("")
        except Exception:
            return pd.read_csv(path, sep=";", dtype=str).fillna("")


def extract_symbols_from_general_csv(df: pd.DataFrame) -> list[str]:
    candidates = []
    if "symbol" in df.columns:
        candidates.extend(df["symbol"].tolist())
    if "code_action" in df.columns:
        candidates.extend(df["code_action"].tolist())

    symbols = []
    seen = set()
    for value in candidates:
        sym = normalize_symbol(value)
        if not sym or sym in seen:
            continue
        seen.add(sym)
        symbols.append(sym)
    return sorted(symbols)


def sync_news_directories_from_csv() -> tuple[Path, int, int]:
    safe_mkdir(BASE_ROOT_DIR)
    general_csv = resolve_general_csv()
    df = read_general_csv(general_csv)
    csv_symbols = extract_symbols_from_general_csv(df)
    existing_dirs = {p.name for p in BASE_ROOT_DIR.iterdir() if p.is_dir()}
    created = 0
    for symbol in csv_symbols:
        target_dir = BASE_ROOT_DIR / symbol
        if symbol not in existing_dirs:
            safe_mkdir(target_dir)
            created += 1
            print(f"[SYNC] dossier créé : {target_dir.name}", flush=True)
    return general_csv, len(csv_symbols), created


# =====================================================
# MANIFEST
# =====================================================

def read_last_datetime(symbol: str):
    manifest_path = BASE_ROOT_DIR / symbol / f"{symbol}_manifest.csv"
    if not manifest_path.exists():
        return None
    try:
        df = pd.read_csv(manifest_path, sep=MANIFEST_SEP, dtype=str, encoding=OUT_ENCODING).fillna("")
    except Exception:
        df = pd.read_csv(manifest_path, sep=MANIFEST_SEP, dtype=str).fillna("")
    if MANIFEST_DT_COL not in df.columns or df.empty:
        return None
    dts = []
    for v in df[MANIFEST_DT_COL]:
        v = str(v).strip()
        if not v:
            continue
        try:
            dts.append(datetime.strptime(v, MANIFEST_DT_FORMAT))
        except Exception:
            pass
    return max(dts) if dts else None


def update_permanent_manifest(symbol: str, new_rows: list[dict]):
    manifest_path = BASE_ROOT_DIR / symbol / f"{symbol}_manifest.csv"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    existing_df = None
    if manifest_path.exists():
        try:
            existing_df = pd.read_csv(manifest_path, sep=MANIFEST_SEP, dtype=str, encoding=OUT_ENCODING).fillna("")
        except Exception:
            try:
                existing_df = pd.read_csv(manifest_path, sep=MANIFEST_SEP, dtype=str).fillna("")
            except Exception:
                existing_df = None

    new_df = pd.DataFrame(new_rows, columns=MANIFEST_COLUMNS)
    combined = pd.concat([existing_df, new_df], ignore_index=True) if existing_df is not None and not existing_df.empty else new_df

    if "url" in combined.columns:
        combined = combined.drop_duplicates(subset=["url"], keep="last")
    if MANIFEST_DT_COL in combined.columns:
        try:
            combined["_sort_dt"] = pd.to_datetime(combined[MANIFEST_DT_COL], format=MANIFEST_DT_FORMAT, errors="coerce")
            combined = combined.sort_values("_sort_dt", ascending=False).drop(columns=["_sort_dt"])
        except Exception:
            pass

    combined.to_csv(manifest_path, sep=MANIFEST_SEP, index=False, encoding=OUT_ENCODING)
    print(f"[MANIFEST_PERM] {symbol} -> {len(combined)} entrées", flush=True)


# =====================================================
# LIST PARSER
# =====================================================

def parse_list_items(html: str, symbol: str):
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for li in soup.select("li.c-list-details-news__line"):
        source = li.select_one("strong.c-source__name")
        source = source.get_text(strip=True) if source else ""
        times = [x.get_text(strip=True) for x in li.select(".c-source__time")]
        if len(times) < 2:
            continue
        try:
            dt = datetime.strptime(f"{times[0]} {times[1]}", "%d.%m.%Y %H:%M")
        except Exception:
            continue
        a = li.select_one(".c-list-details-news__title a[href]")
        if not a:
            continue
        title = a.get_text(strip=True)
        url = urljoin(BASE, a["href"])
        items.append(NewsItem(symbol=symbol, dt=dt, source=source, title=title, url=url, url_kind="href"))
    return items


# =====================================================
# ARTICLE EXTRACTION
# =====================================================

def normalize_problematic_punctuation(text: str) -> str:
    replacements = {
        "–": "-", "—": "-", "−": "-",
        "\u2019": "'", "\u2018": "'",          # guillemets simples typographiques
        "\u201c": '"', "\u201d": '"',           # guillemets doubles typographiques
        "\xa0": " ",                             # espace insécable
        "\u2026": "...",                         # ellipse …
        "\u20ac": "EUR",                         # € non supporté par Helvetica Latin-1
        "\u2022": "*", "\u00b7": "*",            # puces
        "\u00ab": '"', "\u00bb": '"',            # « »
        "\u2013": "-", "\u2014": "-",            # tirets longs
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    # Filet de sécurité : retire tout caractère hors Latin-1
    return text.encode("latin-1", errors="replace").decode("latin-1")



def clean_text_block(text: str) -> str:
    text = normalize_problematic_punctuation(text)
    for bullet in UNICODE_BULLETS:
        text = text.replace(bullet, "*")
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"\s+([,.;:%!?])", r"\1", text)
    text = re.sub(r"([(\[]) ", r"\1", text)
    text = re.sub(r" ([)\]])", r"\1", text)
    return text.strip()


def table_to_text(table) -> str:
    rows = []
    for tr in table.find_all('tr'):
        cells = [clean_text_block(td.get_text(" ", strip=True)) for td in tr.find_all(['td', 'th'])]
        if cells:
            rows.append(" | ".join(cells))
    return "\n".join(rows) if rows else ""


def html_fragment_to_text(html_fragment: str) -> str:
    soup = BeautifulSoup(html_fragment, "html.parser")
    for tag in soup(["script", "style"]):
        tag.decompose()
    blocks = []
    for element in soup.find_all(["p", "li", "br"]):
        if element.name == "br":
            blocks.append("")
            continue
        text = clean_text_block(element.get_text(" ", strip=True))
        if not text:
            continue
        if element.name == "li" and not re.match(r"^(?:\*|-)\s+\S", text):
            text = f"- {text}"
        blocks.append(text)
    if blocks:
        return clean_text_block("\n\n".join(x for x in blocks if x.strip()))
    return clean_text_block(soup.get_text("\n", strip=True))


def _element_get_text(element) -> str:
    brs = element.find_all("br")
    if not brs:
        return element.get_text(" ", strip=True)
    from copy import copy
    clone = copy(element)
    for br in clone.find_all("br"):
        br.replace_with("\n")
    text = clone.get_text(" ", strip=False)
    lines = text.split("\n")
    cleaned_lines = [re.sub(r"[ \t]+", " ", line).strip() for line in lines if re.sub(r"[ \t]+", " ", line).strip()]
    return "\n".join(cleaned_lines)


def _extract_bs4_structured(soup) -> str:
    content_area = soup.select_one(".c-news-detail__content") or soup.find('article') or soup.find('main')
    if not content_area:
        return ""
    blocks = []
    for element in content_area.find_all(['p', 'table', 'li', 'h2', 'h3', 'h4']):
        if element.name == 'table':
            table_text = table_to_text(element)
            if table_text:
                blocks.append(table_text)
        elif element.name in ('h2', 'h3', 'h4'):
            text = clean_text_block(element.get_text(" ", strip=True))
            if text and len(text) > 2:
                blocks.append(text)
        elif element.name == 'li':
            text = clean_text_block(element.get_text(" ", strip=True))
            if text:
                if not re.match(r"^(?:\*|-)\s+\S", text):
                    text = f"- {text}"
                blocks.append(text)
        else:
            text = clean_text_block(_element_get_text(element))
            if text and len(text) > 3:
                blocks.append(text)
    if not blocks:
        return ""
    return clean_text_block("\n\n".join(blocks))


def _extract_jsonld(soup) -> str:
    for sc in soup.select('script[type="application/ld+json"]'):
        raw = (sc.string or sc.get_text() or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        objs = [data] if isinstance(data, dict) else (data if isinstance(data, list) else [])
        for obj in objs:
            if not isinstance(obj, dict):
                continue
            body = obj.get("articleBody")
            if isinstance(body, str) and body.strip():
                cleaned = html_fragment_to_text(body)
                if cleaned:
                    return cleaned
    return ""


def extract_article_body(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    bs4_result = _extract_bs4_structured(soup)
    if bs4_result and len(bs4_result) >= MIN_BODY_CHARS_DEFAULT:
        return bs4_result
    jsonld_result = _extract_jsonld(soup)
    if jsonld_result:
        return jsonld_result
    if bs4_result:
        return bs4_result
    fallback = soup.select_one("article") or soup.select_one("main") or soup
    return clean_text_block(fallback.get_text("\n", strip=True))


# =====================================================
# PDF
# =====================================================

def _is_standalone_line(line: str) -> bool:
    line = line.strip()
    if re.match(r"^[-*]\s", line):
        return True
    if "|" in line:
        return True
    if re.search(r"\.{3,}", line):
        return True
    if re.search(r"[A-Z]{2,}[0-9]*\s*$", line) and len(line) < 120:
        return True
    if len(line) < 40 and not line[0].isupper():
        return True
    return False


def normalize_body_for_pdf(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    paragraphs = re.split(r"\n{2,}", text)
    result_paragraphs = []
    for para in paragraphs:
        para = para.strip()
        if not para:
            continue
        lines = [line.strip() for line in para.split("\n") if line.strip()]
        if not lines:
            continue
        merged_lines = []
        buffer = ""
        for line in lines:
            if not buffer:
                buffer = line
                continue
            if buffer.endswith(".") or _is_standalone_line(buffer) or _is_standalone_line(line):
                merged_lines.append(buffer)
                buffer = line
            else:
                buffer += " " + line
        if buffer:
            merged_lines.append(buffer)
        result_paragraphs.append("\n".join(merged_lines))
    merged_text = "\n".join(result_paragraphs)
    merged_text = re.sub(r"[ \t]{2,}", " ", merged_text)
    merged_text = re.sub(r"\n{3,}", "\n\n", merged_text)
    return merged_text.strip()


def split_long_word(word, pdf_canvas, font_name, font_size, max_width):
    parts = []
    current = ""
    for char in word:
        test = current + char
        if pdf_canvas.stringWidth(test, font_name, font_size) <= max_width:
            current = test
        else:
            if current:
                parts.append(current)
            current = char
    if current:
        parts.append(current)
    return parts


def wrap_paragraph(paragraph, pdf_canvas, font_name, font_size, max_width):
    paragraph = paragraph.strip()
    if not paragraph:
        return [""]
    words = paragraph.split()
    lines = []
    current_line = ""
    for word in words:
        if pdf_canvas.stringWidth(word, font_name, font_size) > max_width:
            if current_line:
                lines.append(current_line)
                current_line = ""
            for chunk in split_long_word(word, pdf_canvas, font_name, font_size, max_width):
                lines.append(chunk)
            continue
        test_line = (current_line + " " + word).strip()
        if pdf_canvas.stringWidth(test_line, font_name, font_size) <= max_width:
            current_line = test_line
        else:
            if current_line:
                lines.append(current_line)
            current_line = word
    if current_line:
        lines.append(current_line)
    return lines


def write_pdf(path: Path, item: NewsItem):
    c = canvas.Canvas(str(path), pagesize=A4)
    width, height = A4
    left = 2 * cm
    top = height - 2 * cm
    bottom = 2 * cm
    y = top
    max_width = width - left - 2 * cm

    # Sanitize tous les champs texte pour Latin-1 (police Helvetica)
    safe_title  = normalize_problematic_punctuation(item.title  or "")
    safe_source = normalize_problematic_punctuation(item.source or "")
    safe_url    = normalize_problematic_punctuation(item.url    or "")
    safe_body   = normalize_problematic_punctuation(item.body   or "")

    def draw_lines(lines, size, line_height):
        nonlocal y
        font_name = "Helvetica"
        c.setFont(font_name, size)
        for line in lines:
            if line == "":
                y -= line_height
                continue
            if y < bottom:
                c.showPage()
                y = top
                c.setFont(font_name, size)
            c.drawString(left, y, line)
            y -= line_height

    draw_lines(wrap_paragraph(safe_title, c, "Helvetica", 12, max_width), 12, 16)
    y -= 8
    meta = []
    meta.extend(wrap_paragraph(f"Source: {safe_source}", c, "Helvetica", 10, max_width))
    meta.extend(wrap_paragraph(f"Date: {item.dt:%Y-%m-%d %H:%M}", c, "Helvetica", 10, max_width))
    meta.extend(wrap_paragraph(f"URL: {safe_url}", c, "Helvetica", 10, max_width))
    draw_lines(meta, 10, 14)
    y -= 10
    for para in normalize_body_for_pdf(safe_body).split("\n"):
        draw_lines(wrap_paragraph(para, c, "Helvetica", 10, max_width), 10, 14)
        y -= 4
    c.save()


# =====================================================
# THREAD-SAFE PRINT
# =====================================================

_print_lock = threading.Lock()


def tprint(msg: str):
    with _print_lock:
        print(msg, flush=True)


# =====================================================
# UPDATE ONE SYMBOL
# =====================================================

def update_symbol(symbol: str, update_root: Path, stamp: str) -> int:
    session = get_session()
    last_dt = read_last_datetime(symbol)
    tprint(f"\n--- {symbol} ---")
    tprint(f"[{symbol}] LAST_DT: {last_dt if last_dt else 'Aucune (rattrapage complet)'}")

    created = 0
    manifest_rows = []
    seen_urls = set()
    update_sym_dir = update_root / symbol

    for page in range(MAX_PAGES):
        offset = page * LIMIT
        list_url = (
            f"{BASE}/actualites/_liste"
            f"?offset={offset}&limit={LIMIT}&filter=news"
            f"&symbol={symbol}&_route=news.list.partial"
        )
        tprint(f"[{symbol}] PAGE {page + 1}")
        sleep_rand(SLEEP_LIST_MIN, SLEEP_LIST_MAX)
        r = get_with_retry(session, list_url)

        if r.status_code != 200:
            tprint(f"[{symbol}] LIST HTTP {r.status_code} -> stop")
            break

        items = parse_list_items(r.text, symbol)
        if not items:
            tprint(f"[{symbol}] LIST 0 item -> stop")
            break

        page_has_new = False
        for it in items:
            if it.url in seen_urls:
                continue
            seen_urls.add(it.url)
            if last_dt and it.dt <= last_dt:
                continue
            page_has_new = True
            tprint(f"[{symbol}] NEW {it.dt:%Y-%m-%d %H:%M} | {it.title}")
            time.sleep(SLEEP_BETWEEN_ARTICLES_SECONDS)
            resp = get_with_retry(session, it.url)
            if resp.status_code != 200:
                tprint(f"[{symbol}] SKIP HTTP {resp.status_code}")
                continue
            body = extract_article_body(resp.text)
            if len(body) < MIN_BODY_CHARS_DEFAULT:
                tprint(f"[{symbol}] SKIP Article trop court")
                continue
            it.body = body
            if created == 0:
                safe_mkdir(update_sym_dir)
            pdf_name = f"{symbol}_{it.dt:%Y-%m-%d_%H-%M}.pdf"
            pdf_path = unique_path_with_dup(update_sym_dir / pdf_name)
            write_pdf(pdf_path, it)
            tprint(f"[{symbol}] PDF {pdf_path.name}")
            manifest_rows.append({
                "symbol": symbol,
                "datetime": it.dt.strftime(MANIFEST_DT_FORMAT),
                "source": it.source,
                "title": it.title,
                "url": it.url,
                "pdf_path": str(pdf_path),
            })
            created += 1

        if last_dt and not page_has_new:
            tprint(f"[{symbol}] plus de news récentes -> stop pagination")
            break

    if created > 0:
        manifest_name = f"{symbol}_manifest_update_{stamp}.csv"
        manifest_path = unique_path_with_dup(update_sym_dir / manifest_name)
        pd.DataFrame(manifest_rows).to_csv(manifest_path, sep=MANIFEST_SEP, index=False, encoding=OUT_ENCODING)
        tprint(f"[{symbol}] MANIFEST_UPDATE {manifest_path.name}")
        update_permanent_manifest(symbol, manifest_rows)

    return created


# =====================================================
# MAIN
# =====================================================

def main():
    if not BASE_ROOT_DIR.exists():
        safe_mkdir(BASE_ROOT_DIR)

    general_csv, csv_symbol_count, created_dirs = sync_news_directories_from_csv()
    stamp = datetime.now().strftime("%Y-%m-%d")
    update_root = UPDATES_PARENT_DIR / f"News_update_{stamp}"
    safe_mkdir(update_root)

    symbols = sorted([p.name for p in BASE_ROOT_DIR.iterdir() if p.is_dir()])

    print(f"[CSV] {general_csv}", flush=True)
    print(f"[CSV_SYMBOLS] {csv_symbol_count}", flush=True)
    print(f"[SYNC_CREATED_DIRS] {created_dirs}", flush=True)
    print(f"[BASE] {BASE_ROOT_DIR}", flush=True)
    print(f"[UPDATE_ROOT] {update_root}", flush=True)
    print(f"[SYMBOLS] {len(symbols)} dossier(s) détecté(s)", flush=True)
    print(f"[WORKERS] {MAX_WORKERS} thread(s)", flush=True)

    total_created = 0
    symbols_with_news = 0
    _counter_lock = threading.Lock()

    def _process_symbol(symbol: str) -> tuple[str, int]:
        try:
            n = update_symbol(symbol, update_root, stamp)
            return (symbol, n)
        except Exception as e:
            tprint(f"[{symbol}] ERROR: {e}")
            tprint(traceback.format_exc())
            return (symbol, 0)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_process_symbol, s): s for s in symbols}
        for future in as_completed(futures):
            symbol, n = future.result()
            with _counter_lock:
                if n > 0:
                    symbols_with_news += 1
                    total_created += n

    try:
        if update_root.exists() and not any(update_root.iterdir()):
            update_root.rmdir()
            print(f"[CLEAN] Aucun PDF créé -> dossier update supprimé: {update_root}", flush=True)
    except Exception:
        pass

    print("\n=== FIN UPDATE ===", flush=True)
    print(f"Symbols traités: {len(symbols)}", flush=True)
    print(f"Symbols avec news: {symbols_with_news}", flush=True)
    print(f"PDF créés: {total_created}", flush=True)

    return {"symbols": len(symbols), "symbols_with_news": symbols_with_news, "pdfs_created": total_created}


if __name__ == "__main__":
    main()
