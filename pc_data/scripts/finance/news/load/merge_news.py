# -*- coding: utf-8 -*-
"""
merge_news.py — Docker-compatible
===================================
Chemin configurable via variable d'environnement :
  DATAOZ_NEWS_ROOT  → dossier racine des news par symbole

Si non définie, utilise le chemin Windows original (standalone).
"""

from __future__ import annotations  # Python 3.8 compatibility (X | Y, tuple[...])

import os
import re
import shutil
import traceback
from datetime import datetime
from pathlib import Path

import pandas as pd


# =====================================================
# CONFIG — chemin via env var (Docker) ou fallback Windows
# =====================================================

BASE_ROOT_DIR = Path(os.environ.get(
    "DATAOZ_NEWS_ROOT",
    r"D:\projet_dataoz\pc_data\data\curated\finance\news"
))

# Chemin du script (pour retrouver des ressources relatives si besoin)
_SCRIPT_DIR = Path(__file__).resolve().parent  # .../scripts/finance/load/

UPDATES_PARENT_DIR = Path(os.environ.get(
    "DATAOZ_NEWS_RAW",
    r"D:\projet_dataoz\pc_data\data\raw\finance"
))

SEP = ";"
ENCODING = "utf-8-sig"
DT_FMT = "%Y-%m-%d %H:%M"

EXPECTED_COLS = ["symbol", "datetime", "source", "title", "url", "pdf_path"]

PDF_NAME_RE = re.compile(
    r"^(?P<symbol>.+?)_(?P<date>\d{4}-\d{2}-\d{2})_(?P<time>\d{2}-\d{2})(?:_dup(?P<dup>\d+))?\.pdf$",
    re.IGNORECASE,
)


# =====================================================
# HELPERS
# =====================================================

def parse_dt(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, format=DT_FMT, errors="coerce")


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


def ensure_expected_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in EXPECTED_COLS:
        if c not in df.columns:
            df[c] = ""
    return df[EXPECTED_COLS].fillna("")


def read_manifest(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, sep=SEP, dtype=str, encoding=ENCODING).fillna("")
    for c in EXPECTED_COLS:
        if c not in df.columns:
            raise ValueError(f"Colonne manquante '{c}' dans {path}")
    return df[EXPECTED_COLS]


def empty_manifest_df() -> pd.DataFrame:
    return pd.DataFrame(columns=EXPECTED_COLS)


def find_update_root_for_today() -> Path | None:
    stamp = datetime.now().strftime("%Y-%m-%d")
    p = UPDATES_PARENT_DIR / f"News_update_{stamp}"
    return p if p.exists() else None


def find_latest_update_root() -> Path | None:
    cands = sorted([p for p in UPDATES_PARENT_DIR.glob("News_update_*") if p.is_dir()], reverse=True)
    return cands[0] if cands else None


def safe_move_pdf(src: Path, dst_dir: Path) -> Path:
    dst = unique_path_with_dup(dst_dir / src.name)
    shutil.move(str(src), str(dst))
    return dst


def remove_dir_if_exists(path: Path) -> None:
    try:
        if path.exists() and path.is_dir():
            shutil.rmtree(path)
    except Exception:
        pass


def parse_pdf_filename(pdf_path: Path, expected_symbol: str | None = None) -> dict | None:
    m = PDF_NAME_RE.match(pdf_path.name)
    if not m:
        return None
    symbol = m.group("symbol").strip()
    date_part = m.group("date").strip()
    time_part = m.group("time").strip()
    if expected_symbol is not None and symbol != expected_symbol:
        return None
    dt_str = f"{date_part} {time_part.replace('-', ':')}"
    try:
        datetime.strptime(dt_str, DT_FMT)
    except ValueError:
        return None
    return {"symbol": symbol, "datetime": dt_str, "source": "", "title": pdf_path.stem, "url": "", "pdf_path": str(pdf_path)}


def scan_pdf_rows_from_dir(symbol: str, folder: Path) -> list[dict]:
    rows = []
    if not folder.exists():
        return rows
    for pdf in sorted(folder.glob("*.pdf")):
        row = parse_pdf_filename(pdf, expected_symbol=symbol)
        if row is None:
            print(f"[WARN] {symbol}: PDF ignoré -> {pdf.name}", flush=True)
            continue
        rows.append(row)
    return rows


def normalize_and_dedup(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return ensure_expected_columns(df)
    df = ensure_expected_columns(df).copy()
    for col in EXPECTED_COLS:
        df[col] = df[col].fillna("").astype(str).str.strip()
    df["_dt"] = parse_dt(df["datetime"])
    df["_pdf_name"] = df["pdf_path"].apply(lambda x: Path(str(x)).name if str(x).strip() else "")
    df["_key"] = ""
    mask_url = df["url"].ne("")
    df.loc[mask_url, "_key"] = "URL||" + df.loc[mask_url, "url"] + "||" + df.loc[mask_url, "datetime"]
    mask_no_url = df["url"].eq("") & df["title"].ne("")
    df.loc[mask_no_url, "_key"] = "TITLE||" + df.loc[mask_no_url, "title"] + "||" + df.loc[mask_no_url, "datetime"]
    mask_fallback = df["url"].eq("") & df["title"].eq("")
    df.loc[mask_fallback, "_key"] = "PDF||" + df.loc[mask_fallback, "_pdf_name"] + "||" + df.loc[mask_fallback, "datetime"]
    df = df[df["_dt"].notna()].copy()
    df = df.sort_values(["_dt", "_pdf_name"], ascending=[True, True])
    df = df.drop_duplicates(subset=["_key"], keep="first")
    return df[EXPECTED_COLS]


def build_manifest_from_existing_pdfs(symbol: str, folder: Path) -> pd.DataFrame:
    rows = scan_pdf_rows_from_dir(symbol, folder)
    df = pd.DataFrame(rows) if rows else empty_manifest_df()
    return normalize_and_dedup(ensure_expected_columns(df))


def backup_manifest_keep_only_latest(symbol: str, base_manifest: Path) -> None:
    if not base_manifest.exists():
        return
    base_sym_dir = base_manifest.parent
    for old_backup in base_sym_dir.glob(f"{symbol}_manifest_backup*.csv"):
        try:
            old_backup.unlink(missing_ok=True)
        except Exception:
            pass
    backup = base_sym_dir / f"{symbol}_manifest_backup.csv"
    shutil.copy2(str(base_manifest), str(backup))
    print(f"[BACKUP] {symbol}: {backup.name}", flush=True)


def ensure_base_ready(symbol: str, base_sym_dir: Path, base_manifest: Path) -> pd.DataFrame:
    if not base_sym_dir.exists():
        base_sym_dir.mkdir(parents=True, exist_ok=True)
        print(f"[INIT_SYMBOL] {symbol}: création dossier base", flush=True)
    if base_manifest.exists():
        return read_manifest(base_manifest)
    existing_pdfs = list(base_sym_dir.glob("*.pdf"))
    if existing_pdfs:
        df_base = build_manifest_from_existing_pdfs(symbol, base_sym_dir)
        df_base.to_csv(base_manifest, sep=SEP, index=False, encoding=ENCODING)
        print(f"[REBUILD_MANIFEST] {symbol}: {len(df_base)} PDF détectés", flush=True)
        return df_base
    df_base = empty_manifest_df()
    df_base.to_csv(base_manifest, sep=SEP, index=False, encoding=ENCODING)
    print(f"[INIT_MANIFEST] {symbol}: dossier vide -> manifest initialisé", flush=True)
    return df_base


def rebuild_update_manifest_if_missing(symbol: str, update_sym_dir: Path, stamp: str) -> Path | None:
    upd_manifests = sorted(update_sym_dir.glob(f"{symbol}_manifest_update_{stamp}*.csv"))
    if not upd_manifests:
        upd_manifests = sorted(update_sym_dir.glob(f"{symbol}_manifest_update_*.csv"))
    if upd_manifests:
        return upd_manifests[0]
    pdf_rows = scan_pdf_rows_from_dir(symbol, update_sym_dir)
    if not pdf_rows:
        return None
    df = normalize_and_dedup(ensure_expected_columns(pd.DataFrame(pdf_rows)))
    update_manifest = update_sym_dir / f"{symbol}_manifest_update_{stamp}.csv"
    df.to_csv(update_manifest, sep=SEP, index=False, encoding=ENCODING)
    print(f"[REBUILD_UPDATE_MANIFEST] {symbol}: {len(df)} PDF -> {update_manifest.name}", flush=True)
    return update_manifest


def resolve_update_pdf_source(row: dict, update_sym_dir: Path) -> Path | None:
    raw = str(row.get("pdf_path", "")).strip()
    if not raw:
        return None
    src = Path(raw)
    if src.is_absolute() and src.exists():
        return src
    guess = update_sym_dir / Path(raw).name
    return guess if guess.exists() else None


# =====================================================
# MERGE ONE SYMBOL
# =====================================================

def merge_symbol(symbol: str, update_sym_dir: Path, stamp: str) -> tuple[int, bool]:
    base_sym_dir = BASE_ROOT_DIR / symbol
    base_manifest = base_sym_dir / f"{symbol}_manifest.csv"
    df_base = ensure_base_ready(symbol, base_sym_dir, base_manifest)
    update_manifest = rebuild_update_manifest_if_missing(symbol, update_sym_dir, stamp)
    if update_manifest is None or not update_manifest.exists():
        print(f"[INFO] {symbol}: aucun manifest update -> dossier conservé", flush=True)
        return 0, False
    df_upd = read_manifest(update_manifest)
    if df_upd.empty:
        print(f"[INFO] {symbol}: manifest update vide -> dossier conservé", flush=True)
        return 0, False
    backup_manifest_keep_only_latest(symbol, base_manifest)
    moved_rows = []
    moved_count = 0
    missing_count = 0
    for _, row in df_upd.iterrows():
        row_dict = dict(row)
        src = resolve_update_pdf_source(row_dict, update_sym_dir)
        if src is None or not src.exists():
            print(f"[WARN] {symbol}: PDF manquant -> skip ({row_dict.get('pdf_path', '')})", flush=True)
            missing_count += 1
            continue
        parsed = parse_pdf_filename(src, expected_symbol=symbol)
        if parsed is None:
            print(f"[WARN] {symbol}: PDF ignoré (nom invalide) -> {src.name}", flush=True)
            missing_count += 1
            continue
        dst = safe_move_pdf(src, base_sym_dir)
        row_dict["symbol"] = symbol
        if not str(row_dict.get("datetime", "")).strip():
            row_dict["datetime"] = parsed["datetime"]
        if not str(row_dict.get("title", "")).strip():
            row_dict["title"] = parsed["title"]
        row_dict["pdf_path"] = str(dst)
        moved_rows.append(row_dict)
        moved_count += 1
    if moved_count == 0:
        print(f"[INFO] {symbol}: aucun PDF déplacé -> dossier conservé", flush=True)
        return 0, False
    df_upd2 = ensure_expected_columns(pd.DataFrame(moved_rows))
    before = len(df_base) + len(df_upd2)
    df_final = normalize_and_dedup(pd.concat([df_base, df_upd2], ignore_index=True))
    after = len(df_final)
    df_final.to_csv(base_manifest, sep=SEP, index=False, encoding=ENCODING)
    status = "MERGE_OK" if missing_count == 0 else "MERGE_PARTIAL"
    print(f"[{status}] {symbol}: {moved_count} PDF | dedup {before}->{after}", flush=True)
    return moved_count, missing_count == 0


# =====================================================
# MAIN
# =====================================================

def main():
    if not BASE_ROOT_DIR.exists():
        raise SystemExit(f"[ERROR] BASE_ROOT_DIR introuvable: {BASE_ROOT_DIR}")

    stamp = datetime.now().strftime("%Y-%m-%d")
    update_root = find_update_root_for_today() or find_latest_update_root()

    if update_root is None or not update_root.exists():
        print("[STOP] Aucun dossier News_update_* trouvé -> rien à merger", flush=True)
        return {"total_moved": 0, "symbols_merged": 0}

    print(f"[BASE] {BASE_ROOT_DIR}", flush=True)
    print(f"[UPDATE_ROOT] {update_root}", flush=True)

    update_symbols = sorted([p.name for p in update_root.iterdir() if p.is_dir()])
    if not update_symbols:
        print("[INFO] Dossier update vide -> suppression", flush=True)
        try:
            shutil.rmtree(update_root)
        except Exception:
            pass
        return {"total_moved": 0, "symbols_merged": 0}

    total_moved = 0
    symbols_merged_ok = 0
    symbols_kept = []

    for symbol in update_symbols:
        update_sym_dir = update_root / symbol
        try:
            moved_count, merge_success = merge_symbol(symbol, update_sym_dir, stamp)
            total_moved += moved_count
            if merge_success:
                symbols_merged_ok += 1
                remove_dir_if_exists(update_sym_dir)
                print(f"[CLEAN_OK] {symbol}: sous-dossier update supprimé", flush=True)
            else:
                symbols_kept.append(symbol)
        except Exception as e:
            symbols_kept.append(symbol)
            print(f"[ERROR] {symbol} -> {e}", flush=True)
            print(traceback.format_exc(), flush=True)

    try:
        remaining = list(update_root.iterdir())
        if not remaining:
            shutil.rmtree(update_root)
            print(f"[CLEAN] Dossier update supprimé: {update_root}", flush=True)
        else:
            print(f"[KEEP_UPDATE_ROOT] {len(remaining)} élément(s) restant(s)", flush=True)
    except Exception as e:
        print(f"[WARN] Nettoyage final impossible -> {e}", flush=True)

    print("\n=== FIN MERGE ===", flush=True)
    print(f"Symbols dans update: {len(update_symbols)}", flush=True)
    print(f"Symbols mergés OK: {symbols_merged_ok}", flush=True)
    print(f"PDF déplacés: {total_moved}", flush=True)

    return {"total_moved": total_moved, "symbols_merged": symbols_merged_ok, "symbols_kept": symbols_kept}


if __name__ == "__main__":
    main()
