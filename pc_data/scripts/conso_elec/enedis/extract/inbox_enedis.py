# -*- coding: utf-8 -*-
"""
inbox_enedis.py
===============
Branche MANUELLE du pipeline Enedis : dépôt de fichiers XLSX (ou CSV) exportés
depuis l'espace client particulier Enedis (`mon-compte-particulier.enedis.fr`).

Philosophie
-----------
Tant que l'app `data_oz_perso` reste en bac à sable (pas d'accès API prod
pour les particuliers), l'utilisateur télécharge manuellement ses fichiers et
les dépose dans `inbox_enedis/`. Cette tâche les détecte, les parse,
upsert dans Postgres, puis les archive.

Formats d'entrée supportés
------------------------------------------------------------------
  · XLSX Enedis (export espace client)
      Structure : bloc de métadonnées (~15 lignes) suivi d'un tableau
      avec les colonnes : Début | Fin | Valeur (en kW)
      Le PRM est extrait automatiquement des métadonnées du fichier.
      → `enedis.f_conso_30min` (kW × 0.5h × 1000 = Wh)

  · CSV 30 MIN  : "Date;Time;Conso (W)"
      → `enedis.f_conso_30min` (W moyen × 0.5 = Wh)

  · CSV JOUR    : "Date;<quelque chose avec Wh ou kWh>"
      → `enedis.f_conso_jour`

  · CSV PMAX JOUR : "Date;<quelque chose avec VA ou Puissance>"
      → `enedis.f_pmax_jour`

Les fichiers reconnus et traités sont déplacés dans `archive/`
avec un suffixe `_<YYYYMMDD_HHMMSS>` pour préserver l'historique.
Les fichiers inconnus restent dans l'inbox et sont listés dans le
rapport de retour (`fichiers_rejetes`).

IDEMPOTENT : relançable sans risque — upserts `ON CONFLICT DO UPDATE`.
"""
from __future__ import annotations

import csv
import logging
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
except ImportError:                                                     # pragma: no cover
    from backports.zoneinfo import ZoneInfo                             # type: ignore

# Import des fonctions d'upsert (module frère)
_ENEDIS_ROOT = Path(__file__).resolve().parents[1]
if str(_ENEDIS_ROOT) not in sys.path:
    sys.path.insert(0, str(_ENEDIS_ROOT))

from load.load_enedis import (                                          # noqa: E402
    upsert_conso_30min,
    upsert_conso_jour,
    upsert_pmax_jour,
    upsert_prm,
)

log = logging.getLogger(__name__)

TZ_PARIS   = ZoneInfo("Europe/Paris")
TZ_UTC     = timezone.utc
BATCH_SIZE = 1000

# Chemin de la table de référence DST (dans le container Airflow)
DST_TABLE_PATH = Path(
    "/opt/airflow/data/curated/calendaire/chgt_heure/table_chgt_heure.csv"
)


def _load_spring_forward_dates(path: Path) -> set[str]:
    """
    Charge les dates de passage à l'heure d'été (spring-forward) depuis
    `table_chgt_heure.csv`.

    Retourne un ensemble de dates ISO (str) : {"2023-03-26", "2024-03-31", ...}
    """
    spring_dates: set[str] = set()
    if not path.exists():
        log.warning("[DST] Table de référence introuvable : %s", path)
        return spring_dates
    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            type_chgt = (row.get("ete/hivers") or "").strip().lower()
            d = (row.get("Date") or "").strip()
            if type_chgt == "ete" and d:
                spring_dates.add(d)
    log.info("[DST] Table référence chargée : %d dates spring-forward", len(spring_dates))
    return spring_dates

# Extensions de fichiers acceptées dans l'inbox
_INBOX_EXTENSIONS = {".xlsx", ".csv"}


# =============================================================================
# PARSEUR XLSX — Export espace client Enedis
# =============================================================================

def _parse_xlsx_enedis(xlsx_path: Path, fallback_prm: str) -> tuple[str, list[tuple]]:
    """
    Parse un export XLSX de l'espace client Enedis (courbe de charge 30 min).

    Structure du fichier :
      · Lignes 0-13 : métadonnées (vides ou libellés)
          - Ligne PRM : col contenant "Point Référence Mesure" puis numéro PRM
      · Ligne N   : en-tête données → "Début" | "Fin" | "Valeur (en kW)"
      · Ligne N+1 : première ligne de données

    Valeur : puissance MOYENNE en kW sur la tranche de 30 min.
    Wh = kW × 0.5h × 1000

    Retourne : (prm_détecté, [(prm, ts_debut, wh, source_file), ...])
    """
    try:
        import openpyxl
    except ImportError:
        raise ImportError(
            "openpyxl est requis pour lire les fichiers XLSX Enedis. "
            "Installez-le avec : pip install openpyxl"
        )

    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb.active
    all_rows = list(ws.iter_rows(values_only=True))
    wb.close()

    # -- 1. Extraction du PRM depuis les métadonnées -------------------------
    prm_detected = fallback_prm
    for row in all_rows[:25]:
        for i, cell in enumerate(row):
            if cell and "f" in str(cell).lower() and "r" in str(cell).lower() and "rence" in str(cell).lower():
                # Cherche la valeur PRM dans les colonnes suivantes
                for j in range(i + 1, len(row)):
                    candidate = str(row[j]).strip() if row[j] else ""
                    if candidate.isdigit() and len(candidate) >= 10:
                        prm_detected = candidate
                        break
            if prm_detected != fallback_prm:
                break
        if prm_detected != fallback_prm:
            break

    # Fallback : cherche directement une valeur numérique de 14 chiffres
    if prm_detected == fallback_prm:
        for row in all_rows[:20]:
            for cell in row:
                s = str(cell).strip() if cell else ""
                if s.isdigit() and len(s) == 14:
                    prm_detected = s
                    break
            if prm_detected != fallback_prm:
                break

    # -- 2. Localisation de la ligne d'en-tête des données ------------------
    data_start_idx = None
    for idx, row in enumerate(all_rows):
        values_lower = [str(c).strip().lower() if c else "" for c in row]
        if "début" in values_lower or "debut" in values_lower:
            data_start_idx = idx + 1
            break

    if data_start_idx is None:
        log.warning("[inbox xlsx] %s — en-tête 'Début/Fin/Valeur' introuvable",
                    xlsx_path.name)
        return prm_detected, []

    # -- 3. Parse des lignes de données -------------------------------------
    source_file = xlsx_path.name
    rows_raw: list[tuple] = []
    n_na = 0

    for row in all_rows[data_start_idx:]:
        # Format standard export espace client : [None, None, Début, Fin, Valeur(kW), None]
        debut_raw  = None
        valeur_raw = None

        if len(row) >= 5 and row[2] is not None and row[4] is not None:
            debut_raw  = row[2]
            valeur_raw = row[4]
        elif len(row) >= 3 and row[0] is not None and row[2] is not None:
            debut_raw  = row[0]
            valeur_raw = row[2]
        else:
            continue

        if debut_raw is None or valeur_raw is None:
            continue

        # Parse du timestamp de début
        ts_debut_paris = None
        if isinstance(debut_raw, datetime):
            ts_debut_paris = debut_raw.replace(tzinfo=TZ_PARIS)
        else:
            debut_str = str(debut_raw).strip()
            for fmt in ("%d/%m/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S",
                        "%d/%m/%Y %H:%M",    "%Y-%m-%d %H:%M"):
                try:
                    ts_debut_paris = datetime.strptime(debut_str, fmt).replace(tzinfo=TZ_PARIS)
                    break
                except ValueError:
                    continue

        if ts_debut_paris is None:
            continue

        # Normalisation UTC immédiate — corrige le bug de comparaison ZoneInfo :
        # deux datetime Europe/Paris avec le même objet tzinfo sont comparés de
        # façon naïve (sans appliquer l'offset), rendant les doublons DST
        # invisibles aux dicts Python alors que PostgreSQL les rejette.
        ts_debut = ts_debut_paris.astimezone(TZ_UTC)

        # Parse de la valeur en kW — "NA" ou chaîne non numérique = tranche manquante (gap DST)
        valeur_str = str(valeur_raw).strip() if not isinstance(valeur_raw, (int, float)) else None
        if valeur_str is not None and valeur_str.upper() in ("NA", "N/A", "", "-"):
            n_na += 1
            log.debug("[inbox xlsx] tranche NA ignorée : ts_debut=%s", ts_debut.isoformat())
            continue
        try:
            if isinstance(valeur_raw, (int, float)):
                kw = float(valeur_raw)
            else:
                kw = float(valeur_str.replace(",", "."))
        except (ValueError, TypeError):
            n_na += 1
            continue

        # kW × 0.5h × 1000 = Wh
        wh = max(0, int(round(kw * 500)))
        rows_raw.append((prm_detected, ts_debut, wh, source_file))

    if n_na:
        log.info("[inbox xlsx] %s — %d tranche(s) NA ignorée(s) (gap DST spring-forward)",
                 xlsx_path.name, n_na)

    # -- 4. Déduplication globale par (prm, ts_debut_utc) --------------------
    # Les jours de fall-back DST (heure d'hiver) produisent deux tranches locales
    # distinctes (01:30 CEST et 01:30 CET) qui mappent au même instant UTC.
    # PostgreSQL les rejette (CardinalityViolation) ; on garde la première occurrence.
    seen: dict[tuple, tuple] = {}
    dup_details: list[tuple[str, str]] = []

    for r in rows_raw:
        key = (r[0], r[1])   # (prm, ts_debut_utc)
        if key in seen:
            dup_details.append((str(r[3]), r[1].isoformat()))
        else:
            seen[key] = r

    if dup_details:
        log.warning("[inbox xlsx] %s — %d doublon(s) DST éliminé(s) :",
                    xlsx_path.name, len(dup_details))
        for src, ts_str in dup_details:
            log.warning("    ts_utc=%s", ts_str)

    rows = list(seen.values())
    return prm_detected, rows


# =============================================================================
# Détection du format d'un CSV Enedis d'après son en-tête
# =============================================================================

def _read_header(csv_path: Path) -> list[str]:
    """Lit la première ligne non-vide, nettoie BOM et espaces."""
    with csv_path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f, delimiter=";")
        for row in reader:
            if row and any(c.strip() for c in row):
                return [c.strip() for c in row]
    return []


def _detect_format(csv_path: Path) -> str:
    """
    Renvoie :
      · '30min'       : courbe de charge (Date;Time;...)
      · 'daily_conso' : conso quotidienne (Date;<Wh|kWh|Conso>)
      · 'daily_pmax'  : puissance max   (Date;<VA|Puissance>)
      · 'unknown'     : en-tête non reconnu
    """
    header = _read_header(csv_path)
    if not header:
        return "unknown"

    lower  = [h.lower() for h in header]
    joined = " | ".join(lower)

    if any("time" in h for h in lower):
        return "30min"

    if len(header) >= 2:
        if ("va" in joined) or ("puissance" in joined):
            return "daily_pmax"
        if ("wh" in joined) or ("conso" in joined) or ("consommation" in joined):
            return "daily_conso"

    return "unknown"


# =============================================================================
# Parseurs CSV par format
# =============================================================================

def _parse_30min(csv_path: Path, prm: str, source_file: str) -> list[tuple]:
    """
    "Date;Time;Conso (W)"  ->  (prm, ts_debut, wh, source_file)

    Convention Enedis : Time = FIN de la tranche de 30 min.
    "23:59:59" = artefact CSV pour 24:00:00 -> ts_debut = 23:30:00 du jour.
    Unité : puissance MOYENNE en W -> Wh = W × 0.5.
    """
    rows: list[tuple] = []
    with csv_path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        for r in reader:
            date_str = (r.get("Date") or "").strip()
            time_str = (r.get("Time") or "").strip()
            val_str = (
                r.get("Conso (W)")
                or r.get("Puissance (W)")
                or next(
                    (v for k, v in r.items()
                     if k and k.lower() not in ("date", "time") and v is not None),
                    "",
                )
                or ""
            ).strip()

            if not date_str or not time_str or not val_str:
                continue

            if time_str == "23:59:59":
                ts_end_naive = datetime.fromisoformat(date_str) + timedelta(days=1)
            else:
                try:
                    ts_end_naive = datetime.fromisoformat(f"{date_str}T{time_str}")
                except ValueError:
                    continue

            # Normalisation UTC immédiate (même fix que _parse_xlsx_enedis)
            ts_debut = (ts_end_naive - timedelta(minutes=30)).replace(tzinfo=TZ_PARIS).astimezone(TZ_UTC)

            try:
                watts = float(val_str.replace(",", "."))
            except ValueError:
                continue

            wh = max(0, int(round(watts * 0.5)))
            rows.append((prm, ts_debut, wh, source_file))
    return rows


def _parse_daily_conso(csv_path: Path, prm: str, source_file: str) -> list[tuple]:
    """
    "Date;<Conso en Wh ou kWh>"  ->  (prm, jour, wh, source_file)
    """
    header = _read_header(csv_path)
    if len(header) < 2:
        return []
    col_date, col_val = header[0], header[1]
    is_kwh     = "kwh" in col_val.lower()
    multiplier = 1000 if is_kwh else 1

    rows: list[tuple] = []
    with csv_path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        for r in reader:
            date_str = (r.get(col_date) or "").strip()
            val_str  = (r.get(col_val)  or "").strip()
            if not date_str or not val_str:
                continue
            try:
                jour = _parse_date(date_str)
                val  = float(val_str.replace(",", "."))
            except (ValueError, TypeError):
                continue
            wh = max(0, int(round(val * multiplier)))
            rows.append((prm, jour, wh, source_file))
    return rows


def _parse_daily_pmax(csv_path: Path, prm: str, source_file: str) -> list[tuple]:
    """
    "Date;<Puissance en VA ou kVA>"  ->  (prm, jour, pmax_va, ts_pmax, source_file)
    """
    header = _read_header(csv_path)
    if len(header) < 2:
        return []

    col_date   = header[0]
    col_val    = header[1]
    has_time   = len(header) >= 3 and "time" in header[2].lower()
    col_time   = header[2] if has_time else None
    is_kva     = "kva" in col_val.lower()
    multiplier = 1000 if is_kva else 1

    rows: list[tuple] = []
    with csv_path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        for r in reader:
            date_str = (r.get(col_date) or "").strip()
            val_str  = (r.get(col_val)  or "").strip()
            if not date_str or not val_str:
                continue
            try:
                jour = _parse_date(date_str)
                val  = float(val_str.replace(",", "."))
            except (ValueError, TypeError):
                continue

            ts_pmax = None
            if has_time:
                time_str = (r.get(col_time) or "").strip()
                if time_str and time_str != "23:59:59":
                    try:
                        ts_pmax = datetime.strptime(
                            f"{jour.isoformat()} {time_str}",
                            "%Y-%m-%d %H:%M:%S",
                        ).replace(tzinfo=TZ_PARIS)
                    except ValueError:
                        ts_pmax = None

            pmax_va = max(0, int(round(val * multiplier)))
            rows.append((prm, jour, pmax_va, ts_pmax, source_file))
    return rows


def _parse_date(s: str):
    """Tolère YYYY-MM-DD, DD/MM/YYYY, DD-MM-YYYY."""
    from datetime import date
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return date.fromisoformat(s[:10])


# =============================================================================
# Orchestrateur : scan de l'inbox + import + archive
# =============================================================================

def _upsert_rows_batched(fmt: str, rows: list[tuple]) -> int:
    """Upsert en batch selon le format détecté."""
    if not rows:
        return 0
    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        chunk = rows[i : i + BATCH_SIZE]
        if fmt == "30min":
            total += upsert_conso_30min(chunk)
        elif fmt == "daily_conso":
            total += upsert_conso_jour(chunk)
        elif fmt == "daily_pmax":
            total += upsert_pmax_jour(chunk)
    return total


def _archive(file_path: Path, archive_dir: Path) -> Path:
    """Déplace le fichier traité dans archive/ avec un suffixe horodaté."""
    archive_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest  = archive_dir / f"{file_path.stem}__{stamp}{file_path.suffix}"
    shutil.move(str(file_path), str(dest))
    return dest


def process_inbox(
    inbox_dir:   str | Path,
    archive_dir: str | Path,
    prm:         str,
) -> dict:
    """
    Scanne `inbox_dir`, importe chaque fichier Enedis reconnu, archive le fichier.

    Fichiers supportés : *.xlsx (export espace client) et *.csv (anciens exports).

    Pour les XLSX, le PRM est extrait automatiquement des métadonnées du fichier
    (le paramètre `prm` sert de fallback uniquement).

    Retour :
        {
          "status":  "ok" | "no_files",
          "prm":     "...",
          "fichiers_trouves":  int,
          "fichiers_traites":  int,
          "fichiers_rejetes":  [(nom, raison), ...],
          "par_format": {
              "30min":       {"fichiers": N, "lignes": L, "upserts": U},
              "daily_conso": {"fichiers": N, "lignes": L, "upserts": U},
              "daily_pmax":  {"fichiers": N, "lignes": L, "upserts": U},
          },
          "details": [ {"fichier": ..., "format": ..., "prm": ...,
                        "lignes": ..., "upserts": ...}, ... ]
        }
    """
    inbox   = Path(inbox_dir)
    archive = Path(archive_dir)
    inbox.mkdir(parents=True, exist_ok=True)

    # Scan : CSV et XLSX — on exclut le sous-dossier archive/
    files = sorted(
        p for p in inbox.iterdir()
        if p.suffix.lower() in _INBOX_EXTENSIONS
        and p.is_file()
        and p.parent != archive
    )

    if not files:
        return {
            "status":           "no_files",
            "prm":              prm,
            "message":          f"Aucun fichier CSV/XLSX dans {inbox}",
            "fichiers_trouves": 0,
            "fichiers_traites": 0,
            "fichiers_rejetes": [],
            "par_format":       {},
            "details":          [],
        }

    # PRM de fallback présent dans dim_prm (FK)
    upsert_prm(prm, libelle="Import inbox")

    par_format: dict[str, dict] = {
        "30min":       {"fichiers": 0, "lignes": 0, "upserts": 0},
        "daily_conso": {"fichiers": 0, "lignes": 0, "upserts": 0},
        "daily_pmax":  {"fichiers": 0, "lignes": 0, "upserts": 0},
    }
    rejetes:  list[tuple[str, str]] = []
    details:  list[dict]            = []
    n_traites = 0

    for file_path in files:
        source_file = file_path.name
        try:
            # -- Branche XLSX (export espace client Enedis) ------------------
            if file_path.suffix.lower() == ".xlsx":
                prm_fichier, rows = _parse_xlsx_enedis(file_path, fallback_prm=prm)
                fmt = "30min"

                if not rows:
                    rejetes.append((file_path.name, "XLSX : aucune ligne parsable"))
                    log.warning("[inbox] ✗ %s — XLSX sans données", file_path.name)
                    continue

                # S'assurer que le PRM extrait est dans dim_prm
                if prm_fichier != prm:
                    upsert_prm(prm_fichier, libelle="Import inbox XLSX (PRM extrait)")
                    log.info("[inbox] PRM extrait du XLSX : %s", prm_fichier)

                n_up = _upsert_rows_batched(fmt, rows)

            # -- Branche CSV (anciens exports ou format simple) ---------------
            else:
                prm_fichier = prm
                fmt = _detect_format(file_path)
                if fmt == "unknown":
                    rejetes.append((file_path.name,
                                    "Format CSV non reconnu (en-tête inconnu)"))
                    log.warning("[inbox] ✗ %s — format CSV non reconnu", file_path.name)
                    continue

                if fmt == "30min":
                    rows = _parse_30min(file_path, prm_fichier, source_file)
                elif fmt == "daily_conso":
                    rows = _parse_daily_conso(file_path, prm_fichier, source_file)
                elif fmt == "daily_pmax":
                    rows = _parse_daily_pmax(file_path, prm_fichier, source_file)
                else:
                    rows = []

                if not rows:
                    rejetes.append((file_path.name,
                                    f"Aucune ligne parsable ({fmt})"))
                    log.warning("[inbox] ✗ %s — aucune ligne parsable", file_path.name)
                    continue

                n_up = _upsert_rows_batched(fmt, rows)

            # -- Archive + compteurs (commun CSV et XLSX) ---------------------
            par_format[fmt]["fichiers"] += 1
            par_format[fmt]["lignes"]   += len(rows)
            par_format[fmt]["upserts"]  += n_up

            archived  = _archive(file_path, archive)
            n_traites += 1

            details.append({
                "fichier":  file_path.name,
                "format":   fmt,
                "prm":      prm_fichier,
                "lignes":   len(rows),
                "upserts":  n_up,
                "archive":  archived.name,
            })
            log.info("[inbox] ✓ %s  [%s]  PRM=%s  %d lignes → %d upserts",
                     file_path.name, fmt, prm_fichier, len(rows), n_up)

        except Exception as e:
            rejetes.append((file_path.name, f"Erreur: {e}"))
            log.exception("[inbox] ✗ %s — erreur : %s", file_path.name, e)

    # Nettoyage des compteurs à zéro
    par_format = {k: v for k, v in par_format.items() if v["fichiers"] > 0}

    return {
        "status":           "ok",
        "prm":              prm,
        "fichiers_trouves": len(files),
        "fichiers_traites": n_traites,
        "fichiers_rejetes": rejetes,
        "par_format":       par_format,
        "details":          details,
    }
