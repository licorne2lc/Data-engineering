# -*- coding: utf-8 -*-
"""
load_bresser.py
================
Processus LOAD — fusion sécurisée des données météo Bresser MeteoChamp HD
dans la base de données originale common_weather_database.csv.

═══════════════════════════════════════════════════════════════════════════════
FLUX
═══════════════════════════════════════════════════════════════════════════════

  bresser_usb_YYYY-MM-DD.csv  (source=usb,  qualite=exacte)
  bresser_wc_YYYY-MM-DD.csv   (source=wc,   qualite=approx_30min)
        ──► [1] Contrôle cohérence USB ↔ WC (temp/humidity)
        ──► [2] Backup horodaté de la base originale
        ──► [3] Déduplication (créneaux déjà présents)
        ──► [4] Fusion USB prioritaire sur créneaux communs
        ──► [5] Validation de la fusion (lignes, période, colonnes)
        ──► [6] Écriture atomique (temp → rename)
        ──► [7] Rotation des backups (30 derniers conservés)
        ──► [8] Audit log

═══════════════════════════════════════════════════════════════════════════════
CONTRÔLE DE COHÉRENCE USB ↔ WC
═══════════════════════════════════════════════════════════════════════════════

  Sur les créneaux communs (Date + Time) entre USB et WC :
    ✔ Température  : écart accepté ≤ 1.0°C
    ✔ Humidité     : écart accepté ≤ 15 %

  Si le taux de lignes incohérentes dépasse MAX_TAUX_INCOHERENCE (défaut 20 %),
  le load est BLOQUÉ — cela indique probablement un décalage de format de date
  ou d'heure entre les deux sources.

Importé depuis le DAG :
    from load_bresser import run_load
    result = run_load(usb_file, wc_file, database_file, output_dir, run_date)

Standalone :
    python load_bresser.py
"""

import logging
import os
import shutil
import uuid
from datetime import date, datetime
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# PARAMÈTRES DE CONTRÔLE
# ─────────────────────────────────────────────────────────────────────────────

# Seuils de cohérence USB ↔ WC sur les créneaux communs
SEUIL_TEMP_C    = 1.0    # °C  — écart max température accepté
SEUIL_HUMID_PCT = 15.0   # %   — écart max humidité accepté

# Si ce pourcentage de lignes communes dépasse les seuils → BLOCAGE du load
MAX_TAUX_INCOHERENCE = 0.20   # 20 %

# Nombre minimum de créneaux communs pour activer le contrôle
MIN_LIGNES_COMMUNES = 5

# Colonnes vérifiées lors du contrôle de cohérence
COLS_TEMP  = ["IN Temperature", "Out Temperature"]
COLS_HUMID = ["IN Humidity",    "Out Humidity"]

# Nombre de backups à conserver
MAX_BACKUPS = 10


# ─────────────────────────────────────────────────────────────────────────────
# SÉCURITÉ 1 — Contrôle de cohérence USB ↔ WC
# ─────────────────────────────────────────────────────────────────────────────

def _controle_coherence(df_usb: pd.DataFrame, df_wc: pd.DataFrame) -> dict:
    """
    Vérifie la cohérence des mesures entre USB et WC sur les créneaux communs.

    Retourne un dict avec :
      - ok         : True si le contrôle est passé, False si bloquant
      - lignes_communes
      - incoherences_temp   : nombre de lignes avec écart temp > SEUIL_TEMP_C
      - incoherences_humid  : nombre de lignes avec écart humid > SEUIL_HUMID_PCT
      - taux_incoherence    : max(taux_temp, taux_humid)
      - details             : liste des anomalies détectées
    """
    result = {
        "ok":                  True,
        "lignes_communes":     0,
        "incoherences_temp":   0,
        "incoherences_humid":  0,
        "taux_incoherence":    0.0,
        "details":             [],
    }

    # Fusion sur Date + Time
    merged = pd.merge(
        df_usb[["Date", "Time"] + COLS_TEMP + COLS_HUMID],
        df_wc [["Date", "Time"] + COLS_TEMP + COLS_HUMID],
        on=["Date", "Time"],
        suffixes=("_usb", "_wc"),
    )
    n = len(merged)
    result["lignes_communes"] = n

    if n < MIN_LIGNES_COMMUNES:
        msg = (f"Contrôle cohérence ignoré : seulement {n} créneau(x) commun(s) "
               f"(minimum requis : {MIN_LIGNES_COMMUNES})")
        log.info("  ℹ  %s", msg)
        result["details"].append(msg)
        return result

    log.info("Contrôle cohérence sur %d créneaux communs USB ↔ WC", n)

    # ── Températures ──────────────────────────────────────────────────────────
    n_inco_temp = 0
    for col in COLS_TEMP:
        c_usb = merged[f"{col}_usb"]
        c_wc  = merged[f"{col}_wc"]
        mask  = c_usb.notna() & c_wc.notna()
        if mask.sum() == 0:
            continue
        diff      = (c_usb[mask] - c_wc[mask]).abs()
        n_hors    = (diff > SEUIL_TEMP_C).sum()
        max_ecart = diff.max()
        if n_hors > 0:
            msg = (f"Température [{col}] : {n_hors}/{mask.sum()} lignes "
                   f"avec écart > {SEUIL_TEMP_C}°C (max={max_ecart:.2f}°C)")
            log.warning("  ⚠  %s", msg)
            result["details"].append(msg)
        n_inco_temp = max(n_inco_temp, n_hors)
    result["incoherences_temp"] = n_inco_temp

    # ── Humidité ──────────────────────────────────────────────────────────────
    n_inco_humid = 0
    for col in COLS_HUMID:
        c_usb = merged[f"{col}_usb"]
        c_wc  = merged[f"{col}_wc"]
        mask  = c_usb.notna() & c_wc.notna()
        if mask.sum() == 0:
            continue
        diff      = (c_usb[mask] - c_wc[mask]).abs()
        n_hors    = (diff > SEUIL_HUMID_PCT).sum()
        max_ecart = diff.max()
        if n_hors > 0:
            msg = (f"Humidité [{col}] : {n_hors}/{mask.sum()} lignes "
                   f"avec écart > {SEUIL_HUMID_PCT}% (max={max_ecart:.1f}%)")
            log.warning("  ⚠  %s", msg)
            result["details"].append(msg)
        n_inco_humid = max(n_inco_humid, n_hors)
    result["incoherences_humid"] = n_inco_humid

    # ── Taux global ───────────────────────────────────────────────────────────
    taux_temp  = n_inco_temp  / n
    taux_humid = n_inco_humid / n
    taux       = max(taux_temp, taux_humid)
    result["taux_incoherence"] = round(taux, 4)

    if taux > MAX_TAUX_INCOHERENCE:
        msg = (
            f"BLOCAGE LOAD — taux d'incohérence trop élevé : {taux:.1%} "
            f"(seuil : {MAX_TAUX_INCOHERENCE:.0%}). "
            f"Vérifiez le format de date/heure des deux sources."
        )
        log.error("  ✗  %s", msg)
        result["details"].append(msg)
        result["ok"] = False
    else:
        log.info("  ✔ Cohérence USB ↔ WC OK — taux incohérence : %.1f%%",
                 taux * 100)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# SÉCURITÉ 2 — Backup horodaté
# ─────────────────────────────────────────────────────────────────────────────

def _backup(database_file: Path) -> Path:
    """Copie la base dans backup/ avec horodatage. Retourne le chemin du backup."""
    backup_dir = database_file.parent / "backup"
    backup_dir.mkdir(exist_ok=True)
    ts          = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = backup_dir / f"{database_file.stem}_{ts}{database_file.suffix}"
    shutil.copy2(database_file, backup_file)
    log.info("  ✔ Backup créé : %s", backup_file.name)
    return backup_file


# ─────────────────────────────────────────────────────────────────────────────
# SÉCURITÉ 7 — Rotation des backups
# ─────────────────────────────────────────────────────────────────────────────

def _rotation_backups(database_file: Path) -> int:
    """Supprime les backups les plus anciens au-delà de MAX_BACKUPS."""
    backup_dir = database_file.parent / "backup"
    if not backup_dir.exists():
        return 0
    backups = sorted(backup_dir.glob(f"{database_file.stem}_*.csv"))
    n_suppr = 0
    while len(backups) > MAX_BACKUPS:
        backups[0].unlink()
        log.info("  🗑  Backup supprimé (rotation) : %s", backups[0].name)
        backups.pop(0)
        n_suppr += 1
    return n_suppr


# ─────────────────────────────────────────────────────────────────────────────
# SÉCURITÉ 8 — Audit log
# ─────────────────────────────────────────────────────────────────────────────

def _audit_log(database_file: Path, stats: dict) -> None:
    """Ajoute une ligne dans load_audit.csv."""
    audit_file = database_file.parent / "load_audit.csv"
    ligne = {
        "timestamp":          datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "run_date":           stats.get("run_date"),
        "status":             stats.get("status"),
        "lignes_avant":       stats.get("lignes_avant"),
        "lignes_apres":       stats.get("lignes_apres"),
        "lignes_ajoutees":    stats.get("lignes_ajoutees"),
        "doublons_ignores":   stats.get("doublons_ignores"),
        "lignes_usb":         stats.get("lignes_usb"),
        "lignes_wc":          stats.get("lignes_wc"),
        "lignes_wc_conserv":  stats.get("lignes_wc_conserv"),
        "taux_incoherence":   stats.get("taux_incoherence"),
        "backup":             stats.get("backup"),
        "erreur":             stats.get("erreur", ""),
    }
    df_log = pd.DataFrame([ligne])
    if audit_file.exists():
        df_log.to_csv(audit_file, mode="a", header=False, index=False)
    else:
        df_log.to_csv(audit_file, index=False)
    log.info("  ✔ Audit log mis à jour : %s", audit_file.name)


# ─────────────────────────────────────────────────────────────────────────────
# Fonction principale
# ─────────────────────────────────────────────────────────────────────────────

def run_load(usb_file:      Path | None,
             wc_file:       Path | None,
             database_file: Path,
             run_date:      date = None) -> dict:
    """
    Fusionne les fichiers USB et WC dans la base de données originale.

    Args:
        usb_file      : bresser_usb_YYYY-MM-DD.csv  (None si absent)
        wc_file       : bresser_wc_YYYY-MM-DD.csv   (None si absent)
        database_file : common_weather_database.csv (base originale)
        run_date      : date du jour

    Returns:
        dict : status, lignes_avant, lignes_apres, lignes_ajoutees,
               doublons_ignores, coherence, backup, warnings, ...
    """
    if run_date is None:
        run_date = date.today()

    usb_file      = Path(usb_file)      if usb_file  else None
    wc_file       = Path(wc_file)       if wc_file   else None
    database_file = Path(database_file)

    log.info("=" * 60)
    log.info("LOAD BRESSER — %s", run_date)
    log.info("=" * 60)

    warnings = []
    stats    = {"run_date": str(run_date), "status": "error", "erreur": ""}

    # ── Lecture des fichiers sources ──────────────────────────────────────────
    df_usb = None
    df_wc  = None

    if usb_file and usb_file.exists():
        df_usb = pd.read_csv(usb_file, low_memory=False)
        log.info("USB  : %d lignes (%s)", len(df_usb), usb_file.name)
    else:
        msg = f"Fichier USB absent : {usb_file}"
        log.warning("  ⚠  %s", msg)
        warnings.append(msg)

    if wc_file and wc_file.exists():
        df_wc = pd.read_csv(wc_file, low_memory=False)
        log.info("WC   : %d lignes (%s)", len(df_wc), wc_file.name)
    else:
        msg = f"Fichier WC absent : {wc_file}"
        log.warning("  ⚠  %s", msg)
        warnings.append(msg)

    if df_usb is None and df_wc is None:
        raise RuntimeError("Aucun fichier source disponible (USB ni WC).")

    # ── Lecture de la base originale ──────────────────────────────────────────
    if not database_file.exists():
        raise FileNotFoundError(f"Base de données introuvable : {database_file}")

    df_base = pd.read_csv(database_file, low_memory=False)
    lignes_avant = len(df_base)
    log.info("Base : %d lignes (%s → %s)",
             lignes_avant, df_base["Date"].min(), df_base["Date"].max())
    stats["lignes_avant"] = lignes_avant

    # ═════════════════════════════════════════════════════════════════════════
    # SÉCURITÉ 1 — Contrôle cohérence USB ↔ WC
    # ═════════════════════════════════════════════════════════════════════════
    log.info("── Sécurité 1 : contrôle cohérence USB ↔ WC ──")
    coherence = {"ok": True, "lignes_communes": 0, "taux_incoherence": 0.0,
                 "details": ["Contrôle ignoré : une seule source disponible"]}

    if df_usb is not None and df_wc is not None:
        coherence = _controle_coherence(df_usb, df_wc)
        if not coherence["ok"]:
            stats["coherence"]        = coherence
            stats["taux_incoherence"] = coherence["taux_incoherence"]
            stats["erreur"]           = coherence["details"][-1]
            _audit_log(database_file, stats)
            raise ValueError(
                f"Load bloqué — incohérence USB/WC détectée.\n"
                + "\n".join(coherence["details"])
            )

    stats["taux_incoherence"] = coherence["taux_incoherence"]
    log.info("  ✔ Cohérence OK")

    # ═════════════════════════════════════════════════════════════════════════
    # SÉCURITÉ 2 — Backup horodaté
    # ═════════════════════════════════════════════════════════════════════════
    log.info("── Sécurité 2 : backup ──")
    backup_file = _backup(database_file)
    stats["backup"] = str(backup_file)

    try:
        # ── Fusion USB + WC avec priorité USB ────────────────────────────────
        log.info("── Fusion USB + WC ──")
        lignes_usb    = len(df_usb) if df_usb is not None else 0
        lignes_wc     = len(df_wc)  if df_wc  is not None else 0
        lignes_wc_conserv = 0

        if df_usb is not None and df_wc is not None:
            cles_usb = set(zip(df_usb["Date"], df_usb["Time"]))
            mask_wc_exclusif = ~(
                pd.Series(zip(df_wc["Date"], df_wc["Time"])).isin(cles_usb)
            )
            df_wc_exclusif    = df_wc[mask_wc_exclusif.values].copy()
            lignes_wc_conserv = len(df_wc_exclusif)
            df_nouvelles      = pd.concat([df_usb, df_wc_exclusif], ignore_index=True)
            log.info("  Créneaux WC communs écartés (USB prioritaire) : %d",
                     lignes_wc - lignes_wc_conserv)
            log.info("  Créneaux WC exclusifs conservés : %d", lignes_wc_conserv)
        elif df_usb is not None:
            df_nouvelles = df_usb.copy()
        else:
            df_nouvelles = df_wc.copy()

        log.info("  Nouvelles lignes à intégrer : %d", len(df_nouvelles))

        # ═════════════════════════════════════════════════════════════════════
        # SÉCURITÉ 3 — Contrôle colonnes (schéma)
        # ═════════════════════════════════════════════════════════════════════
        log.info("── Sécurité 3 : contrôle schéma ──")
        cols_base     = set(df_base.columns)
        cols_nouvelles = set(df_nouvelles.columns)
        cols_inconnues = cols_nouvelles - cols_base
        cols_manquantes = cols_base - cols_nouvelles

        if cols_inconnues:
            msg = f"Colonnes inconnues dans les nouvelles données : {sorted(cols_inconnues)}"
            log.warning("  ⚠  %s", msg)
            warnings.append(msg)
            # On ajoute ces colonnes à la base avec NaN
            for col in cols_inconnues:
                df_base[col] = pd.NA

        if cols_manquantes:
            msg = f"Colonnes manquantes dans les nouvelles données : {sorted(cols_manquantes)}"
            log.warning("  ⚠  %s", msg)
            warnings.append(msg)
            for col in cols_manquantes:
                df_nouvelles[col] = pd.NA

        log.info("  ✔ Schéma compatible")

        # ═════════════════════════════════════════════════════════════════════
        # SÉCURITÉ 4 — Déduplication (créneaux déjà dans la base)
        # ═════════════════════════════════════════════════════════════════════
        log.info("── Sécurité 4 : déduplication ──")
        cles_base = set(zip(df_base["Date"], df_base["Time"], df_base["source"]))
        mask_nouveaux = ~(
            pd.Series(zip(df_nouvelles["Date"],
                          df_nouvelles["Time"],
                          df_nouvelles["source"]))
            .isin(cles_base)
        )
        doublons_ignores = (~mask_nouveaux).sum()
        df_a_ajouter     = df_nouvelles[mask_nouveaux.values].copy()

        log.info("  Créneaux déjà présents (ignorés) : %d", doublons_ignores)
        log.info("  Créneaux réellement nouveaux     : %d", len(df_a_ajouter))

        if len(df_a_ajouter) == 0:
            msg = "Aucune nouvelle donnée à intégrer — toutes déjà présentes."
            log.info("  ℹ  %s", msg)
            warnings.append(msg)

        # ═════════════════════════════════════════════════════════════════════
        # Fusion avec la base
        # ═════════════════════════════════════════════════════════════════════
        df_final = pd.concat([df_base, df_a_ajouter], ignore_index=True)
        df_final.sort_values(["Date", "Time"], inplace=True, ignore_index=True)

        lignes_apres   = len(df_final)
        lignes_ajoutees = lignes_apres - lignes_avant

        # ═════════════════════════════════════════════════════════════════════
        # SÉCURITÉ 5 — Validation de la fusion
        # ═════════════════════════════════════════════════════════════════════
        log.info("── Sécurité 5 : validation ──")

        # La base ne peut que croître
        if lignes_apres < lignes_avant:
            raise RuntimeError(
                f"ANOMALIE : la base a perdu des lignes "
                f"({lignes_avant} → {lignes_apres}). Load annulé."
            )

        # La période ne peut pas rétrécir
        date_min_avant = df_base["Date"].min()
        date_max_avant = df_base["Date"].max()
        date_min_apres = df_final["Date"].min()
        date_max_apres = df_final["Date"].max()

        if date_min_apres > date_min_avant:
            raise RuntimeError(
                f"ANOMALIE : la date de début a avancé "
                f"({date_min_avant} → {date_min_apres}). Load annulé."
            )
        if date_max_apres < date_max_avant:
            raise RuntimeError(
                f"ANOMALIE : la date de fin a reculé "
                f"({date_max_avant} → {date_max_apres}). Load annulé."
            )

        log.info("  ✔ Lignes : %d → %d (+%d)", lignes_avant, lignes_apres, lignes_ajoutees)
        log.info("  ✔ Période : %s → %s", date_min_apres, date_max_apres)

        # ═════════════════════════════════════════════════════════════════════
        # SÉCURITÉ 6 — Écriture atomique (tmp → rename)
        # ═════════════════════════════════════════════════════════════════════
        log.info("── Sécurité 6 : écriture atomique ──")
        tmp_file = database_file.with_suffix(f".tmp_{uuid.uuid4().hex[:8]}.csv")
        df_final.to_csv(tmp_file, index=False, encoding="utf-8")

        # Vérification du fichier temporaire avant remplacement
        df_check = pd.read_csv(tmp_file, nrows=5)
        if len(df_check) == 0:
            tmp_file.unlink()
            raise RuntimeError("Fichier temporaire vide après écriture. Load annulé.")

        tmp_file.replace(database_file)
        log.info("  ✔ Base mise à jour : %s", database_file.name)

        # ═════════════════════════════════════════════════════════════════════
        # SÉCURITÉ 7 — Rotation des backups
        # ═════════════════════════════════════════════════════════════════════
        log.info("── Sécurité 7 : rotation backups ──")
        n_suppr = _rotation_backups(database_file)
        log.info("  ✔ %d backup(s) supprimé(s) (limite : %d)", n_suppr, MAX_BACKUPS)

        # ── Résultat ─────────────────────────────────────────────────────────
        stats.update({
            "status":           "ok",
            "lignes_avant":     lignes_avant,
            "lignes_apres":     lignes_apres,
            "lignes_ajoutees":  lignes_ajoutees,
            "doublons_ignores": doublons_ignores,
            "lignes_usb":       lignes_usb,
            "lignes_wc":        lignes_wc,
            "lignes_wc_conserv": lignes_wc_conserv,
            "coherence":        coherence,
            "debut":            date_min_apres,
            "fin":              date_max_apres,
            "taille_ko":        round(database_file.stat().st_size / 1024, 1),
            "warnings":         warnings,
            "erreur":           "",
        })

    except Exception as exc:
        # En cas d'erreur, le backup permet de restaurer manuellement
        stats["erreur"] = str(exc)
        log.error("✗ Load échoué : %s", exc)
        log.error("  → Restaurez depuis le backup : %s", stats.get("backup"))
        _audit_log(database_file, stats)
        raise

    # ═════════════════════════════════════════════════════════════════════════
    # SÉCURITÉ 8 — Audit log
    # ═════════════════════════════════════════════════════════════════════════
    log.info("── Sécurité 8 : audit log ──")
    _audit_log(database_file, stats)

    log.info("✅ LOAD TERMINÉ")
    log.info("   Lignes avant    : %d", lignes_avant)
    log.info("   Lignes ajoutées : %d", lignes_ajoutees)
    log.info("   Lignes après    : %d", lignes_apres)
    log.info("   Période         : %s → %s", stats["debut"], stats["fin"])
    log.info("   Taille          : %.1f Ko", stats["taille_ko"])

    return stats


# ─────────────────────────────────────────────────────────────────────────────
# STANDALONE
# ─────────────────────────────────────────────────────────────────────────────

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

    usb_dir       = Path(os.environ.get("USB_OUTPUT_DIR",
        r"D:\projet_dataoz\pc_data\data\raw\météo_bresser\clé_usb"))
    wc_dir        = Path(os.environ.get("WC_RAW_DIR",
        r"D:\projet_dataoz\pc_data\data\raw\météo_bresser\weathercloud"))
    database_file = Path(os.environ.get("DATABASE_FILE",
        r"D:\projet_dataoz\pc_data\data\curated\météo\bresser\common_weather_database.csv"))

    usb_files = sorted(usb_dir.glob("bresser_usb_*.csv"), reverse=True)
    wc_files  = sorted(wc_dir.glob("bresser_wc_*.csv"),   reverse=True)

    usb_file = usb_files[0] if usb_files else None
    wc_file  = wc_files[0]  if wc_files  else None

    print(f"USB source    : {usb_file.name if usb_file else '—'}")
    print(f"WC source     : {wc_file.name  if wc_file  else '—'}")
    print(f"Base cible    : {database_file.name}")

    result = run_load(usb_file, wc_file, database_file, date.today())

    print(f"\n{'='*58}")
    print("RÉSULTAT LOAD BRESSER")
    print(f"{'='*58}")
    for k, v in result.items():
        if k not in ("warnings", "coherence"):
            print(f"  {k:28s}: {v}")
    if result.get("coherence"):
        c = result["coherence"]
        print(f"\n  Cohérence USB ↔ WC :")
        print(f"    Lignes communes     : {c.get('lignes_communes')}")
        print(f"    Incoh. température  : {c.get('incoherences_temp')}")
        print(f"    Incoh. humidité     : {c.get('incoherences_humid')}")
        print(f"    Taux incohérence    : {c.get('taux_incoherence', 0):.1%}")
    if result.get("warnings"):
        print(f"\n  ⚠  Warnings :")
        for w in result["warnings"]:
            print(f"      - {w}")


if __name__ == "__main__":
    main()
