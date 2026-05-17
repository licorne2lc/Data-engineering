# -*- coding: utf-8 -*-
"""
etl_usb_bresser.py
===================
Processus EXTRACT — données météo clé USB, station Bresser MeteoChamp HD.

Flux :
  inbox_bresser/Data_*.csv  ──► vérifications ──► clé_usb/bresser_YYYYMMDD_pretraited.csv

Étapes :
  1. Lecture de tous les fichiers CSV dans inbox_bresser/
  2. Pour chaque fichier :
       - Suppression ligne 2 (unités : ℃, %, hPa…)
       - Suppression virgule finale sur chaque ligne
       - Vérification nombre de colonnes
       - Vérification présence des colonnes obligatoires (catalog)
       - Conversion date dd/mm/yyyy → yyyy-mm-dd
       - Suppression colonne NO.
  3. Concaténation de tous les fichiers valides
  4. Vérification et suppression des doublons (Date + Time)
  5. Tri chronologique
  6. Écriture : clé_usb/bresser_YYYYMMDD_pretraited.csv
  7. Archive des fichiers source dans inbox_bresser/archive/

Utilisé depuis le DAG :
    from etl_usb_bresser import run_extract_usb
    result = run_extract_usb(inbox_dir, output_dir, archive_dir)

Standalone :
    python etl_usb_bresser.py
"""

import logging
import os
import shutil
from datetime import date
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# CATALOGUE DES COLONNES ATTENDUES (issu de catalog.json)
# ─────────────────────────────────────────────────────────────────────────────

# Colonnes obligatoires — leur absence bloque le fichier
COLS_OBLIGATOIRES = [
    "Date",
    "Time",
    "IN Temperature",
    "IN Humidity",
    "Out Temperature",
    "Out Humidity",
    "Feels Like",
    "Dew Point",
    "Wind speed",
    "Wind Gust",
    "Wind Direction",
    "Rain Rate",
    "Hourly Rain",
    "UVI",
    "Light intensity",
]

# Colonnes optionnelles reconnues (présentes selon le modèle de console)
COLS_OPTIONNELLES = [
    "Baro Pressure Abs",
    "Baro Pressure Rel",
    "Heat Index",
    "Wind Chill",
    "CH1 Temperature", "CH1 Humidity",
    "CH2 Temperature", "CH2 Humidity",
    "CH3 Temperature", "CH3 Humidity",
    "CH4 Temperature", "CH4 Humidity",
    "CH5 Temperature", "CH5 Humidity",
    "CH6 Temperature", "CH6 Humidity",
    "CH7 Temperature", "CH7 Humidity",
]

# Nombre de colonnes attendu après suppression NO. (tolérance ±2)
NB_COLS_ATTENDU   = 33   # NO. + 32 colonnes (avec CH1-CH7)
NB_COLS_TOLERANCE = 2


# ─────────────────────────────────────────────────────────────────────────────
# LECTURE ET NETTOYAGE D'UN FICHIER USB
# ─────────────────────────────────────────────────────────────────────────────

def _lire_et_nettoyer(file_path: Path) -> tuple[pd.DataFrame | None, list[str]]:
    """
    Lit un fichier Data_*.csv de la clé USB et retourne (DataFrame, warnings).

    Retourne (None, erreurs) si le fichier ne peut pas être traité.
    """
    warnings = []
    erreurs  = []

    try:
        # ── Lecture brute ────────────────────────────────────────────────────
        with open(file_path, "r", encoding="utf-8-sig") as f:
            lines = f.readlines()

        if len(lines) < 3:
            erreurs.append(f"{file_path.name} : trop peu de lignes ({len(lines)})")
            return None, erreurs

        # ── Suppression ligne 2 (unités) ────────────────────────────────────
        lines.pop(1)

        # ── Suppression virgule finale sur chaque ligne ─────────────────────
        lines = [line.rstrip(",\n") + "\n" for line in lines]

        # ── Fichier temporaire ───────────────────────────────────────────────
        tmp_path = file_path.with_suffix("._tmp.csv")
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.writelines(lines)

        df = pd.read_csv(tmp_path, dtype=str)
        tmp_path.unlink(missing_ok=True)

    except Exception as exc:
        return None, [f"{file_path.name} : erreur lecture — {exc}"]

    # ── Vérification nombre de colonnes ─────────────────────────────────────
    nb_cols = len(df.columns)
    if abs(nb_cols - NB_COLS_ATTENDU) > NB_COLS_TOLERANCE:
        warnings.append(
            f"{file_path.name} : {nb_cols} colonnes "
            f"(attendu {NB_COLS_ATTENDU} ±{NB_COLS_TOLERANCE})"
        )

    # ── Suppression colonne NO. ──────────────────────────────────────────────
    if "NO." in df.columns:
        df.drop(columns=["NO."], inplace=True)
    else:
        warnings.append(f"{file_path.name} : colonne 'NO.' absente (non bloquant)")

    # ── Vérification colonnes obligatoires ───────────────────────────────────
    cols_manquantes = [c for c in COLS_OBLIGATOIRES if c not in df.columns]
    if cols_manquantes:
        erreurs.append(
            f"{file_path.name} : colonnes obligatoires manquantes → "
            + ", ".join(cols_manquantes)
        )
        return None, erreurs

    # ── Colonnes optionnelles absentes (informatif) ──────────────────────────
    cols_opt_absentes = [c for c in COLS_OPTIONNELLES if c not in df.columns]
    if cols_opt_absentes:
        warnings.append(
            f"{file_path.name} : colonnes optionnelles absentes → "
            + ", ".join(cols_opt_absentes)
        )

    # ── Conversion date dd/mm/yyyy → yyyy-mm-dd ──────────────────────────────
    if "Date" in df.columns:
        nb_avant = len(df)
        df["Date"] = pd.to_datetime(
            df["Date"], format="%d/%m/%Y", errors="coerce"
        ).dt.strftime("%Y-%m-%d")
        dates_invalides = df["Date"].isna().sum()
        if dates_invalides > 0:
            warnings.append(
                f"{file_path.name} : {dates_invalides} date(s) invalide(s) "
                f"sur {nb_avant} lignes"
            )

    # ── Suppression lignes sans date ni heure valides ────────────────────────
    df = df.dropna(subset=["Date", "Time"])

    log.info("  ✔ %-35s  %4d relevés  %d colonnes",
             file_path.name, len(df), len(df.columns))
    return df, warnings


# ─────────────────────────────────────────────────────────────────────────────
# FONCTION PRINCIPALE — importable depuis le DAG
# ─────────────────────────────────────────────────────────────────────────────

def run_extract_usb(inbox_dir: Path, output_dir: Path,
                    archive_dir: Path = None,
                    run_date: date = None) -> dict:
    """
    Lit tous les Data_*.csv de inbox_dir, valide, concatène,
    et écrit bresser_YYYYMMDD_pretraited.csv dans output_dir.

    Args:
        inbox_dir   : dossier de dépôt des fichiers USB
                      ex: /opt/airflow/data/raw/météo_bresser/clé_usb/inbox_bresser/
        output_dir  : dossier de sortie du fichier prétraité
                      ex: /opt/airflow/data/raw/météo_bresser/clé_usb/
        archive_dir : si fourni, les fichiers traités y sont déplacés
                      ex: inbox_bresser/archive/
                      Si None → les fichiers sources sont supprimés
        run_date    : date pour le nommage (défaut = today)

    Returns:
        dict : status, out_file, fichiers_traites, total_releves,
               doublons_supprimes, warnings, erreurs_fichiers
    """
    inbox_dir  = Path(inbox_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if archive_dir:
        archive_dir = Path(archive_dir)
        archive_dir.mkdir(parents=True, exist_ok=True)

    if run_date is None:
        run_date = date.today()

    log.info("=" * 60)
    log.info("EXTRACT USB — démarré le %s", run_date)
    log.info("Source  : %s", inbox_dir)
    log.info("Sortie  : %s", output_dir)
    log.info("=" * 60)

    # ── 1. Lister les fichiers CSV dans inbox ────────────────────────────────
    csv_files = sorted(inbox_dir.glob("*.csv"))
    if not csv_files:
        msg = f"Aucun fichier CSV dans {inbox_dir}"
        log.warning(msg)
        return {"status": "no_files", "message": msg}

    log.info("%d fichier(s) trouvé(s) :", len(csv_files))
    for f in csv_files:
        log.info("  → %s  (%s Ko)", f.name,
                 round(f.stat().st_size / 1024, 1))

    # ── 2. Lecture, nettoyage et validation de chaque fichier ────────────────
    dfs_valides      = []
    fichiers_ok      = []
    tous_warnings    = []
    tous_erreurs     = []

    for fp in csv_files:
        df, messages = _lire_et_nettoyer(fp)
        if df is not None:
            dfs_valides.append(df)
            fichiers_ok.append(fp)
            if messages:
                tous_warnings.extend(messages)
        else:
            tous_erreurs.extend(messages)
            log.error("  ✗ %s — REJETÉ", fp.name)

    # Rapport des warnings
    for w in tous_warnings:
        log.warning("  ⚠  %s", w)
    for e in tous_erreurs:
        log.error("  ✗  %s", e)

    if not dfs_valides:
        raise ValueError(
            f"Aucun fichier valide dans {inbox_dir}.\n"
            + "\n".join(tous_erreurs)
        )

    # ── 3. Concaténation ─────────────────────────────────────────────────────
    df_concat = pd.concat(dfs_valides, ignore_index=True)
    log.info("Relevés après concaténation : %d", len(df_concat))

    # ── 4. Vérification et suppression des doublons (Date + Time) ────────────
    nb_avant       = len(df_concat)
    df_concat.drop_duplicates(subset=["Date", "Time"], keep="last", inplace=True)
    doublons_suppr = nb_avant - len(df_concat)

    if doublons_suppr > 0:
        log.warning("  ⚠  %d doublon(s) supprimé(s) (clé : Date + Time)",
                    doublons_suppr)
    else:
        log.info("  ✔  Aucun doublon détecté")

    # ── 5. Tri chronologique ─────────────────────────────────────────────────
    df_concat.sort_values(["Date", "Time"], inplace=True, ignore_index=True)

    # ── 6. Écriture du fichier prétraité ─────────────────────────────────────
    out_name = f"bresser_{run_date.strftime('%Y%m%d')}_pretraited.csv"
    out_file = output_dir / out_name
    df_concat.to_csv(out_file, index=False, encoding="utf-8")

    log.info("✅ Fichier prétraité : %s", out_file)
    log.info("   Relevés          : %d", len(df_concat))
    log.info("   Colonnes         : %d", len(df_concat.columns))
    log.info("   Période          : %s → %s",
             df_concat["Date"].min(), df_concat["Date"].max())
    log.info("   Taille           : %.1f Ko", out_file.stat().st_size / 1024)

    # ── 7. Rapport de synthèse ────────────────────────────────────────────────
    log.info("-" * 60)
    log.info("RAPPORT EXTRACT USB")
    log.info("  Fichiers dans inbox     : %d", len(csv_files))
    log.info("  Fichiers traités (OK)   : %d", len(fichiers_ok))
    log.info("  Fichiers rejetés        : %d", len(csv_files) - len(fichiers_ok))
    log.info("  Relevés concaténés      : %d", len(df_concat))
    log.info("  Doublons supprimés      : %d", doublons_suppr)
    log.info("  Warnings                : %d", len(tous_warnings))
    log.info("  Erreurs fichiers        : %d", len(tous_erreurs))
    log.info("-" * 60)

    # ── 8. Archive ou suppression des fichiers source ────────────────────────
    for fp in fichiers_ok:
        if archive_dir:
            dest = archive_dir / fp.name
            # Évite l'écrasement si un fichier du même nom existe déjà
            if dest.exists():
                dest = archive_dir / f"{fp.stem}_{run_date.strftime('%Y%m%d')}{fp.suffix}"
            shutil.move(str(fp), str(dest))
            log.info("  Archivé : %s → %s", fp.name, dest.name)
        else:
            fp.unlink()
            log.info("  Supprimé : %s", fp.name)

    return {
        "status":             "ok",
        "out_file":           str(out_file),
        "fichiers_inbox":     len(csv_files),
        "fichiers_traites":   len(fichiers_ok),
        "fichiers_rejetes":   len(csv_files) - len(fichiers_ok),
        "total_releves":      len(df_concat),
        "doublons_supprimes": doublons_suppr,
        "debut":              str(df_concat["Date"].min()),
        "fin":                str(df_concat["Date"].max()),
        "colonnes":           len(df_concat.columns),
        "taille_ko":          round(out_file.stat().st_size / 1024, 1),
        "warnings":           tous_warnings,
        "erreurs_fichiers":   tous_erreurs,
    }


# ─────────────────────────────────────────────────────────────────────────────
# STANDALONE
# ─────────────────────────────────────────────────────────────────────────────

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

    inbox_dir  = Path(os.environ.get(
        "USB_INBOX_DIR",
        r"D:\projet_dataoz\pc_data\data\raw\météo_bresser\clé_usb\inbox_bresser"
    ))
    output_dir = Path(os.environ.get(
        "USB_OUTPUT_DIR",
        r"D:\projet_dataoz\pc_data\data\raw\météo_bresser\clé_usb"
    ))
    archive_dir = Path(os.environ.get(
        "USB_ARCHIVE_DIR",
        str(inbox_dir / "archive")
    ))

    result = run_extract_usb(inbox_dir, output_dir, archive_dir)

    print(f"\n{'='*55}")
    print("RÉSULTAT EXTRACT USB")
    print(f"{'='*55}")
    for k, v in result.items():
        if k not in ("warnings", "erreurs_fichiers"):
            print(f"  {k:25s}: {v}")
    if result.get("warnings"):
        print(f"\n  ⚠  Warnings ({len(result['warnings'])}) :")
        for w in result["warnings"]:
            print(f"      - {w}")
    if result.get("erreurs_fichiers"):
        print(f"\n  ✗  Fichiers rejetés ({len(result['erreurs_fichiers'])}) :")
        for e in result["erreurs_fichiers"]:
            print(f"      - {e}")


if __name__ == "__main__":
    main()
