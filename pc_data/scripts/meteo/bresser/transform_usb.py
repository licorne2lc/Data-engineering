# -*- coding: utf-8 -*-
"""
transform_usb.py
=================
Processus TRANSFORM — données météo clé USB (Bresser MeteoChamp HD).

Flux :
  clé_usb/bresser_YYYYMMDD_pretraited.csv
        ──► vérifications colonnes + nettoyage valeurs
        ──► clé_usb/bresser_YYYYMMDD_transformed.csv

Opérations :
  1. Lecture du fichier prétraité (sortie de usb_extract)
  2. Vérification concordance des colonnes (présence + nombre)
  3. Nettoyage des valeurs "sans capteur" : -.- et - - → NaN
  4. Vérification et comptage des doublons (Date + Time)
  5. Suppression des doublons résiduels
  6. Normalisation des types numériques
  7. Renommage capteurs additionnels : CH1 → Etage, CH2 → Cave
  8. Tri chronologique
  9. Écriture : bresser_YYYYMMDD_transformed.csv
 10. Suppression du fichier _pretraited.csv (intermédiaire)

Importé depuis le DAG :
    from transform_usb import run_transform_usb
    result = run_transform_usb(pretraited_file, output_dir)

Standalone :
    python transform_usb.py
"""

import logging
import os
from datetime import date
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# SCHÉMA DE RÉFÉRENCE — colonnes attendues (même ordre que common_weather_database)
# ─────────────────────────────────────────────────────────────────────────────

COLS_OBLIGATOIRES = [
    "Date", "Time",
    "IN Temperature", "IN Humidity",
    "Baro Pressure Rel",
    "Out Temperature", "Out Humidity",
    "Feels Like", "Dew Point",
    "Wind speed", "Wind Gust", "Wind Direction",
    "Rain Rate", "Hourly Rain",
    "UVI", "Light intensity",
]

COLS_OPTIONNELLES = [
    "Baro Pressure Abs", "Heat Index", "Wind Chill",
    "CH1 Temperature", "CH1 Humidity",
    "CH2 Temperature", "CH2 Humidity",
    "CH3 Temperature", "CH3 Humidity",
    "CH4 Temperature", "CH4 Humidity",
    "CH5 Temperature", "CH5 Humidity",
    "CH6 Temperature", "CH6 Humidity",
    "CH7 Temperature", "CH7 Humidity",
]

# Valeurs "sans capteur" produites par la console Bresser
VALEURS_SANS_CAPTEUR = {"-.-", "- -", "--", "---", " - ", "-"}


# ─────────────────────────────────────────────────────────────────────────────
# Fonction principale
# ─────────────────────────────────────────────────────────────────────────────

def run_transform_usb(pretraited_file: Path, output_dir: Path) -> dict:
    """
    Transforme le fichier prétraité USB en fichier propre et typé.

    Args:
        pretraited_file : sortie de usb_extract
                          ex: clé_usb/bresser_20260418_pretraited.csv
        output_dir      : répertoire de sortie
                          ex: /opt/airflow/data/raw/météo_bresser/clé_usb/

    Returns:
        dict : status, out_file, lignes, colonnes, doublons_supprimes,
               cols_manquantes, cols_optionnelles_absentes, warnings, ...
    """
    pretraited_file = Path(pretraited_file)
    output_dir      = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    log.info("=" * 60)
    log.info("TRANSFORM USB — %s", pretraited_file.name)
    log.info("=" * 60)

    if not pretraited_file.exists():
        raise FileNotFoundError(f"Fichier prétraité introuvable : {pretraited_file}")

    warnings = []

    # ── 1. Lecture ────────────────────────────────────────────────────────────
    df = pd.read_csv(pretraited_file, dtype=str)
    log.info("Fichier lu : %d lignes, %d colonnes", len(df), len(df.columns))

    # ── 2. Vérification concordance des colonnes ──────────────────────────────
    cols_presentes = set(df.columns.tolist())

    # Colonnes obligatoires manquantes → bloquant
    cols_manquantes = [c for c in COLS_OBLIGATOIRES if c not in cols_presentes]
    if cols_manquantes:
        raise ValueError(
            f"Colonnes obligatoires manquantes : {cols_manquantes}\n"
            f"Colonnes présentes : {df.columns.tolist()}"
        )

    # Colonnes optionnelles absentes → warning uniquement
    cols_opt_absentes = [c for c in COLS_OPTIONNELLES if c not in cols_presentes]
    if cols_opt_absentes:
        msg = f"Colonnes optionnelles absentes : {cols_opt_absentes}"
        log.warning("  ⚠  %s", msg)
        warnings.append(msg)

    log.info("✔ Concordance colonnes OK — %d colonnes présentes", len(df.columns))

    # ── 3. Nettoyage valeurs "sans capteur" (-.- / - -) → NaN ─────────────────
    cols_capteurs = [c for c in df.columns if c not in ("Date", "Time")]
    nb_remplaces  = 0
    for col in cols_capteurs:
        masque = df[col].isin(VALEURS_SANS_CAPTEUR)
        nb_remplaces += masque.sum()
        df.loc[masque, col] = pd.NA

    if nb_remplaces > 0:
        msg = f"{nb_remplaces} valeur(s) 'sans capteur' remplacée(s) par NaN"
        log.info("  ✔ %s", msg)
        warnings.append(msg)
    else:
        log.info("  ✔ Aucune valeur 'sans capteur' détectée")

    # ── 4. Vérification et suppression des doublons (Date + Time) ────────────
    avant          = len(df)
    df.drop_duplicates(subset=["Date", "Time"], keep="last", inplace=True)
    doublons_suppr = avant - len(df)

    if doublons_suppr > 0:
        msg = f"{doublons_suppr} doublon(s) supprimé(s) (clé : Date + Time)"
        log.warning("  ⚠  %s", msg)
        warnings.append(msg)
    else:
        log.info("  ✔ Aucun doublon détecté")

    # ── 5. Normalisation numérique ────────────────────────────────────────────
    for col in cols_capteurs:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # ── 6. Renommage des capteurs additionnels ────────────────────────────────
    # CH1 → Etage, CH2 → Cave (noms métier de la station Bresser)
    RENAME_CAPTEURS = {
        "CH1 Temperature": "Etage Temperature",
        "CH1 Humidity":    "Etage Humidity",
        "CH2 Temperature": "Cave Temperature",
        "CH2 Humidity":    "Cave Humidity",
    }
    rename_effectue = {src: dst for src, dst in RENAME_CAPTEURS.items()
                       if src in df.columns}
    if rename_effectue:
        df.rename(columns=rename_effectue, inplace=True)
        for src, dst in rename_effectue.items():
            log.info("  ✔ Renommage : %s → %s", src, dst)
    else:
        log.info("  ✔ Renommage capteurs : aucune colonne CH1/CH2 présente")

    # ── 7. Tri chronologique ──────────────────────────────────────────────────
    df.sort_values(["Date", "Time"], inplace=True, ignore_index=True)

    if len(df) == 0:
        raise RuntimeError("Aucune ligne de données après transformation USB.")

    # ── 9. Ajout des colonnes de traçabilité ──────────────────────────────────
    df.insert(2, "source",  "usb")
    df.insert(3, "qualite", "exacte")
    log.info("  ✔ Colonnes source='usb' et qualite='exacte' ajoutées")

    # ── 10. Écriture ──────────────────────────────────────────────────────────
    # Nom : bresser_usb_YYYY-MM-DD.csv
    # La date est extraite du fichier prétraité source :
    #   bresser_20260418_pretraited.csv → 20260418 → 2026-04-18
    raw_date = pretraited_file.stem.replace("_pretraited", "").replace("bresser_", "")
    date_str = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"
    out_file = output_dir / f"bresser_usb_{date_str}.csv"
    df.to_csv(out_file, index=False, encoding="utf-8")

    stats = {
        "status":               "ok",
        "out_file":             str(out_file),
        "lignes":               len(df),
        "colonnes":             len(df.columns),
        "debut":                str(df["Date"].min()),
        "fin":                  str(df["Date"].max()),
        "doublons_supprimes":   doublons_suppr,
        "valeurs_sans_capteur": int(nb_remplaces),
        "cols_manquantes":      cols_manquantes,
        "cols_opt_absentes":    cols_opt_absentes,
        "taille_ko":            round(out_file.stat().st_size / 1024, 1),
        "warnings":             warnings,
    }

    log.info("✅ Fichier transformé : %s", out_file)
    log.info("   Lignes            : %d", stats["lignes"])
    log.info("   Colonnes          : %d", stats["colonnes"])
    log.info("   Période           : %s → %s", stats["debut"], stats["fin"])
    log.info("   Taille            : %.1f Ko", stats["taille_ko"])

    # ── 11. Suppression du fichier prétraité (intermédiaire) ──────────────────
    # Le _transformed.csv remplace le _pretraited.csv — on le supprime
    # pour ne laisser qu'un seul fichier de sortie dans le répertoire clé_usb/.
    try:
        pretraited_file.unlink()
        log.info("🗑  Fichier prétraité supprimé : %s", pretraited_file.name)
    except OSError as exc:
        msg = f"Impossible de supprimer le fichier prétraité : {exc}"
        log.warning("  ⚠  %s", msg)
        warnings.append(msg)

    return stats


# ─────────────────────────────────────────────────────────────────────────────
# STANDALONE
# ─────────────────────────────────────────────────────────────────────────────

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

    usb_dir = Path(os.environ.get(
        "USB_OUTPUT_DIR",
        r"D:\projet_dataoz\pc_data\data\raw\météo_bresser\clé_usb"
    ))

    # Cherche le dernier fichier _pretraited
    fichiers = sorted(usb_dir.glob("*_pretraited.csv"), reverse=True)
    if not fichiers:
        raise FileNotFoundError(f"Aucun fichier *_pretraited.csv dans {usb_dir}")

    pretraited_file = fichiers[0]
    print(f"Fichier source : {pretraited_file.name}")

    result = run_transform_usb(pretraited_file, usb_dir)

    print(f"\n{'='*55}")
    print("RÉSULTAT TRANSFORM USB")
    print(f"{'='*55}")
    for k, v in result.items():
        if k != "warnings":
            print(f"  {k:28s}: {v}")
    if result.get("warnings"):
        print(f"\n  ⚠  Warnings :")
        for w in result["warnings"]:
            print(f"      - {w}")


if __name__ == "__main__":
    main()
