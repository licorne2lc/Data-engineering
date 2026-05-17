# -*- coding: utf-8 -*-
"""
extract_tuya.py
===============
Fonctions d'extraction des statistiques de consommation électrique Tuya
appelées par le DAG Airflow `dag_conso_elec_tuya.py`.

Chaque fonction :
  · instancie un TuyaClient (crédentiels lus dans l'environnement)
  · parcourt la liste d'appareils fournie (ou la récupère)
  · écrit un fichier CSV par appareil dans le dossier RAW
  · renvoie un dict récapitulatif (nb lignes, nb appareils, total kWh, chemins)

Sortie :
    {dossier_raw}/{appareil}_{id8}_mois.csv
    {dossier_raw}/{appareil}_{id8}_jours.csv
    {dossier_raw}/{appareil}_{id8}_heures.csv
    {dossier_raw}/{appareil}_{id8}_15min.csv

Les fichiers CSV sont compatibles Excel français (sep=";", utf-8-sig).
"""
from __future__ import annotations

import csv
import logging
import os
from pathlib import Path
from typing import Callable

from tuya_client import TuyaClient

log = logging.getLogger(__name__)


# =============================================================================
# Helpers CSV
# =============================================================================

def nom_propre(nom: str) -> str:
    """Supprime les caractères interdits dans un nom de fichier."""
    for c in r'\/:*?"<>|':
        nom = nom.replace(c, "_")
    return nom.strip() or "sans_nom"


def periode_vers_date(periode: str) -> str:
    """
    Convertit une clé de période Tuya en date lisible.
      · YYYYMMDD     → "YYYY-MM-DD"
      · YYYYMM       → "YYYY-MM"
      · YYYYMMDDHH   → "YYYY-MM-DD HH:00"
      · YYYYMMDDHHmm → "YYYY-MM-DD HH:mm"
    """
    p = str(periode)
    if len(p) == 10:
        return f"{p[:4]}-{p[4:6]}-{p[6:8]} {p[8:]}:00"
    if len(p) == 8:
        return f"{p[:4]}-{p[4:6]}-{p[6:]}"
    if len(p) == 6:
        return f"{p[:4]}-{p[4:]}"
    if len(p) == 12:
        return f"{p[:4]}-{p[4:6]}-{p[6:8]} {p[8:10]}:{p[10:]}"
    return p


def _ecrire_csv(
    chemin: Path,
    donnees: dict,
    appareil_nom: str,
    appareil_id: str,
    col_periode: str,
    col_valeur: str,
    unite: str,
) -> dict:
    """
    Écrit un dict {periode: valeur} en CSV trié chronologiquement.
    Retourne un récap + la liste des lignes écrites.
    """
    chemin.parent.mkdir(parents=True, exist_ok=True)

    lignes = []
    for periode, valeur in sorted(donnees.items()):
        lignes.append({
            col_periode:    periode,
            "date_lisible": periode_vers_date(periode),
            col_valeur:     valeur,
            "unite":        unite,
            "appareil_nom": appareil_nom,
            "appareil_id":  appareil_id,
        })

    champs = [col_periode, "date_lisible", col_valeur, "unite",
              "appareil_nom", "appareil_id"]

    with chemin.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=champs, delimiter=";")
        writer.writeheader()
        writer.writerows(lignes)

    nb_non_nul = sum(1 for l in lignes if float(l[col_valeur] or 0) > 0)
    total_kwh  = sum(float(l[col_valeur] or 0) for l in lignes)

    log.info(
        "   %s — %d lignes (%d > 0) — %.2f kWh",
        chemin.name, len(lignes), nb_non_nul, total_kwh,
    )
    return {
        "fichier":      str(chemin),
        "lignes":       len(lignes),
        "lignes_non_nul": nb_non_nul,
        "total_kwh":    round(total_kwh, 2),
        "lignes_data":  lignes,   # utile pour la synthèse
    }


# =============================================================================
# Utilitaires communs aux tâches
# =============================================================================

def _iter_appareils(
    client: TuyaClient,
    appareils_override: list[dict] | None = None,
) -> list[dict]:
    """Récupère les appareils soit depuis XCom (override) soit via l'API."""
    if appareils_override:
        return appareils_override
    return client.lister_appareils()


def _appareils_valides(appareils: list[dict]) -> list[dict]:
    """Filtre les appareils sans id."""
    return [a for a in appareils if a.get("id")]


def _nom_fichier(appareil: dict, suffixe: str) -> str:
    name  = appareil.get("name", "sans_nom")
    dev   = appareil.get("id", "")
    return f"{nom_propre(name)}_{dev[:8]}_{suffixe}.csv"


# =============================================================================
# Tâches Airflow — extraction par période
# =============================================================================

def lister_appareils(**_) -> list[dict]:
    """Tâche `list_devices` : renvoie la liste brute des appareils SmartLife."""
    client = TuyaClient()
    appareils = client.lister_appareils()
    # On ne garde que les clés utiles pour XCom (léger)
    return [
        {
            "id":       a.get("id"),
            "name":     a.get("name"),
            "model":    a.get("model", ""),
            "category": a.get("category", ""),
        }
        for a in _appareils_valides(appareils)
    ]


def extraire_mois(
    dossier_raw: str,
    annee_debut: int,
    appareils: list[dict] | None = None,
    **_,
) -> dict:
    """Tâche `extract_monthly` : consommation mensuelle par appareil."""
    client = TuyaClient()
    appareils = _appareils_valides(_iter_appareils(client, appareils))
    dossier   = Path(dossier_raw)

    recaps    = []
    for appareil in appareils:
        fichier = dossier / _nom_fichier(appareil, "mois")
        mois    = client.get_stats_mois(appareil["id"], annee_debut)
        if not mois:
            log.warning("   ⚠ %s : aucune statistique mensuelle", appareil["name"])
            continue
        recap = _ecrire_csv(
            fichier, mois, appareil["name"], appareil["id"],
            col_periode="mois", col_valeur="kWh", unite="kWh",
        )
        recaps.append({**recap, "appareil": appareil["name"], "id": appareil["id"]})

    return {
        "niveau":       "mois",
        "nb_appareils": len(recaps),
        "total_kwh":    round(sum(r["total_kwh"] for r in recaps), 2),
        "fichiers":     [r["fichier"] for r in recaps],
        # lignes détaillées pour la synthèse (XCom)
        "lignes":       [
            {k: v for k, v in l.items() if k != "lignes_data"}
            for r in recaps for l in r["lignes_data"]
        ],
    }


def extraire_jours(
    dossier_raw: str,
    annee_debut: int,
    appareils: list[dict] | None = None,
    **_,
) -> dict:
    """Tâche `extract_daily` : consommation journalière par appareil."""
    client    = TuyaClient()
    appareils = _appareils_valides(_iter_appareils(client, appareils))
    dossier   = Path(dossier_raw)

    recaps = []
    for appareil in appareils:
        fichier = dossier / _nom_fichier(appareil, "jours")
        jours   = client.get_stats_jours(appareil["id"], annee_debut)
        if not jours:
            log.warning("   ⚠ %s : aucune statistique journalière", appareil["name"])
            continue
        recap = _ecrire_csv(
            fichier, jours, appareil["name"], appareil["id"],
            col_periode="jour", col_valeur="kWh", unite="kWh",
        )
        recaps.append({**recap, "appareil": appareil["name"], "id": appareil["id"]})

    return {
        "niveau":       "jour",
        "nb_appareils": len(recaps),
        "total_kwh":    round(sum(r["total_kwh"] for r in recaps), 2),
        "fichiers":     [r["fichier"] for r in recaps],
        "lignes":       [
            {k: v for k, v in l.items() if k != "lignes_data"}
            for r in recaps for l in r["lignes_data"]
        ],
    }


def extraire_heures(
    dossier_raw: str,
    jours: int = 7,
    appareils: list[dict] | None = None,
    **_,
) -> dict:
    """Tâche `extract_hourly` : consommation horaire (7 derniers jours, >0)."""
    client    = TuyaClient()
    appareils = _appareils_valides(_iter_appareils(client, appareils))
    dossier   = Path(dossier_raw)

    recaps = []
    for appareil in appareils:
        fichier = dossier / _nom_fichier(appareil, "heures")
        heures  = client.get_stats_heures(appareil["id"], jours=jours)
        if not heures:
            log.warning("   ⚠ %s : aucune heure > 0 sur %d jours",
                        appareil["name"], jours)
            continue
        recap = _ecrire_csv(
            fichier, heures, appareil["name"], appareil["id"],
            col_periode="heure", col_valeur="kWh", unite="kWh",
        )
        recaps.append({**recap, "appareil": appareil["name"], "id": appareil["id"]})

    return {
        "niveau":       "heure",
        "nb_appareils": len(recaps),
        "total_kwh":    round(sum(r["total_kwh"] for r in recaps), 2),
        "fichiers":     [r["fichier"] for r in recaps],
    }


def extraire_15min(
    dossier_raw: str,
    jours: int = 7,
    appareils: list[dict] | None = None,
    **_,
) -> dict:
    """Tâche `extract_quarters` : consommation 15 min (7 derniers jours)."""
    client    = TuyaClient()
    appareils = _appareils_valides(_iter_appareils(client, appareils))
    dossier   = Path(dossier_raw)

    recaps = []
    for appareil in appareils:
        fichier = dossier / _nom_fichier(appareil, "15min")
        quarts  = client.get_stats_15min(appareil["id"], jours=jours)
        if not quarts:
            log.warning("   ⚠ %s : aucun quart d'heure sur %d jours",
                        appareil["name"], jours)
            continue
        recap = _ecrire_csv(
            fichier, quarts, appareil["name"], appareil["id"],
            col_periode="periode_15min", col_valeur="kWh", unite="kWh",
        )
        recaps.append({**recap, "appareil": appareil["name"], "id": appareil["id"]})

    return {
        "niveau":       "15min",
        "nb_appareils": len(recaps),
        "total_kwh":    round(sum(r["total_kwh"] for r in recaps), 2),
        "fichiers":     [r["fichier"] for r in recaps],
    }
