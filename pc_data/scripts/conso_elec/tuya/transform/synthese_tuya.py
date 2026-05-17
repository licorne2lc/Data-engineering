# -*- coding: utf-8 -*-
"""
synthese_tuya.py
================
Génère les fichiers de synthèse tous-appareils — format pivot croisé
(une ligne par période, une colonne par appareil, + TOTAL_kWh).

Deux familles :

· Mois / jours   → source = XCom `lignes_*` remonté par extract_tuya
                   sortie  = _SYNTHESE_MENSUELLE.csv / _SYNTHESE_JOURNALIERE.csv

· Heures / 15min → source = tables Postgres `tuya.f_conso_heure` et
                   `tuya.f_conso_15min` (JOIN dim_appareil pour les noms)
                   sortie  = _SYNTHESE_HORAIRE.csv / _SYNTHESE_15MIN.csv

Toutes les sorties vont dans `data/curated/conso_elec/tuya/`.
"""
from __future__ import annotations

import csv
import logging
from collections import defaultdict
from pathlib import Path

log = logging.getLogger(__name__)


def periode_vers_date(periode: str) -> str:
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


def _ecrire_synthese(
    chemin: Path,
    lignes_tous: list[dict],
    col_periode: str,
    col_valeur: str,
    trim_leading_zeros: bool = True,
) -> dict:
    """
    Écrit une synthèse pivot (période × appareil) en CSV.

    Args:
        trim_leading_zeros : si True (défaut), élimine les périodes initiales
            où TOTAL_kWh == 0 (avant installation des modules Tuya).
    """
    chemin.parent.mkdir(parents=True, exist_ok=True)

    appareils = sorted({l["appareil_nom"] for l in lignes_tous})
    periodes  = sorted({l[col_periode]    for l in lignes_tous})

    pivot: dict[str, dict[str, str]] = defaultdict(dict)
    for l in lignes_tous:
        pivot[l[col_periode]][l["appareil_nom"]] = l[col_valeur]

    # Pré-calcul : totaux par période (permet le trim des zéros initiaux)
    totaux: dict[str, float] = {}
    for periode in periodes:
        t = 0.0
        for app in appareils:
            try:
                t += float(pivot[periode].get(app, 0) or 0)
            except ValueError:
                pass
        totaux[periode] = t

    # Trim : sauter les périodes initiales où TOTAL = 0
    periodes_filtrees = list(periodes)
    nb_trim = 0
    if trim_leading_zeros:
        idx = 0
        while idx < len(periodes_filtrees) and totaux[periodes_filtrees[idx]] == 0:
            idx += 1
        nb_trim = idx
        periodes_filtrees = periodes_filtrees[idx:]
        if nb_trim:
            log.info(
                "   %s — trim : %d période(s) à 0 éliminée(s) avant %s",
                chemin.name, nb_trim,
                periodes_filtrees[0] if periodes_filtrees else "N/A",
            )

    champs = [col_periode, "date_lisible", *appareils, "TOTAL_kWh"]

    total_global = 0.0
    with chemin.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=champs, delimiter=";")
        writer.writeheader()
        for periode in periodes_filtrees:
            ligne = {
                col_periode:    periode,
                "date_lisible": periode_vers_date(periode),
            }
            for app in appareils:
                val = pivot[periode].get(app, "0.00")
                ligne[app] = val
            ligne["TOTAL_kWh"] = f"{totaux[periode]:.2f}"
            total_global += totaux[periode]
            writer.writerow(ligne)

    log.info(
        "   %s — %d périodes × %d appareils — %.2f kWh total",
        chemin.name, len(periodes_filtrees), len(appareils), total_global,
    )
    return {
        "fichier":      str(chemin),
        "periodes":     len(periodes_filtrees),
        "periodes_trim":nb_trim,
        "appareils":    len(appareils),
        "total_kwh":    round(total_global, 2),
    }


# =============================================================================
# Tâches Airflow
# =============================================================================

def synthese_mensuelle(
    dossier_curated: str,
    lignes_mois: list[dict] | None,
    **_,
) -> dict:
    """Tâche `synthese_mensuelle` : génère _SYNTHESE_MENSUELLE.csv."""
    if not lignes_mois:
        log.warning("Aucune ligne mensuelle à agréger")
        return {"status": "empty"}
    chemin = Path(dossier_curated) / "_SYNTHESE_MENSUELLE.csv"
    recap  = _ecrire_synthese(chemin, lignes_mois, "mois", "kWh")
    return {"status": "ok", **recap}


def synthese_journaliere(
    dossier_curated: str,
    lignes_jours: list[dict] | None,
    **_,
) -> dict:
    """Tâche `synthese_journaliere` : génère _SYNTHESE_JOURNALIERE.csv."""
    if not lignes_jours:
        log.warning("Aucune ligne journalière à agréger")
        return {"status": "empty"}
    chemin = Path(dossier_curated) / "_SYNTHESE_JOURNALIERE.csv"
    recap  = _ecrire_synthese(chemin, lignes_jours, "jour", "kWh")
    return {"status": "ok", **recap}


# =============================================================================
# Synthèses fines — SOURCE = Postgres (tables tuya.*)
# =============================================================================

def _lire_postgres_pivot(
    dsn: str,
    table: str,              # 'tuya.f_conso_heure' ou 'tuya.f_conso_15min'
    masque_format: str,      # 'YYYYMMDDHH24' ou 'YYYYMMDDHH24MI'
    jours: int | None,
) -> list[dict]:
    """
    Extrait les lignes agrégées (appareil_nom × période) depuis Postgres.
    Renvoie une liste prête pour `_ecrire_synthese`.
    """
    import psycopg2

    where = ""
    if jours is not None and jours > 0:
        where = f"WHERE h.ts_debut >= now() - INTERVAL '{int(jours)} days'"

    sql = f"""
        SELECT to_char(h.ts_debut AT TIME ZONE 'Europe/Paris',
                       '{masque_format}')  AS periode,
               da.nom                       AS appareil_nom,
               h.kwh::text                  AS kwh
        FROM   {table} h
        JOIN   tuya.dim_appareil da USING (appareil_id)
        {where}
        ORDER  BY periode, da.nom;
    """
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return [
        {"_periode": r[0], "appareil_nom": r[1], "kWh": r[2]}
        for r in rows
    ]


def synthese_horaire_db(
    dossier_curated: str,
    db_url: str | None = None,
    jours: int | None = 30,
    **_,
) -> dict:
    """
    Lit `tuya.f_conso_heure` et génère `_SYNTHESE_HORAIRE.csv`.

    Args:
        dossier_curated : dossier de sortie
        db_url          : DSN Postgres (à défaut : env CONSO_ELEC_DB_URL)
        jours           : fenêtre (jours) ; None = tout l'historique en base
    """
    import os
    dsn = db_url or os.environ.get("CONSO_ELEC_DB_URL", "").strip()
    if not dsn:
        log.warning("CONSO_ELEC_DB_URL manquante — synthèse horaire ignorée")
        return {"status": "empty", "reason": "no_db_url"}

    lignes = _lire_postgres_pivot(
        dsn=dsn,
        table="tuya.f_conso_heure",
        masque_format="YYYYMMDDHH24",
        jours=jours,
    )
    if not lignes:
        log.warning("Aucune ligne horaire en base (jours=%s)", jours)
        return {"status": "empty", "jours": jours}

    # _ecrire_synthese attend col_periode comme clé ; on renomme _periode → heure
    for l in lignes:
        l["heure"] = l.pop("_periode")

    chemin = Path(dossier_curated) / "_SYNTHESE_HORAIRE.csv"
    recap  = _ecrire_synthese(chemin, lignes, "heure", "kWh")
    return {"status": "ok", "jours": jours, **recap}


def test_sql_last_day(
    dossier_curated: str,
    db_url: str | None = None,
    **_,
) -> dict:
    """
    Test d'intégration DB : exécute une requête SQL combinant
    `tuya.f_conso_heure` et `tuya.f_conso_15min` sur la **dernière journée
    présente en base** puis exporte le résultat dans
    `{curated}/last_day_sql_test.csv`.

    Ce fichier sert de preuve que :
      · la DB Postgres est alimentée
      · les deux granularités (heure + 15min) coexistent
      · la jointure avec `dim_appareil` fonctionne
    """
    import csv as _csv
    import os
    dsn = db_url or os.environ.get("CONSO_ELEC_DB_URL", "").strip()
    if not dsn:
        log.warning("CONSO_ELEC_DB_URL manquante — test SQL last_day ignoré")
        return {"status": "empty", "reason": "no_db_url"}

    # Requête SQL : pour chaque table, on prend le DERNIER jour où elle
    # contient des données (les fenêtres peuvent différer : Tuya ne renvoie
    # que 7j de 15min mais l'horaire peut être plus récent après un run).
    # On UNION les deux pour produire un seul fichier.
    sql = """
        WITH j_heure AS (
            SELECT max((ts_debut AT TIME ZONE 'Europe/Paris')::date) AS jour
              FROM tuya.f_conso_heure
        ),
        j_15min AS (
            SELECT max((ts_debut AT TIME ZONE 'Europe/Paris')::date) AS jour
              FROM tuya.f_conso_15min
        )
        SELECT 'tuya.f_conso_heure'                                   AS source_table,
               (h.ts_debut AT TIME ZONE 'Europe/Paris')::date         AS jour,
               to_char(h.ts_debut AT TIME ZONE 'Europe/Paris',
                       'HH24:MI')                                     AS heure,
               'heure'                                                AS granularite,
               h.appareil_id                                          AS appareil_id,
               da.nom                                                 AS appareil_nom,
               round(h.kwh, 3)                                        AS kwh
        FROM   tuya.f_conso_heure h
        JOIN   tuya.dim_appareil  da USING (appareil_id)
        CROSS  JOIN j_heure
        WHERE  (h.ts_debut AT TIME ZONE 'Europe/Paris')::date = j_heure.jour

        UNION ALL

        SELECT 'tuya.f_conso_15min',
               (q.ts_debut AT TIME ZONE 'Europe/Paris')::date,
               to_char(q.ts_debut AT TIME ZONE 'Europe/Paris', 'HH24:MI'),
               '15min',
               q.appareil_id,
               da.nom,
               round(q.kwh, 3)
        FROM   tuya.f_conso_15min q
        JOIN   tuya.dim_appareil  da USING (appareil_id)
        CROSS  JOIN j_15min
        WHERE  (q.ts_debut AT TIME ZONE 'Europe/Paris')::date = j_15min.jour

        ORDER  BY granularite, heure, appareil_nom;
    """

    import psycopg2
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(sql)
        colonnes = [d[0] for d in cur.description]
        lignes   = cur.fetchall()

    if not lignes:
        log.warning("Aucune donnée HF en base — fichier non créé")
        return {"status": "empty"}

    # Écriture du CSV (format tidy, facilement ouvrable dans Excel)
    chemin = Path(dossier_curated) / "last_day_sql_test.csv"
    chemin.parent.mkdir(parents=True, exist_ok=True)
    with chemin.open("w", newline="", encoding="utf-8-sig") as f:
        writer = _csv.writer(f, delimiter=";")
        writer.writerow(colonnes)
        for l in lignes:
            writer.writerow(l)

    # Stats — chaque granularité peut pointer sur un jour distinct
    jours_heure = sorted({str(l[1]) for l in lignes if l[3] == "heure"})
    jours_15min = sorted({str(l[1]) for l in lignes if l[3] == "15min"})
    nb_heure = sum(1 for l in lignes if l[3] == "heure")
    nb_15min = sum(1 for l in lignes if l[3] == "15min")
    total_kwh = round(sum(float(l[6] or 0) for l in lignes if l[3] == "heure"), 3)

    jour_heure = jours_heure[-1] if jours_heure else "—"
    jour_15min = jours_15min[-1] if jours_15min else "—"

    log.info(
        "   %s — heure(%s): %d lignes, 15min(%s): %d lignes — total %.2f kWh",
        chemin.name, jour_heure, nb_heure, jour_15min, nb_15min, total_kwh,
    )
    return {
        "status":      "ok",
        "fichier":     str(chemin),
        "jour_heure":  jour_heure,
        "jour_15min":  jour_15min,
        "nb_heure":    nb_heure,
        "nb_15min":    nb_15min,
        "total_kwh":   total_kwh,
    }


def synthese_15min_db(
    dossier_curated: str,
    db_url: str | None = None,
    jours: int | None = 7,
    **_,
) -> dict:
    """
    Lit `tuya.f_conso_15min` et génère `_SYNTHESE_15MIN.csv`.

    Args:
        dossier_curated : dossier de sortie
        db_url          : DSN Postgres (à défaut : env CONSO_ELEC_DB_URL)
        jours           : fenêtre (jours) ; None = tout l'historique en base
    """
    import os
    dsn = db_url or os.environ.get("CONSO_ELEC_DB_URL", "").strip()
    if not dsn:
        log.warning("CONSO_ELEC_DB_URL manquante — synthèse 15min ignorée")
        return {"status": "empty", "reason": "no_db_url"}

    lignes = _lire_postgres_pivot(
        dsn=dsn,
        table="tuya.f_conso_15min",
        masque_format="YYYYMMDDHH24MI",
        jours=jours,
    )
    if not lignes:
        log.warning("Aucune ligne 15min en base (jours=%s)", jours)
        return {"status": "empty", "jours": jours}

    for l in lignes:
        l["periode_15min"] = l.pop("_periode")

    chemin = Path(dossier_curated) / "_SYNTHESE_15MIN.csv"
    recap  = _ecrire_synthese(chemin, lignes, "periode_15min", "kWh")
    return {"status": "ok", "jours": jours, **recap}
