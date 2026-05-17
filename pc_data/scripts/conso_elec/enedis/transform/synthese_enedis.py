# -*- coding: utf-8 -*-
"""
synthese_enedis.py
==================
Export de syntheses CSV "curated" depuis les tables Postgres `enedis.*`.

Fichiers produits (dans `data/curated/conso_elec/enedis/`) :
  . _SYNTHESE_ENEDIS_JOUR.csv    - jour, kwh_jour, pmax_va, ts_pmax
  . _SYNTHESE_ENEDIS_30MIN.csv   - ts_debut, kwh (7 derniers jours glissants)
  . Database_Enedis_30_min.csv   - historique COMPLET au format Enedis origine
                                    (Date;Time;Conso (W), Time = fin de tranche)
"""
from __future__ import annotations

import csv
import logging
from datetime import timedelta
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore

from load.db import get_conn

log = logging.getLogger(__name__)

TZ_PARIS = ZoneInfo("Europe/Paris")


# --- Synthese JOURNALIERE --- join conso_jour + pmax_jour --------------------

def synthese_jour(dossier_curated: str | Path) -> dict:
    dossier = Path(dossier_curated)
    dossier.mkdir(parents=True, exist_ok=True)
    fichier = dossier / "_SYNTHESE_ENEDIS_JOUR.csv"

    sql = """
        SELECT c.jour,
               c.wh                          AS wh_jour,
               c.kwh                         AS kwh_jour,
               p.pmax_va,
               p.ts_pmax
          FROM enedis.f_conso_jour c
          LEFT JOIN enedis.f_pmax_jour p
                 ON p.prm = c.prm AND p.jour = c.jour
         ORDER BY c.jour;
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    with fichier.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["jour", "wh_jour", "kwh_jour", "pmax_va", "ts_pmax"])
        for jour, wh, kwh, pmax, ts_pmax in rows:
            w.writerow([
                jour.isoformat(),
                wh if wh is not None else "",
                f"{kwh:.3f}" if kwh is not None else "",
                pmax if pmax is not None else "",
                ts_pmax.isoformat(sep=" ") if ts_pmax else "",
            ])

    return {"fichier": fichier.name, "lignes": len(rows)}


# --- Synthese 30 MIN --- 7 derniers jours glissants --------------------------

def synthese_30min(dossier_curated: str | Path, jours: int = 7) -> dict:
    dossier = Path(dossier_curated)
    dossier.mkdir(parents=True, exist_ok=True)
    fichier = dossier / "_SYNTHESE_ENEDIS_30MIN.csv"

    sql = """
        SELECT ts_debut,
               wh,
               kwh
          FROM enedis.f_conso_30min
         WHERE ts_debut >= now() - (%s || ' days')::interval
         ORDER BY ts_debut;
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (str(jours),))
        rows = cur.fetchall()

    with fichier.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["ts_debut", "wh", "kwh"])
        for ts_debut, wh, kwh in rows:
            w.writerow([
                ts_debut.isoformat(sep=" "),
                wh,
                f"{kwh:.4f}" if kwh is not None else "",
            ])

    return {"fichier": fichier.name, "lignes": len(rows), "jours": jours}


# --- Database complete 30 MIN --- format origine Enedis ----------------------

def synthese_database_30min(dossier_curated: str | Path) -> dict:
    """
    Exporte l integralite de `enedis.f_conso_30min` dans
    `Database_Enedis_30_min.csv` au format d origine Enedis :

        Date;Time;Conso (W)
        2022-08-09;00:30:00;388.0
        ...

    Conventions :
      . Date  = date locale Europe/Paris du debut de tranche
      . Time  = heure de FIN de tranche (ts_debut + 30 min) en Europe/Paris
                si la fin tombe a minuit, on ecrit "23:59:59" (convention Enedis)
      . Conso (W) = puissance moyenne en W = wh * 2  (reconstitution depuis Wh)

    Tous les PRM presents dans la table sont exportes (historique fusionne).
    Le fichier est trie par ts_debut croissant.

    GARDE-FOU : si le fichier existant contient plus de lignes que Postgres,
    l ecriture est annulee afin de proteger l historique CSV contre un
    ecrasement par des donnees partielles (ex. : sandbox API uniquement).

    Retourne : {"fichier": ..., "lignes": N, "skipped": True|False}
    """
    dossier = Path(dossier_curated)
    dossier.mkdir(parents=True, exist_ok=True)
    fichier = dossier / "Database_Enedis_30_min.csv"

    sql = """
        SELECT ts_debut,
               wh
          FROM enedis.f_conso_30min
         ORDER BY ts_debut;
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    nb_postgres = len(rows)

    # Garde-fou : ne jamais ecraser un fichier plus complet que Postgres
    if fichier.exists():
        with fichier.open(encoding="utf-8", newline="") as _f:
            nb_existant = sum(1 for _ in _f) - 1  # -1 pour l'en-tete
        if nb_existant > nb_postgres:
            raison = (
                "fichier existant ("
                + str(nb_existant)
                + " lignes) > Postgres ("
                + str(nb_postgres)
                + " lignes) -- import_historique non encore complete ?"
            )
            log.warning(
                "[synthese] GARDE-FOU : %s -- ecriture annulee.", raison,
            )
            return {
                "fichier": fichier.name,
                "lignes":  nb_existant,
                "skipped": True,
                "raison":  raison,
            }

    with fichier.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["Date", "Time", "Conso (W)"])

        for ts_debut, wh in rows:
            # Conversion UTC -> Europe/Paris (psycopg2 retourne TIMESTAMPTZ en UTC)
            ts_paris     = ts_debut.astimezone(TZ_PARIS)
            ts_fin_paris = (ts_debut + timedelta(minutes=30)).astimezone(TZ_PARIS)

            date_str = ts_paris.date().isoformat()

            # Convention Enedis : si la fin de tranche = minuit -> "23:59:59"
            if ts_fin_paris.hour == 0 and ts_fin_paris.minute == 0:
                time_str = "23:59:59"
            else:
                time_str = ts_fin_paris.strftime("%H:%M:%S")

            # Reconstitution W depuis Wh (wh = W * 0.5  =>  W = wh * 2)
            conso_w = f"{wh * 2:.1f}" if wh is not None else "0.0"

            w.writerow([date_str, time_str, conso_w])

    log.info("[synthese] Database_Enedis_30_min.csv -> %d lignes", nb_postgres)
    return {"fichier": fichier.name, "lignes": nb_postgres, "skipped": False}
