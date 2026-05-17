# -*- coding: utf-8 -*-
"""
one_time_trim_curated.py
========================
Script one-shot pour nettoyer rétroactivement les synthèses curated
en supprimant les périodes initiales où TOTAL_kWh == 0 (avant l'installation
des modules Tuya, soit avant le 2023-10-27).

À exécuter UNE SEULE FOIS depuis le conteneur Airflow (qui a les droits
d'écriture sur le volume partagé) :

    docker-compose exec airflow-scheduler \\
        python /opt/airflow/scripts/conso_elec/tuya/one_time_trim_curated.py

Après cette passe, la tâche synthese_mensuelle / synthese_journaliere du DAG
continuera de produire des fichiers trimés automatiquement (option
trim_leading_zeros=True par défaut dans _ecrire_synthese).
"""
import csv
import shutil
from pathlib import Path

# Chemin curated tel que monté dans le conteneur Airflow
# (mapping docker-compose : ./data -> /opt/airflow/data)
CURATED = Path("/opt/airflow/data/curated/conso_elec/tuya")


def trim_file(fichier: Path) -> None:
    with fichier.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        champs = reader.fieldnames
        rows = list(reader)

    n_avant = len(rows)
    idx = 0
    while idx < len(rows):
        try:
            t = float(rows[idx].get("TOTAL_kWh", "0").replace(",", "."))
        except ValueError:
            t = 0.0
        if t > 0:
            break
        idx += 1

    if idx == 0:
        print(f"   {fichier.name:<32} rien à faire (démarre déjà par non-zéro)")
        return
    if idx >= len(rows):
        print(f"   {fichier.name:<32} TOUT est à zéro — non modifié")
        return

    backup = fichier.with_suffix(fichier.suffix + ".bak")
    # shutil.copyfile : ne copie QUE le contenu, pas de chmod/utime.
    # Necessaire sur les volumes Windows/WSL qui refusent ces metadonnees.
    shutil.copyfile(fichier, backup)

    rows_kept = rows[idx:]
    with fichier.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=champs, delimiter=";")
        writer.writeheader()
        writer.writerows(rows_kept)

    premiere_periode = list(rows[idx].values())[0]
    premiere_date = list(rows[idx].values())[1] if len(rows[idx]) > 1 else "-"
    print(f"   {fichier.name:<32} "
          f"{n_avant:>5} -> {len(rows_kept):>5} lignes  "
          f"(trim {idx}, demarre a {premiere_periode} = {premiere_date})  "
          f"[backup: {backup.name}]")


if __name__ == "__main__":
    print(f"Nettoyage dans : {CURATED}")
    print("-" * 80)
    # Mensuelle + journaliere ont besoin du trim.
    # Horaire + 15min : pas necessaire (fenetres glissantes courtes).
    for nom in ("_SYNTHESE_MENSUELLE.csv", "_SYNTHESE_JOURNALIERE.csv"):
        f = CURATED / nom
        if f.exists():
            trim_file(f)
        else:
            print(f"   {nom} absent")
    print("\nTermine.")
