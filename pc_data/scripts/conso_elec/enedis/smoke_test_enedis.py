# -*- coding: utf-8 -*-
"""
smoke_test_enedis.py
====================
Test de bout en bout minimal du client Enedis Data Hub.

Objectifs :
  1. Vérifier que les variables d'env ENEDIS_* sont lues correctement
  2. Obtenir un access_token via OAuth2 client_credentials
  3. Appeler les 3 endpoints clés et afficher un extrait lisible de chaque JSON

Usage — depuis le conteneur Airflow (recommandé, .env déjà chargé) :

    docker-compose exec airflow-scheduler \\
        python /opt/airflow/scripts/conso_elec/enedis/smoke_test_enedis.py

Usage — en local (Windows / WSL) avec python-dotenv installé :

    python scripts/conso_elec/enedis/smoke_test_enedis.py

Le script ne modifie AUCUNE donnée Postgres. Il se contente d'afficher les
réponses brutes Enedis pour valider la connectivité et les credentials.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path

# Charger .env si python-dotenv dispo (exécution locale hors container)
try:
    from dotenv import load_dotenv
    # On cherche .env dans la racine du projet, en remontant de ce fichier
    for parent in Path(__file__).resolve().parents:
        env_file = parent / ".env"
        if env_file.exists():
            load_dotenv(env_file, override=False)
            print(f".env chargé depuis {env_file}")
            break
except ImportError:
    pass  # Dans Airflow, les variables sont déjà dans l'environnement

# Import du client (chemin relatif au script)
sys.path.insert(0, str(Path(__file__).resolve().parent))
from extract.enedis_client import (  # noqa: E402
    EnedisAPIError,
    EnedisAuthError,
    EnedisClient,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s : %(message)s",
)
log = logging.getLogger("smoke_enedis")


def _dump_extract(label: str, payload: dict, n_points: int = 3) -> None:
    """Affiche l'entête + les N premières mesures pour un endpoint."""
    print("\n" + "─" * 70)
    print(f"  {label}")
    print("─" * 70)
    mr = payload.get("meter_reading", payload)
    if not isinstance(mr, dict):
        print(json.dumps(payload, indent=2, ensure_ascii=False)[:500])
        return
    usage_point = mr.get("usage_point_id", "?")
    quality     = mr.get("quality", "?")
    start       = mr.get("start", "?")
    end         = mr.get("end", "?")
    readings    = mr.get("interval_reading", [])
    print(f"  PRM               : {usage_point}")
    print(f"  Fenêtre           : {start} → {end}")
    print(f"  Quality           : {quality}")
    print(f"  Nb de mesures     : {len(readings)}")
    if readings:
        print(f"  Premières {min(n_points, len(readings))} valeurs :")
        for r in readings[:n_points]:
            print(f"    {r}")


# PRM de test officiellement documenté par Enedis pour le bac à sable.
# En prod, le PRM réel du compteur est utilisé.
PRM_SANDBOX_TEST = "22516914714270"


def main() -> int:
    print("=" * 70)
    print("  SMOKE TEST — Enedis Data Hub")
    print("=" * 70)

    # 1) Lecture des credentials
    client_id     = os.environ.get("ENEDIS_API_KEY", "")
    client_secret = os.environ.get("ENEDIS_SECRET_KEY", "")
    prm_reel      = os.environ.get("PRM_ID", "")
    env           = os.environ.get("ENEDIS_ENV", "sandbox")
    # Override explicite via ENEDIS_TEST_PRM (priorité max)
    prm_override  = os.environ.get("ENEDIS_TEST_PRM", "")

    if prm_override:
        prm    = prm_override
        source = "ENEDIS_TEST_PRM (override)"
    elif env == "sandbox":
        prm    = PRM_SANDBOX_TEST
        source = "PRM de test sandbox Enedis (fallback auto)"
    else:
        prm    = prm_reel
        source = "PRM_ID (.env)"

    print(f"  ENEDIS_ENV        : {env}")
    print(f"  ENEDIS_API_KEY    : "
          f"{'OK (' + client_id[:6] + '…)' if client_id else 'MANQUANT'}")
    print(f"  ENEDIS_SECRET_KEY : "
          f"{'OK (' + client_secret[:6] + '…)' if client_secret else 'MANQUANT'}")
    print(f"  PRM utilisé       : {prm or 'MANQUANT'}  [{source}]")
    if prm_reel and prm_reel != prm:
        print(f"  PRM_ID .env       : {prm_reel}  (non utilisé en sandbox)")

    if not (client_id and client_secret and prm):
        log.error(
            "Credentials incomplets — vérifier ENEDIS_API_KEY, "
            "ENEDIS_SECRET_KEY et PRM_ID dans .env"
        )
        return 1

    # 2) Création du client + authentification
    try:
        client = EnedisClient()
    except EnedisAuthError as e:
        log.error("Init client échouée : %s", e)
        return 2

    try:
        token = client.authenticate()
    except EnedisAuthError as e:
        log.error("Authentification OAuth2 échouée : %s", e)
        return 3
    print("\n✓ Authentification OAuth2 OK")
    print(f"  Token (tronqué)   : {token[:20]}…")

    # 3) Fenêtres de test — sandbox accepte des dates récentes
    #    (en prod, Enedis limite aux 24 derniers mois pour CLC, 36 pour daily)
    today      = date.today()
    end_7j     = today
    start_7j   = today - timedelta(days=7)

    # 4) Appels aux 3 endpoints
    try:
        print(f"\n→ consumption_load_curve({start_7j} → {end_7j}) …")
        clc = client.consumption_load_curve(prm, start_7j, end_7j)
        _dump_extract("consumption_load_curve (30 min, Wh)", clc, n_points=3)
    except EnedisAPIError as e:
        log.warning("consumption_load_curve a échoué : %s", e)

    try:
        print(f"\n→ daily_consumption({start_7j} → {end_7j}) …")
        dc = client.daily_consumption(prm, start_7j, end_7j)
        _dump_extract("daily_consumption (jour, Wh)", dc, n_points=5)
    except EnedisAPIError as e:
        log.warning("daily_consumption a échoué : %s", e)

    try:
        print(f"\n→ daily_consumption_max_power({start_7j} → {end_7j}) …")
        pmax = client.daily_consumption_max_power(prm, start_7j, end_7j)
        _dump_extract("daily_consumption_max_power (jour, VA)", pmax, n_points=5)
    except EnedisAPIError as e:
        log.warning("daily_consumption_max_power a échoué : %s", e)

    print("\n" + "=" * 70)
    print("  Smoke test terminé.")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
