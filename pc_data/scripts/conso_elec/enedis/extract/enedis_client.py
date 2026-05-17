# -*- coding: utf-8 -*-
"""
enedis_client.py
================
Client HTTP authentifié pour l'API Enedis Data Hub (particuliers).

Flux OAuth2 : `client_credentials` — pas de redirect utilisateur.
Le token a une durée de vie (~2 h) et est mis en cache :
  · En mémoire sur l'instance
  · Sur disque dans un fichier JSON (partagé entre runs / DAG / CLI)

Variables d'environnement lues :
    ENEDIS_API_KEY          client_id de l'application Data Hub
    ENEDIS_SECRET_KEY       client_secret de l'application Data Hub
    ENEDIS_ENV              "sandbox" (défaut) ou "prod"
    ENEDIS_TOKEN_CACHE      chemin du fichier JSON de cache token
                            (défaut : <cwd>/_tokens/token.json)

Endpoints Data Hub v5 (particuliers) :
    /metering_data_clc/v5/consumption_load_curve
    /metering_data_dc/v5/daily_consumption
    /metering_data_dcmp/v5/daily_consumption_max_power

Toutes les méthodes renvoient le JSON brut Enedis. Le parsing et
l'insertion Postgres sont faits ailleurs (load/, transform/).
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

import requests

log = logging.getLogger(__name__)

# ── Endpoints Enedis Data Hub ─────────────────────────────────────────────────
ENEDIS_BASE_URLS: dict[str, str] = {
    "sandbox": "https://gw.ext.prod-sandbox.api.enedis.fr",
    "prod":    "https://gw.ext.prod.api.enedis.fr",
}

TOKEN_PATH           = "/oauth2/v3/token"
PATH_LOAD_CURVE      = "/metering_data_clc/v5/consumption_load_curve"
PATH_DAILY_CONSO     = "/metering_data_dc/v5/daily_consumption"
PATH_DAILY_MAX_POWER = "/metering_data_dcmp/v5/daily_consumption_max_power"


class EnedisAuthError(RuntimeError):
    """Erreur d'authentification OAuth2 auprès d'Enedis."""


class EnedisAPIError(RuntimeError):
    """Erreur HTTP / métier renvoyée par l'API Enedis."""


def _fmt_date(d: str | date | datetime) -> str:
    """Normalise en chaîne ISO YYYY-MM-DD."""
    if isinstance(d, (date, datetime)):
        return d.strftime("%Y-%m-%d")
    return str(d)


class EnedisClient:
    """Client HTTP authentifié pour l'API Enedis Data Hub."""

    def __init__(
        self,
        client_id: str | None = None,
        client_secret: str | None = None,
        env: str | None = None,
        token_cache: str | Path | None = None,
        timeout_connect: int = 15,
        timeout_read: int = 30,
    ) -> None:
        self.client_id     = client_id     or os.environ.get("ENEDIS_API_KEY",    "")
        self.client_secret = client_secret or os.environ.get("ENEDIS_SECRET_KEY", "")
        env                = env           or os.environ.get("ENEDIS_ENV", "sandbox")
        env                = env.lower()
        if env not in ENEDIS_BASE_URLS:
            raise EnedisAuthError(
                f"ENEDIS_ENV='{env}' invalide — attendu 'sandbox' ou 'prod'"
            )
        self.env      = env
        self.base_url = ENEDIS_BASE_URLS[env]

        self.timeout_connect = timeout_connect
        self.timeout_read    = timeout_read

        if not self.client_id or not self.client_secret:
            raise EnedisAuthError(
                "ENEDIS_API_KEY / ENEDIS_SECRET_KEY manquants dans l'environnement"
            )

        # Cache token ─ mémoire
        self._token: str = ""
        self._token_expiry: float = 0.0

        # Cache token ─ disque
        if token_cache is None:
            token_cache = os.environ.get("ENEDIS_TOKEN_CACHE",
                                         str(Path.cwd() / "_tokens" / "token.json"))
        self.token_cache_path = Path(token_cache)

    # ── Authentification ────────────────────────────────────────────────────

    def _load_token_from_disk(self) -> bool:
        """Recharge un token valide depuis le cache disque si présent."""
        if not self.token_cache_path.exists():
            return False
        try:
            data = json.loads(self.token_cache_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            log.warning("Cache token illisible (%s) — ignoré", e)
            return False
        tok = data.get("access_token", "")
        exp = float(data.get("expires_at", 0))
        if tok and time.time() < exp:
            self._token        = tok
            self._token_expiry = exp
            log.info("Token Enedis rechargé depuis cache (expire dans %d min)",
                     int((exp - time.time()) / 60))
            return True
        return False

    def _save_token_to_disk(self) -> None:
        try:
            self.token_cache_path.parent.mkdir(parents=True, exist_ok=True)
            self.token_cache_path.write_text(
                json.dumps({
                    "access_token": self._token,
                    "expires_at":   self._token_expiry,
                    "env":          self.env,
                }, indent=2),
                encoding="utf-8",
            )
        except OSError as e:
            # Non bloquant — on continue avec le cache mémoire uniquement.
            log.warning("Impossible d'écrire le cache token (%s)", e)

    def authenticate(self, force: bool = False) -> str:
        """
        Retourne un access_token valide. Réutilise le cache mémoire → disque
        avant de solliciter Enedis (marge de sécurité de 60 s avant expiration).
        """
        # 1) Cache mémoire encore valide ?
        if not force and self._token and time.time() < self._token_expiry:
            return self._token

        # 2) Cache disque encore valide ?
        if not force and self._load_token_from_disk():
            return self._token

        # 3) Appel OAuth2
        url  = self.base_url + TOKEN_PATH
        data = {
            "grant_type":    "client_credentials",
            "client_id":     self.client_id,
            "client_secret": self.client_secret,
        }
        try:
            resp = requests.post(
                url, data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=(self.timeout_connect, self.timeout_read),
            )
        except requests.RequestException as e:
            raise EnedisAuthError(f"Erreur réseau lors de l'auth : {e}") from e

        if resp.status_code != 200:
            raise EnedisAuthError(
                f"Auth Enedis échouée — HTTP {resp.status_code} : {resp.text[:300]}"
            )
        try:
            payload = resp.json()
        except ValueError as e:
            raise EnedisAuthError(f"Réponse token non-JSON : {resp.text[:200]}") from e

        self._token = payload.get("access_token", "")
        if not self._token:
            raise EnedisAuthError(
                f"Réponse Enedis sans access_token : {payload}"
            )
        # expires_in en secondes (~7200)
        expires_in         = int(payload.get("expires_in", 7200))
        self._token_expiry = time.time() + expires_in - 60
        self._save_token_to_disk()
        log.info(
            "Auth Enedis OK — env=%s, token valide %d min",
            self.env, expires_in // 60,
        )
        return self._token

    # ── GET authentifié ─────────────────────────────────────────────────────

    def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        token = self.authenticate()
        url   = self.base_url + path
        try:
            resp = requests.get(
                url,
                params=params,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept":        "application/json",
                },
                timeout=(self.timeout_connect, self.timeout_read),
            )
        except requests.RequestException as e:
            raise EnedisAPIError(f"Erreur réseau GET {path} : {e}") from e

        # Token périmé côté serveur → on refait une tentative (une seule fois)
        if resp.status_code == 401:
            log.info("401 — token invalidé, on retente après refresh")
            token = self.authenticate(force=True)
            resp  = requests.get(
                url,
                params=params,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept":        "application/json",
                },
                timeout=(self.timeout_connect, self.timeout_read),
            )

        if resp.status_code >= 400:
            raise EnedisAPIError(
                f"HTTP {resp.status_code} sur {path} — {resp.text[:400]}"
            )
        try:
            return resp.json()
        except ValueError as e:
            raise EnedisAPIError(f"Réponse non-JSON sur {path} : {resp.text[:200]}") from e

    # ── Endpoints métier ────────────────────────────────────────────────────

    def consumption_load_curve(
        self,
        prm: str,
        start: str | date | datetime,
        end:   str | date | datetime,
    ) -> dict[str, Any]:
        """
        Courbe de charge 30 min (energie en Wh par pas).

        Fenêtre max autorisée par Enedis : **7 jours** par appel.
        `start` inclus, `end` exclus (ISO YYYY-MM-DD).
        """
        return self._get(
            PATH_LOAD_CURVE,
            params={
                "usage_point_id": prm,
                "start":          _fmt_date(start),
                "end":            _fmt_date(end),
            },
        )

    def daily_consumption(
        self,
        prm: str,
        start: str | date | datetime,
        end:   str | date | datetime,
    ) -> dict[str, Any]:
        """
        Conso quotidienne (Wh par jour). Fenêtre max : **36 mois** par appel.
        """
        return self._get(
            PATH_DAILY_CONSO,
            params={
                "usage_point_id": prm,
                "start":          _fmt_date(start),
                "end":            _fmt_date(end),
            },
        )

    def daily_consumption_max_power(
        self,
        prm: str,
        start: str | date | datetime,
        end:   str | date | datetime,
    ) -> dict[str, Any]:
        """
        Puissance max atteinte par jour (VA). Fenêtre max : **36 mois**.
        """
        return self._get(
            PATH_DAILY_MAX_POWER,
            params={
                "usage_point_id": prm,
                "start":          _fmt_date(start),
                "end":            _fmt_date(end),
            },
        )
