# -*- coding: utf-8 -*-
"""
tuya_client.py
==============
Client API Tuya (SmartLife / Beta APIs) — version module réutilisable.

Adapté de test_export_data_7j.py pour être appelé depuis un DAG Airflow :
  · Lecture des credentials depuis os.environ (injectés par docker-compose)
  · Plus de prints bloquants — utilise logging
  · Pas de side-effects au niveau module

Variables d'environnement lues :
    TUYA_API_ID                 Identifiant API Tuya
    TUYA_API_SECRET             Secret API Tuya
    TUYA_API_REGION             Région API (eu, us, cn, ...)  [défaut : eu]

Endpoints Tuya :
    cn   : https://openapi.tuyacn.com
    us   : https://openapi.tuyaus.com
    us-e : https://openapi-ueaz.tuyaus.com
    eu   : https://openapi.tuyaeu.com
    eu-w : https://openapi-weaz.tuyaeu.com
    in   : https://openapi.tuyain.com

Mapping Device ID → Nom canonique (DEVICE_ALIAS)
=================================================
Garantit que chaque appareil conserve son nom correct dans le pipeline
quel que soit le nom configuré dans l'app SmartLife.
Mise à jour nécessaire uniquement si un nouvel appareil est ajouté.

Vérification des IDs : regarder les fichiers
  data/raw/conso_elec/Tuya/*_mois.csv  (colonne appareil_id)
"""
from __future__ import annotations

import hashlib
import hmac
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any

import requests

log = logging.getLogger(__name__)

# =============================================================================
# Mapping fixe device_id → nom canonique
# =============================================================================
# Source de vérité des IDs : data/raw/conso_elec/Tuya/*_mois.csv (col appareil_id)
# Priorité : si l'ID est trouvé ici, ce nom remplace celui retourné par l'API Tuya.
# Cela protège le pipeline des renommages accidentels dans l'app SmartLife.
#
# ⚠ IMPORTANT : ne pas confondre les deux colonnes "ballon / chauffage" —
#   bf082e49 est le BALLON EAU CHAUDE (chauffe la nuit, 2-2.5 kWh/h en HC)
#   bf28133c est le CHAUFFAGE        (actif en hiver, veille 0.01 kWh/h en été)
#
DEVICE_ALIAS: dict[str, str] = {
    # --- Mesure eau chaude sanitaire ---
    "bf082e49099b355a3dz19o": "ballon d'eau chaude",

    # --- Chauffage (clim/radiateur) ---
    "bf28133c02cbbd0433cefp": "chauffage",

    # --- Froid alimentaire ---
    "bf38e325a4a7f8e094ftwr": "frigo",

    # --- Prises de courant ---
    "bf262e7a108c3fafd08w0j": "prise generale PC",
    "bf85772fe4d3a0857ds752": "prise parfum ch.parents",

    # --- Audiovisuel ---
    "bf77c4b7a17ef0201fjkkd": "teleprojecteur",
    "bf4374fce2062c0e02dxug": "tv chambre",
    "bf18c4297f9ca8bb44wflo": "tv salon",

    # --- Divers ---
    "bf4dc4a5679f195e618af8": "jaccuzzi",
    "bfad9bcfa2e9605f82i7ha": "loan",
    "bf5c5798f756e8b4bafvub": "parfum salon",
}


def apply_device_alias(appareil: dict) -> dict:
    """
    Remplace le nom d'un appareil par son alias canonique si son ID est connu.
    Retourne le dict modifié (ou inchangé si l'ID n'est pas dans DEVICE_ALIAS).
    """
    device_id = appareil.get("id", "")
    if device_id in DEVICE_ALIAS:
        api_name = appareil.get("name", "")
        canonical = DEVICE_ALIAS[device_id]
        if api_name != canonical:
            log.info(
                "DEVICE_ALIAS : %s — nom API=%r → alias=%r",
                device_id, api_name, canonical,
            )
        return {**appareil, "name": canonical}
    return appareil


TUYA_ENDPOINTS: dict[str, str] = {
    "cn":   "https://openapi.tuyacn.com",
    "us":   "https://openapi.tuyaus.com",
    "us-e": "https://openapi-ueaz.tuyaus.com",
    "eu":   "https://openapi.tuyaeu.com",
    "eu-w": "https://openapi-weaz.tuyaeu.com",
    "in":   "https://openapi.tuyain.com",
}


class TuyaAuthError(RuntimeError):
    """Erreur d'authentification auprès de l'API Tuya."""


class TuyaSubscriptionExpiredError(RuntimeError):
    """
    Abonnement Tuya IoT Core expiré (code API 28841002).

    Action requise : se connecter sur https://iot.tuya.com →
    Cloud → Mes abonnements → renouveler "IoT Core" (Trial Edition gratuite).
    Durée : 6 mois renouvelables.
    """


class TuyaClient:
    """Client HTTP authentifié pour l'API Tuya (Beta statistics)."""

    def __init__(
        self,
        api_id: str | None = None,
        api_secret: str | None = None,
        region: str | None = None,
        timeout_connect: int = 15,
        timeout_read: int = 20,
    ) -> None:
        self.api_id     = api_id     or os.environ.get("TUYA_API_ID",     "")
        self.api_secret = api_secret or os.environ.get("TUYA_API_SECRET", "")
        region          = region     or os.environ.get("TUYA_API_REGION", "eu")
        self.base_url   = TUYA_ENDPOINTS.get(region, TUYA_ENDPOINTS["eu"])
        self.timeout_connect = timeout_connect
        self.timeout_read    = timeout_read
        self.token           = ""
        self.token_expiry    = 0.0

        if not self.api_id or not self.api_secret:
            raise TuyaAuthError(
                "TUYA_API_ID / TUYA_API_SECRET manquants dans l'environnement"
            )

    # ── Signatures / headers ────────────────────────────────────────────────

    @staticmethod
    def _ts() -> str:
        return str(int(time.time() * 1000))

    def _sign(self, msg: str) -> str:
        return hmac.new(
            self.api_secret.encode(),
            msg.encode(),
            hashlib.sha256,
        ).hexdigest().upper()

    def _headers(self, path: str, token: str = "",
                  params: dict | None = None) -> dict[str, str]:
        ts           = self._ts()
        content_hash = hashlib.sha256(b"").hexdigest()
        query_str    = ""
        if params:
            query_str = "?" + "&".join(
                f"{k}={v}" for k, v in sorted(params.items())
            )
        str_to_sign = "\n".join(["GET", content_hash, "", path + query_str])
        signature   = self._sign(self.api_id + token + ts + str_to_sign)
        return {
            "client_id":    self.api_id,
            "access_token": token,
            "t":            ts,
            "sign":         signature,
            "sign_method":  "HMAC-SHA256",
            "Content-Type": "application/json",
        }

    # ── Authentification ────────────────────────────────────────────────────

    def authenticate(self) -> bool:
        """Obtient ou renouvelle le token d'accès (cache 60 s avant expiration)."""
        if self.token and time.time() < self.token_expiry:
            return True
        path   = "/v1.0/token"
        params = {"grant_type": "1"}
        try:
            resp = requests.get(
                self.base_url + path,
                headers=self._headers(path, token="", params=params),
                params=params,
                timeout=(self.timeout_connect, self.timeout_read),
            )
            data = resp.json()
        except Exception as e:
            log.error("Tuya auth — erreur réseau : %s", e)
            raise TuyaAuthError(f"Erreur réseau : {e}") from e

        if not data.get("success"):
            msg = data.get("msg", "inconnue")
            code = data.get("code", "?")
            log.error("Tuya auth échouée : %s (code=%s)", msg, code)
            raise TuyaAuthError(f"Auth Tuya échouée : {msg} (code={code})")

        self.token        = data["result"]["access_token"]
        expire            = data["result"].get("expire_time", 7200)
        self.token_expiry = time.time() + expire - 60
        log.info("Authentification Tuya OK — token valide %d min", expire // 60)
        return True

    # ── GET authentifié ─────────────────────────────────────────────────────

    def get(self, path: str, params: dict | None = None) -> dict[str, Any]:
        self.authenticate()
        try:
            resp = requests.get(
                self.base_url + path,
                headers=self._headers(path, token=self.token, params=params),
                params=params,
                timeout=(self.timeout_connect, self.timeout_read),
            )
            return resp.json()
        except Exception as e:
            log.warning("Tuya GET %s — erreur : %s", path, e)
            return {"success": False, "msg": str(e)}

    # ── Appareils ───────────────────────────────────────────────────────────

    def lister_appareils(self) -> list[dict]:
        """
        Retourne la liste complète de tous les appareils du compte.

        Lève TuyaSubscriptionExpiredError si l'API retourne le code 28841002
        (abonnement IoT Core expiré). Action : renouveler l'abonnement sur
        https://iot.tuya.com → Cloud → Mes abonnements → IoT Core (Trial, 6 mois).
        """
        appareils: list[dict] = []
        page      = 1
        page_size = 100
        while True:
            data   = self.get(
                "/v1.0/iot-01/associated-users/devices",
                params={"page_no": page, "page_size": page_size},
            )

            # ── Détection abonnement expiré (code 28841002) ─────────────────
            if not data.get("success") and data.get("code") == 28841002:
                msg = (
                    "Abonnement Tuya IoT Core EXPIRÉ (code 28841002). "
                    "Renouveler sur https://iot.tuya.com → Cloud → "
                    "Mes abonnements → IoT Core (Trial Edition, gratuit, 6 mois)."
                )
                log.error("🚨 %s", msg)
                raise TuyaSubscriptionExpiredError(msg)

            result = data.get("result", {})
            liste  = result.get("devices", result.get("list", []))
            if not liste:
                break
            appareils.extend(liste)
            if len(liste) < page_size:
                break
            page += 1
        log.info("Tuya : %d appareil(s) récupéré(s)", len(appareils))
        # Appliquer le mapping fixe device_id → nom canonique
        appareils = [apply_device_alias(a) for a in appareils]
        return appareils

    # ── Statistiques ────────────────────────────────────────────────────────

    def get_stats_mois(self, device_id: str, annee_debut: int) -> dict:
        """
        Statistiques mensuelles add_ele (kWh) depuis `annee_debut` jusqu'à
        aujourd'hui. Tuya impose que start/end_month soient dans la même
        année : on boucle année par année.
        """
        tous_mois: dict[str, str] = {}
        annee_fin = datetime.now().year

        for annee in range(annee_debut, annee_fin + 1):
            fin_mois = (f"{annee}12"
                        if annee < annee_fin
                        else datetime.now().strftime("%Y%m"))
            data = self.get(
                f"/v1.0/devices/{device_id}/statistics/months",
                params={
                    "code":        "add_ele",
                    "start_month": f"{annee}01",
                    "end_month":   fin_mois,
                },
            )
            if data.get("success"):
                mois = data.get("result", {}).get("months", {}) or {}
                tous_mois.update(mois)
        return tous_mois

    def get_stats_jours(self, device_id: str, annee_debut: int) -> dict:
        """Statistiques journalières add_ele (kWh), mois par mois."""
        tous_jours: dict[str, str] = {}
        now        = datetime.now()
        curseur    = datetime(annee_debut, 1, 1)

        while curseur <= now:
            debut_j = curseur.strftime("%Y%m01")
            if curseur.month == 12:
                fin_j = curseur.strftime("%Y%m31")
            else:
                dernier = (
                    datetime(curseur.year, curseur.month + 1, 1)
                    - timedelta(days=1)
                )
                fin_j = dernier.strftime("%Y%m%d")

            data = self.get(
                f"/v1.0/devices/{device_id}/statistics/days",
                params={
                    "code":      "add_ele",
                    "start_day": debut_j,
                    "end_day":   fin_j,
                },
            )
            if data.get("success"):
                jours = data.get("result", {}).get("days", {}) or {}
                tous_jours.update(jours)

            # Mois suivant
            if curseur.month == 12:
                curseur = datetime(curseur.year + 1, 1, 1)
            else:
                curseur = datetime(curseur.year, curseur.month + 1, 1)

        return tous_jours

    def get_stats_heures(self, device_id: str, jours: int = 7) -> dict:
        """
        Statistiques horaires add_ele (kWh) sur les N derniers jours
        (max 7 — limite API Tuya). Un appel par jour.
        Ne retourne que les heures avec consommation > 0.
        """
        toutes_heures: dict[str, str] = {}
        now = datetime.now()
        for i in range(jours - 1, -1, -1):
            date     = now - timedelta(days=i)
            jour_str = date.strftime("%Y%m%d")

            data = self.get(
                f"/v1.0/devices/{device_id}/statistics/hours",
                params={
                    "code":       "add_ele",
                    "one_day":    jour_str,
                    "start_hour": jour_str + "00",
                    "end_hour":   jour_str + "23",
                },
            )
            if data.get("success"):
                heures = data.get("result", {}).get("hours", {}) or {}
                heures_actives = {
                    k: v for k, v in heures.items()
                    if float(v or 0) > 0
                }
                toutes_heures.update(heures_actives)
        return toutes_heures

    def get_stats_15min(self, device_id: str, jours: int = 7) -> dict:
        """
        Statistiques par quart dheure add_ele (kWh) sur les N derniers jours.

        Lendpoint /statistics/quarters ne renvoie quune journee par
        appel (96 quarts max). On boucle donc jour par jour.
        """
        tous_quarts: dict[str, str] = {}
        now = datetime.now()
        for i in range(jours - 1, -1, -1):
            date     = now - timedelta(days=i)
            jour_str = date.strftime("%Y%m%d")
            debut_q  = jour_str + "0000"
            fin_q    = jour_str + "2345"

            data = self.get(
                f"/v1.0/devices/{device_id}/statistics/quarters",
                params={
                    "code":         "add_ele",
                    "start_minute": debut_q,
                    "end_minute":   fin_q,
                },
            )
            if data.get("success"):
                quarts = data.get("result", {}).get("quarters", {}) or {}
                tous_quarts.update(quarts)
        return tous_quarts

    def get_total(self, device_id: str) -> str:
        """Total cumule de consommation en kWh pour un appareil."""
        data = self.get(
            f"/v1.0/devices/{device_id}/statistics/total",
            params={"code": "add_ele"},
        )
        if data.get("success"):
            return str(data.get("result", {}).get("total", "0"))
        return "0"

