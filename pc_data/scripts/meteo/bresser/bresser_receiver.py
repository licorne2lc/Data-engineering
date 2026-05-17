# -*- coding: utf-8 -*-
"""
bresser_receiver.py
===================
Serveur HTTP récepteur pour station météo Bresser WiFi.

La station Bresser pousse ses relevés toutes les ~16 secondes via des requêtes
GET au format WeatherUnderground vers :
    GET /weatherstation/updateweatherstation.php?ID=...&tempf=...&humidity=...&...

Ce serveur :
  1. Reçoit ces requêtes et convertit les valeurs impériales → métriques
  2. Écrit une ligne par relevé dans un CSV journalier :
       <BRESSER_DATA_DIR>/<YYYY>/<MM>/<YYYY-MM-DD>.csv
  3. Expose /status pour un healthcheck rapide
  4. Expose /latest pour voir le dernier relevé (JSON)

Usage (direct) :
    python bresser_receiver.py

Variables d'environnement :
    BRESSER_DATA_DIR   Répertoire racine des CSV
                       (défaut : D:\\projet_dataoz\\pc_data\\data\\curated\\météo\\bresser)
    BRESSER_PORT       Port d'écoute (défaut : 8765)

Configuration de la station :
    Dans l'appli WSlink (ou l'interface web 192.168.4.1),
    saisir l'IP locale de ton PC (ex. 192.168.1.X) et le port 8765.
    Le path attendu est automatiquement /weatherstation/updateweatherstation.php.
"""

import csv
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlparse

from http.server import BaseHTTPRequestHandler, HTTPServer

# ── Configuration ────────────────────────────────────────────────────────────

DEFAULT_DATA_DIR = Path(
    os.environ.get(
        "BRESSER_DATA_DIR",
        r"D:\projet_dataoz\pc_data\data\curated\météo\bresser",
    )
)
PORT = int(os.environ.get("BRESSER_PORT", "8765"))

# Colonnes du CSV de sortie (toutes en unités métriques)
CSV_COLUMNS = [
    "ts",                   # Timestamp ISO 8601 UTC de réception
    "dateutc",              # Timestamp fourni par la station (UTC)
    "temp_ext_c",           # Température extérieure (°C)
    "temp_int_c",           # Température intérieure (°C)
    "humidite_ext_pct",     # Humidité extérieure (%)
    "humidite_int_pct",     # Humidité intérieure (%)
    "pression_hpa",         # Pression atmosphérique (hPa)
    "vitesse_vent_kmh",     # Vitesse du vent (km/h)
    "rafale_vent_kmh",      # Rafale max (km/h)
    "direction_vent_deg",   # Direction du vent (°)
    "pluie_mm_h",           # Pluie horaire (mm/h)
    "pluie_mm_jour",        # Pluie journalière (mm)
    "pluie_mm_semaine",     # Pluie hebdomadaire (mm)
    "pluie_mm_mois",        # Pluie mensuelle (mm)
    "pluie_mm_annee",       # Pluie annuelle (mm)
    "rayonnement_wm2",      # Rayonnement solaire (W/m²)
    "uv_index",             # Indice UV
    "point_rosee_c",        # Point de rosée (°C)
    "raw_query",            # Paramètres bruts reçus (pour audit)
]

# ── Conversions impériales → métriques ───────────────────────────────────────

def f2c(f: Optional[float]) -> Optional[float]:
    """Fahrenheit → Celsius."""
    if f is None:
        return None
    return round((f - 32) * 5 / 9, 2)


def mph2kmh(mph: Optional[float]) -> Optional[float]:
    """Miles/h → km/h."""
    if mph is None:
        return None
    return round(mph * 1.60934, 2)


def inhg2hpa(inhg: Optional[float]) -> Optional[float]:
    """Inch Hg → hPa."""
    if inhg is None:
        return None
    return round(inhg * 33.8639, 2)


def inch2mm(inch: Optional[float]) -> Optional[float]:
    """Inches → millimètres."""
    if inch is None:
        return None
    return round(inch * 25.4, 2)


def _float(params: Dict, key: str) -> Optional[float]:
    """Extrait un float depuis les query params (liste de valeurs)."""
    vals = params.get(key, [])
    if not vals or vals[0] in ("", "None", "null"):
        return None
    try:
        return float(vals[0])
    except (ValueError, TypeError):
        return None


def _str(params: Dict, key: str) -> Optional[str]:
    vals = params.get(key, [])
    return vals[0] if vals else None


# ── Parsing et conversion ─────────────────────────────────────────────────────

def parse_wu_params(params: Dict[str, list]) -> Dict[str, Any]:
    """
    Convertit les paramètres WU (impériaux) vers le format CSV métrique.
    """
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    row: Dict[str, Any] = {
        "ts":                  now_utc,
        "dateutc":             _str(params, "dateutc"),
        "temp_ext_c":          f2c(_float(params, "tempf")),
        "temp_int_c":          f2c(_float(params, "indoortempf")),
        "humidite_ext_pct":    _float(params, "humidity"),
        "humidite_int_pct":    _float(params, "indoorhumidity"),
        "pression_hpa":        inhg2hpa(_float(params, "baromin")),
        "vitesse_vent_kmh":    mph2kmh(_float(params, "windspeedmph")),
        "rafale_vent_kmh":     mph2kmh(_float(params, "windgustmph")),
        "direction_vent_deg":  _float(params, "winddir"),
        "pluie_mm_h":          inch2mm(_float(params, "rainin")),
        "pluie_mm_jour":       inch2mm(_float(params, "dailyrainin")),
        "pluie_mm_semaine":    inch2mm(_float(params, "weeklyrainin")),
        "pluie_mm_mois":       inch2mm(_float(params, "monthlyrainin")),
        "pluie_mm_annee":      inch2mm(_float(params, "yearlyrainin")),
        "rayonnement_wm2":     _float(params, "solarradiation"),
        "uv_index":            _float(params, "UV"),
        "point_rosee_c":       f2c(_float(params, "dewptf")),
        "raw_query":           str({k: v[0] if len(v) == 1 else v
                                    for k, v in params.items()
                                    if k not in ("ID", "PASSWORD")}),
    }
    return row


# ── Écriture CSV ──────────────────────────────────────────────────────────────

_write_lock = threading.Lock()


def append_to_csv(row: Dict[str, Any], data_dir: Path) -> Path:
    """
    Écrit la ligne dans le CSV journalier correspondant à ts.
    Crée le fichier (avec en-tête) si nécessaire.
    Retourne le chemin du fichier écrit.
    """
    ts = row["ts"]  # "2024-01-15T14:30:00Z"
    dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")

    csv_dir = data_dir / dt.strftime("%Y") / dt.strftime("%m")
    csv_path = csv_dir / dt.strftime("%Y-%m-%d.csv")

    with _write_lock:
        csv_dir.mkdir(parents=True, exist_ok=True)
        file_exists = csv_path.exists()
        with open(csv_path, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, delimiter=";",
                                    extrasaction="ignore")
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)

    return csv_path


# ── Serveur HTTP ──────────────────────────────────────────────────────────────

_latest_row: Dict[str, Any] = {}
_counter: int = 0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("bresser_receiver")


class BresserHandler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        # Supprime les logs HTTP bruts (trop verbeux)
        pass

    def do_GET(self):
        global _latest_row, _counter

        parsed = urlparse(self.path)

        # ── Endpoint de réception station ───────────────────────────────
        if parsed.path in (
            "/weatherstation/updateweatherstation.php",
            "/weatherstation/updateweatherstation",
        ):
            params = parse_qs(parsed.query, keep_blank_values=True)
            row = parse_wu_params(params)

            try:
                csv_path = append_to_csv(row, DEFAULT_DATA_DIR)
                _latest_row = row
                _counter += 1
                if _counter % 20 == 0:  # log toutes les ~5 min (20 × 16 s)
                    log.info(
                        "Relevé #%d | ext=%.1f°C hum=%s%% | fichier=%s",
                        _counter,
                        row.get("temp_ext_c") or 0,
                        row.get("humidite_ext_pct", "?"),
                        csv_path.name,
                    )
            except Exception as e:
                log.error("Erreur écriture CSV : %s", e)

            # La station attend un "success" ou simplement 200
            self._send_text(200, "success")

        # ── Healthcheck ─────────────────────────────────────────────────
        elif parsed.path == "/status":
            self._send_text(
                200,
                f"bresser_receiver OK | relevés reçus: {_counter}",
            )

        # ── Dernier relevé JSON ──────────────────────────────────────────
        elif parsed.path == "/latest":
            import json
            body = json.dumps(_latest_row, ensure_ascii=False, indent=2)
            self._send_text(200, body, content_type="application/json")

        else:
            self._send_text(404, "Not found")

    def _send_text(self, code: int, body: str,
                   content_type: str = "text/plain; charset=utf-8"):
        data = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


# ── Point d'entrée ────────────────────────────────────────────────────────────

def main():
    DEFAULT_DATA_DIR.mkdir(parents=True, exist_ok=True)
    log.info("Bresser Receiver démarré")
    log.info("  Port         : %d", PORT)
    log.info("  Données vers : %s", DEFAULT_DATA_DIR)
    log.info("  Endpoint     : /weatherstation/updateweatherstation.php")
    log.info("  Healthcheck  : http://localhost:%d/status", PORT)
    log.info("  Dernier rel. : http://localhost:%d/latest", PORT)

    server = HTTPServer(("0.0.0.0", PORT), BresserHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Arrêt du serveur.")
        server.server_close()


if __name__ == "__main__":
    main()
