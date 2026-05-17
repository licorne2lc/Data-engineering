# -*- coding: utf-8 -*-
"""
explore_bresser_station.py
===========================
Script de diagnostic pour la station météo Bresser WSX3001.

  1. Ping TCP (vérifie la liaison réseau sur le port 80)
  2. Sonde les endpoints HTTP connus (API JSON, interface web)
  3. Explore récursivement les répertoires accessibles (CSV, logs, USB…)
  4. Sauvegarde les réponses CSV/JSON intéressantes

Lancer depuis Windows (hors Docker) :
    python explore_bresser_station.py

Variables d'environnement :
    BRESSER_IP      IP de la station  (défaut : 10.253.1.17)
    BRESSER_PORT    Port HTTP         (défaut : 80)
    BRESSER_TIMEOUT Timeout (sec)     (défaut : 4)
"""

import json
import os
import socket
import subprocess
import sys
import time
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from urllib import error, request
from urllib.parse import urljoin, urlparse

# ── Configuration ─────────────────────────────────────────────────────────────
STATION_IP   = os.environ.get("BRESSER_IP",      "10.253.1.17")
STATION_PORT = int(os.environ.get("BRESSER_PORT", "80"))
TIMEOUT      = int(os.environ.get("BRESSER_TIMEOUT", "4"))
BASE_URL     = f"http://{STATION_IP}:{STATION_PORT}"

SAVE_DIR = Path(os.environ.get(
    "BRESSER_DATA_DIR",
    r"D:\projet_dataoz\pc_data\data\curated\météo\bresser",
)) / "explore_results"

# ── Couleurs ANSI ──────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
GREY   = "\033[90m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

# ═══════════════════════════════════════════════════════════════════════════════
# 1. PING / LIAISON RÉSEAU
# ═══════════════════════════════════════════════════════════════════════════════

def ping_icmp(ip: str, count: int = 4) -> dict:
    """Ping ICMP via la commande système (Windows)."""
    try:
        cmd = ["ping", "-n", str(count), "-w", "1000", ip]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        output = result.stdout

        # Extraire les stats du ping Windows
        reachable = "TTL=" in output or "ttl=" in output
        lines = [l.strip() for l in output.splitlines() if l.strip()]

        # Chercher la ligne de stats (paquets envoyés/reçus/perdus)
        stats_line = next((l for l in lines if "Envoyés" in l or "Sent" in l or "envoyés" in l), "")
        rtt_line   = next((l for l in lines if "ms" in l and ("min" in l.lower() or "Minimum" in l)), "")

        return {
            "ok":       reachable,
            "output":   output.strip(),
            "stats":    stats_line,
            "rtt":      rtt_line,
        }
    except Exception as e:
        return {"ok": False, "output": str(e), "stats": "", "rtt": ""}


def ping_tcp(ip: str, port: int, timeout: int = 3) -> dict:
    """Vérifie si le port TCP est ouvert (connexion socket)."""
    t0 = time.time()
    try:
        sock = socket.create_connection((ip, port), timeout=timeout)
        sock.close()
        ms = round((time.time() - t0) * 1000)
        return {"ok": True, "ms": ms}
    except Exception as e:
        return {"ok": False, "ms": None, "err": str(e)}


# Ports à scanner sur la station
SCAN_PORTS = [
    (80,   "HTTP"),
    (8080, "HTTP alt"),
    (8081, "HTTP alt"),
    (8888, "HTTP alt"),
    (443,  "HTTPS"),
    (8443, "HTTPS alt"),
    (21,   "FTP"),
    (22,   "SSH"),
    (23,   "Telnet"),
    (4567, "HTTP IoT"),
    (5000, "HTTP IoT"),
    (8765, "HTTP IoT"),
    (9000, "HTTP IoT"),
    (1883, "MQTT"),
    (502,  "Modbus"),
]


def scan_ports(ip: str) -> list:
    """Scanne les ports courants et retourne ceux qui sont ouverts."""
    print(f"  {CYAN}[SCAN PORTS]{RESET}  Scan de {len(SCAN_PORTS)} ports sur {ip} …", flush=True)
    open_ports = []
    for port, label in SCAN_PORTS:
        r = ping_tcp(ip, port, timeout=1)
        if r["ok"]:
            print(f"  {GREEN}  ✅ Port {port:>5}  ({label})  — ouvert  ({r['ms']} ms){RESET}")
            open_ports.append((port, label))
        else:
            print(f"  {GREY}     Port {port:>5}  ({label})  — fermé{RESET}")
    return open_ports


def run_ping_section():
    print(f"\n{BOLD}{'═'*65}{RESET}")
    print(f"{BOLD}  1. VÉRIFICATION LIAISON RÉSEAU → {STATION_IP}{RESET}")
    print(f"{BOLD}{'═'*65}{RESET}\n")

    # ── Ping ICMP ─────────────────────────────────────────────────────────────
    print(f"  {CYAN}[ICMP PING]{RESET}  ping -n 4 {STATION_IP} …", flush=True)
    icmp = ping_icmp(STATION_IP)
    if icmp["ok"]:
        print(f"  {GREEN}✅ ICMP : station joignable{RESET}")
        if icmp["rtt"]:
            print(f"     {icmp['rtt']}")
    else:
        print(f"  {YELLOW}⚠️  ICMP : pas de réponse (firewall possible ?){RESET}")
    print()

    # ── Scan de ports ─────────────────────────────────────────────────────────
    open_ports = scan_ports(STATION_IP)
    print()

    if not open_ports:
        print(f"  {RED}❌ Aucun port ouvert détecté sur {STATION_IP}.{RESET}")
        print(f"  {YELLOW}→ La station est joignable (ping OK) mais n'accepte aucune connexion.")
        print(f"     Elle ne semble pas exposer de serveur HTTP/FTP accessible.{RESET}")
        return None

    # Choisir le meilleur port HTTP pour la suite
    http_ports = [(p, l) for p, l in open_ports if "HTTP" in l]
    ftp_ports  = [(p, l) for p, l in open_ports if "FTP"  in l]

    chosen_port = None
    if http_ports:
        chosen_port = http_ports[0][0]
        print(f"  {GREEN}→ Port HTTP retenu pour l'exploration : {chosen_port}{RESET}")
    elif ftp_ports:
        print(f"  {YELLOW}→ Seul FTP (port 21) est ouvert — pas d'interface HTTP.")
        print(f"     Un accès FTP aux fichiers CSV de la clé USB est possible !{RESET}")
    else:
        print(f"  {YELLOW}→ Ports ouverts : {open_ports} — pas de HTTP détecté.{RESET}")

    return chosen_port, open_ports


# ═══════════════════════════════════════════════════════════════════════════════
# 2. EXPLORATION FTP (si port 21 ouvert — accès direct aux fichiers USB)
# ═══════════════════════════════════════════════════════════════════════════════

def run_ftp_exploration(ip: str):
    """Tente de lister et télécharger les fichiers CSV via FTP (anonyme)."""
    import ftplib

    print(f"\n{BOLD}{'═'*65}{RESET}")
    print(f"{BOLD}  FTP — EXPLORATION DES FICHIERS SUR {ip}:21{RESET}")
    print(f"{BOLD}{'═'*65}{RESET}\n")

    credentials = [
        ("anonymous", "dataoz@local"),
        ("admin",     "admin"),
        ("admin",     ""),
        ("user",      "user"),
        ("bresser",   "bresser"),
        ("",          ""),
    ]

    ftp = None
    for user, pwd in credentials:
        try:
            ftp = ftplib.FTP(timeout=TIMEOUT)
            ftp.connect(ip, 21)
            ftp.login(user, pwd)
            print(f"  {GREEN}✅ Connexion FTP réussie (user={user!r}){RESET}\n")
            break
        except ftplib.error_perm as e:
            print(f"  {GREY}  ✗ user={user!r} : {e}{RESET}")
            ftp = None
        except Exception as e:
            print(f"  {RED}Erreur FTP : {e}{RESET}")
            ftp = None
            break

    if ftp is None:
        print(f"  {RED}❌ Impossible de se connecter au FTP (tous les identifiants refusés).{RESET}")
        return

    # Lister récursivement
    def list_dir(path="/", depth=0):
        indent = "  " + "  " * depth
        try:
            items = []
            ftp.retrlines(f"LIST {path}", items.append)
            for item in items:
                parts = item.split()
                if not parts:
                    continue
                name    = parts[-1]
                is_dir  = item.startswith("d")
                is_link = item.startswith("l")
                full    = f"{path.rstrip('/')}/{name}"

                if is_dir and name not in (".", ".."):
                    print(f"{indent}{CYAN}📁 {full}/{RESET}")
                    list_dir(full + "/", depth + 1)
                elif not is_dir:
                    ext  = Path(name).suffix.lower()
                    icon = "📊" if ext == ".csv" else "📄"
                    size = parts[4] if len(parts) > 4 else "?"
                    print(f"{indent}{GREEN}{icon} {full}  ({size} octets){RESET}")

                    # Télécharger les CSV
                    if ext == ".csv":
                        SAVE_DIR.mkdir(parents=True, exist_ok=True)
                        local = SAVE_DIR / name
                        try:
                            with open(local, "wb") as f:
                                ftp.retrbinary(f"RETR {full}", f.write)
                            print(f"{indent}  {GREEN}💾 Sauvegardé → {local}{RESET}")
                        except Exception as e:
                            print(f"{indent}  {RED}Erreur téléchargement : {e}{RESET}")
        except Exception as e:
            print(f"{indent}{YELLOW}Erreur listage {path} : {e}{RESET}")

    print(f"  Structure des fichiers :\n")
    list_dir("/")
    ftp.quit()
    print(f"\n  {GREEN}Fichiers CSV sauvegardés dans : {SAVE_DIR}{RESET}")


# ═══════════════════════════════════════════════════════════════════════════════
# 3. REQUÊTES HTTP
# ═══════════════════════════════════════════════════════════════════════════════

def http_get(path: str) -> dict:
    """Effectue un GET HTTP et retourne statut + corps."""
    url = f"{BASE_URL}{path}"
    try:
        req = request.Request(url, method="GET")
        req.add_header("User-Agent", "DataOZ-BresserExplorer/1.0")
        with request.urlopen(req, timeout=TIMEOUT) as resp:
            body  = resp.read(32768)          # 32 Ko max
            ctype = resp.headers.get("Content-Type", "")
            return {"ok": True, "status": resp.status, "body": body,
                    "ctype": ctype, "url": url}
    except error.HTTPError as e:
        return {"ok": False, "status": e.code,  "body": b"", "ctype": "", "url": url}
    except Exception as e:
        return {"ok": False, "status": 0, "body": b"", "ctype": "", "url": url,
                "err": str(e)}


# ── Parser HTML pour extraire les liens d'un listing de répertoire ─────────────
class LinkParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            for name, val in attrs:
                if name == "href" and val and not val.startswith(("http", "?", "#", "mailto")):
                    self.links.append(val)


def extract_links(html: bytes) -> list:
    try:
        parser = LinkParser()
        parser.feed(html.decode("utf-8", errors="replace"))
        return parser.links
    except Exception:
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# 3. EXPLORATION DES ENDPOINTS CONNUS
# ═══════════════════════════════════════════════════════════════════════════════

ENDPOINTS = [
    # Interface web
    ("/",                                   "Page d'accueil"),
    ("/index.html",                         "Index HTML"),
    ("/livedata.html",                      "Live data page"),
    ("/setup.html",                         "Setup page"),

    # API JSON Ecowitt / Fine Offset (même chipset que Bresser)
    ("/get_livedata_info",                  "Live data JSON (Ecowitt)"),
    ("/get_sensors_info",                   "Capteurs JSON"),
    ("/get_device_info",                    "Infos appareil JSON"),
    ("/get_ws_settings",                    "Paramètres station JSON"),
    ("/get_weather_services",               "Services météo JSON"),
    ("/get_calibration_info",               "Calibration JSON"),
    ("/get_units_info",                     "Unités JSON"),
    ("/get_iot_settings",                   "IoT settings JSON"),
    ("/get_network_info",                   "Réseau JSON"),

    # Accès fichiers / répertoires
    ("/data/",                              "Répertoire /data/"),
    ("/data/report/",                       "Rapports CSV"),
    ("/record/",                            "Enregistrements"),
    ("/logs/",                              "Logs"),
    ("/usb/",                               "Clé USB"),
    ("/sdcard/",                            "Carte SD"),
    ("/files/",                             "Fichiers"),
    ("/csv/",                               "CSV"),
    ("/download/",                          "Téléchargements"),
    ("/history/",                           "Historique"),
    ("/backup/",                            "Sauvegarde"),
    ("/export/",                            "Export"),

    # API divers
    ("/api/v1/data",                        "API v1"),
    ("/api/data",                           "API data"),
    ("/status",                             "Status"),
    ("/info",                               "Info"),
    ("/measure/now",                        "Mesure temps réel"),
]


def status_color(code: int) -> str:
    if code == 200:           return GREEN
    if code in (301,302,307): return YELLOW
    if code == 404:           return GREY
    if code == 0:             return RED
    return YELLOW


def fmt_body(body: bytes, ctype: str, maxlen: int = 500) -> str:
    if not body:
        return ""
    text = body.decode("utf-8", errors="replace").strip()
    if "json" in ctype.lower():
        try:
            parsed = json.loads(text)
            text = json.dumps(parsed, ensure_ascii=False, indent=2)
        except Exception:
            pass
    lines = text[:maxlen].splitlines()
    return "\n".join("      " + l for l in lines[:20])


def run_endpoints_section() -> list:
    print(f"\n{BOLD}{'═'*65}{RESET}")
    print(f"{BOLD}  2. SONDAGE DES ENDPOINTS HTTP{RESET}")
    print(f"{BOLD}{'═'*65}{RESET}\n")

    found = []

    for path, label in ENDPOINTS:
        r = http_get(path)
        c = status_color(r["status"])
        s = str(r["status"]) if r["status"] else "ERR"
        print(f"  {c}[{s:>3}]{RESET}  {path:<40}  {GREY}{label}{RESET}")

        if r["ok"] and r["status"] == 200:
            found.append((path, label, r))
            body_str = fmt_body(r["body"], r["ctype"])
            if body_str:
                print(body_str)
                print()

    return found


# ═══════════════════════════════════════════════════════════════════════════════
# 4. EXPLORATION RÉCURSIVE DES RÉPERTOIRES
# ═══════════════════════════════════════════════════════════════════════════════

def explore_directory(path: str, depth: int = 0, max_depth: int = 3,
                      visited: set = None) -> list:
    """Explore récursivement un répertoire HTTP et retourne les fichiers trouvés."""
    if visited is None:
        visited = set()
    if path in visited or depth > max_depth:
        return []
    visited.add(path)

    r = http_get(path)
    if not r["ok"] or r["status"] != 200:
        return []

    files = []
    body  = r["body"]
    ctype = r["ctype"]
    indent = "  " + "  " * depth

    # Si c'est un listing de répertoire HTML → extraire les liens
    if "html" in ctype.lower() or b"<html" in body[:200].lower():
        links = extract_links(body)
        for link in links:
            if link in ("../", "./", "/"):
                continue
            child_path = path.rstrip("/") + "/" + link.lstrip("/")
            if link.endswith("/"):
                # Sous-répertoire → explorer récursivement
                print(f"{indent}{CYAN}📁 {child_path}{RESET}")
                sub = explore_directory(child_path, depth + 1, max_depth, visited)
                files.extend(sub)
            else:
                # Fichier
                ext = Path(link).suffix.lower()
                icon = "📊" if ext == ".csv" else "📄"
                print(f"{indent}{GREEN}{icon} {child_path}{RESET}")
                files.append(child_path)

    # Si c'est du JSON ou du CSV → c'est déjà un fichier de données
    elif "json" in ctype.lower() or "csv" in ctype.lower() or "text" in ctype.lower():
        print(f"{indent}{GREEN}📊 {path}  ({len(body)} octets){RESET}")
        files.append(path)

    return files


def run_directory_exploration(found_endpoints: list) -> list:
    print(f"\n{BOLD}{'═'*65}{RESET}")
    print(f"{BOLD}  3. EXPLORATION RÉCURSIVE DES RÉPERTOIRES{RESET}")
    print(f"{BOLD}{'═'*65}{RESET}\n")

    # Répertoires racines à explorer
    dir_roots = ["/data/", "/record/", "/logs/", "/usb/", "/sdcard/",
                 "/files/", "/csv/", "/download/", "/history/", "/export/"]

    # Ajouter les endpoints trouvés qui semblent être des répertoires
    for path, label, r in found_endpoints:
        if path.endswith("/") and path not in dir_roots:
            dir_roots.append(path)

    all_files = []
    visited   = set()

    for root in dir_roots:
        r = http_get(root)
        if r["ok"] and r["status"] == 200:
            print(f"  {GREEN}📂 Exploration de {root}{RESET}")
            files = explore_directory(root, depth=1, max_depth=3, visited=visited)
            all_files.extend(files)
            if not files:
                print(f"    {GREY}(répertoire vide ou structure non reconnue){RESET}")
            print()
        # Silencieux si 404

    if not all_files:
        print(f"  {YELLOW}Aucun répertoire de fichiers accessible trouvé via HTTP.{RESET}")
        print(f"  {GREY}→ Les CSV de la clé USB ne sont probablement pas exposés via web.{RESET}")
        print(f"  {GREY}→ Le mode récepteur (bresser-receiver) est la méthode à utiliser.{RESET}")

    return all_files


# ═══════════════════════════════════════════════════════════════════════════════
# 5. SAUVEGARDE + RÉSUMÉ
# ═══════════════════════════════════════════════════════════════════════════════

def save_interesting(found_endpoints: list, file_paths: list):
    """Télécharge et sauvegarde les fichiers CSV/JSON trouvés."""
    interesting = [
        (p, l, r) for p, l, r in found_endpoints
        if (b"," in r["body"][:200] or b";" in r["body"][:200]
            or "csv" in r["ctype"].lower()
            or "json" in r["ctype"].lower())
        and len(r["body"]) > 50
    ]

    # Ajouter les fichiers CSV découverts dans l'exploration
    for fpath in file_paths:
        if fpath.lower().endswith((".csv", ".json", ".txt")):
            r = http_get(fpath)
            if r["ok"] and r["status"] == 200:
                interesting.append((fpath, "découvert", r))

    if interesting:
        SAVE_DIR.mkdir(parents=True, exist_ok=True)
        print(f"\n{BOLD}{'═'*65}{RESET}")
        print(f"{BOLD}  4. SAUVEGARDE DES DONNÉES{RESET}")
        print(f"{BOLD}{'═'*65}{RESET}\n")
        for path, label, r in interesting:
            fname = path.strip("/").replace("/", "_") or "root"
            ext   = Path(path).suffix or ".txt"
            fpath = SAVE_DIR / f"{fname}{ext}"
            fpath.write_bytes(r["body"])
            print(f"  {GREEN}💾 Sauvegardé : {fpath}  ({len(r['body'])} octets){RESET}")


def run_summary(found_endpoints: list, all_files: list, tcp_ok: bool):
    print(f"\n{BOLD}{'═'*65}{RESET}")
    print(f"{BOLD}  RÉSUMÉ FINAL{RESET}")
    print(f"{BOLD}{'═'*65}{RESET}\n")

    print(f"  Liaison réseau  : {'✅ OK' if tcp_ok else '❌ Inaccessible'}")
    print(f"  Endpoints 200   : {len(found_endpoints)} / {len(ENDPOINTS)}")
    print(f"  Fichiers trouvés: {len(all_files)}")

    if found_endpoints:
        print(f"\n  {GREEN}Endpoints accessibles :{RESET}")
        for path, label, r in found_endpoints:
            print(f"    • {path}  →  {label}  [{len(r['body'])} octets]")

    if all_files:
        print(f"\n  {GREEN}Fichiers accessibles :{RESET}")
        for f in all_files:
            print(f"    • {f}")

    print(f"""
  {BOLD}PROCHAINE ÉTAPE :{RESET}""")

    if any("/get_livedata_info" in p for p, _, _ in found_endpoints):
        print(f"""  {GREEN}✅ L'API JSON Ecowitt est disponible !
  → Le DAG peut interroger directement http://{STATION_IP}/get_livedata_info
  → Mettez à jour dag_bresser_meteo.py pour utiliser cet endpoint.{RESET}""")
    elif all_files:
        print(f"""  {GREEN}✅ Des fichiers sont accessibles via HTTP.
  → Vérifiez les fichiers sauvegardés dans :
     {SAVE_DIR}{RESET}""")
    else:
        print(f"""  {YELLOW}→ Aucun accès direct aux CSV de la clé USB détecté.
  → Configurez la station pour pousser vers le récepteur :
       Serveur : 10.253.1.27   Port : 8765
       Path    : /weatherstation/updateweatherstation.php
  → Puis démarrez : docker compose up -d bresser-receiver{RESET}""")

    print()


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    global BASE_URL, STATION_PORT

    print(f"\n{BOLD}{'═'*65}")
    print(f"  EXPLORATION STATION BRESSER WSX3001 — {STATION_IP}")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'═'*65}{RESET}")

    # 1. Ping ICMP + scan de ports
    result = run_ping_section()

    if result is None:
        print(f"\n  {RED}Arrêt : aucun port accessible sur {STATION_IP}.{RESET}\n")
        sys.exit(1)

    chosen_port, open_ports = result

    # Vérifier si FTP disponible sans HTTP
    ftp_open = any(p == 21 for p, _ in open_ports)
    if ftp_open and chosen_port is None:
        run_ftp_exploration(STATION_IP)
        sys.exit(0)

    if chosen_port is None:
        print(f"\n  {YELLOW}Pas de port HTTP disponible. La station n'expose pas d'interface web.{RESET}")
        print(f"  {YELLOW}→ Utilisez le mode récepteur (bresser-receiver) pour collecter les données.{RESET}\n")
        sys.exit(0)

    # Adapter l'URL de base au port détecté
    STATION_PORT = chosen_port
    BASE_URL     = f"http://{STATION_IP}:{chosen_port}"
    print(f"\n  {CYAN}→ Exploration HTTP sur {BASE_URL}{RESET}")

    # 2. Endpoints connus
    found = run_endpoints_section()

    # 3. Exploration récursive des répertoires
    all_files = run_directory_exploration(found)

    # 4. Sauvegarde
    save_interesting(found, all_files)

    # 5. Résumé
    run_summary(found, all_files, True)


if __name__ == "__main__":
    main()
