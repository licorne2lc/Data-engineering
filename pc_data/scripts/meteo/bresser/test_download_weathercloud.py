# -*- coding: utf-8 -*-
"""
test_download_weathercloud.py
==============================
Script autonome pour tester le téléchargement du CSV Weathercloud.
Exécution :
    python test_download_weathercloud.py

Prérequis :
    pip install playwright
    playwright install chromium
"""

import os
import shutil
import time
import logging
from datetime import date
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────

EMAIL       = os.environ.get("WEATHERCLOUD_EMAIL",    "licorne2lc@msn.com")
PASSWORD    = os.environ.get("WEATHERCLOUD_PASSWORD",  "Wilmoul17@")
STATION_ID  = os.environ.get("WEATHERCLOUD_STATION_ID","92fc230f9ee474d9")

# Répertoires de sortie (adaptez si besoin)
TMP_DIR  = Path(r"D:\projet_dataoz\pc_data\data\tmp\weathercloud_dl")
RAW_DIR  = Path(r"D:\projet_dataoz\pc_data\data\raw\météo_bresser")

# Mois à télécharger (mois courant par défaut)
TODAY   = date.today()
YEAR    = TODAY.year
MONTH   = TODAY.month

MOIS_FR = {
    1:"Janvier", 2:"Février",  3:"Mars",     4:"Avril",
    5:"Mai",     6:"Juin",     7:"Juillet",  8:"Août",
    9:"Septembre",10:"Octobre",11:"Novembre",12:"Décembre",
}

WC_LOGIN_URL    = "https://app.weathercloud.net/signin"
WC_DATABASE_URL = "https://app.weathercloud.net/database"

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Fonctions ─────────────────────────────────────────────────────────────────

def screenshot(page, tmp_path, name):
    path = tmp_path / f"debug_{name}.png"
    try:
        page.screenshot(path=str(path), full_page=True)
        log.info("📸 Screenshot → %s", path)
    except Exception as e:
        log.warning("Screenshot échoué : %s", e)


def run(year: int, month: int):
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

    tmp_path = TMP_DIR
    raw_path = RAW_DIR / f"{year}-{month:02d}"
    tmp_path.mkdir(parents=True, exist_ok=True)
    raw_path.mkdir(parents=True, exist_ok=True)

    log.info("═" * 60)
    log.info("Téléchargement Weathercloud  %d-%02d", year, month)
    log.info("Tmp  : %s", tmp_path)
    log.info("Raw  : %s", raw_path)
    log.info("═" * 60)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage",
                  "--disable-gpu", "--window-size=1400,900"],
        )
        ctx = browser.new_context(
            accept_downloads=True,
            locale="fr-FR",
            timezone_id="Europe/Paris",
        )
        page = ctx.new_page()
        page.set_default_timeout(20_000)

        # ── 1. Login ──────────────────────────────────────────────────────────
        log.info("1/4  Login…")
        page.goto(WC_LOGIN_URL, wait_until="domcontentloaded", timeout=30_000)
        time.sleep(3)
        screenshot(page, tmp_path, "01_login_page")

        # Remplir email
        for sel in ['input[placeholder*="mail" i]', 'input[type="email"]',
                    'input[name="email"]', 'input[name="UserLogin[email]"]']:
            try:
                page.fill(sel, EMAIL, timeout=5_000)
                log.info("   Email saisi via : %s", sel)
                break
            except PWTimeout:
                continue

        # Remplir mot de passe
        for sel in ['input[type="password"]', 'input[name="password"]',
                    'input[name="UserLogin[password]"]']:
            try:
                page.fill(sel, PASSWORD, timeout=5_000)
                log.info("   Password saisi via : %s", sel)
                break
            except PWTimeout:
                continue

        # Submit
        submitted = False
        for sel in ['button[type="submit"]', 'input[type="submit"]',
                    '.btn-primary', 'button:has-text("Sign in")',
                    'button:has-text("Connexion")', 'button:has-text("Entrar")']:
            try:
                page.click(sel, timeout=3_000)
                submitted = True
                log.info("   Submit cliqué : %s", sel)
                break
            except PWTimeout:
                continue
        if not submitted:
            page.keyboard.press("Enter")
            log.info("   Submit via Enter")

        page.wait_for_load_state("domcontentloaded", timeout=20_000)
        time.sleep(2)
        screenshot(page, tmp_path, "02_post_login")
        log.info("   URL post-login : %s", page.url)

        if any(x in page.url for x in ["signin", "login", "sign_in"]):
            screenshot(page, tmp_path, "02_login_FAILED")
            raise RuntimeError(f"Échec login — toujours sur {page.url}")
        log.info("   ✅ Login OK")

        # ── 2. Page Base de données ───────────────────────────────────────────
        log.info("2/4  Navigation → %s", WC_DATABASE_URL)
        page.goto(WC_DATABASE_URL, wait_until="domcontentloaded", timeout=30_000)
        time.sleep(4)
        screenshot(page, tmp_path, "03_database_page")
        log.info("   URL : %s", page.url)

        # ── 3. Sélection via les IDs connus ──────────────────────────────────
        log.info("3/4  Sélection des filtres…")

        # Appareil
        try:
            page.locator("#database-select-device").select_option(index=1)
            log.info("   ✅ Appareil sélectionné (index 1)")
            time.sleep(1)
        except Exception as e:
            log.warning("   ⚠️  Sélection appareil échouée : %s", e)

        # Année
        try:
            page.locator("#database-select-year").select_option(label=str(year))
            log.info("   ✅ Année : %d", year)
            time.sleep(1)
        except Exception as e:
            log.warning("   ⚠️  Sélection année échouée : %s", e)

        # Mois
        try:
            page.locator("#database-select-month").select_option(label=MOIS_FR[month])
            log.info("   ✅ Mois  : %s", MOIS_FR[month])
            time.sleep(2)
        except Exception as e:
            log.warning("   ⚠️  Sélection mois échouée : %s", e)

        screenshot(page, tmp_path, "04_filtres_selectionnes")

        # ── 4. Export ─────────────────────────────────────────────────────────
        log.info("4/4  Clic sur Exporter…")

        export_selectors = [
            "a.database-button",
            "a.btn.btn-primary.database-button",
            "a:has-text('Exporter')",
            "button:has-text('Exporter')",
            "a:has-text('Export')",
        ]

        with ctx.expect_download(timeout=60_000) as dl_info:
            clicked = False
            for sel in export_selectors:
                try:
                    page.wait_for_selector(sel, timeout=5_000, state="visible")
                    page.click(sel, timeout=5_000)
                    log.info("   Bouton cliqué : %s", sel)
                    clicked = True
                    break
                except Exception:
                    continue

            if not clicked:
                screenshot(page, tmp_path, "05_export_NOT_FOUND")
                # Lister tous les liens/boutons visibles pour debug
                elements = page.evaluate("""
                    () => Array.from(document.querySelectorAll('a, button')).map(el => ({
                        tag:  el.tagName,
                        text: el.innerText.trim().slice(0, 60),
                        cls:  el.className,
                        href: el.href || ''
                    })).filter(el => el.text.length > 0)
                """)
                log.info("   Éléments cliquables sur la page :")
                for el in elements:
                    log.info("     [%s] cls='%s' text='%s' href='%s'",
                             el['tag'], el['cls'], el['text'], el['href'])
                raise RuntimeError(
                    "Bouton Exporter introuvable — voir screenshot debug_05_export_NOT_FOUND.png"
                )

        download = dl_info.value
        fname    = download.suggested_filename or f"weathercloud_bresser_{year}-{month:02d}.csv"
        tmp_file = tmp_path / fname
        download.save_as(str(tmp_file))

        raw_file = raw_path / f"weathercloud_bresser_{year}-{month:02d}.csv"
        shutil.move(str(tmp_file), str(raw_file))

        log.info("═" * 60)
        log.info("✅ Fichier téléchargé : %s", raw_file)
        log.info("   Taille : %d octets", raw_file.stat().st_size)
        log.info("═" * 60)

        browser.close()
        return raw_file


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        result = run(YEAR, MONTH)
        print(f"\n✅ Succès : {result}")
    except Exception as e:
        log.error("❌ Erreur : %s", e)
        raise
