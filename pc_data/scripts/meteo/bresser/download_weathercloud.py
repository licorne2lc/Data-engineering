# -*- coding: utf-8 -*-
"""
download_weathercloud.py
=========================
Télécharge le CSV hebdomadaire depuis Weathercloud.net via Playwright.

Corrections v3 :
  - Playwright isolé dans un thread (évite "This event loop is already running")
  - Sélecteurs login validés depuis les logs (placeholder mail + type password)
  - Tentative URL directe /device/csv/{id} avant navigation UI
  - Screenshots à chaque étape pour debug
"""

import logging
import os
import shutil
import threading
import time
from datetime import date, timedelta
from pathlib import Path

log = logging.getLogger(__name__)

WC_BASE        = "https://app.weathercloud.net"
WC_LOGIN_URL   = f"{WC_BASE}/signin"
WC_DATABASE_URL = f"{WC_BASE}/database"
WAIT_NAV       = 4    # secondes après navigation
WAIT_DL        = 30   # timeout téléchargement (secondes)

# Noms des mois en français (Weathercloud FR)
MOIS_FR = {
    1:"Janvier", 2:"Février",  3:"Mars",     4:"Avril",
    5:"Mai",     6:"Juin",     7:"Juillet",  8:"Août",
    9:"Septembre",10:"Octobre",11:"Novembre",12:"Décembre",
}


# ── Calcul plage de dates ─────────────────────────────────────────────────────

def get_target_month(ref_date: date = None):
    """
    Retourne (année, mois) du mois en cours.
    Le DAG étant déclenché manuellement, on télécharge le mois courant.
    """
    if ref_date is None:
        ref_date = date.today()
    return ref_date.year, ref_date.month


def raw_filename(run_date: date = None) -> str:
    """
    Nom du fichier téléchargé : weathercloud_bresser_YYYY-MM-DD.csv
    Utilise la date du jour (date d'exécution du DAG).
    """
    if run_date is None:
        run_date = date.today()
    return f"weathercloud_bresser_{run_date.strftime('%Y-%m-%d')}.csv"


# ── Cœur Playwright (tourne dans un thread dédié) ────────────────────────────

def _playwright_run(email, password, station_id, tmp_path, raw_path,
                    year, month, result_holder, run_date=None):
    """
    Tout le code Playwright s'exécute ici, dans un thread séparé,
    pour éviter le conflit avec l'event loop d'Airflow.
    """
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

    tmp_path.mkdir(parents=True, exist_ok=True)
    raw_path.mkdir(parents=True, exist_ok=True)

    def screenshot(name):
        path = tmp_path / f"debug_{name}.png"
        try:
            page.screenshot(path=str(path), full_page=True)
            log.info("Screenshot : %s", path)
        except Exception:
            pass

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
        page.set_default_timeout(15_000)
        page.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
        )

        def dismiss_cookies():
            """Ferme les popups de consentement cookies."""
            for sel in [
                "button.fc-button.fc-cta-consent",
                "button[class*='fc-cta-consent']",
                "button:has-text('Autoriser')",
                ".btn.btn-primary.pull-right:has-text('I agree')",
                "button:has-text('I agree')",
                "button:has-text('Accept')",
            ]:
                try:
                    page.click(sel, timeout=2_000)
                    log.info("Cookie popup fermée : %s", sel)
                    time.sleep(1)
                    return
                except Exception:
                    continue

        # ── 1. Login ──────────────────────────────────────────────────────
        log.info("Login Weathercloud…")
        page.goto(WC_LOGIN_URL, wait_until="domcontentloaded", timeout=30_000)
        time.sleep(3)
        dismiss_cookies()
        screenshot("01_login_page")

        # Sélecteurs validés par les logs précédents
        try:
            page.fill('input[placeholder*="mail" i]', email, timeout=8000)
        except PWTimeout:
            # fallback
            page.fill('input[type="email"], input[name="email"]', email)

        try:
            page.fill('input[type="password"]', password, timeout=5000)
        except PWTimeout:
            page.fill('input[name="password"]', password)

        # Submit : bouton ou Enter
        submitted = False
        for sel in ['button[type="submit"]', 'input[type="submit"]',
                    '.btn-primary', 'button:has-text("Sign in")',
                    'button:has-text("Entrar")', 'button:has-text("Log in")']:
            try:
                page.click(sel, timeout=2000)
                submitted = True
                log.info("Submit cliqué : %s", sel)
                break
            except PWTimeout:
                continue
        if not submitted:
            page.keyboard.press("Enter")
            log.info("Submit via Enter")

        page.wait_for_load_state("domcontentloaded", timeout=20_000)
        time.sleep(2)
        screenshot("02_post_login")
        log.info("URL post-login : %s", page.url)

        if any(x in page.url for x in ["signin", "login", "sign_in"]):
            screenshot("02_login_failed")
            raise RuntimeError(f"Échec login — toujours sur {page.url}")
        log.info("Login OK")

        # ── 2. Page "Base de données" ─────────────────────────────────────
        log.info("Navigation → %s", WC_DATABASE_URL)
        page.goto(WC_DATABASE_URL, wait_until="domcontentloaded", timeout=30_000)
        time.sleep(WAIT_NAV)
        dismiss_cookies()
        screenshot("03_database_page")
        log.info("URL database : %s", page.url)

        # ── 3. Sélection Appareil + Année + Mois ─────────────────────────
        # IDs confirmés par les logs : #database-select-device / -year / -month
        log.info("Sélection : Année=%d  Mois=%s (%d)",
                 year, MOIS_FR[month], month)

        # 3a. Appareil → premier index non-placeholder (ozoir-la-ferriere)
        try:
            page.locator("#database-select-device").select_option(index=1)
            log.info("Appareil sélectionné (index 1)")
            time.sleep(1)
        except Exception as exc:
            log.warning("Sélection appareil échouée : %s", exc)

        # 3b. Année (label = "2026", "2025" …)
        try:
            page.locator("#database-select-year").select_option(label=str(year))
            log.info("Année sélectionnée : %d", year)
            time.sleep(1)
        except Exception as exc:
            log.warning("Sélection année échouée : %s", exc)

        # 3c. Mois (label en français : "Janvier", "Avril" …)
        try:
            page.locator("#database-select-month").select_option(label=MOIS_FR[month])
            log.info("Mois sélectionné : %s", MOIS_FR[month])
            time.sleep(1)
        except Exception as exc:
            log.warning("Sélection mois échouée : %s", exc)

        dismiss_cookies()
        time.sleep(2)
        screenshot("04_after_dropdowns")

        # ── 4. Clic sur "Exporter" ────────────────────────────────────────
        log.info("Clic sur Exporter…")
        export_selectors = [
            "a.database-button",              # sélecteur exact Weathercloud
            "a.btn.btn-primary.database-button",
            "a:has-text('Exporter')",
            "button:has-text('Exporter')",
            "a:has-text('Export')",
            ".btn:has-text('Exporter')",
        ]

        with page.expect_download(timeout=WAIT_DL * 1000) as dl_info:
            clicked = False
            for sel in export_selectors:
                try:
                    page.wait_for_selector(sel, timeout=4000, state="visible")
                    page.click(sel, timeout=4000)
                    log.info("Bouton Exporter cliqué : %s", sel)
                    clicked = True
                    break
                except (PWTimeout, Exception):
                    continue

            if not clicked:
                screenshot("05_exporter_not_found")
                # Log tous les boutons pour debug
                btns = page.evaluate("""
                    Array.from(document.querySelectorAll('button, a.btn')).map(el => ({
                        tag:  el.tagName,
                        text: el.innerText.trim().substring(0, 50),
                        cls:  el.className,
                        href: el.href || ''
                    }))
                """)
                log.info("Boutons présents :")
                for b in btns:
                    log.info("  [%s] text='%s' cls='%s' href='%s'",
                             b['tag'], b['text'], b['cls'], b['href'])
                raise RuntimeError(
                    f"Bouton Exporter introuvable. "
                    f"Screenshot : {tmp_path}/debug_05_exporter_not_found.png"
                )

        download = dl_info.value
        fname    = raw_filename(run_date)
        tmp_file = tmp_path / (download.suggested_filename or fname)
        download.save_as(str(tmp_file))
        raw_file = raw_path / fname
        shutil.move(str(tmp_file), str(raw_file))
        log.info("✅ Fichier Raw : %s", raw_file)

        result_holder["file"] = raw_file
        browser.close()


# ── Fonction publique : lance Playwright dans un thread ──────────────────────

def download_csv(email, password, station_id, tmp_dir, raw_dir,
                 year: int, month: int, run_date: date = None) -> Path:
    """
    Lance le téléchargement dans un thread dédié pour éviter le conflit
    avec l'event loop Airflow ("This event loop is already running").

    year, month   : utilisés pour sélectionner le mois dans l'UI Weathercloud
    run_date      : date pour le nommage du fichier (défaut = date.today())
                    → weathercloud_bresser_YYYY-MM-DD.csv
    raw_dir       : répertoire de sortie (météo_bresser/weathercloud/ — sans sous-dossier daté)
    """
    if run_date is None:
        run_date = date.today()

    tmp_path = Path(tmp_dir)
    raw_path = Path(raw_dir)
    raw_path.mkdir(parents=True, exist_ok=True)

    result_holder = {"file": None, "error": None}

    def _target():
        try:
            _playwright_run(email, password, station_id,
                            tmp_path, raw_path,
                            year, month,
                            result_holder, run_date=run_date)
        except Exception as exc:
            result_holder["error"] = exc

    t = threading.Thread(target=_target, daemon=True)
    t.start()
    t.join(timeout=600)   # 10 min max

    if t.is_alive():
        raise RuntimeError("Timeout : Playwright n'a pas terminé en 10 minutes.")
    if result_holder["error"]:
        raise result_holder["error"]
    if result_holder["file"] is None:
        raise RuntimeError("Playwright n'a pas retourné de fichier.")

    return result_holder["file"]


# ── Standalone ────────────────────────────────────────────────────────────────

def main():
    import sys
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")

    email      = os.environ["WC_EMAIL"]
    password   = os.environ["WC_PASSWORD"]
    station_id = os.environ["WC_STATION_ID"]
    tmp_dir    = os.environ.get("WC_TMP_DIR",
                    r"D:\projet_dataoz\pc_data\data\tmp\weathercloud_dl")
    # Répertoire unique weathercloud/ — pas de sous-dossier daté
    raw_dir    = os.environ.get("WC_RAW_DIR",
                    r"D:\projet_dataoz\pc_data\data\raw\météo_bresser\weathercloud")

    year, month = get_target_month()
    result = download_csv(email, password, station_id,
                          tmp_dir, raw_dir, year, month)
    print(f"✅ Fichier créé : {result}")


if __name__ == "__main__":
    main()
