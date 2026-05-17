# -*- coding: utf-8 -*-
"""
scrapping_enedis.py
====================
Téléchargement automatisé du fichier "Courbe de charge" depuis l'espace
client Enedis (mon-compte-particulier.enedis.fr) via Playwright.

Pattern calqué sur scripts/meteo/bresser/download_weathercloud.py :
  - Playwright isolé dans un thread (évite "This event loop is already running")
  - Screenshots à chaque étape pour debug
  - Sélecteurs validés par les éléments HTML fournis par l'utilisateur

Workflow
--------
  1. Login            → identifiants depuis ENEDIS_IDENTIFIANT / ENEDIS_PASSWORD
  2. "Ma consommation" → menu déroulant
  3. "Suivre ma consommation" → page de courbe
  4. "J'ai compris"   → fermeture popup d'avertissement
  5. Sélecteur type   → "Courbe de charge (kW)"
  6. Sélecteur période → (J-7) → (J-1)
  7. Téléchargement   → XLSX déposé dans inbox_enedis/

Le fichier téléchargé est renommé en :
   scrap_enedis_YYYYMMDD-YYYYMMDD__YYYYMMDD_HHMMSS.xlsx
de façon à être pris en charge par etl_inbox_enedis.phase_extract().

Variables d'environnement attendues
-----------------------------------
  ENEDIS_IDENTIFIANT   identifiant de connexion (email)
  ENEDIS_PASSWORD      mot de passe
"""
from __future__ import annotations

import logging
import os
import shutil
import threading
import time
from datetime import date, datetime, timedelta
from pathlib import Path

log = logging.getLogger(__name__)

# ── URLs Enedis ──────────────────────────────────────────────────────────────
ENEDIS_BASE      = "https://mon-compte-particulier.enedis.fr"
ENEDIS_LOGIN_URL = f"{ENEDIS_BASE}/dataconnect/v1/oauth2/authorize"  # redirige vers SSO
# URL réelle de la page de courbe (ex /suivi-de-consommation, qui retournait 404)
ENEDIS_HOME_URL  = f"{ENEDIS_BASE}/visualiser-vos-mesures-consommation"

WAIT_NAV = 4         # secondes après une navigation
WAIT_CLICK = 1       # petit délai après un clic
WAIT_DL  = 60        # timeout téléchargement (secondes)


# ── Calcul de la fenêtre J-7 → J-1 ───────────────────────────────────────────

def get_target_window(ref_date: date | None = None) -> tuple[date, date]:
    """
    Retourne (date_debut, date_fin) = (J-7, J-1) par rapport à ref_date.
    Exemple si ref_date = 2026-04-26 → (2026-04-19, 2026-04-25).

    NB : la "Courbe de charge (kW)" n'est accessible sur l'UI Enedis que pour
    une période ≤ 7 jours. On prend donc une fenêtre de 6 jours (J-7 → J-1)
    pour maximiser la marge de rattrapage en cas de run manqué.
    """
    if ref_date is None:
        ref_date = date.today()
    return ref_date - timedelta(days=7), ref_date - timedelta(days=1)


def output_filename(d_start: date, d_end: date,
                    run_dt: datetime | None = None) -> str:
    """
    Nom du fichier déposé dans inbox_enedis/.
    Le préfixe 'scrap_enedis_' permet de le repérer dans l'archive.
    Format compatible avec etl_inbox_enedis (qui lit les .xlsx).
    """
    if run_dt is None:
        run_dt = datetime.now()
    return (
        f"scrap_enedis_"
        f"{d_start.strftime('%d%m%Y')}-{d_end.strftime('%d%m%Y')}"
        f"__{run_dt.strftime('%Y%m%d_%H%M%S')}.xlsx"
    )


# ── Cœur Playwright (tourne dans un thread dédié) ────────────────────────────

def _playwright_run(identifiant: str, password: str,
                    tmp_path: Path, inbox_path: Path,
                    d_start: date, d_end: date,
                    result_holder: dict) -> None:
    """
    Tout le code Playwright s'exécute ici, dans un thread séparé,
    pour éviter le conflit avec l'event loop d'Airflow.
    """
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

    tmp_path.mkdir(parents=True, exist_ok=True)
    inbox_path.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as pw:
        # ── Anti-détection FriendlyCaptcha / WebDriver ─────────────────────
        # FriendlyCaptcha renvoie ".HEADLESS_ERROR" si le navigateur ressemble
        # à un bot. On masque les signatures les plus communes :
        #   • User-Agent (ne pas contenir "HeadlessChrome")
        #   • navigator.webdriver (doit être undefined)
        #   • navigator.plugins (vide en headless, ≥3 en vrai)
        #   • navigator.languages
        #   • window.chrome runtime
        #   • permissions API
        #   • flag launch --disable-blink-features=AutomationControlled
        REAL_CHROME_UA = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36"
        )
        browser = pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--window-size=1400,900",
                "--disable-blink-features=AutomationControlled",
                "--disable-features=IsolateOrigins,site-per-process",
            ],
        )
        ctx = browser.new_context(
            accept_downloads=True,
            locale="fr-FR",
            timezone_id="Europe/Paris",
            viewport={"width": 1400, "height": 900},
            user_agent=REAL_CHROME_UA,
            extra_http_headers={
                "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
            },
        )
        page = ctx.new_page()
        page.set_default_timeout(20_000)
        # Init script appliqué à TOUS les frames AVANT le 1er JS de la page
        page.add_init_script("""
            // 1) navigator.webdriver = undefined
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});

            // 2) Plugins (un vrai Chrome en a 3-5, headless n'en a 0)
            Object.defineProperty(navigator, 'plugins', {
                get: () => [
                    {name: 'PDF Viewer',      filename: 'internal-pdf-viewer'},
                    {name: 'Chrome PDF Viewer', filename: 'internal-pdf-viewer'},
                    {name: 'Chromium PDF Viewer', filename: 'internal-pdf-viewer'},
                    {name: 'Microsoft Edge PDF Viewer', filename: 'internal-pdf-viewer'},
                    {name: 'WebKit built-in PDF', filename: 'internal-pdf-viewer'},
                ],
            });

            // 3) Langues (cohérentes avec User-Agent)
            Object.defineProperty(navigator, 'languages', {
                get: () => ['fr-FR', 'fr', 'en-US', 'en'],
            });

            // 4) chrome runtime (HeadlessChrome n'a pas window.chrome)
            window.chrome = window.chrome || {
                runtime: {},
                loadTimes: function() {},
                csi: function() {},
                app: {},
            };

            // 5) Permissions API (HeadlessChrome retourne 'denied' pour notifications)
            const originalQuery = window.navigator.permissions && window.navigator.permissions.query;
            if (originalQuery) {
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications'
                        ? Promise.resolve({state: Notification.permission})
                        : originalQuery(parameters)
                );
            }

            // 6) HardwareConcurrency / DeviceMemory (souvent suspects en headless)
            try {
                Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 8});
                Object.defineProperty(navigator, 'deviceMemory',        {get: () => 8});
            } catch (e) {}
        """)

        def screenshot(name: str) -> None:
            path = tmp_path / f"debug_{name}.png"
            try:
                page.screenshot(path=str(path), full_page=True)
                log.info("Screenshot : %s", path)
            except Exception:
                pass

        def dismiss_cookies() -> None:
            """Ferme les popups de consentement cookies."""
            for sel in [
                "#popin_tc_privacy_button_2",     # accept TrustCommander
                "#popin_tc_privacy_button_3",
                "button:has-text('Tout accepter')",
                "button:has-text('Accepter')",
                "button:has-text('OK')",
                ".cookie-consent button",
            ]:
                try:
                    page.click(sel, timeout=2_000)
                    log.info("Cookie popup fermée : %s", sel)
                    time.sleep(1)
                    return
                except Exception:
                    continue

        # ── 1. Login ───────────────────────────────────────────────────────────
        # Enedis a utilisé ForgeRock OpenAM (IDToken1 / callback_0) puis a migré.
        # On attend networkidle pour laisser le JS de redirection SSO s'exécuter
        # avant de chercher le formulaire de connexion.
        log.info("Login Enedis…")
        page.goto(ENEDIS_HOME_URL, wait_until="domcontentloaded", timeout=30_000)
        # Attendre que la page (SPA) finisse de charger et de rediriger vers le SSO
        try:
            page.wait_for_load_state("networkidle", timeout=15_000)
        except Exception:
            log.warning("networkidle non atteint après goto — on continue")
        time.sleep(WAIT_NAV)
        dismiss_cookies()
        screenshot("01_login_page")
        log.info("URL initiale : %s", page.url)

        def dump_visible_buttons(stage: str) -> None:
            """Diagnostic : log tous les boutons/inputs visibles (utile si selector miss)."""
            try:
                infos = page.evaluate("""
                    () => {
                      const out = [];
                      document.querySelectorAll('button, input[type=\"submit\"], input[type=\"button\"], a[role=\"button\"]')
                        .forEach(el => {
                          const r = el.getBoundingClientRect();
                          if (r.width > 0 && r.height > 0) {
                            out.push({
                              tag: el.tagName,
                              id: el.id || '',
                              name: el.getAttribute('name') || '',
                              type: el.getAttribute('type') || '',
                              text: (el.innerText || el.value || '').trim().slice(0, 60),
                              cls: (el.className || '').toString().slice(0, 80)
                            });
                          }
                        });
                      return out;
                    }
                """)
                log.warning("[%s] Boutons visibles (%d) :", stage, len(infos))
                for b in infos[:25]:
                    log.warning("   • %s", b)
            except Exception as e:
                log.warning("dump_visible_buttons KO : %s", e)

        # ── Étape 1.a : champ identifiant ──
        # Sélecteurs par ordre de priorité :
        #   • ForgeRock OpenAM (callback_0 / IDToken1 / idToken1)
        #   • input[type="email"]  — page login classique
        #   • input[type="text"]   — fallback générique
        #   • input[name="username"] / input[name="login"] — Keycloak / autres SSO
        IDENT_SELECTORS = (
            'input[name="callback_0"]',
            'input[name="IDToken1"]',
            'input#idToken1',
            'input[type="email"]',
            'input[name="username"]',
            'input[name="login"]',
            'input[name="identifier"]',
            'input[autocomplete="username"]',
            'input[autocomplete="email"]',
            'input[type="text"]',
        )
        identifiant_saisi = False
        for sel in IDENT_SELECTORS:
            try:
                page.wait_for_selector(sel, timeout=5_000, state="visible")
                page.fill(sel, identifiant)
                log.info("Identifiant saisi avec sélecteur : %s", sel)
                identifiant_saisi = True
                break
            except PWTimeout:
                continue
            except Exception as e:
                log.warning("Sélecteur %s KO : %s", sel, e)
                continue
        if not identifiant_saisi:
            screenshot("01_login_FAIL_no_input")
            # Dump de tous les inputs visibles pour diagnostic
            try:
                inputs_diag = page.evaluate("""
                    () => {
                      const out = [];
                      document.querySelectorAll('input, textarea, [contenteditable]')
                        .forEach(el => {
                          const r = el.getBoundingClientRect();
                          if (r.width > 0 && r.height > 0) {
                            out.push({
                              tag: el.tagName,
                              id: el.id || '',
                              name: el.getAttribute('name') || '',
                              type: el.getAttribute('type') || '',
                              placeholder: el.getAttribute('placeholder') || '',
                              autocomplete: el.getAttribute('autocomplete') || '',
                              cls: (el.className || '').toString().slice(0, 80),
                            });
                          }
                        });
                      return out;
                    }
                """)
                log.error("Aucun champ identifiant trouvé. URL=%s | Inputs visibles (%d) :",
                          page.url, len(inputs_diag))
                for inp in inputs_diag:
                    log.error("   • %s", inp)
            except Exception as dump_err:
                log.error("Dump inputs KO : %s", dump_err)
            dump_visible_buttons("login_FAIL")
            raise RuntimeError(
                f"Impossible de trouver le champ identifiant sur {page.url}. "
                "Voir screenshot debug_01_login_FAIL_no_input.png et les logs ci-dessus."
            )

        screenshot("01b_after_email")

        # ── Étape 1.b : FriendlyCaptcha (frc-button) ──
        # Enedis utilise FriendlyCaptcha en page de login. Tant que le PoW n'est pas
        # résolu, le bouton "Suivant" reste désactivé. On clique sur le widget puis on
        # attend que la solution apparaisse dans input[name="frc-captcha-solution"].
        frc_present = page.locator('.frc-button, .frc-captcha, [data-name="frc-captcha-solution"]').first
        try:
            if frc_present.is_visible(timeout=2_000):
                log.info("FriendlyCaptcha détecté → résolution en cours…")
                # 1) Clic sur le widget pour démarrer le PoW
                try:
                    page.click('.frc-button', timeout=3_000)
                    log.info("Clic sur '.frc-button' (démarrage PoW)")
                except PWTimeout:
                    log.warning(".frc-button non cliquable — peut être déjà en auto-start")

                # 2) Attendre que la solution soit calculée
                #    FriendlyCaptcha écrit la solution dans input[name="frc-captcha-solution"]
                #    valeur initiale = ".UNSTARTED" / ".UNFINISHED" / ".FETCHING" → puis le token
                solved = False
                for attempt in range(60):  # max 60s (PoW peut être long sur 1ʳᵉ utilisation)
                    try:
                        val = page.evaluate("""
                            () => {
                              const el = document.querySelector('input[name=\"frc-captcha-solution\"]');
                              return el ? el.value : null;
                            }
                        """)
                    except Exception:
                        val = None
                    if val and not val.startswith("."):
                        log.info("FriendlyCaptcha résolu (token len=%d) après %ds", len(val), attempt)
                        solved = True
                        break
                    if attempt % 5 == 0:
                        log.info("  …captcha en cours (val=%r)", val)
                    time.sleep(1)
                if not solved:
                    log.warning("FriendlyCaptcha non résolu après 60s — on tente quand même le clic Suivant")
        except Exception as e:
            log.info("Pas de FriendlyCaptcha détecté (%s)", type(e).__name__)

        screenshot("01b2_after_captcha")

        # ── Étape 1.c : bouton "Suivant" ──
        # Sélecteur réel observé dans les logs : input#idToken3_0[name="callback_2"]
        suivant_clicked = False
        for sel in [
            '#idToken3_0',
            'input[name="callback_2"][type="submit"]',
            '#loginButton_0',
            'button[id^="loginButton"]',
            'input[id^="loginButton"]',
            'input[type="submit"]:not(.frc-button)',
            'button[type="submit"]',
            'button:has-text("Suivant")',
            'button:has-text("Continuer")',
        ]:
            try:
                page.click(sel, timeout=3_000)
                log.info("Bouton 'Suivant' cliqué : %s", sel)
                suivant_clicked = True
                break
            except PWTimeout:
                continue
        if not suivant_clicked:
            log.warning("Aucun sélecteur 'Suivant' n'a matché — fallback Enter")
            try:
                page.keyboard.press("Enter")
            except Exception:
                pass

        # Attendre que la 2ᵉ étape (mot de passe) apparaisse
        try:
            page.wait_for_selector(
                'input[name="callback_0"][type="password"], input[name="IDToken2"], input[type="password"], input#idToken2, input[name^="callback"][type="password"]',
                timeout=20_000,
                state="visible",
            )
            log.info("Étape 2 (mot de passe) détectée")
        except PWTimeout:
            log.error("Étape 2 introuvable — la page n'a pas avancé après 'Suivant'")
            screenshot("01c_no_password_field")
            dump_visible_buttons("après Suivant")
            raise

        screenshot("01c_after_suivant")

        # ── Étape 1.c : mot de passe ──
        try:
            page.fill(
                'input[name="callback_0"][type="password"], input[name="IDToken2"], input[type="password"], input#idToken2',
                password,
            )
            log.info("Mot de passe saisi")
        except PWTimeout:
            page.fill('input[type="password"]', password)

        screenshot("01d_after_password")

        # ── Étape 1.d : submit final (Connexion) ──
        submitted = False
        for sel in [
            '#loginButton_0',
            'button[id^="loginButton"]',
            'input[id^="loginButton"]',
            'button[type="submit"]',
            'input[type="submit"]',
            'button:has-text("Connexion")',
            'button:has-text("Se connecter")',
            'button:has-text("Valider")',
        ]:
            try:
                page.click(sel, timeout=3_000)
                submitted = True
                log.info("Submit cliqué : %s", sel)
                break
            except PWTimeout:
                continue
        if not submitted:
            page.keyboard.press("Enter")
            log.info("Submit via Enter")

        # Après le submit, ForgeRock chaîne plusieurs redirections JS :
        #   /auth/XUI/#login → /auth/oauth2/authorize → microapplication SP →
        #   mon-compte-particulier.enedis.fr/suivi-de-consommation
        # On attend ACTIVEMENT que l'URL atteigne le bon domaine (et plus le SSO).
        log.info("Attente de la redirection finale post-login…")
        try:
            page.wait_for_url(
                lambda url: "mon-compte-particulier.enedis.fr" in url
                            and "/auth/" not in url
                            and "#login" not in url,
                timeout=45_000,
            )
            log.info("Redirection finale détectée")
        except PWTimeout:
            log.warning("Redirection finale non détectée en 45s — on continue quand même")

        # On laisse aussi la page finir de se rendre
        try:
            page.wait_for_load_state("networkidle", timeout=20_000)
        except PWTimeout:
            log.warning("networkidle non atteint — on continue")

        time.sleep(2)
        dismiss_cookies()
        screenshot("02_post_login")
        log.info("URL post-login : %s", page.url)

        # Test de succès : on doit être sur mon-compte-particulier.enedis.fr
        # ET PLUS sur la page de login XUI
        url_low = page.url.lower()
        on_xui_login = (
            "mon-compte.enedis.fr/auth/xui" in url_low
            or "#login" in url_low
        )
        on_target_domain = "mon-compte-particulier.enedis.fr" in url_low

        if on_xui_login or not on_target_domain:
            screenshot("02_login_failed")
            dump_visible_buttons("post-login échec")
            raise RuntimeError(f"Échec login — toujours sur {page.url}")
        log.info("Login OK")

        # ── 2. Navigation DIRECTE vers la page de courbe ──────────────────────
        # L'ancien flux passait par le menu burger ("Ma consommation" → "Suivre
        # ma consommation"), mais l'URL /suivi-de-consommation n'existe plus
        # (404). On va donc DIRECTEMENT sur /visualiser-vos-mesures-consommation
        # qui est la nouvelle URL réelle de la page graphique.
        log.info("Navigation directe vers la page de courbe : %s", ENEDIS_HOME_URL)
        page.goto(ENEDIS_HOME_URL, wait_until="domcontentloaded", timeout=30_000)
        time.sleep(WAIT_NAV)
        dismiss_cookies()

        # ── 3. Attente du composant sds-select sur la page ────────────────────
        # D'après les captures utilisateur, la page de courbe est rendue
        # directement dans `page` (pas dans une iframe). On poll donc page
        # pendant 30s pour que le sds-select "Energie" apparaisse. Si on ne
        # trouve rien, fallback : on scanne les iframes (au cas où).
        log.info("Attente du sds-select 'Energie' sur la page…")
        ctx = page
        sds_ready = False
        for attempt in range(30):
            try:
                if page.evaluate("""
                    () => {
                      const sds = Array.from(document.querySelectorAll('sds-select'));
                      return sds.some(el => /energ/i.test(el.innerText || ''));
                    }
                """):
                    log.info("sds-select 'Energie' trouvé sur la page (attempt=%d)", attempt)
                    sds_ready = True
                    break
            except Exception:
                pass
            time.sleep(1)

        if not sds_ready:
            # Fallback : recherche dans les iframes
            log.warning("sds-select non trouvé sur page — scan des iframes…")
            for f in page.frames:
                if f == page.main_frame:
                    continue
                try:
                    if f.evaluate("""
                        () => {
                          const sds = Array.from(document.querySelectorAll('sds-select'));
                          return sds.some(el => /energ/i.test(el.innerText || ''));
                        }
                    """):
                        ctx = f
                        log.info("sds-select trouvé dans iframe : url=%s", f.url)
                        sds_ready = True
                        break
                except Exception:
                    continue

        # Liste tous les frames pour debug
        try:
            for i, f in enumerate(page.frames):
                log.info("   frame[%d] : url=%s name=%s", i, (f.url or "")[:120], f.name)
        except Exception:
            pass

        # Networkidle pour attendre la fin des XHR (les données arrivent en JSON)
        try:
            page.wait_for_load_state("networkidle", timeout=30_000)
        except PWTimeout:
            log.warning("networkidle non atteint — on continue")

        time.sleep(3)
        screenshot("04_suivi_consommation")

        # Diagnostic : URL et état du contexte (page ou frame)
        log.info("URL page de courbe : %s", page.url)
        log.info("Contexte d'interaction : %s", "frame" if ctx is not page else "page")

        # ── 4. Popup "J'ai compris" (peut être absente) ──────────────────────
        log.info("Recherche popup 'J'ai compris'…")
        for sel in [
            'button.second_link:has-text("J\'ai compris")',
            'button:has-text("J\'ai compris")',
            'button:has-text("J\u2019ai compris")',  # apostrophe typographique
        ]:
            for c in (ctx, page):
                try:
                    c.click(sel, timeout=3_000)
                    log.info("Popup 'J'ai compris' fermée : %s (%s)", sel, "frame" if c is ctx and c is not page else "page")
                    time.sleep(WAIT_CLICK)
                    break
                except PWTimeout:
                    continue
            else:
                continue
            break
        screenshot("05_after_popup")

        # ── 5. Sélecteur de PÉRIODE via CALENDAR (J-7 → J-1 = 6 jours) ───────
        # IMPORTANT : la "Courbe de charge (kW)" n'est accessible dans le
        # sélecteur de type QUE si la période est ≤ 7 jours. On fixe donc la
        # période AVANT de toucher au type.
        # L'UI Enedis ouvre un calendar widget : on clique sur la cellule du
        # jour de début, puis sur la cellule du jour de fin.
        d_start_str = d_start.strftime("%d/%m/%Y")
        d_end_str   = d_end.strftime("%d/%m/%Y")
        log.info("Période cible : %s → %s (J-7 → J-1)", d_start_str, d_end_str)

        # Dump initial : tous les inputs visibles dans la frame métier
        try:
            inputs_info = ctx.evaluate("""
                () => {
                  const out = [];
                  document.querySelectorAll('input').forEach((el, i) => {
                    const r = el.getBoundingClientRect();
                    if (r.width > 0 && r.height > 0) {
                      out.push({
                        idx: i, type: el.type || '', name: el.name || '',
                        id: el.id || '', placeholder: el.placeholder || '',
                        value: (el.value || '').slice(0, 40),
                        x: Math.round(r.x), y: Math.round(r.y),
                      });
                    }
                  });
                  return out;
                }
            """)
            log.info("Inputs visibles dans le contexte (%d) :", len(inputs_info))
            for it in inputs_info[:20]:
                log.info("   • %s", it)
        except Exception as e:
            log.warning("Dump inputs KO : %s", e)

        # 5a. Ouverture du picker — d'après les captures, l'input période est
        # accompagné d'une icône calendrier 📅 à droite (sds-icon "event" ou
        # "calendar_today"). Cliquer cette icône est plus fiable que l'input.
        log.info("Ouverture du picker de période (icône calendrier puis input)…")

        # Diagnostic : dump des inputs + icônes calendar visibles
        try:
            picker_dbg = ctx.evaluate("""
                () => {
                  const out = { inputs: [], icons: [] };
                  document.querySelectorAll('input').forEach((el, i) => {
                    const r = el.getBoundingClientRect();
                    if (r.width > 0 && r.height > 0) {
                      out.inputs.push({
                        idx: i, type: el.type || '', value: (el.value || '').slice(0,40),
                        placeholder: el.placeholder || '',
                        x: Math.round(r.x), y: Math.round(r.y),
                      });
                    }
                  });
                  document.querySelectorAll('span.sds-icon, [sdsicon]').forEach((el, i) => {
                    const r = el.getBoundingClientRect();
                    if (r.width === 0 || r.height === 0) return;
                    const lbl = el.getAttribute('aria-label') || '';
                    if (/event|calend|date/i.test(lbl) || /event|calend/i.test(el.innerText || '')) {
                      out.icons.push({
                        idx: i, label: lbl, text: (el.innerText || '').slice(0,30),
                        parent: el.parentElement ? el.parentElement.tagName : '',
                        x: Math.round(r.x), y: Math.round(r.y),
                      });
                    }
                  });
                  return out;
                }
            """)
            log.info("Inputs visibles (%d) :", len(picker_dbg["inputs"]))
            for it in picker_dbg["inputs"][:10]:
                log.info("   • input %s", it)
            log.info("Icônes calendrier candidates (%d) :", len(picker_dbg["icons"]))
            for ic in picker_dbg["icons"][:10]:
                log.info("   • icon %s", ic)
        except Exception as e:
            log.warning("Dump picker KO : %s", e)

        period_opened = False
        for sel in [
            # 1) icône calendar typique (Material/SDS)
            'span.sds-icon[aria-label="event"]',
            'span.sds-icon[aria-label="calendar_today"]',
            'span[sdsicon][aria-label="event"]',
            'span[sdsicon][aria-label="calendar_today"]',
            'lnc-icon[icon="event"]',
            'lnc-icon[icon="calendar_today"]',
            'button[aria-label*="calendrier" i]',
            'button[aria-label*="période" i]',
            # 2) input avec format date (valeur "JJ/MM/AAAA – JJ/MM/AAAA")
            'input[value*="–"]',          # tiret cadratin
            'input[value*=" - "]',        # tiret normal
            'input[value*="/"]',          # contient au moins un /
            'input[placeholder*="période" i]',
            'input[aria-label*="période" i]',
            # 3) en dernier recours, le 1er input texte
            'label:has-text("Période") ~ * input',
            'input[type="text"]',
        ]:
            try:
                ctx.locator(sel).first.click(timeout=3_000)
                period_opened = True
                log.info("Picker période ouvert : %s", sel)
                break
            except Exception:
                continue
        time.sleep(WAIT_CLICK + 1)  # laisser le widget calendar s'animer
        screenshot("05_periode_open")

        # 5b. Navigation calendrier : on s'assure d'être sur le bon mois.
        # Le header du calendar affiche "AVR. 2026" (selon ton screenshot).
        # On compare avec d_start.month/year ; sinon on clique sur la flèche.
        target_month_fr = [
            "JANV.", "FÉVR.", "MARS", "AVR.", "MAI", "JUIN",
            "JUIL.", "AOÛT", "SEPT.", "OCT.", "NOV.", "DÉC.",
        ][d_start.month - 1]
        target_year = d_start.year
        log.info("Mois cible du calendrier : %s %s", target_month_fr, target_year)

        def _nav_prev():
            """Clique sur la flèche 'mois précédent' du calendrier."""
            ctx.locator(
                'button[aria-label*="précédent" i], '
                'button[aria-label*="previous" i], '
                'button:has-text("‹"), '
                'button:has-text("<")'
            ).first.click(timeout=3_000)
            time.sleep(0.4)

        def _nav_next():
            """Clique sur la flèche 'mois suivant' du calendrier."""
            ctx.locator(
                'button[aria-label*="suivant" i], '
                'button[aria-label*="next" i], '
                'button:has-text("›"), '
                'button:has-text(">")'
            ).first.click(timeout=3_000)
            time.sleep(0.4)

        def _read_calendar_header() -> str | None:
            try:
                return ctx.evaluate("""
                    () => {
                      const all = document.querySelectorAll('button, h1, h2, h3, h4, span, div');
                      for (const el of all) {
                        const r = el.getBoundingClientRect();
                        if (r.width === 0 || r.height === 0) continue;
                        const txt = (el.innerText || el.textContent || '').trim();
                        if (/^(JANV|FÉVR|FEVR|MARS|AVR|MAI|JUIN|JUIL|AOÛT|AOUT|SEPT|OCT|NOV|DÉC|DEC)\\.?\\s+\\d{4}$/i.test(txt)) {
                          return txt;
                        }
                      }
                      return null;
                    }
                """)
            except Exception as e:
                log.warning("Lecture header calendar KO : %s", e)
                return None

        for nav_attempt in range(6):
            hdr_text = _read_calendar_header()
            log.info("Header calendar lu : %s", hdr_text)

            if not hdr_text:
                # Header illisible (souvent dans un Shadow DOM ou Web Component).
                # On suppose que le calendrier s'ouvre sur le mois courant et on
                # navigue en arrière selon l'écart entre aujourd'hui et d_start.
                from datetime import date as _date
                today_first = _date.today().replace(day=1)
                target_first = d_start.replace(day=1)
                months_back = (today_first.year - target_first.year) * 12 + \
                              (today_first.month - target_first.month)
                if months_back > 0:
                    log.info("Header illisible — navigation aveugle : %d mois en arrière", months_back)
                    for _ in range(min(months_back, 6)):
                        try:
                            _nav_prev()
                        except Exception as e:
                            log.warning("Navigation aveugle KO : %s", e)
                            break
                else:
                    log.info("Header illisible — mois courant = mois cible, pas de navigation")
                break  # sortir de la boucle, on a fait ce qu'on pouvait

            # Compare avec target_month_fr + target_year
            expected = f"{target_month_fr} {target_year}".upper()
            if expected in hdr_text.upper():
                log.info("Bon mois affiché (%s)", hdr_text)
                break
            # Sinon, clique sur la flèche précédente (on est en avant typiquement)
            try:
                _nav_prev()
                log.info("Navigation calendrier : flèche précédente cliquée (attempt=%d)", nav_attempt)
            except Exception as e:
                log.warning("Navigation calendrier KO : %s", e)
                break

        screenshot("05b_calendar_month")

        # 5c. Clic sur la cellule du jour de début, puis du jour de fin.
        #     Si début et fin sont sur des mois différents (ex: 29/04 → 02/05),
        #     on navigue vers le mois suivant entre les deux clics.
        d_start_day_zfill = f"{d_start.day:02d}"
        d_end_day_zfill   = f"{d_end.day:02d}"
        cross_month = (d_start.month != d_end.month or d_start.year != d_end.year)

        def click_day(day_num: int) -> bool:
            d_str   = str(day_num)
            d_zfill = f"{day_num:02d}"
            for sel in [
                f'td:not([disabled]):has-text("{d_zfill}"):not(:has-text("0{d_zfill}"))',
                f'td button:has-text("{d_zfill}"):not([disabled])',
                f'button[aria-label*="{d_zfill}/"]:not([disabled])',
                f'[role="gridcell"]:has-text("{d_zfill}"):not([aria-disabled="true"])',
                f'[role="button"]:has-text("{d_zfill}"):not([aria-disabled="true"])',
                f'text=/^\\s*{d_zfill}\\s*$/',
                f'text=/^\\s*{d_str}\\s*$/',
            ]:
                try:
                    loc = ctx.locator(sel).first
                    loc.wait_for(state="visible", timeout=2_500)
                    loc.click(timeout=2_000)
                    log.info("Jour %s cliqué : %s", d_zfill, sel)
                    return True
                except Exception:
                    continue
            return False

        period_set = False
        if click_day(d_start.day):
            time.sleep(0.5)
            # Si la période enjambe deux mois, naviguer vers le mois de fin
            if cross_month:
                log.info("Période inter-mois détectée (%s/%s → %s/%s) : navigation vers le mois suivant",
                         d_start.month, d_start.year, d_end.month, d_end.year)
                try:
                    _nav_next()
                    time.sleep(0.5)
                    screenshot("05c_calendar_next_month")
                except Exception as e:
                    log.warning("Navigation vers mois suivant KO : %s", e)
            if click_day(d_end.day):
                period_set = True
                log.info("Période %s → %s sélectionnée via calendar%s",
                         d_start_day_zfill, d_end_day_zfill,
                         " (inter-mois)" if cross_month else "")

        if not period_set:
            screenshot("06_periode_calendar_failed")
            # Dump des cellules de jour visibles pour aider au debug
            try:
                cells = ctx.evaluate("""
                    () => {
                      const out = [];
                      document.querySelectorAll('td, button, [role="gridcell"], [role="button"]').forEach(el => {
                        const r = el.getBoundingClientRect();
                        if (r.width === 0 || r.height === 0) return;
                        const txt = (el.innerText || el.textContent || '').trim();
                        if (/^\\d{1,2}$/.test(txt)) {
                          out.push({
                            tag: el.tagName, cls: el.className || '',
                            text: txt, disabled: el.hasAttribute('disabled') || el.getAttribute('aria-disabled') === 'true',
                          });
                        }
                      });
                      return out.slice(0, 50);
                    }
                """)
                log.warning("Cellules numériques visibles (%d) :", len(cells))
                for c in cells[:30]:
                    log.warning("   • %s", c)
            except Exception as e:
                log.warning("Dump cellules KO : %s", e)
            log.warning("Sélection période via calendar KO — on tente le fallback inputs")
            try:
                inputs = ctx.locator('input:visible').all()
                if len(inputs) >= 2:
                    inputs[-2].fill(d_start_str); time.sleep(0.3)
                    inputs[-1].fill(d_end_str);   time.sleep(0.3)
                    page.keyboard.press("Tab")
                    period_set = True
                    log.info("Fallback inputs : dates saisies")
            except Exception as e:
                log.warning("Fallback inputs KO : %s", e)

        time.sleep(WAIT_NAV)
        screenshot("06_periode_set")

        # 5d. Bouton "Valider"/"Appliquer" éventuel
        for sel in [
            'button:has-text("Valider")',
            'button:has-text("Appliquer")',
            'button:has-text("OK")',
        ]:
            try:
                ctx.click(sel, timeout=3_000)
                log.info("Bouton validation période cliqué : %s", sel)
                time.sleep(WAIT_NAV)
                break
            except PWTimeout:
                continue
        screenshot("07_periode_validated")

        # Laisse le temps à l'UI de recalculer les options du sélecteur de type
        time.sleep(3)

        # ── 6. Sélecteur de TYPE → "Courbe de charge (kW)" ───────────────────
        # L'UI Enedis utilise un Web Component sds-select :
        #   - toggle  = <span class="sds-icon" aria-label="expand_more">
        #   - options = <span class="sds-option_content">…</span>
        log.info("Ouverture du sélecteur de type (sds-select)…")

        # Dump diagnostique des sds-select et icônes expand_more visibles
        try:
            sds_info = ctx.evaluate("""
                () => {
                  const out = { selects: [], icons: [] };
                  document.querySelectorAll('sds-select, [class*="sds-select"]').forEach((el, i) => {
                    const r = el.getBoundingClientRect();
                    if (r.width > 0 && r.height > 0) {
                      out.selects.push({
                        idx: i, tag: el.tagName, cls: el.className || '',
                        text: (el.innerText || '').trim().slice(0, 60),
                        x: Math.round(r.x), y: Math.round(r.y),
                      });
                    }
                  });
                  document.querySelectorAll('span.sds-icon[aria-label="expand_more"], span[sdsicon][aria-label="expand_more"]').forEach((el, i) => {
                    const r = el.getBoundingClientRect();
                    if (r.width > 0 && r.height > 0) {
                      out.icons.push({
                        idx: i, parent: el.parentElement ? el.parentElement.tagName : '',
                        parentText: el.parentElement ? (el.parentElement.innerText || '').trim().slice(0, 60) : '',
                        x: Math.round(r.x), y: Math.round(r.y),
                      });
                    }
                  });
                  return out;
                }
            """)
            log.info("sds-select visibles (%d) :", len(sds_info["selects"]))
            for s in sds_info["selects"]:
                log.info("   • select %s", s)
            log.info("expand_more icons visibles (%d) :", len(sds_info["icons"]))
            for s in sds_info["icons"]:
                log.info("   • icon %s", s)
        except Exception as e:
            log.warning("Dump sds-* KO : %s", e)

        opened = False
        for sel in [
            'sds-select:has-text("Energie")',
            'sds-select:has-text("Énergie")',
            'sds-select',
            'span.sds-icon[aria-label="expand_more"]',
        ]:
            try:
                ctx.locator(sel).first.click(timeout=4_000)
                opened = True
                log.info("Sélecteur de type ouvert : %s", sel)
                break
            except Exception:
                continue
        if not opened:
            screenshot("08_select_not_opened")
            log.warning("Sélecteur de type pas explicitement ouvert")

        time.sleep(WAIT_CLICK + 1)
        screenshot("08_select_open")

        # Sélection 'Courbe de charge (kW)'
        log.info("Sélection 'Courbe de charge (kW)' (span.sds-option_content)…")
        selected = False
        for sel in [
            'span.sds-option_content:has-text("Courbe de charge")',
            'sds-option:has(span.sds-option_content:has-text("Courbe de charge"))',
            'sds-option:has-text("Courbe de charge")',
            '[role="option"]:has-text("Courbe de charge")',
            'li:has-text("Courbe de charge (kW)")',
            'text="Courbe de charge (kW)"',
        ]:
            try:
                loc = ctx.locator(sel).first
                loc.wait_for(state="visible", timeout=5_000)
                loc.click(timeout=3_000)
                selected = True
                log.info("Option 'Courbe de charge' cliquée : %s", sel)
                break
            except Exception:
                continue

        if not selected:
            screenshot("09_courbe_de_charge_not_found")
            try:
                opts = ctx.evaluate("""
                    () => {
                      const out = [];
                      document.querySelectorAll('sds-option, span.sds-option_content, [role="option"]').forEach((el, i) => {
                        const r = el.getBoundingClientRect();
                        out.push({
                          idx: i, tag: el.tagName, cls: el.className || '',
                          text: (el.innerText || el.textContent || '').trim().slice(0, 80),
                          visible: r.width > 0 && r.height > 0,
                        });
                      });
                      return out.slice(0, 50);
                    }
                """)
                log.warning("Options sds-* trouvées (%d) :", len(opts))
                for o in opts:
                    log.warning("   • %s", o)
            except Exception as e:
                log.warning("Dump options sds-* KO : %s", e)
            raise RuntimeError("Option 'Courbe de charge (kW)' introuvable")

        time.sleep(WAIT_NAV)
        screenshot("10_courbe_selected")

        # ── 7. Bouton de téléchargement ──────────────────────────────────────
        # Le bouton "Télécharger le .xlsx" apparaît UNIQUEMENT quand le chart
        # de la courbe de charge est complètement chargé. On attend donc
        # activement sa présence (jusqu'à 60s) AVANT de cliquer.
        log.info("Attente du bouton 'Télécharger le .xlsx' (chart chargé)…")
        btn_ready = False
        for attempt in range(60):
            try:
                # On considère le bouton prêt s'il existe un button/a contenant
                # ".xlsx" dans son texte visible.
                if ctx.evaluate("""
                    () => {
                      return Array.from(document.querySelectorAll('button, a')).some(el => {
                        const r = el.getBoundingClientRect();
                        if (r.width === 0 || r.height === 0) return false;
                        const txt = (el.innerText || el.textContent || '').toLowerCase();
                        return /\\.xlsx/.test(txt) || /xlsx/.test(txt);
                      });
                    }
                """):
                    log.info("Bouton .xlsx détecté (attempt=%d)", attempt)
                    btn_ready = True
                    break
            except Exception:
                pass
            time.sleep(1)

        if not btn_ready:
            log.warning("Bouton .xlsx non détecté après 60s — on tente quand même")

        # Dump détaillé de TOUS les boutons cliquables (pour debug)
        try:
            all_btn = ctx.evaluate("""
                () => {
                  const out = [];
                  document.querySelectorAll('button, a').forEach((el, i) => {
                    const r = el.getBoundingClientRect();
                    if (r.width === 0 || r.height === 0) return;
                    const txt = (el.innerText || el.textContent || '').trim();
                    if (txt) {
                      out.push({
                        idx: i, tag: el.tagName,
                        cls: (el.className || '').toString().slice(0, 60),
                        text: txt.slice(0, 80),
                        x: Math.round(r.x), y: Math.round(r.y),
                      });
                    }
                  });
                  return out;
                }
            """)
            log.info("Tous boutons/liens visibles dans ctx (%d) :", len(all_btn))
            for b in all_btn[:30]:
                log.info("   • %s", b)
        except Exception as e:
            log.warning("Dump boutons KO : %s", e)

        screenshot("11_before_download_click")

        # Sélecteurs SPÉCIFIQUES (pas de fallback générique "Télécharger" qui
        # matcherait des boutons type "Télécharger l'app").
        download_selectors = [
            # Sélecteur le plus stable : aria-label complet du bouton
            'button[aria-label="Télécharger vos données de consommation au format .xlsx"]',
            'button[aria-label*="Télécharger" i][aria-label*="xlsx" i]',
            'button[aria-label*="Télécharger" i][aria-label*="consommation" i]',
            # Texte visible exact du <span> à l'intérieur du bouton
            'button:has-text("Télécharger le .xlsx")',
            'a:has-text("Télécharger le .xlsx")',
            'button:has-text(".xlsx")',
            'a:has-text(".xlsx")',
            # SVG fourni par l'utilisateur — sélection du parent cliquable
            'button:has(svg path[d^="M18 15V18H6V15"])',
            'a:has(svg path[d^="M18 15V18H6V15"])',
            'lnc-icon[icon="file_download"]',
            'button[aria-label*="xlsx" i]',
        ]

        with page.expect_download(timeout=WAIT_DL * 1000) as dl_info:
            clicked = False
            for sel in download_selectors:
                try:
                    loc = ctx.locator(sel).first
                    loc.wait_for(state="visible", timeout=5_000)
                    loc.click(timeout=4_000)
                    log.info("Bouton téléchargement cliqué (via ctx) : %s", sel)
                    clicked = True
                    break
                except Exception:
                    # Fallback page parent (rare)
                    try:
                        page.wait_for_selector(sel, timeout=2_000, state="visible")
                        page.click(sel, timeout=2_000)
                        log.info("Bouton téléchargement cliqué (via page) : %s", sel)
                        clicked = True
                        break
                    except Exception:
                        continue

            if not clicked:
                screenshot("11_download_btn_not_found")
                raise RuntimeError(
                    "Bouton de téléchargement '.xlsx' introuvable — "
                    f"voir screenshot {tmp_path}/debug_11_download_btn_not_found.png "
                    "et le dump 'Tous boutons/liens visibles' du log"
                )

        download = dl_info.value
        suggested = download.suggested_filename or "enedis_courbe.xlsx"
        # On télécharge dans tmp puis on déplace dans inbox_enedis/
        tmp_file = tmp_path / suggested
        download.save_as(str(tmp_file))
        log.info("Fichier téléchargé (tmp) : %s", tmp_file)

        target_name = output_filename(d_start, d_end)
        target_file = inbox_path / target_name
        shutil.move(str(tmp_file), str(target_file))
        log.info("✅ Fichier déposé dans inbox : %s", target_file)

        result_holder["file"]      = target_file
        result_holder["raw_name"]  = suggested
        result_holder["d_start"]   = d_start
        result_holder["d_end"]     = d_end
        browser.close()


# ── Fonction publique : lance Playwright dans un thread ──────────────────────

def download_courbe_de_charge(identifiant: str,
                              password: str,
                              tmp_dir: str | Path,
                              inbox_dir: str | Path,
                              ref_date: date | None = None,
                              ) -> Path:
    """
    Lance le téléchargement dans un thread dédié pour éviter le conflit
    avec l'event loop Airflow ("This event loop is already running").

    Paramètres
    ----------
    identifiant   email Enedis (ENEDIS_IDENTIFIANT)
    password      mot de passe Enedis (ENEDIS_PASSWORD)
    tmp_dir       répertoire temporaire pour les screenshots de debug et
                  le fichier téléchargé avant déplacement
    inbox_dir     répertoire d'arrivée (ex. inbox_enedis/) — le XLSX y est
                  déposé pour être pris en charge par etl_inbox_enedis
    ref_date      date de référence pour calculer la fenêtre J-3 → J-2
                  (défaut : date.today())

    Retourne
    --------
    Le chemin (Path) du XLSX déposé dans inbox_dir.
    """
    if ref_date is None:
        ref_date = date.today()

    d_start, d_end = get_target_window(ref_date)
    log.info("Fenêtre cible : %s → %s", d_start, d_end)

    tmp_path   = Path(tmp_dir)
    inbox_path = Path(inbox_dir)
    inbox_path.mkdir(parents=True, exist_ok=True)

    result_holder: dict = {"file": None, "error": None}

    def _target() -> None:
        try:
            _playwright_run(identifiant, password,
                            tmp_path, inbox_path,
                            d_start, d_end,
                            result_holder)
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


# ── Standalone (test local) ──────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    identifiant = os.environ["ENEDIS_IDENTIFIANT"]
    password    = os.environ["ENEDIS_PASSWORD"]
    tmp_dir     = os.environ.get(
        "ENEDIS_SCRAP_TMP_DIR",
        r"D:\projet_dataoz\pc_data\data\tmp\enedis_scrap",
    )
    inbox_dir   = os.environ.get(
        "ENEDIS_SCRAP_INBOX_DIR",
        r"D:\projet_dataoz\pc_data\data\raw\conso_elec\enedis\inbox_enedis",
    )

    fichier = download_courbe_de_charge(
        identifiant=identifiant,
        password=password,
        tmp_dir=tmp_dir,
        inbox_dir=inbox_dir,
    )
    print(f"✅ Fichier créé : {fichier}")


if __name__ == "__main__":
    main()
