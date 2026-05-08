# ─────────────────────────────────────────────────────────────
# Image DataOZ Airflow — Python 3.11 + Playwright (Chromium)
# Build : docker compose build
# ─────────────────────────────────────────────────────────────
FROM apache/airflow:2.8.0-python3.11

# Passage en root pour les dépendances système de Playwright
USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    # Chromium core
    libglib2.0-0 \
    libnss3 \
    libnspr4 \
    libdbus-1-3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libx11-6 \
    libx11-xcb1 \
    libxcb1 \
    libxext6 \
    fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

# Retour sur l'utilisateur airflow pour les installations Python
USER airflow

# Installation de playwright + navigateurs
RUN pip install --no-cache-dir playwright==1.43.0 \
    && playwright install chromium
