#!/bin/bash
# ============================================================
#  setup_vm.sh  -  Installation sur VM Oracle E2.1.Micro
#  Ubuntu 22.04 LTS — 1 OCPU / 1 GB RAM
#  Optimisé mémoire : swap 2 GB + PostgreSQL allégé
#
#  Usage :
#    chmod +x setup_vm.sh
#    sudo ./setup_vm.sh
# ============================================================

DB_NAME="dataoz"
DB_USER="dataoz_user"
DB_PASSWORD="CHANGEZ_MOI_ICI"        # ← modifier
DOMAIN="votre-domaine.fr"             # ← votre domaine Ionos
APP_USER="ubuntu"

# ── 1. Mises à jour système ────────────────────────────────────────────────────
echo "=== Mise à jour du système ==="
apt update && apt upgrade -y
apt install -y curl wget git python3-pip python3-venv nginx certbot python3-certbot-nginx ufw

# ── 2. Swap 2 GB (indispensable sur 1 GB RAM) ─────────────────────────────────
echo "=== Création du swap 2 GB ==="
if [ ! -f /swapfile ]; then
    fallocate -l 2G /swapfile
    chmod 600 /swapfile
    mkswap /swapfile
    swapon /swapfile
    echo '/swapfile none swap sw 0 0' >> /etc/fstab
    echo 'vm.swappiness=10' >> /etc/sysctl.conf
    sysctl -p
    echo "Swap activé."
else
    echo "Swap déjà présent."
fi

# ── 3. PostgreSQL 15 (version allégée pour 1 GB RAM) ─────────────────────────
echo "=== Installation PostgreSQL ==="
apt install -y postgresql-15 postgresql-client-15

systemctl enable postgresql
systemctl start postgresql

# Optimisation mémoire PostgreSQL pour 1 GB RAM
PG_CONF="/etc/postgresql/15/main/postgresql.conf"
cat >> ${PG_CONF} <<PGEOF

# ── Optimisation mémoire DataOZ (1 GB RAM) ──
shared_buffers       = 128MB
work_mem             = 4MB
maintenance_work_mem = 32MB
effective_cache_size = 512MB
max_connections      = 20
wal_buffers          = 4MB
checkpoint_completion_target = 0.9
PGEOF

# Créer la base et l'utilisateur
sudo -u postgres psql <<EOF
CREATE USER ${DB_USER} WITH PASSWORD '${DB_PASSWORD}';
CREATE DATABASE ${DB_NAME} OWNER ${DB_USER};
GRANT ALL PRIVILEGES ON DATABASE ${DB_NAME} TO ${DB_USER};
EOF

# Autoriser les connexions depuis le PC pour le chargement CSV
PG_HBA="/etc/postgresql/15/main/pg_hba.conf"
echo "host    ${DB_NAME}    ${DB_USER}    0.0.0.0/0    scram-sha-256" >> ${PG_HBA}
sed -i "s/#listen_addresses = 'localhost'/listen_addresses = '*'/" ${PG_CONF}

systemctl restart postgresql
echo "PostgreSQL prêt."

# ── 4. Application Python / Streamlit ─────────────────────────────────────────
echo "=== Installation Streamlit ==="
APP_DIR="/opt/dataoz"
mkdir -p ${APP_DIR}

python3 -m venv ${APP_DIR}/venv
${APP_DIR}/venv/bin/pip install --upgrade pip
${APP_DIR}/venv/bin/pip install streamlit psycopg2-binary pandas

# Fichier de configuration des variables d'environnement
cat > ${APP_DIR}/.env <<ENVEOF
DB_HOST=localhost
DB_PORT=5432
DB_NAME=${DB_NAME}
DB_USER=${DB_USER}
DB_PASSWORD=${DB_PASSWORD}
ENVEOF
chmod 600 ${APP_DIR}/.env

# Configuration Streamlit légère
mkdir -p ${APP_DIR}/.streamlit
cat > ${APP_DIR}/.streamlit/config.toml <<STEOF
[server]
port = 8501
address = "127.0.0.1"
headless = true
maxUploadSize = 10

[browser]
gatherUsageStats = false

[runner]
fastReruns = false
STEOF

# Service systemd pour Streamlit
cat > /etc/systemd/system/dataoz-streamlit.service <<SVCEOF
[Unit]
Description=DataOZ Streamlit App
After=network.target postgresql.service

[Service]
User=${APP_USER}
WorkingDirectory=${APP_DIR}
EnvironmentFile=${APP_DIR}/.env
ExecStart=${APP_DIR}/venv/bin/streamlit run ${APP_DIR}/streamlit_app.py
Restart=always
RestartSec=10
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
SVCEOF

systemctl daemon-reload
systemctl enable dataoz-streamlit

# ── 5. Nginx - reverse proxy ───────────────────────────────────────────────────
echo "=== Configuration Nginx ==="

cat > /etc/nginx/sites-available/dataoz <<NGINXEOF
server {
    listen 80;
    server_name ${DOMAIN} www.${DOMAIN};

    location / {
        proxy_pass         http://127.0.0.1:8501;
        proxy_http_version 1.1;
        proxy_set_header   Upgrade \$http_upgrade;
        proxy_set_header   Connection "upgrade";
        proxy_set_header   Host \$host;
        proxy_set_header   X-Real-IP \$remote_addr;
        proxy_cache_bypass \$http_upgrade;
        proxy_read_timeout 86400;
    }
}
NGINXEOF

ln -sf /etc/nginx/sites-available/dataoz /etc/nginx/sites-enabled/dataoz
rm -f /etc/nginx/sites-enabled/default
nginx -t && systemctl reload nginx

# ── 6. Pare-feu UFW ────────────────────────────────────────────────────────────
echo "=== Pare-feu ==="
ufw allow OpenSSH
ufw allow 'Nginx Full'
ufw allow 5432/tcp          # PostgreSQL — fermer après chargement CSV
ufw --force enable

# ── 7. Résumé des prochaines étapes ────────────────────────────────────────────
echo ""
echo "=========================================="
echo "  Installation terminée !"
echo "  Mémoire disponible :"
free -h
echo "  Swap :"
swapon --show
echo "=========================================="
echo ""
echo "=== Prochaines étapes ==="
echo ""
echo "1. Copier les fichiers depuis le PC :"
echo "   scp schema_postgresql.sql ubuntu@<IP_VM>:~/"
echo "   scp streamlit_app.py ubuntu@<IP_VM>:/opt/dataoz/"
echo ""
echo "2. Appliquer le schéma PostgreSQL :"
echo "   psql -U ${DB_USER} -d ${DB_NAME} -h localhost -f ~/schema_postgresql.sql"
echo ""
echo "3. Charger les données depuis le PC :"
echo "   python load_data.py --host <IP_VM> --password <mdp> --all"
echo ""
echo "4. Démarrer Streamlit :"
echo "   sudo systemctl start dataoz-streamlit"
echo "   sudo systemctl status dataoz-streamlit"
echo ""
echo "5. SSL (après config DNS Ionos) :"
echo "   sudo certbot --nginx -d ${DOMAIN} -d www.${DOMAIN} --agree-tos -m votre@email.fr"
echo ""
echo "6. Fermer le port PostgreSQL après chargement :"
echo "   sudo ufw delete allow 5432/tcp"
