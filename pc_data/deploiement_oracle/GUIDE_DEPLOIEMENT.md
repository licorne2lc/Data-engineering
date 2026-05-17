# Guide de déploiement — DataOZ sur Oracle Free Tier

## Architecture cible

```
PC local (Airflow)
   └── CSV curated  ──── load_data.py ──→  PostgreSQL
                                               ↑
                                        VM Oracle ARM A1
                                        Ubuntu 22.04
                                        PostgreSQL 15
                                        Streamlit 8501
                                        Nginx (80/443)
                                               ↑
                                        votre-domaine.fr (Ionos DNS)
                                               ↑
                                        Navigateur utilisateur
```

---

## Étape 1 — Créer la VM Oracle Free Tier

### 1.1 Créer le compte Oracle Cloud
- Aller sur https://cloud.oracle.com
- S'inscrire avec un compte Always Free
- Choisir la région **Germany Central (Frankfurt)** ou **UK South (London)**
- Renseigner une carte bancaire (non débitée si Free Tier respecté)

### 1.2 Créer le réseau (VCN)
Menu → Networking → Virtual Cloud Networks → Start VCN Wizard
- Choisir "Create VCN with Internet Connectivity"
- Nom : `dataoz-vcn`
- Laisser les CIDR par défaut
- Cliquer "Create"

### 1.3 Ouvrir les ports dans la Security List
Menu → Networking → Virtual Cloud Networks → `dataoz-vcn` → Security Lists → Default Security List

Ajouter les règles Ingress suivantes :

| Port | Protocole | Description |
|------|-----------|-------------|
| 22   | TCP | SSH |
| 80   | TCP | HTTP |
| 443  | TCP | HTTPS |
| 5432 | TCP | PostgreSQL (chargement initial, fermer ensuite) |

### 1.4 Créer la VM ARM A1
Menu → Compute → Instances → Create Instance

- **Nom** : `dataoz-vm`
- **Image** : Canonical Ubuntu 22.04 (Minimal)
- **Shape** : VM.Standard.A1.Flex
  - OCPUs : **4** (gratuit)
  - RAM : **24 GB** (gratuit)
- **Réseau** : dataoz-vcn, subnet public
- **SSH Key** : uploader votre clé publique `.pub`
- Cliquer "Create"

### 1.5 Réserver une IP publique fixe
Menu → Networking → Reserved Public IPs → Reserve Public IP
Associer l'IP à votre VM.
**Notez cette IP** — vous en aurez besoin pour DNS et SSH.

---

## Étape 2 — Configurer le DNS Ionos

Dans votre espace client Ionos → Domaines → Gérer le DNS :

| Type | Nom | Valeur | TTL |
|------|-----|--------|-----|
| A    | @   | `<IP_VM_Oracle>` | 3600 |
| A    | www | `<IP_VM_Oracle>` | 3600 |

Attendre 15–30 min pour la propagation DNS.

---

## Étape 3 — Installer la VM

Se connecter en SSH :
```bash
ssh ubuntu@<IP_VM>
```

Copier les fichiers sur la VM :
```bash
# Depuis votre PC Windows
scp schema_postgresql.sql ubuntu@<IP_VM>:~/
scp setup_vm.sh ubuntu@<IP_VM>:~/
scp streamlit_app.py ubuntu@<IP_VM>:~/
```

Lancer le script d'installation :
```bash
# Sur la VM
nano setup_vm.sh   # Modifier DB_PASSWORD et DOMAIN
chmod +x setup_vm.sh
sudo ./setup_vm.sh
```

---

## Étape 4 — Créer le schéma PostgreSQL

Sur la VM :
```bash
psql -U dataoz_user -d dataoz -h localhost -f ~/schema_postgresql.sql
# Mot de passe : celui défini dans setup_vm.sh
```

Vérifier les tables créées :
```bash
psql -U dataoz_user -d dataoz -c "\dt"
```

---

## Étape 5 — Charger les données depuis le PC

Sur votre PC Windows, installer les dépendances :
```bash
pip install psycopg2-binary pandas
```

Charger toutes les tables :
```bash
python load_data.py --host <IP_VM> --password <DB_PASSWORD> --all
```

Ou table par table :
```bash
python load_data.py --host <IP_VM> --password <DB_PASSWORD> --table meteo_bresser
python load_data.py --host <IP_VM> --password <DB_PASSWORD> --table enedis_30min
python load_data.py --host <IP_VM> --password <DB_PASSWORD> --table tuya_journalier
```

**Après le chargement**, fermer le port 5432 dans la Security List Oracle (sécurité).

---

## Étape 6 — Déployer Streamlit

Sur la VM :
```bash
cp ~/streamlit_app.py /opt/dataoz/
sudo systemctl start dataoz-streamlit
sudo systemctl status dataoz-streamlit
```

Tester en local sur la VM :
```bash
curl http://127.0.0.1:8501
```

---

## Étape 7 — Certificat SSL

Une fois le DNS propagé (vérifier avec `ping votre-domaine.fr`) :
```bash
sudo certbot --nginx -d votre-domaine.fr -d www.votre-domaine.fr \
  --non-interactive --agree-tos -m votre@email.fr
```

Votre app est accessible sur **https://votre-domaine.fr**

---

## Étape 8 — Intégration Airflow (mises à jour automatiques)

Dans votre DAG Airflow existant, ajouter une tâche finale qui exécute `load_data.py` après chaque pipeline de collecte. Exemple :

```python
from airflow.operators.bash import BashOperator

charger_bdd = BashOperator(
    task_id='charger_postgresql',
    bash_command=(
        'python D:/projet_dataoz/pc_data/deploiement_oracle/load_data.py '
        '--host <IP_VM> --password <DB_PASSWORD> --table meteo_bresser'
    ),
)

collecter_meteo >> traiter_meteo >> charger_bdd
```

---

## Structure des fichiers livrés

```
deploiement_oracle/
├── GUIDE_DEPLOIEMENT.md      ← ce fichier
├── schema_postgresql.sql     ← DDL complet (9 tables + 1 vue)
├── load_data.py              ← chargement CSV → PostgreSQL
├── streamlit_app.py          ← application web
└── setup_vm.sh               ← installation VM Oracle
```

---

## Tables créées

| Table | Source | Granularité | ~Lignes |
|-------|--------|-------------|---------|
| `meteo_bresser` | Station Bresser | 30 min | 26 500 |
| `enedis_30min` | ENEDIS | 30 min | 63 000 |
| `enedis_journalier` | ENEDIS | Jour | ~1 300 |
| `tuya_15min` | Tuya appareils | 15 min | 670 |
| `tuya_horaire` | Tuya appareils | Heure | - |
| `tuya_journalier` | Tuya appareils | Jour | - |
| `tuya_mensuel` | Tuya appareils | Mois | - |
| `calendrier` | Socle calendaire | Jour | ~3 650 |
| `finance_cotations` | Boursorama | Snapshot | - |
| `v_meteo_enedis_journalier` | Vue jointe | Jour | vue |
