# DataOZ — Pipeline de données personnel end-to-end

> Collecte, transformation, stockage cloud et visualisation de données énergétiques, météo et financières — orchestré par Apache Airflow, hébergé sur Oracle Cloud Infrastructure.

---

## Table des matières

1. [Vue d'ensemble](#vue-densemble)
2. [Architecture](#architecture)
3. [Sources de données](#sources-de-données)
4. [Stack technique](#stack-technique)
5. [Structure du pipeline](#structure-du-pipeline)
6. [Composants principaux](#composants-principaux)
7. [Monitoring intégral](#monitoring-intégral)
8. [Déploiement](#déploiement)
9. [Résultats](#résultats)

---

## Vue d'ensemble

DataOZ est un projet personnel de **data engineering end-to-end** conçu pour collecter, transformer et exploiter des données hétérogènes issues de sources variées (capteurs IoT, fournisseurs d'énergie, API financières, stations météo).

Le pipeline va de la collecte brute jusqu'à l'exploration interactive via une application web, en passant par un stockage cloud managé.

**Objectifs :**
- Centraliser dans une base de données cloud des données fragmentées (capteurs, fichiers, sites web)
- Automatiser l'ensemble de la chaîne avec zéro intervention manuelle au quotidien
- Exposer les données via une interface SQL interactive accessible depuis n'importe où
- Valider l'intégrité de la chaîne complète avec un DAG de monitoring dédié

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  PC LOCAL (Docker / Apache Airflow)                                         │
│                                                                             │
│  Sources           DAGs collecte          CSV curated locaux                │
│  ─────────         ─────────────          ─────────────────                 │
│  Station météo ──► dag_meteo_station  ──► météo/bresser/                    │
│  Tuya SmartLife ──► dag_conso_elec_tuya ► conso_elec/tuya/                  │
│  Enedis (scrap) ──► dag_conso_elec_enedis► conso_elec/enedis/               │
│  Boursorama ─────► dag_boursorama_*   ──► finance/cotations/                │
│  API gouv.fr ────► dag_calendaire     ──► calendaire/                       │
│                                                                             │
│                    dag_oracle_load ──────► Upload vers OCI bucket           │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │ HTTPS (OCI SDK)
┌──────────────────────────────────────▼──────────────────────────────────────┐
│  ORACLE CLOUD INFRASTRUCTURE (Always Free Tier)                             │
│                                                                             │
│  Object Storage bucket (dataoz-curated)                                     │
│        │                                                                    │
│        │  DBMS_SCHEDULER COPY_DATA (07h30 UTC)                              │
│        ▼                                                                    │
│  Oracle Autonomous Database (dataozdb)                                      │
│  ┌─────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────────┐   │
│  │ METEO_BRESSER│ │ ENEDIS_30MIN │ │ TUYA_15MIN   │ │ FINANCE_COTATIONS │   │
│  │ ENEDIS_JOUR │ │ TUYA_HORAIRE │ │ TUYA_JOUR    │ │ CALENDRIER       │   │
│  └─────────────┘ └──────────────┘ └──────────────┘ └──────────────────┘   │
│        │                                                                    │
│        │  oracledb (Python, wallet mTLS)                                   │
│        ▼                                                                    │
│  Streamlit — DataOZ Explorateur de données                                  │
│  https://sql-database.dataoz.fr/            (VM Compute + IONOS DNS)        │
└─────────────────────────────────────────────────────────────────────────────┘
```

![Architecture DataOZ](architecture%20data.png)

---

## Sources de données

### Consommation électrique — Tuya / SmartLife
- **API** : Tuya Cloud API (Beta), statistique `add_ele`
- **Appareils** : prises connectées SmartLife mesurant la consommation par appareil
- **Granularités** : 15 minutes, horaire, journalier, mensuel
- **Collecte** : quotidienne à 02h00 (Paris), historique complet depuis l'origine

### Consommation électrique — Enedis
- **Canal A** : scraping automatique via Playwright depuis l'espace client Enedis (courbe de charge J-5 → J-2)
- **Canal B** : intégration manuelle de fichiers XLSX déposés dans un inbox
- **Granularité** : 30 minutes + agrégat journalier
- **Fusion** : les données manuelles ont priorité sur le scraping en cas de doublon

### Météo — Station Bresser MeteoChamp HD
- **Canal A** : export CSV mensuel depuis Weathercloud (Playwright, login automatique)
- **Canal B** : fichiers USB déposés manuellement dans un inbox
- **Variables** : température intérieure/extérieure, humidité, pression, vent, rafales, précipitations, UV, luminosité
- **Mapping** : catalogue JSON de correspondance des colonnes FR↔EN entre les deux canaux

### Finance — Boursorama
- **Scraping** : cotations ETF et valeurs mobilières via Playwright (Chromium headless)
- **Enrichissement** : données ISIN, historiques 5J et 10A
- **Planification** : quotidien à 06h00 (Paris), jours ouvrés

### Calendrier
- **Sources** : API officielle data.gouv.fr (jours fériés, vacances scolaires zones A/B/C)
- **Enrichissement** : indicateurs semaine paire/impaire, nom du jour, fuseau UTC
- **Mise à jour** : mensuelle (9 496 lignes couvrant plusieurs années)

---

## Stack technique

| Couche | Technologie |
|--------|-------------|
| Orchestration | Apache Airflow 2.8.0 (Docker, LocalExecutor) |
| Collecte / scraping | Python 3.11, Playwright (Chromium headless) |
| Transformation | pandas, openpyxl |
| Stockage staging | OCI Object Storage (bucket `dataoz-curated`) |
| Base de données | Oracle Autonomous Database 23ai (Always Free) |
| ETL cloud | DBMS_SCHEDULER + DBMS_CLOUD.COPY_DATA |
| Connectivité Oracle | python-oracledb (thin mode, wallet mTLS) |
| Interface web | Streamlit, hébergé sur OCI Compute VM (Ubuntu 22.04) |
| DNS / domaine | IONOS — `sql-database.dataoz.fr` |
| Infrastructure as code | Docker Compose, scripts SQL de déploiement |

---

## Structure du pipeline

### Étape 1 — Collecte (PC local, Airflow)

Chaque DAG de collecte s'exécute selon son propre schedule et produit un fichier CSV curated normalisé dans `data/curated/`.

```
dag_meteo_station       → common_weather_database.csv
dag_conso_elec_tuya     → _SYNTHESE_15MIN/HORAIRE/JOURNALIERE/MENSUELLE.csv
dag_conso_elec_enedis   → Database_Enedis_30_min.csv + database_enedis_journalier.csv
dag_boursorama_cotation → boursorama_cotations.csv
dag_calendaire          → socle_calendrier.csv
```

### Étape 2 — Upload bucket OCI

`dag_oracle_load` (quotidien 06h00 UTC) upload les 9 fichiers CSV curated vers le bucket OCI `dataoz-curated` via l'OCI Python SDK (`oci.object_storage`).

### Étape 3 — ETL Oracle (cloud, automatique)

`DBMS_SCHEDULER` déclenche 9 jobs à 07h30 UTC. Chaque job appelle `DBMS_CLOUD.COPY_DATA` pour charger le fichier CSV depuis le bucket dans la table Oracle correspondante (TRUNCATE + reload).

```sql
-- Exemple : chargement METEO_BRESSER
BEGIN
  DBMS_CLOUD.COPY_DATA(
    table_name    => 'METEO_BRESSER',
    credential_name => 'OCI_CRED',
    file_uri_list => 'https://objectstorage.eu-paris-1.oraclecloud.com/.../meteo_bresser.csv',
    format        => JSON_OBJECT('delimiter' VALUE ',', 'skipheaders' VALUE '1', ...)
  );
END;
```

### Étape 4 — Exploration SQL (Streamlit)

Application Streamlit déployée sur une VM OCI Compute (Ubuntu 22.04), accessible via HTTPS sur `sql-database.dataoz.fr`. Connectée à Oracle ADB via `python-oracledb` en mode thin (wallet mTLS). Permet de requêter interactivement toutes les tables avec génération de SQL Oracle natif (`FETCH FIRST`, `TO_TIMESTAMP`, filtres dynamiques).

---

## Composants principaux

### DAGs Airflow

| DAG | Schedule | Description |
|-----|----------|-------------|
| `dag_meteo_station` | Quotidien matin | Données station météo Bresser (2 canaux) |
| `dag_conso_elec_tuya` | Quotidien 02h00 | Consommation Tuya SmartLife (4 granularités) |
| `dag_conso_elec_enedis` | Quotidien | Courbe de charge Enedis (scraping + manuel) |
| `dag_boursorama_cotation` | Quotidien 06h00 | Cotations ETF Boursorama |
| `dag_calendaire` | Mensuel | Jours fériés et vacances scolaires |
| `dag_oracle_load` | Quotidien 06h00 | Upload 9 CSV → bucket OCI |
| `dag_check_pipeline` | Manuel | Monitoring intégral de toute la chaîne |

### Tables Oracle ADB

| Table | Description | Granularité |
|-------|-------------|-------------|
| `METEO_BRESSER` | Données météo station personnelle | 30 min |
| `ENEDIS_30MIN` | Consommation électrique réseau | 30 min |
| `ENEDIS_JOURNALIER` | Agrégat journalier Enedis | Jour |
| `TUYA_15MIN` | Consommation appareils connectés | 15 min |
| `TUYA_HORAIRE` | Consommation appareils connectés | Heure |
| `TUYA_JOURNALIER` | Consommation appareils connectés | Jour |
| `TUYA_MENSUEL` | Consommation appareils connectés | Mois |
| `CALENDRIER` | Référentiel calendaire enrichi | Jour |
| `FINANCE_COTATIONS` | Cours ETF et valeurs mobilières | Séance |

---

## Monitoring intégral

`dag_check_pipeline` est un DAG de supervision qui vérifie toute la chaîne en 5 étapes parallèles puis consolide le résultat :

```
check_collection_dags ──┐
check_csv_freshness   ──┤
check_oci_bucket      ──┼──► pipeline_summary  🎉 PIPELINE 100% OPÉRATIONNEL
check_oracle          ──┤
check_streamlit       ──┘
```

**Contrôles effectués :**

- **Étape 1** — Dernier run de chaque DAG de collecte : état (success/failed) et ancienneté
- **Étape 2** — Fraîcheur et taille de chaque fichier CSV curated local
- **Étape 3** — Présence et ancienneté de chaque fichier dans le bucket OCI
- **Étape 4** — Statut des 9 jobs `DBMS_SCHEDULER` + row counts + fraîcheur des données Oracle
- **Étape 5** — Accessibilité HTTP du Streamlit (timeout < 15 s, HTTP 200)

---

## Déploiement

### Prérequis locaux

- Docker Desktop
- Python 3.11
- Playwright (`playwright install chromium`)
- Clé API OCI + fichier `config` + wallet Oracle ADB

### Lancement Airflow

```bash
docker compose up -d
# Airflow accessible sur http://localhost:8080
```

### Variables d'environnement requises

```env
# Oracle ADB
ORACLE_PASSWORD=...
WALLET_PASSWORD=...
ORACLE_DSN=dataozdb_tp
ORACLE_WALLET_DIR=/opt/airflow/wallet

# OCI Object Storage
OCI_CONFIG_FILE=/opt/airflow/oci_key/config
OCI_NAMESPACE=...
OCI_BUCKET=dataoz-curated

# Sources de données
WEATHERCLOUD_EMAIL=...
WEATHERCLOUD_PASSWORD=...
TUYA_ACCESS_ID=...
TUYA_ACCESS_SECRET=...
```

### Déploiement VM Streamlit (OCI Compute)

```bash
# Copie de l'application
scp -i ssh-key.key -O streamlit_app.py ubuntu@<IP>:/opt/dataoz/

# Service systemd
sudo systemctl restart dataoz-streamlit
```

---

## Résultats

- **Pipeline entièrement automatisé** : zéro intervention manuelle au quotidien
- **9 tables Oracle** alimentées chaque matin à 07h30 UTC
- **26 750+ enregistrements météo**, 63 000+ mesures Enedis 30 min, 671+ mesures Tuya 15 min
- **Streamlit accessible publiquement** sur `https://sql-database.dataoz.fr` avec requêtes SQL Oracle interactives
- **Monitoring end-to-end** : `dag_check_pipeline` valide les 5 étapes en < 3 secondes

---

## Points techniques notables

**Gestion du format Oracle VARCHAR2 pour les timestamps**
DBMS_CLOUD.COPY_DATA convertit les timestamps CSV en format NLS Oracle (`DD-MON-RR HH24:MI:SS`) même pour les colonnes VARCHAR2. La requête de fraîcheur utilise `TO_DATE(SUBSTR(TRIM(ts),1,9), 'DD-MON-RR')` pour extraire la partie date de manière robuste, indépendamment des fractions de secondes éventuelles.

**Dual-channel météo avec catalogue de mapping**
Les deux sources (Weathercloud et clé USB) produisent des formats de colonnes différents. Un `catalog.json` centralise la correspondance FR↔EN et normalise les données vers un schéma commun (`common_weather_database`).

**ETL Enedis multi-canal avec audit de divergences**
Le canal scraping et le canal manuel peuvent produire des valeurs contradictoires sur un même créneau horaire. Le système conserve un audit des divergences et applique une règle de priorité configurable (le manuel écrase le scraping par défaut).

**Airflow `start_date` vs `execution_date`**
Le check de fraîcheur des DAGs utilise `DagRun.start_date` (heure réelle d'exécution) et non `execution_date` (date logique de l'intervalle de données, toujours en retard d'une période).

---

*Projet personnel — Moulinier Jérôme | Stack : Python · Airflow · Oracle ADB · OCI · Streamlit*
