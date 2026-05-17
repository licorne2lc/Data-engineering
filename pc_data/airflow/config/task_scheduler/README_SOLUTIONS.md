# DataOZ — Solutions au problème de veille PC

## Contexte

Quand le PC se met en veille, Docker s'arrête → Airflow s'arrête → les DAGs ne tournent pas.
Au réveil, Airflow relance les runs manqués (catchup=False = 1 run max par DAG), mais trop tard
par rapport à la fenêtre de chargement Oracle (DBMS_SCHEDULER tourne à 07h30 UTC).

---

## 3 solutions, du plus simple au plus robuste

### ✅ Solution A — Empêcher la veille pendant les heures critiques (5 min, zéro code)

Windows peut être configuré pour ne jamais se mettre en veille.
Si vous ne voulez pas désactiver totalement la veille, utilisez **PowerToys → Awake** :
il maintient le PC éveillé sur un créneau horaire (ex. 01h00-09h30) et laisse la veille
s'activer le reste du temps.

- Télécharger PowerToys : https://aka.ms/installpowertoys
- Onglet Awake → mode "Indéfiniment" ou "Intervalle" (ex. de 01:00 à 09:30)

**Avantages** : aucun script, aucune configuration Docker  
**Inconvénients** : le PC reste allumé la nuit (consommation électrique)

---

### ✅ Solution B — Tâches Planifiées Windows (recommandée)

Deux couches de protection complémentaires :

**Couche 1 — Démarrage automatique de la stack** (`01_start_docker_airflow.ps1`)  
Lancé au boot Windows ET à la reprise de veille (via l'événement système).
Fait `docker compose up -d` et attend que le scheduler soit healthy.

**Couche 2 — Filet de sécurité DAG** (`02_trigger_dag.ps1`)  
Chaque DAG a sa propre tâche planifiée Windows avec "Réveiller le PC".
Si Airflow a manqué l'heure, Windows le déclenche directement via `airflow dags trigger`.
Vérifie d'abord si le DAG a déjà tourné aujourd'hui pour éviter les doublons.

**Planning des tâches (heure locale Paris UTC+2 en été) :**

| Heure locale | DAG déclenché         | Heure UTC |
|-------------|----------------------|-----------|
| 02:50       | dag_meteo_station     | 00:50     |
| 02:55       | dag_conso_elec_tuya   | 00:55     |
| 03:55       | dag_oracle_load       | 01:55     |
| 10:45       | dag_check_pipeline    | 08:45     |

**Installation en 1 commande** (PowerShell en admin) :
```powershell
powershell -ExecutionPolicy Bypass -File "D:\projet_dataoz\pc_data\airflow\config\task_scheduler\03_import_tasks.ps1"
```

Vérifier ensuite dans : Planificateur de tâches → Bibliothèque → DataOZ

---

### ✅ Solution C — Réveil planifié Windows (complément à B)

Windows peut réveiller le PC à une heure précise sans intervention manuelle.
Activer dans le BIOS/UEFI : **Wake on RTC** ou **Wake on Alarm**.

Puis dans chaque tâche planifiée (solution B) : cocher **"Sortir de veille pour exécuter cette tâche"**
(déjà configuré dans `03_import_tasks.ps1` via `-WakeToRun`).

**Résultat** : le PC se réveille à 02:50, lance les DAGs, puis peut se rendormir.

---

## Architecture cible avec les tâches planifiées

```
PC en veille
    │
    ▼ 02:50 (heure Paris) — Windows réveille le PC
    │
    ├─ Tâche DataOZ_StartStack   → docker compose up -d (si containers arrêtés)
    ├─ Tâche DataOZ_Dag_Meteo    → airflow dags trigger dag_meteo_station
    ├─ Tâche DataOZ_Dag_Tuya     → airflow dags trigger dag_conso_elec_tuya
    │
    ▼ 03:55
    ├─ Tâche DataOZ_Dag_OracleLoad → airflow dags trigger dag_oracle_load
    │
    ▼ 07:30 (UTC) → DBMS_SCHEDULER Oracle charge les données
    │
    ▼ 10:45
    └─ Tâche DataOZ_Dag_CheckPipe  → airflow dags trigger dag_check_pipeline
```

---

## Fichiers

| Fichier | Rôle |
|---------|------|
| `01_start_docker_airflow.ps1` | Démarre Docker + Airflow au boot/réveil |
| `02_trigger_dag.ps1`          | Déclenche un DAG spécifique (filet de sécurité) |
| `03_import_tasks.ps1`         | Crée toutes les tâches dans le Planificateur Windows |
| `README_SOLUTIONS.md`         | Ce fichier |

---

## Recommandation

Combiner **B + C** :
- Solution B pour le déclenchement fiable des DAGs
- Solution C (WakeToRun déjà activé dans le script) pour réveiller le PC automatiquement
- Optionnel : Solution A (PowerToys Awake) pour les nuits où le PC doit rester éveillé plus longtemps
