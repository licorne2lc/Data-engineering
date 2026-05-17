# ============================================================
# wake_acquisition.ps1
# DataOZ -- Reveil PC + acquisition donnees (01h00-02h00 Paris)
# ============================================================
# Declenchee par la tache planifiee DataOZ_Wake_Acquisition
# Heure de declenchement : 00:45 heure locale Paris (UTC+2 ete)
#
# DAGs couverts (heure Paris UTC+2) :
#   01:00  dag_meteo_station
#   01:05  dag_conso_elec_tuya
#   02:00  dag_oracle_load
#
# Le script :
#   1. S'assure que Docker et Airflow tournent
#   2. Declenche les 3 DAGs si Airflow ne l'a pas deja fait
# ============================================================

$logFile = "D:\projet_dataoz\pc_data\airflow\logs\task_scheduler\wake_acquisition.log"
New-Item -ItemType Directory -Force -Path (Split-Path $logFile) | Out-Null

function Write-Log {
    param([string]$msg)
    $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    "$ts  $msg" | Tee-Object -FilePath $logFile -Append
}

function Ensure-DockerUp {
    Write-Log "Verification Docker..."
    $ok = (docker info 2>&1) -notmatch "error"
    if (-not $ok) {
        Write-Log "Docker non disponible - demarrage stack..."
        Set-Location "D:\projet_dataoz"
        docker compose up -d 2>&1 | ForEach-Object { Write-Log "  $_" }
        Write-Log "Attente 30s..."
        Start-Sleep -Seconds 30
    } else {
        Write-Log "Docker OK"
    }
    # Verifier que le scheduler tourne
    $running = docker ps --filter "name=dataoz_airflow_scheduler" --filter "status=running" -q
    if (-not $running) {
        Write-Log "Scheduler arrete - relance..."
        Set-Location "D:\projet_dataoz"
        docker compose up -d airflow-scheduler 2>&1 | ForEach-Object { Write-Log "  $_" }
        Start-Sleep -Seconds 20
    }
}

function Trigger-DagIfNeeded {
    param([string]$DagId)
    $today = (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd")
    $check = docker exec dataoz_airflow_scheduler `
        airflow dags list-runs --dag-id $DagId --state running,success,queued `
        --start-date "$today" 2>&1
    if ($check -match $today) {
        Write-Log "[$DagId] Deja execute aujourd'hui - pas de declenchement"
        return
    }
    Write-Log "[$DagId] Declenchement..."
    $result = docker exec dataoz_airflow_scheduler airflow dags trigger $DagId 2>&1
    Write-Log "[$DagId] $result"
}

# -- Corps principal --------------------------------------------
Write-Log "================================================="
Write-Log "=== REVEIL ACQUISITION - Fenetre 01h00-02h00  ==="
Write-Log "================================================="

Ensure-DockerUp

# Declencher les 3 DAGs d'acquisition (filet de securite)
# Airflow les aurait de toute facon declenches si le scheduler tournait,
# mais on force au cas ou il aurait rate l'heure pendant la veille.
Trigger-DagIfNeeded "dag_meteo_station"
Start-Sleep -Seconds 5
Trigger-DagIfNeeded "dag_conso_elec_tuya"
Start-Sleep -Seconds 5
# dag_conso_elec_enedis schedule a 01:10 UTC (03:10 Paris) - filet de securite
Trigger-DagIfNeeded "dag_conso_elec_enedis"

# dag_oracle_load tourne a 02:00 Paris - on attend et on declenche
Write-Log "Attente 70 min pour dag_oracle_load (02:00 Paris)..."
Start-Sleep -Seconds 4200   # 70 minutes

Trigger-DagIfNeeded "dag_oracle_load"

Write-Log "=== Acquisition terminee ==="
