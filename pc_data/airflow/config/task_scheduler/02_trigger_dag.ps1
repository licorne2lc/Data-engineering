# ============================================================
# 02_trigger_dag.ps1
# DataOZ -- Declenchement d'un DAG Airflow via CLI Docker
# ============================================================
# Ce script est le "filet de securite" : il force le lancement
# d'un DAG meme si le scheduler Airflow a manque son heure
# (PC en veille, scheduler plante, etc.)
#
# Usage (appele par chaque tache planifiee avec -DagId) :
#   powershell -File 02_trigger_dag.ps1 -DagId dag_meteo_station
#   powershell -File 02_trigger_dag.ps1 -DagId dag_conso_elec_tuya
#   powershell -File 02_trigger_dag.ps1 -DagId dag_oracle_load
#   powershell -File 02_trigger_dag.ps1 -DagId dag_check_pipeline
#
# Planning recommande :
#   00:50  dag_meteo_station     (avec reveil PC si veille)
#   00:55  dag_conso_elec_tuya   (avec reveil PC si veille)
#   01:55  dag_oracle_load       (avec reveil PC si veille)
#   08:45  dag_check_pipeline    (avec reveil PC si veille)
#
# Note : le scheduler Airflow gere deja ces declenchements.
# Ces taches Windows ne font que s'assurer qu'ils ont bien lieu,
# meme apres une interruption (veille, redemarrage).
#
# La tache doit etre configuree avec :
#   - Cocher "Sortir de veille pour executer cette tache"
#   - Executer en tant que : compte utilisateur courant (pour Docker Desktop)
# ============================================================

param(
    [Parameter(Mandatory=$true)]
    [string]$DagId
)

$logFile = "D:\projet_dataoz\pc_data\airflow\logs\task_scheduler\dag_trigger.log"
New-Item -ItemType Directory -Force -Path (Split-Path $logFile) | Out-Null

function Write-Log {
    param([string]$msg)
    $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    "$ts  [$DagId]  $msg" | Tee-Object -FilePath $logFile -Append
}

Write-Log "=== Declenchement demande ==="

# -- Verifier que Docker tourne ---------------------------------
$dockerOk = (docker info 2>&1) -notmatch "error"
if (-not $dockerOk) {
    Write-Log "Docker non disponible -- tentative de demarrage stack..."
    Set-Location "D:\projet_dataoz"
    docker compose up -d 2>&1 | ForEach-Object { Write-Log $_ }
    Start-Sleep -Seconds 30
}

# -- Verifier que le scheduler tourne --------------------------
$schedulerRunning = docker ps --filter "name=dataoz_airflow_scheduler" --filter "status=running" -q
if (-not $schedulerRunning) {
    Write-Log "Scheduler arrete -- relance via docker compose..."
    Set-Location "D:\projet_dataoz"
    docker compose up -d airflow-scheduler 2>&1 | ForEach-Object { Write-Log $_ }
    Start-Sleep -Seconds 20
}

# -- Verifier si le DAG a deja tourne aujourd'hui --------------
# (evite les doublons si le scheduler a deja declenche le run)
$today = (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd")
$existing = docker exec dataoz_airflow_scheduler `
    airflow dags list-runs --dag-id $DagId --state running,success,queued `
    --start-date "$today" 2>&1

if ($existing -match $today) {
    Write-Log "DAG deja execute ou en cours aujourd'hui ($today) -- pas de declenchement."
    Write-Log "=== Fin (aucune action) ==="
    exit 0
}

# -- Declencher le DAG -----------------------------------------
Write-Log "Declenchement du DAG..."
$triggerResult = docker exec dataoz_airflow_scheduler `
    airflow dags trigger $DagId 2>&1
Write-Log $triggerResult

if ($triggerResult -match "Created.*DagRun|triggered") {
    Write-Log "DAG declenche avec succes."
} else {
    Write-Log "AVERTISSEMENT : reponse inattendue -- verifier l'interface Airflow."
}

Write-Log "=== Fin ==="
