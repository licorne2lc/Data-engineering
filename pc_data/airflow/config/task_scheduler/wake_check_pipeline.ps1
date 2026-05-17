# ============================================================
# wake_check_pipeline.ps1
# DataOZ -- Reveil PC + verification check pipeline (04:30 Paris)
# ============================================================
# Declenchee par la tache planifiee DataOZ_Wake_CheckPipeline
# Heure de declenchement : 04:30 heure locale Paris (UTC+2 ete) = 02:30 UTC
#
# Contexte :
#   02:00 UTC (04:00 Paris) : dag_oracle_load uploade les CSV vers OCI
#   02:00 UTC (04:00 Paris) : DBMS_SCHEDULER Oracle charge les tables depuis OCI
#   03:15 UTC (05:15 Paris) : dag_check_pipeline verifie que tout est OK
#
#   Ce script reveille le PC a 04:30 Paris pour laisser 45 min a Docker/Airflow
#   de demarrer avant le dag_check_pipeline a 05:15 Paris.
#   Il sert aussi de filet de securite si Airflow a rate son heure schedulee.
# ============================================================

$logFile = "D:\projet_dataoz\pc_data\airflow\logs\task_scheduler\wake_check_pipeline.log"
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
        Start-Sleep -Seconds 30
    } else {
        Write-Log "Docker OK"
    }
    $running = docker ps --filter "name=dataoz_airflow_scheduler" --filter "status=running" -q
    if (-not $running) {
        Write-Log "Scheduler arrete - relance..."
        Set-Location "D:\projet_dataoz"
        docker compose up -d airflow-scheduler 2>&1 | ForEach-Object { Write-Log "  $_" }
        Start-Sleep -Seconds 20
    }
}

# -- Corps principal --------------------------------------------
Write-Log "================================================="
Write-Log "=== REVEIL CHECK PIPELINE - 04:30 Paris       ==="
Write-Log "================================================="

Ensure-DockerUp

$today = (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd")
$check = docker exec dataoz_airflow_scheduler `
    airflow dags list-runs --dag-id dag_check_pipeline --state running,success,queued `
    --start-date "$today" 2>&1

if ($check -match $today) {
    Write-Log "[dag_check_pipeline] Deja execute aujourd'hui - pas de declenchement"
} else {
    Write-Log "[dag_check_pipeline] Declenchement..."
    $result = docker exec dataoz_airflow_scheduler airflow dags trigger dag_check_pipeline 2>&1
    Write-Log "[dag_check_pipeline] $result"
}

Write-Log "=== Fin check pipeline ==="
