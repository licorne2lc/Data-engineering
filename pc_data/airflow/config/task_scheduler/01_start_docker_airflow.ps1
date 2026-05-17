# ============================================================
# 01_start_docker_airflow.ps1
# DataOZ -- Demarrage automatique de la stack Docker/Airflow
# ============================================================
# Usage :
#   Declencher via Tache Planifiee Windows a :
#     - Au demarrage de Windows (trigger : "Au demarrage")
#     - Apres reprise de veille (trigger : "Sur evenement" -> EventID 107, source "Power-Troubleshooter")
#
# La tache doit etre configuree avec :
#   - Executer en tant que : SYSTEM ou compte admin
#   - Cocher "Executer avec les autorisations maximales"
#   - Decocher "Demarrer uniquement si l'alimentation secteur est branchee"
# ============================================================

$logFile  = "D:\projet_dataoz\pc_data\airflow\logs\task_scheduler\startup.log"
$compose  = "D:\projet_dataoz"

# Creer le dossier de log si inexistant
New-Item -ItemType Directory -Force -Path (Split-Path $logFile) | Out-Null

function Write-Log {
    param([string]$msg)
    $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    "$ts  $msg" | Tee-Object -FilePath $logFile -Append
}

Write-Log "=== Demarrage DataOZ Stack ==="

# -- Attendre que Docker Desktop soit operationnel --------------
Write-Log "Attente Docker Desktop..."
$maxWait = 120   # secondes
$waited  = 0
do {
    Start-Sleep -Seconds 5
    $waited += 5
    $dockerOk = (docker info 2>&1) -notmatch "error"
} while (-not $dockerOk -and $waited -lt $maxWait)

if (-not $dockerOk) {
    Write-Log "ERREUR : Docker Desktop non disponible apres ${maxWait}s -- abandon."
    exit 1
}
Write-Log "Docker Desktop OK (${waited}s)"

# -- Lancer / relancer la stack docker compose ------------------
Write-Log "docker compose up -d ..."
Set-Location $compose
$result = docker compose up -d 2>&1
Write-Log $result

# -- Attendre que le scheduler Airflow soit healthy -------------
Write-Log "Attente scheduler Airflow..."
$waited = 0
do {
    Start-Sleep -Seconds 10
    $waited += 10
    $health = docker inspect --format="{{.State.Health.Status}}" dataoz_airflow_scheduler 2>&1
} while ($health -ne "healthy" -and $waited -lt 120)

if ($health -eq "healthy") {
    Write-Log "Airflow scheduler HEALTHY (${waited}s)"
} else {
    Write-Log "AVERTISSEMENT : scheduler pas encore healthy apres ${waited}s (etat: $health)"
}

Write-Log "=== Stack demarree ==="
