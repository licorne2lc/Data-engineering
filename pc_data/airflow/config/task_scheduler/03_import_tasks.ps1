# ============================================================
# 03_import_tasks.ps1
# DataOZ -- Creation des taches planifiees Windows
# ============================================================
# Executer UNE SEULE FOIS en PowerShell administrateur :
#   powershell -ExecutionPolicy Bypass -File "D:\projet_dataoz\pc_data\airflow\config\task_scheduler\03_import_tasks.ps1"
#
# Cree 3 taches dans le Planificateur de taches Windows (\DataOZ\) :
#
#   1. DataOZ_StartStack         -- au boot + connexion (sans reveil)
#   2. DataOZ_Wake_Acquisition   -- 00:45 Paris  REVEIL PC  fenetre 01h-02h
#   3. DataOZ_Wake_CheckPipeline -- 10:45 Paris  REVEIL PC  apres DBMS_SCHEDULER
#
# Heures en HEURE LOCALE Paris (UTC+2 ete / UTC+1 hiver).
# ============================================================

$baseDir = "D:\projet_dataoz\pc_data\airflow\config\task_scheduler"
$logDir  = "D:\projet_dataoz\pc_data\airflow\logs\task_scheduler"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

# Helper : action PowerShell cachee
function New-PSAction {
    param([string]$script)
    return New-ScheduledTaskAction `
        -Execute "powershell.exe" `
        -Argument "-ExecutionPolicy Bypass -NonInteractive -WindowStyle Hidden -File `"$script`""
}

# ===============================================================
# TACHE 1 -- Demarrage stack au boot / connexion (pas de reveil)
# ===============================================================
$t1_settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 10) `
    -MultipleInstances IgnoreNew `
    -StartWhenAvailable

$t1_triggers = @(
    (New-ScheduledTaskTrigger -AtStartup),
    (New-ScheduledTaskTrigger -AtLogOn)
)

Register-ScheduledTask `
    -TaskName   "DataOZ_StartStack" `
    -TaskPath   "\DataOZ\" `
    -Action     (New-PSAction "$baseDir\01_start_docker_airflow.ps1") `
    -Trigger    $t1_triggers `
    -Settings   $t1_settings `
    -RunLevel   Highest `
    -Description "DataOZ -- Demarre Docker/Airflow au boot et a la connexion" `
    -Force | Out-Null
Write-Host "OK  DataOZ_StartStack (boot + connexion)"

# ===============================================================
# TACHE 2 -- REVEIL 00:45 Paris -- Acquisition donnees 01h-02h
# ===============================================================
# Reveille le PC a 00:45 pour couvrir la fenetre :
#   01:00  dag_meteo_station
#   01:05  dag_conso_elec_tuya
#   02:00  dag_oracle_load
# Duree max : 2h (le script attend 70 min avant de lancer oracle_load)
# ===============================================================
$t2_settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Hours 2) `
    -MultipleInstances IgnoreNew `
    -StartWhenAvailable `
    -WakeToRun

Register-ScheduledTask `
    -TaskName   "DataOZ_Wake_Acquisition" `
    -TaskPath   "\DataOZ\" `
    -Action     (New-PSAction "$baseDir\wake_acquisition.ps1") `
    -Trigger    (New-ScheduledTaskTrigger -Daily -At "00:45") `
    -Settings   $t2_settings `
    -RunLevel   Highest `
    -Description "DataOZ -- REVEIL PC + acquisition donnees (meteo, Tuya, oracle_load)" `
    -Force | Out-Null
Write-Host "OK  DataOZ_Wake_Acquisition (00:45 - REVEIL PC)"

# ===============================================================
# TACHE 3 -- REVEIL 10:45 Paris -- Verification check pipeline
# ===============================================================
# Reveille le PC a 10:45 pour verifier le pipeline apres :
#   07:30 UTC  DBMS_SCHEDULER Oracle charge les donnees
#   09:00 UTC  dag_check_pipeline schedule Airflow (11:00 Paris)
# Cette tache est le filet de securite si Airflow a rate l'heure.
# ===============================================================
$t3_settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 15) `
    -MultipleInstances IgnoreNew `
    -StartWhenAvailable `
    -WakeToRun

Register-ScheduledTask `
    -TaskName   "DataOZ_Wake_CheckPipeline" `
    -TaskPath   "\DataOZ\" `
    -Action     (New-PSAction "$baseDir\wake_check_pipeline.ps1") `
    -Trigger    (New-ScheduledTaskTrigger -Daily -At "10:45") `
    -Settings   $t3_settings `
    -RunLevel   Highest `
    -Description "DataOZ -- REVEIL PC + declenchement dag_check_pipeline (apres DBMS_SCHEDULER)" `
    -Force | Out-Null
Write-Host "OK  DataOZ_Wake_CheckPipeline (10:45 - REVEIL PC)"

# ===============================================================
Write-Host ""
Write-Host "3 taches creees dans \DataOZ\"
Write-Host "Planificateur de taches -> Bibliotheque -> DataOZ"
Write-Host ""
Write-Host "Recapitulatif (heure locale Paris UTC+2 ete) :"
Write-Host "  Boot/Connexion  DataOZ_StartStack         -> docker compose up -d"
Write-Host "  00:45  REVEIL   DataOZ_Wake_Acquisition   -> meteo + tuya + oracle_load"
Write-Host "  10:45  REVEIL   DataOZ_Wake_CheckPipeline -> dag_check_pipeline"
