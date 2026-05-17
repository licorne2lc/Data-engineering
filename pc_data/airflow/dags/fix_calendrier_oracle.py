"""
fix_calendrier_oracle.py
========================
1. Re-uploade calendrier.csv corrigé vers le bucket OCI
2. Charge directement dans la table Oracle CALENDRIER

Usage :
    python3 /opt/airflow/dags/fix_calendrier_oracle.py
"""
import csv, os, sys
import pandas as pd

try:
    import oracledb
except ImportError:
    print("oracledb non disponible"); sys.exit(1)

# ── Paramètres ────────────────────────────────────────────────────────────────
CSV_PATH   = "/opt/airflow/data/curated/calendaire/calendrier.csv"
WALLET_DIR = "/opt/airflow/wallet"
ORA_USER   = "ADMIN"
ORA_PASS   = os.environ.get("ORACLE_PASSWORD", "")
ORA_DSN    = "dataozdb_tp"
WALLET_PW  = os.environ.get("WALLET_PASSWORD", "")
BATCH_SIZE = 500

# ── Étape 1 : upload vers OCI bucket ─────────────────────────────────────────
print("=== ÉTAPE 1 : upload calendrier.csv → bucket OCI ===")
sys.path.insert(0, "/opt/airflow/plugins")
try:
    import upload_to_bucket as bkt
    bkt.upload_calendrier()
    print("  Upload OCI OK")
except Exception as e:
    print(f"  AVERTISSEMENT upload OCI : {e}")
    print("  On continue quand même avec le chargement direct Oracle.")

# ── Helpers ───────────────────────────────────────────────────────────────────
def safe_int(v):
    try:
        return int(float(str(v).strip()))
    except Exception:
        return None

def clean_str(v, maxlen=60):
    s = str(v).strip() if v is not None else "--"
    return s[:maxlen] if s else "--"

# ── Étape 2 : lecture CSV ─────────────────────────────────────────────────────
print("\n=== ÉTAPE 2 : chargement direct Oracle ===")
print(f"Lecture {CSV_PATH} ...")
rows_to_insert = []
with open(CSV_PATH, encoding="utf-8") as f:
    reader = csv.DictReader(f, delimiter=";")
    for r in reader:
        date_val = str(r.get("Date", "")).strip()
        if not date_val or len(date_val) != 10:
            continue
        rows_to_insert.append((
            date_val,
            clean_str(r.get("Jour de la semaine", ""), 20),
            clean_str(r.get("jour Sem", ""), 20),
            safe_int(r.get("N° semaine ISO")),
            safe_int(r.get("Sem. Impaire")),
            clean_str(r.get("UTC", ""), 15),
            clean_str(r.get("nom_jour_ferie", ""), 60),
            clean_str(r.get("vac_scol_A", ""), 60),
            clean_str(r.get("vac_scol_B", ""), 60),
            clean_str(r.get("vac_scol_C", ""), 60),
        ))

feries = sum(1 for r in rows_to_insert if r[6] not in ("--", ""))
vac_a  = sum(1 for r in rows_to_insert if r[7] not in ("--", ""))
print(f"  {len(rows_to_insert)} lignes | Fériés: {feries} | Vacances A: {vac_a}")

# ── Étape 3 : Oracle TRUNCATE + INSERT ───────────────────────────────────────
print("Connexion Oracle ...")
conn = oracledb.connect(
    user=ORA_USER, password=ORA_PASS, dsn=ORA_DSN,
    config_dir=WALLET_DIR, wallet_location=WALLET_DIR, wallet_password=WALLET_PW,
)
cur = conn.cursor()
print("  Connecté — TRUNCATE ...")
cur.execute("TRUNCATE TABLE calendrier")

SQL = """INSERT INTO calendrier
  (date_jour, jour_semaine, jour_sem, num_semaine_iso, sem_impaire,
   utc, nom_jour_ferie, vac_scol_a, vac_scol_b, vac_scol_c)
VALUES (TO_DATE(:1,'YYYY-MM-DD'), :2, :3, :4, :5, :6, :7, :8, :9, :10)"""

total = len(rows_to_insert)
for i in range(0, total, BATCH_SIZE):
    cur.executemany(SQL, rows_to_insert[i:i + BATCH_SIZE])
    print(f"  Inséré {min(i+BATCH_SIZE, total)}/{total} ...")

conn.commit()

# ── Vérification ──────────────────────────────────────────────────────────────
cur.execute("SELECT COUNT(*) FROM calendrier")
count = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM calendrier WHERE nom_jour_ferie != '--'")
f_ora = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM calendrier WHERE vac_scol_a != '--'")
v_ora = cur.fetchone()[0]
cur.execute("""SELECT date_jour, jour_semaine, jour_sem, nom_jour_ferie
               FROM calendrier WHERE ROWNUM <= 5 ORDER BY date_jour DESC""")
print(f"\n=== VÉRIFICATION ===")
print(f"  Lignes: {count} | Fériés: {f_ora} | Vacances A: {v_ora}")
for row in cur.fetchall():
    print(f"    {row}")

cur.close(); conn.close()
print("\nTerminé. NE PAS relancer DBMS_SCHEDULER manuellement.")
print("Le cycle nuit (dag_oracle_load 02h → DBMS_SCHEDULER 07h30) prendra le relais.")
