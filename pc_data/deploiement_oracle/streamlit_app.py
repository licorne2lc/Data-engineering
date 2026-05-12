"""
streamlit_app.py  -  Explorateur de données DataOZ
====================================================
Connecté à Oracle Autonomous Database (migration depuis PostgreSQL).

Lancement :
    set ORACLE_PASSWORD=xxxx
    set WALLET_PASSWORD=xxxx
    streamlit run streamlit_app.py

Variables d'environnement (optionnelles — valeurs par défaut ci-dessous) :
    ORACLE_PASSWORD   : mot de passe ADMIN Oracle
    WALLET_PASSWORD   : mot de passe du wallet
    ORACLE_DSN        : nom du service TNS (défaut : dataozdb_tp)
    ORACLE_WALLET_DIR : chemin vers le wallet extrait
"""

import os
import oracledb
import streamlit as st
import pandas as pd
from datetime import date, timedelta

# ── Config Oracle ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="DataOZ - Explorateur de données",
    page_icon="📊",
    layout="wide",
)

ORA_USER    = "ADMIN"
ORA_PASS    = os.getenv("ORACLE_PASSWORD", "")
ORA_DSN     = os.getenv("ORACLE_DSN", "dataozdb_tp")
WALLET_DIR  = os.getenv("ORACLE_WALLET_DIR", r"D:\projet_dataoz\pc_data\wallet_oracle")
WALLET_PASS = os.getenv("WALLET_PASSWORD", "")

# Le référentiel des valeurs est chargé directement depuis Oracle (pas de fichier local sur la VM)

# ── Sources et colonnes ────────────────────────────────────────────────────────
# ts_type  : "timestamp" | "date" | "varchar"
# ts_col   : nom réel de la colonne dans Oracle
# id_cols  : colonnes d'identification toujours incluses dans le SELECT
#            (non agrégées, toujours dans le GROUP BY si agrégation)
# filter_col : colonne Oracle utilisée pour filtrer (IN clause) via le sidebar
# ── Structure groupée : Source → Granularité → config Oracle ─────────────────
# Chaque source peut avoir une ou plusieurs granularités.
# Quand une seule granularité est définie, le sélecteur ne s'affiche pas.
SOURCES = {
    "Météo Bresser": {
        "granularites": {
            "5 min": {
                "table":   "meteo_bresser",
                "ts_col":  "ts",
                "ts_type": "timestamp",
                "mesures": {
                    "Température extérieure (°C)": "temp_exterieure",
                    "Température intérieure (°C)": "temp_interieure",
                    "Température étage (°C)":      "temp_etage",
                    "Température cave (°C)":       "temp_cave",
                    "Humidité extérieure (%)":     "hum_exterieure",
                    "Humidité intérieure (%)":     "hum_interieure",
                    "Pression absolue (hPa)":      "pression_abs",
                    "Pression relative (hPa)":     "pression_rel",
                    "Vent vitesse (km/h)":         "vent_vitesse",
                    "Vent rafale (km/h)":          "vent_rafale",
                    "Pluie horaire (mm)":          "pluie_horaire",
                    "UVI":                         "uvi",
                    "Luminosité (lux)":            "luminosite",
                },
            },
        },
    },
    "ENEDIS": {
        "granularites": {
            "30 min": {
                "table":   "enedis_30min",
                "ts_col":  "ts",
                "ts_type": "timestamp",
                "mesures": {"Consommation réseau (W)": "conso_w"},
            },
            "Horaire": {
                "table":   "enedis_horaire",
                "ts_col":  "ts",
                "ts_type": "timestamp",
                "mesures": {"Consommation réseau (kWh)": "conso_kwh"},
            },
            "Journalier": {
                "table":   "enedis_journalier",
                "ts_col":  "date_jour",
                "ts_type": "date",
                "mesures": {"Consommation réseau (kWh)": "conso_kwh"},
            },
        },
    },
    "Tuya": {
        "granularites": {
            "15 min": {
                "table":   "tuya_15min",
                "ts_col":  "ts",
                "ts_type": "timestamp",
                "mesures": {
                    "Ballon eau chaude (kWh)": "ballon_eau_chaude",
                    "Chauffage (kWh)":         "chauffage",
                    "Frigo (kWh)":             "frigo",
                    "Jacuzzi (kWh)":           "jaccuzzi",
                    "Loan (kWh)":              "loan",
                    "Parfum salon (kWh)":      "parfum_salon",
                    "Prise PC (kWh)":          "prise_pc",
                    "Prise parfum (kWh)":      "prise_parfum_ch_parents",
                    "Téléprojecteur (kWh)":    "teleprojecteur",
                    "TV chambre (kWh)":        "tv_chambre",
                    "TV salon (kWh)":          "tv_salon",
                    "TOTAL (kWh)":             "total_kwh",
                },
            },
            "Horaire": {
                # 6 devices : les autres ne remontent pas de stats horaires via l'API Tuya
                "table":   "tuya_horaire",
                "ts_col":  "ts",
                "ts_type": "timestamp",
                "mesures": {
                    "Ballon eau chaude (kWh)": "ballon_eau_chaude",
                    "Chauffage (kWh)":         "chauffage",
                    "Frigo (kWh)":             "frigo",
                    "Prise PC (kWh)":          "prise_pc",
                    "Téléprojecteur (kWh)":    "teleprojecteur",
                    "TV chambre (kWh)":        "tv_chambre",
                    "TOTAL (kWh)":             "total_kwh",
                },
            },
            "Journalier": {
                "table":   "tuya_journalier",
                "ts_col":  "date_jour",
                "ts_type": "date",
                "mesures": {
                    "Ballon eau chaude (kWh)": "ballon_eau_chaude",
                    "Chauffage (kWh)":         "chauffage",
                    "Frigo (kWh)":             "frigo",
                    "Jacuzzi (kWh)":           "jaccuzzi",
                    "Loan (kWh)":              "loan",
                    "Parfum salon (kWh)":      "parfum_salon",
                    "Prise PC (kWh)":          "prise_pc",
                    "Prise parfum (kWh)":      "prise_parfum_ch_parents",
                    "Téléprojecteur (kWh)":    "teleprojecteur",
                    "TV chambre (kWh)":        "tv_chambre",
                    "TV salon (kWh)":          "tv_salon",
                    "TOTAL (kWh)":             "total_kwh",
                },
            },
            "Mensuel": {
                "table":   "tuya_mensuel",
                "ts_col":  "mois",
                "ts_type": "varchar",
                "mesures": {
                    "Ballon eau chaude (kWh)": "ballon_eau_chaude",
                    "Chauffage (kWh)":         "chauffage",
                    "Frigo (kWh)":             "frigo",
                    "Jacuzzi (kWh)":           "jaccuzzi",
                    "Loan (kWh)":              "loan",
                    "Parfum salon (kWh)":      "parfum_salon",
                    "Prise PC (kWh)":          "prise_pc",
                    "Prise parfum (kWh)":      "prise_parfum_ch_parents",
                    "Téléprojecteur (kWh)":    "teleprojecteur",
                    "TV chambre (kWh)":        "tv_chambre",
                    "TV salon (kWh)":          "tv_salon",
                    "TOTAL (kWh)":             "total_kwh",
                },
            },
        },
    },
    "Calendrier": {
        "granularites": {
            "Journalier": {
                "table":   "calendrier",
                "ts_col":  "date_jour",
                "ts_type": "date",
                "mesures": {
                    "Jour de la semaine":  "jour_semaine",
                    "N° jour sem.":        "jour_sem",
                    "N° semaine ISO":      "num_semaine_iso",
                    "Semaine impaire":     "sem_impaire",
                    "Offset UTC":          "utc",
                    "Jour férié":          "nom_jour_ferie",
                    "Vacances zone A":     "vac_scol_a",
                    "Vacances zone B":     "vac_scol_b",
                    "Vacances zone C":     "vac_scol_c",
                },
            },
        },
    },
    "Finance cotations": {
        "granularites": {
            "Journalier": {
                "table":      "finance_cotations",
                "ts_col":     "date_import",
                "ts_type":    "date",
                "id_cols":    ["label", "symbol", "secteur"],
                "filter_col": "symbol",
                "mesures": {
                    "Dernier cours":   "dernier",
                    "Cours précédent": "precedent",
                    "Haut":            "haut",
                    "Bas":             "bas",
                    "Variation (%)":   "variation",
                    "Volume":          "volume",
                },
            },
        },
    },
}

AGREGATIONS = {
    "Données brutes":   None,
    "Moyenne":          "AVG",
    "Maximum":          "MAX",
    "Minimum":          "MIN",
    "Somme":            "SUM",
    "Nombre de lignes": "COUNT",
}

GRANULARITES = {
    "15 minutes": "MI",
    "Heure":      "HH",
    "Jour":       "DD",
    "Semaine":    "IW",
    "Mois":       "MM",
    "Année":      "YYYY",
}

PERIODES_RAPIDES = {
    "Personnalisée":    None,
    "Aujourd'hui":      (date.today(), date.today()),
    "Hier":             (date.today()-timedelta(1), date.today()-timedelta(1)),
    "7 derniers jours": (date.today()-timedelta(7), date.today()),
    "30 derniers jours":(date.today()-timedelta(30), date.today()),
    "Ce mois":          (date.today().replace(day=1), date.today()),
    "Cette année":      (date.today().replace(month=1, day=1), date.today()),
    "Année 2024":       (date(2024, 1, 1), date(2024, 12, 31)),
    "Année 2025":       (date(2025, 1, 1), date(2025, 12, 31)),
}

# ── Référentiel des valeurs boursières (chargé depuis Oracle) ────────────────
@st.cache_data(ttl=3600)
def load_finance_referentiel() -> pd.DataFrame:
    """Charge les valeurs boursières distinctes depuis Oracle pour les filtres sidebar."""
    sql = (
        "SELECT DISTINCT label, symbol, secteur "
        "FROM finance_cotations "
        "WHERE label IS NOT NULL AND symbol IS NOT NULL "
        "ORDER BY label"
    )
    df = run_query(sql)
    df["secteur"] = df["secteur"].fillna("-")
    return df.reset_index(drop=True)

# ── Connexion Oracle ───────────────────────────────────────────────────────────
@st.cache_resource
def get_connection():
    return oracledb.connect(
        user=ORA_USER,
        password=ORA_PASS,
        dsn=ORA_DSN,
        config_dir=WALLET_DIR,
        wallet_location=WALLET_DIR,
        wallet_password=WALLET_PASS,
    )

def run_query(sql: str) -> pd.DataFrame:
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [d[0].lower() for d in cur.description]
            rows = cur.fetchmany(10_000)
            return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        st.cache_resource.clear()
        raise e

# ── Générateur SQL Oracle ──────────────────────────────────────────────────────
def build_sql(
    source_cfg,
    selected_mesures,
    date_debut,
    date_fin,
    agg_label,
    gran_label,
    filter_values=None,   # liste de symboles sélectionnés (Finance uniquement)
):
    table      = source_cfg["table"]
    ts_col     = source_cfg["ts_col"]
    ts_type    = source_cfg["ts_type"]
    agg_fn     = AGREGATIONS[agg_label]
    id_cols    = source_cfg.get("id_cols", [])
    filter_col = source_cfg.get("filter_col", None)

    if agg_fn is None:
        # ── Données brutes ──
        # id_cols en tête, puis ts_col, puis mesures
        select_parts  = id_cols + [ts_col] + [source_cfg["mesures"][m] for m in selected_mesures]
        select_clause = ", ".join(select_parts)
        group_clause  = ""
        order_by      = f"ORDER BY {ts_col} DESC"
        if id_cols:
            # Tri secondaire par label pour regrouper les valeurs
            order_by = f"ORDER BY {ts_col} DESC, {id_cols[0]}"
    else:
        # ── Avec agrégation + granularité ──
        gran_fmt = GRANULARITES[gran_label]
        if ts_type in ("timestamp", "date"):
            trunc_expr = f"TRUNC({ts_col}, '{gran_fmt}')"
        else:
            trunc_expr = ts_col

        agg_cols = ", ".join(
            f"{agg_fn}({source_cfg['mesures'][m]}) AS "
            f"{source_cfg['mesures'][m]}_{agg_fn.lower()}"
            for m in selected_mesures
        )
        # id_cols + période dans le SELECT et le GROUP BY
        id_select     = (", ".join(id_cols) + ", ") if id_cols else ""
        id_group      = (", ".join(id_cols) + ", ") if id_cols else ""
        select_clause = f"{id_select}{trunc_expr} AS periode, {agg_cols}"
        group_clause  = f"GROUP BY {id_group}{trunc_expr}"
        order_by      = "ORDER BY periode DESC"
        if id_cols:
            order_by = f"ORDER BY periode DESC, {id_cols[0]}"

    # ── Clause WHERE — date ──
    if ts_type == "timestamp":
        where_date = (
            f"{ts_col} BETWEEN "
            f"TO_TIMESTAMP('{date_debut}', 'YYYY-MM-DD') AND "
            f"TO_TIMESTAMP('{date_fin} 23:59:59', 'YYYY-MM-DD HH24:MI:SS')"
        )
    elif ts_type == "date":
        where_date = (
            f"{ts_col} BETWEEN "
            f"TO_DATE('{date_debut}', 'YYYY-MM-DD') AND "
            f"TO_DATE('{date_fin}', 'YYYY-MM-DD')"
        )
    else:
        where_date = None

    # ── Clause WHERE — filtre valeurs boursières ──
    where_filter = None
    if filter_col and filter_values:
        quoted = ", ".join(f"'{v}'" for v in filter_values)
        where_filter = f"{filter_col} IN ({quoted})"

    # Assemblage du WHERE
    conditions = [c for c in [where_date, where_filter] if c]
    where      = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    sql = (
        f"SELECT {select_clause}\n"
        f"FROM   {table}\n"
        f"{where}\n"
        f"{group_clause}\n"
        f"{order_by}\n"
        f"FETCH FIRST 10000 ROWS ONLY"
    )
    return sql.strip()

# ── Affichage enrichi du tableau Finance ──────────────────────────────────────
def render_finance_dataframe(df: pd.DataFrame):
    """Affiche le DataFrame Finance avec coloration de la variation."""
    if "variation" not in df.columns:
        st.dataframe(df, use_container_width=True, height=450)
        return

    def color_variation(val):
        try:
            v = float(val)
            if v > 0:
                return "color: #27ae60; font-weight: bold"
            elif v < 0:
                return "color: #e74c3c; font-weight: bold"
        except (TypeError, ValueError):
            pass
        return ""

    # Formatage lisible de la variation en %
    df_display = df.copy()
    if "variation" in df_display.columns:
        df_display["variation"] = df_display["variation"].apply(
            lambda x: f"{float(x)*100:+.2f} %" if pd.notna(x) else "-"
        )

    styled = df_display.style.map(color_variation, subset=["variation"])
    st.dataframe(styled, use_container_width=True, height=450)

# ── Interface Streamlit ────────────────────────────────────────────────────────
st.title("📊 DataOZ — Explorateur de données")
st.caption(
    "Sélectionnez vos données, votre période et votre agrégation. "
    "La requête SQL Oracle est générée automatiquement."
)

col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("① Source")
    source_name = st.pills(
        "Source de données",
        list(SOURCES.keys()),
        default=list(SOURCES.keys())[0],
        label_visibility="collapsed",
    )
    # Sécurité : si pills renvoie None (désélection), garder la première source
    if source_name is None:
        source_name = list(SOURCES.keys())[0]

    source_entry = SOURCES[source_name]
    granularites = source_entry["granularites"]
    gran_keys    = list(granularites.keys())

    # ── Granularité de la source (masquée si une seule option) ──────────────
    if len(gran_keys) > 1:
        st.subheader("① bis — Granularité")
        gran_source = st.pills(
            "Granularité de la source",
            gran_keys,
            default=gran_keys[0],
            label_visibility="collapsed",
        )
        if gran_source is None:
            gran_source = gran_keys[0]
    else:
        gran_source = gran_keys[0]

    source_cfg = granularites[gran_source]
    is_finance = source_name == "Finance cotations"

    st.subheader("② Mesures")
    mesure_labels    = list(source_cfg["mesures"].keys())
    selected_mesures = st.multiselect(
        "Mesures à extraire",
        mesure_labels,
        default=mesure_labels[:min(3, len(mesure_labels))],
        label_visibility="collapsed",
    )

    # ── Filtre valeurs boursières (Finance uniquement) ──────────────────────
    filter_values = None
    if is_finance:
        try:
            ref_df = load_finance_referentiel()
        except Exception as _e:
            st.error(f"Erreur chargement référentiel Finance : {_e}")
            ref_df = pd.DataFrame(columns=["label", "symbol", "secteur"])

        st.subheader("③ Valeurs boursières")
        # Filtre par secteur d'abord
        secteurs_dispos = sorted(ref_df["secteur"].unique().tolist())
        secteurs_selec  = st.multiselect(
            "Filtrer par secteur",
            secteurs_dispos,
            default=[],
            placeholder="Tous les secteurs",
            label_visibility="visible",
        )

        # Puis filtre par nom de valeur
        if secteurs_selec:
            valeurs_df = ref_df[ref_df["secteur"].isin(secteurs_selec)]
        else:
            valeurs_df = ref_df

        valeur_options = {
            f"{row['label']}  ({row['symbol']})": row["symbol"]
            for _, row in valeurs_df.iterrows()
        }
        valeurs_selec = st.multiselect(
            "Sélectionner les valeurs",
            list(valeur_options.keys()),
            default=[],
            placeholder="Toutes les valeurs",
            label_visibility="visible",
        )
        if valeurs_selec:
            # Valeurs spécifiques choisies → filtrer sur ces symboles uniquement
            filter_values = [valeur_options[v] for v in valeurs_selec]
        elif secteurs_selec:
            # Secteur(s) choisi(s) mais pas de valeur spécifique → filtrer sur tous les symboles du secteur
            filter_values = valeurs_df["symbol"].tolist()

        if valeurs_selec:
            st.caption(f"✅ {len(filter_values)} valeur(s) sélectionnée(s)")
        elif secteurs_selec:
            st.caption(f"✅ {len(filter_values)} valeurs dans {len(secteurs_selec)} secteur(s)")
        else:
            st.caption(f"ℹ️ {len(ref_df)} valeurs disponibles (aucun filtre)")

    st.subheader("④ Période" if is_finance else "③ Période")
    periode_rapide = st.selectbox("Raccourci", list(PERIODES_RAPIDES.keys()))
    if PERIODES_RAPIDES[periode_rapide]:
        date_debut, date_fin = PERIODES_RAPIDES[periode_rapide]
    else:
        c1, c2 = st.columns(2)
        with c1:
            date_debut = st.date_input("Du", value=date.today() - timedelta(30))
        with c2:
            date_fin   = st.date_input("Au", value=date.today())

    # Agrégation SQL désactivée — données brutes uniquement
    agg_label  = "Données brutes"
    gran_label = "Jour"

with col2:
    if not selected_mesures:
        st.info("Sélectionnez au moins une mesure.")
    else:
        sql = build_sql(
            source_cfg,
            selected_mesures,
            date_debut,
            date_fin,
            agg_label,
            gran_label,
            filter_values=filter_values,
        )

        st.subheader("SQL généré")
        st.code(sql, language="sql")

        if st.button("▶ Exécuter la requête", type="primary", use_container_width=True):
            try:
                df = run_query(sql)
                if df.empty:
                    st.info("Aucune donnée pour cette période / sélection.")
                elif is_finance:
                    render_finance_dataframe(df)
                else:
                    st.dataframe(df, use_container_width=True, height=450)
                if not df.empty:
                    csv_export = df.to_csv(index=False, sep=";").encode("utf-8")
                    st.download_button(
                        label="⬇ Télécharger les données (CSV)",
                        data=csv_export,
                        file_name=f"{source_cfg['table']}_{date_debut}_{date_fin}.csv",
                        mime="text/csv",
                    )
            except Exception as exc:
                st.error(f"Erreur Oracle : {exc}")
