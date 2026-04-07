import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import pyodbc

# ================= CONFIG =================
st.set_page_config(
    page_title="Amazon Sales Dashboard",
    page_icon="📊",
    layout="wide"
)

# ================= STYLE =================
st.markdown("""
<style>
    .main { background-color: #E8F8F5; }
    .metric-card {
        background: white;
        border-radius: 10px;
        padding: 20px;
        border-left: 4px solid #8B2346;
        margin: 5px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.06);
    }
    .metric-value {
        font-size: 28px;
        font-weight: bold;
        color: #8B2346;
    }
    .metric-label {
        font-size: 13px;
        color: #1B6B7B;
    }
    h1 { color: #3D1A24; }
    h2, h3 { color: #8B2346; }
</style>
""", unsafe_allow_html=True)

COLORS = ["#8B2346", "#1B6B7B", "#2D8A6E", "#C4748A", "#5B4B8A", "#D4956A"]

# ================= CONNEXION AZURE SQL =================
@st.cache_data
def load_data():
    try:
        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=amazon--server.database.windows.net;"
            "DATABASE=amazone_dw;"
            "UID=CloudSA456cceb9;"
            "PWD=Leilaamazonserver04;"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            "Connection Timeout=30;"
        )

        fact = pd.read_sql("SELECT * FROM Fact_Commandes", conn)
        prod = pd.read_sql("SELECT * FROM Dim_Product", conn)
        loc  = pd.read_sql("SELECT * FROM Dim_Location", conn)
        date = pd.read_sql("SELECT * FROM Dim_Date", conn)
        cust = pd.read_sql("SELECT * FROM Dim_Customer", conn)

        conn.close()
        return fact, prod, loc, date, cust

    except Exception as e:
        st.error(f"❌ Erreur de connexion Azure SQL : {e}")
        return None, None, None, None, None

fact, prod, loc, date, cust = load_data()

if fact is None:
    st.stop()

# ================= PREPARATION DES DONNEES =================
df = (
    fact.merge(prod, on="product_id", how="left")
        .merge(loc, on="location_id", how="left")
        .merge(date, on="date_id", how="left")
        .merge(cust, on="customer_id", how="left")
)

# ================= SIDEBAR FILTRES =================
st.sidebar.title("Filtres")

years = st.sidebar.multiselect(
    "Année",
    sorted(df["year"].dropna().unique()),
    default=sorted(df["year"].dropna().unique())
)

categories = st.sidebar.multiselect(
    "Catégorie",
    sorted(df["category"].dropna().unique()),
    default=sorted(df["category"].dropna().unique())
)

countries = st.sidebar.multiselect(
    "Pays",
    sorted(df["country"].dropna().unique()),
    default=sorted(df["country"].dropna().unique())
)

statuses = st.sidebar.multiselect(
    "Statut commande",
    sorted(df["order_status"].dropna().unique()),
    default=sorted(df["order_status"].dropna().unique())
)

payment_methods = st.sidebar.multiselect(
    "Méthode de paiement",
    sorted(df["payment_method"].dropna().unique()),
    default=sorted(df["payment_method"].dropna().unique())
)

filtered = df[
    (df["year"].isin(years)) &
    (df["category"].isin(categories)) &
    (df["country"].isin(countries)) &
    (df["order_status"].isin(statuses)) &
    (df["payment_method"].isin(payment_methods))
].copy()

prev_years = [y - 1 for y in years]
prev = df[
    (df["year"].isin(prev_years)) &
    (df["category"].isin(categories)) &
    (df["country"].isin(countries)) &
    (df["order_status"].isin(statuses)) &
    (df["payment_method"].isin(payment_methods))
].copy()

# ================= TITRE =================
st.title("Amazon Sales Dashboard")
st.caption(f"{filtered['order_id'].nunique():,} commandes filtrées")
st.markdown("---")

# ================= KPI =================
ca_total = filtered["ca_net"].sum()
nb_commandes = filtered["order_id"].nunique()
marge_totale = filtered["marge"].sum()
remise_moyenne = filtered["discount"].mean() * 100 if len(filtered) > 0 else 0
panier_moyen = ca_total / nb_commandes if nb_commandes > 0 else 0

col1, col2, col3, col4, col5 = st.columns(5)

for col, label, value in [
    (col1, "CA Total", f"${ca_total/1e6:.1f}M"),
    (col2, "Commandes", f"{nb_commandes:,}"),
    (col3, "Marge totale", f"${marge_totale/1e6:.1f}M"),
    (col4, "Remise moyenne", f"{remise_moyenne:.1f}%"),
    (col5, "Panier moyen", f"${panier_moyen:.0f}")
]:
    with col:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">{label}</div>
            <div class="metric-value">{value}</div>
        </div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ================= CHART EXAMPLE =================
fig_line = px.line(title="Exemple")
fig_line.update_layout(
    plot_bgcolor="white",
    paper_bgcolor="#E8F8F5"
)
st.plotly_chart(fig_line)

st.caption("Amazon Sales Dashboard • Projet BI 2026")