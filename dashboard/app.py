import streamlit as st
import plotly.express as px
import pandas as pd
import pyodbc

# Thème baby pink / burgundy / pétrole
st.set_page_config(
    page_title="Amazon Sales Dashboard",
    page_icon="📊",
    layout="wide"
)

# CSS personnalisé — ton thème girly pro
st.markdown("""
<style>
    .main { background-color: #FDF0F3; }
    .metric-card {
        background: white;
        border-radius: 10px;
        padding: 20px;
        border-left: 4px solid #8B2346;
        margin: 5px;
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
    h2 { color: #8B2346; }
</style>
""", unsafe_allow_html=True)

# Connexion Azure SQL
@st.cache_data
def load_data():
    try:
        st.write("🔄 Connexion à Azure...")
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
        st.write("✅ Connexion réussie !")

        fact = pd.read_sql("SELECT COUNT(*) as n FROM Fact_Commandes", conn)
        st.write(f"✅ Fact_Commandes : {fact['n'][0]} lignes")

        fact = pd.read_sql("SELECT * FROM Fact_Commandes", conn)
        prod = pd.read_sql("SELECT * FROM Dim_Product", conn)
        loc  = pd.read_sql("SELECT * FROM Dim_Location", conn)
        date = pd.read_sql("SELECT * FROM Dim_Date", conn)
        conn.close()
        st.write("✅ Toutes les tables chargées !")
        return fact, prod, loc, date

    except Exception as e:
        st.error(f"❌ Erreur : {e}")
        return None, None, None, None

fact, prod, loc, date = load_data()
if fact is None:
    st.error("❌ Impossible de charger les données — vérifiez la connexion Azure")
    st.stop()
st.write(f"✅ {len(fact)} lignes chargées dans fact")
# Merge
df = fact.merge(prod, on='product_id') \
         .merge(loc,  on='location_id') \
         .merge(date, on='date_id')

# ============ SIDEBAR FILTRES ============
st.sidebar.title("Filtres")
years      = st.sidebar.multiselect("Année",    sorted(df['year'].unique()),    default=sorted(df['year'].unique()))
categories = st.sidebar.multiselect("Catégorie", df['category'].unique(),       default=df['category'].unique())
countries  = st.sidebar.multiselect("Pays",      df['country'].unique(),        default=df['country'].unique())

# Appliquer filtres
filtered = df[
    (df['year'].isin(years)) &
    (df['category'].isin(categories)) &
    (df['country'].isin(countries))
]

# ============ TITRE ============
st.title("Amazon Sales Dashboard")
st.markdown("---")

# ============ KPI CARDS ============
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">CA Total</div>
        <div class="metric-value">${filtered['total_amount'].sum()/1e6:.1f}M</div>
    </div>""", unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Commandes</div>
        <div class="metric-value">{len(filtered):,}</div>
    </div>""", unsafe_allow_html=True)

with col3:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Marge moyenne</div>
        <div class="metric-value">${filtered['marge'].mean():.1f}</div>
    </div>""", unsafe_allow_html=True)

with col4:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Remise moyenne</div>
        <div class="metric-value">{filtered['discount'].mean()*100:.1f}%</div>
    </div>""", unsafe_allow_html=True)

with col5:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Panier moyen</div>
        <div class="metric-value">${filtered['total_amount'].mean():.0f}</div>
    </div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ============ COURBE CA PAR MOIS ============
col_left, col_right = st.columns([2, 1])

with col_left:
    ca_mois = filtered.groupby(['year','month'])['total_amount'].sum().reset_index()
    ca_mois['periode'] = ca_mois['year'].astype(str) + "-" + ca_mois['month'].astype(str).str.zfill(2)
    
    fig_line = px.line(
        ca_mois, x='periode', y='total_amount',
        title="Evolution du CA par mois",
        color_discrete_sequence=["#1B6B7B"]
    )
    fig_line.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='#FDF0F3',
        title_font_color='#3D1A24'
    )
    st.plotly_chart(fig_line, use_container_width=True)

# ============ TREEMAP CATEGORIES ============
with col_right:
    ca_cat = filtered.groupby('category')['total_amount'].sum().reset_index()
    
    fig_tree = px.treemap(
        ca_cat, path=['category'], values='total_amount',
        title="CA par catégorie",
        color_discrete_sequence=["#8B2346","#1B6B7B","#2D8A6E","#C4748A","#5B4B8A","#D4956A"]
    )
    fig_tree.update_layout(paper_bgcolor='#FDF0F3')
    st.plotly_chart(fig_tree, use_container_width=True)

# ============ CARTE GEO + DONUT ============
col3a, col3b = st.columns([2, 1])

with col3a:
    ca_pays = filtered.groupby('country')['total_amount'].sum().reset_index()
    
    fig_map = px.choropleth(
        ca_pays, locations='country',
        locationmode='country names',
        color='total_amount',
        title="Répartition géographique",
        color_continuous_scale=["#FDF0F3","#C4748A","#8B2346"]
    )
    fig_map.update_layout(paper_bgcolor='#FDF0F3')
    st.plotly_chart(fig_map, use_container_width=True)

with col3b:
    status = filtered.groupby('order_status')['order_id'].count().reset_index()
    
    fig_donut = px.pie(
        status, names='order_status', values='order_id',
        title="Statut des commandes", hole=0.5,
        color_discrete_sequence=["#8B2346","#1B6B7B","#2D8A6E","#C4748A","#5B4B8A"]
    )
    fig_donut.update_layout(paper_bgcolor='#FDF0F3')
    st.plotly_chart(fig_donut, use_container_width=True)

# ============ TOP PRODUITS ============
st.subheader("Top produits")
top = filtered.groupby(['product_name','category']).agg(
    CA=('total_amount','sum'),
    Commandes=('order_id','count')
).reset_index().sort_values('CA', ascending=False).head(10)

st.dataframe(
    top.style.background_gradient(subset=['CA'], cmap='RdPu'),
    use_container_width=True
)