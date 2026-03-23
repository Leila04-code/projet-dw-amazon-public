import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import pyodbc

st.set_page_config(page_title="Amazon Sales Dashboard", page_icon="📊", layout="wide")

st.markdown("""
<style>
    .main { background-color: #FDF0F3; }
    .block-container { padding-top: 1rem; padding-bottom: 1rem; }
    .metric-card {
        background: white;
        border-radius: 14px;
        padding: 16px 20px;
        border-left: 5px solid #8B2346;
        box-shadow: 0 3px 10px rgba(139,35,70,0.12);
        margin: 4px;
    }
    .metric-icon { font-size: 22px; margin-bottom: 4px; }
    .metric-value { font-size: 24px; font-weight: 800; color: #8B2346; margin: 4px 0; }
    .metric-label { font-size: 11px; color: #1B6B7B; font-weight: 700;
                    letter-spacing: 0.8px; text-transform: uppercase; }
    .section-title {
        font-size: 14px; font-weight: 700; color: #8B2346;
        border-bottom: 2px solid #F0C4D0; padding-bottom: 5px; margin-bottom: 10px;
    }
    h1 { color: #3D1A24; }
</style>
""", unsafe_allow_html=True)

COLORS = ["#8B2346","#1B6B7B","#2D8A6E","#C4748A","#5B4B8A","#D4956A","#3D7AB5","#A0522D"]

@st.cache_data
def load_data():
    try:
        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=amazon--server.database.windows.net;"
            "DATABASE=amazone_dw;"
            "UID=CloudSA456cceb9;"
            "PWD=Leilaamazonserver04;"
            "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
        )
        fact = pd.read_sql("SELECT * FROM Fact_Commandes", conn)
        prod = pd.read_sql("SELECT * FROM Dim_Product", conn)
        loc  = pd.read_sql("SELECT * FROM Dim_Location", conn)
        date = pd.read_sql("SELECT * FROM Dim_Date", conn)
        cust = pd.read_sql("SELECT * FROM Dim_Customer", conn)
        conn.close()
        return fact, prod, loc, date, cust
    except Exception as e:
        st.error(f"❌ Erreur connexion Azure : {e}")
        return None, None, None, None, None

fact, prod, loc, date, cust = load_data()
if fact is None:
    st.stop()

df = fact.merge(prod, on='product_id') \
         .merge(loc,  on='location_id') \
         .merge(date, on='date_id') \
         .merge(cust, on='customer_id')

# ── SIDEBAR ──────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 📊 Amazon Sales")
    st.markdown("---")
    years      = st.multiselect("📅 Année",     sorted(df['year'].unique()),     default=sorted(df['year'].unique()))
    categories = st.multiselect("🗂️ Catégorie", sorted(df['category'].unique()), default=sorted(df['category'].unique()))
    countries  = st.multiselect("🌍 Pays",      sorted(df['country'].unique()),  default=sorted(df['country'].unique()))
    st.markdown("---")
    st.caption(f"📦 Total : {len(df):,} commandes")

filtered = df[
    (df['year'].isin(years)) &
    (df['category'].isin(categories)) &
    (df['country'].isin(countries))
]

prev_years = [y - 1 for y in years]
prev = df[
    (df['year'].isin(prev_years)) &
    (df['category'].isin(categories)) &
    (df['country'].isin(countries))
]

def delta(curr, prev_val):
    if prev_val == 0:
        return ""
    pct = (curr - prev_val) / prev_val * 100
    arrow = "▲" if pct >= 0 else "▼"
    color = "#2D8A6E" if pct >= 0 else "#C0392B"
    return f'<span style="color:{color};font-size:11px;font-weight:600">{arrow} {abs(pct):.1f}% vs N-1</span>'

# ── TITRE ────────────────────────────────────────────────
st.title("📊 Amazon Sales Dashboard")
st.caption(f"**{len(filtered):,}** commandes • Années : **{', '.join(map(str, sorted(years)))}**")
st.markdown("---")

# ── KPI CARDS ────────────────────────────────────────────
c1, c2, c3, c4, c5 = st.columns(5)

ca_curr     = filtered['total_amount'].sum()
ca_prev     = prev['total_amount'].sum()
cmd_curr    = len(filtered)
cmd_prev    = len(prev)
marge_curr  = filtered['marge'].mean()
marge_prev  = prev['marge'].mean() if len(prev) > 0 else 0
panier_curr = filtered['total_amount'].mean()
panier_prev = prev['total_amount'].mean() if len(prev) > 0 else 0
disc_curr   = filtered['discount'].mean() * 100

for col, icon, label, value, d in [
    (c1, "💰", "CA Total",      f"${ca_curr/1e6:.1f}M",   delta(ca_curr, ca_prev)),
    (c2, "🛒", "Commandes",     f"{cmd_curr:,}",           delta(cmd_curr, cmd_prev)),
    (c3, "📈", "Marge moyenne", f"${marge_curr:.1f}",      delta(marge_curr, marge_prev)),
    (c4, "🏷️", "Remise moy.",  f"{disc_curr:.1f}%",       ""),
    (c5, "🧺", "Panier moyen",  f"${panier_curr:.0f}",     delta(panier_curr, panier_prev)),
]:
    with col:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-icon">{icon}</div>
            <div class="metric-label">{label}</div>
            <div class="metric-value">{value}</div>
            <div>{d}</div>
        </div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── ROW 1 : Area CA + Barres catégories ──────────────────
col1, col2 = st.columns([2, 1])

with col1:
    st.markdown('<div class="section-title">📅 Évolution du CA par mois</div>', unsafe_allow_html=True)
    ca_mois = filtered.groupby(['year','month'])['total_amount'].sum().reset_index()
    ca_mois['periode'] = ca_mois['year'].astype(str) + "-" + ca_mois['month'].astype(str).str.zfill(2)
    ca_mois = ca_mois.sort_values('periode')
    fig1 = px.area(ca_mois, x='periode', y='total_amount',
                   color_discrete_sequence=["#8B2346"], template="simple_white")
    fig1.update_traces(fill='tozeroy', fillcolor='rgba(139,35,70,0.12)', line_width=2)
    fig1.update_layout(paper_bgcolor='white', xaxis_title="", yaxis_title="CA ($)",
                       margin=dict(t=10,b=10,l=10,r=10))
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    st.markdown('<div class="section-title">🗂️ CA par catégorie</div>', unsafe_allow_html=True)
    ca_cat = filtered.groupby('category')['total_amount'].sum().reset_index().sort_values('total_amount')
    fig2 = px.bar(ca_cat, x='total_amount', y='category', orientation='h',
                  color='category', color_discrete_sequence=COLORS, template="simple_white")
    fig2.update_layout(showlegend=False, paper_bgcolor='white',
                       xaxis_title="CA ($)", yaxis_title="", margin=dict(t=10,b=10))
    st.plotly_chart(fig2, use_container_width=True)

# ── ROW 2 : Carte + Donut statut ─────────────────────────
col3, col4 = st.columns([2, 1])

with col3:
    st.markdown('<div class="section-title">🌍 Répartition géographique</div>', unsafe_allow_html=True)
    ca_pays = filtered.groupby('country')['total_amount'].sum().reset_index()
    fig3 = px.choropleth(ca_pays, locations='country', locationmode='country names',
                         color='total_amount',
                         color_continuous_scale=["#FDF0F3","#C4748A","#8B2346"])
    fig3.update_layout(paper_bgcolor='white', margin=dict(t=10,b=10,l=0,r=0),
                       geo=dict(bgcolor='white', showframe=False))
    st.plotly_chart(fig3, use_container_width=True)

with col4:
    st.markdown('<div class="section-title">📦 Statut des commandes</div>', unsafe_allow_html=True)
    status = filtered.groupby('order_status')['order_id'].count().reset_index()
    fig4 = px.pie(status, names='order_status', values='order_id', hole=0.55,
                  color_discrete_sequence=COLORS)
    fig4.update_layout(paper_bgcolor='white', margin=dict(t=10,b=10),
                       legend=dict(orientation="h", y=-0.2))
    fig4.update_traces(textposition='outside', textinfo='percent')
    st.plotly_chart(fig4, use_container_width=True)

# ── ROW 3 : Top produits + Paiement ─────────────────────
col5, col6 = st.columns([1, 1])

with col5:
    st.markdown('<div class="section-title">🏆 Top 10 produits par CA</div>', unsafe_allow_html=True)
    top10 = filtered.groupby('product_name')['total_amount'].sum() \
                    .reset_index().sort_values('total_amount', ascending=True).tail(10)
    fig5 = px.bar(top10, x='total_amount', y='product_name', orientation='h',
                  color='total_amount', color_continuous_scale=["#F0C4D0","#8B2346"],
                  template="simple_white")
    fig5.update_layout(showlegend=False, paper_bgcolor='white', coloraxis_showscale=False,
                       xaxis_title="CA ($)", yaxis_title="", margin=dict(t=10,b=10))
    st.plotly_chart(fig5, use_container_width=True)

with col6:
    st.markdown('<div class="section-title">💳 CA par méthode de paiement</div>', unsafe_allow_html=True)
    pay = filtered.groupby('payment_method')['total_amount'].sum().reset_index()
    fig6 = px.pie(pay, names='payment_method', values='total_amount', hole=0.45,
                  color_discrete_sequence=COLORS)
    fig6.update_layout(paper_bgcolor='white', margin=dict(t=10,b=10),
                       legend=dict(orientation="h", y=-0.2))
    fig6.update_traces(textposition='outside', textinfo='percent+label')
    st.plotly_chart(fig6, use_container_width=True)

# ── ROW 4 : Comparaison N vs N-1 ─────────────────────────
st.markdown('<div class="section-title">📊 Comparaison CA — Année sélectionnée vs Année précédente</div>', unsafe_allow_html=True)
ca_curr_mois = filtered.groupby('month')['total_amount'].sum().reset_index()
ca_prev_mois = prev.groupby('month')['total_amount'].sum().reset_index()
fig7 = go.Figure()
fig7.add_trace(go.Bar(x=ca_curr_mois['month'], y=ca_curr_mois['total_amount'],
                      name="Année sélectionnée", marker_color='#8B2346'))
fig7.add_trace(go.Bar(x=ca_prev_mois['month'], y=ca_prev_mois['total_amount'],
                      name="Année précédente", marker_color='#C4748A', opacity=0.6))
fig7.update_layout(barmode='group', paper_bgcolor='white', plot_bgcolor='white',
                   xaxis_title="Mois", yaxis_title="CA ($)",
                   legend=dict(orientation="h", y=1.1), margin=dict(t=20,b=10))
st.plotly_chart(fig7, use_container_width=True)

# ── TABLE TOP 20 ─────────────────────────────────────────
st.markdown('<div class="section-title">📋 Détail Top 20 produits</div>', unsafe_allow_html=True)
top20 = filtered.groupby(['product_name','category']).agg(
    CA=('total_amount','sum'),
    Commandes=('order_id','count'),
    Marge_moy=('marge','mean'),
    Remise=('discount','mean')
).reset_index().sort_values('CA', ascending=False).head(20)
top20['Remise']   = (top20['Remise']*100).round(1).astype(str) + "%"
top20['Marge_moy'] = top20['Marge_moy'].round(1)
top20['CA']        = top20['CA'].round(0)

st.dataframe(top20, use_container_width=True, height=400)

st.markdown("---")
st.caption("Amazon Sales Dashboard • Projet BI 2026")
