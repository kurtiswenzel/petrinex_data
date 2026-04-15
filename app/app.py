"""Petrinex M&A Deal Screener -- interactive Streamlit app."""
import os

import numpy as np
import pandas as pd
import pydeck as pdk
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

st.set_page_config(
    page_title="Petrinex M&A Deal Screener",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
<style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@500;600;700&family=Inter:wght@400;500;600&display=swap');

    html, body, [class*="st-"], [class*="css-"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    }
    .block-container { padding-top: 2.25rem; padding-bottom: 1rem; }
    .main-header {
        font-family: 'Space Grotesk', 'Inter', system-ui, sans-serif;
        font-size: 2.4rem;
        font-weight: 700;
        color: #0b2545;
        letter-spacing: -0.025em;
        line-height: 1.35;
        padding: 0.35rem 0 0.25rem 0;
        margin: 0 0 0.25rem 0;
        display: block;
        overflow: visible;
    }
    .sub-header {
        font-family: 'Inter', system-ui, sans-serif;
        color: #64748b;
        font-size: 1rem;
        line-height: 1.5;
        margin-bottom: 1.5rem;
    }
    [data-testid="stMetric"] {
        background: linear-gradient(135deg, #f8fafc 0%, #eef2f7 100%);
        padding: 1rem 1.2rem;
        border-radius: 0.75rem;
        border-left: 5px solid #0b2545;
        box-shadow: 0 1px 2px rgba(15, 23, 42, 0.05);
    }
    [data-testid="stMetricValue"] {
        font-size: 1.9rem;
        font-weight: 700;
        color: #0b2545;
    }
    [data-testid="stMetricLabel"] {
        color: #64748b;
        font-weight: 500;
        text-transform: uppercase;
        font-size: 0.75rem;
        letter-spacing: 0.05em;
    }
    section[data-testid="stSidebar"] {
        background: #f8fafc;
    }
    section[data-testid="stSidebar"] > div:first-child {
        padding-top: 1rem;
    }
    section[data-testid="stSidebar"] h2 {
        color: #0b2545;
        font-size: 1rem;
        margin-top: 0.5rem;
        margin-bottom: 0.25rem;
    }
    section[data-testid="stSidebar"] [data-testid="stWidgetLabel"] p {
        font-size: 0.78rem;
        font-weight: 600;
        color: #334155;
        margin-bottom: 0.15rem;
    }
    section[data-testid="stSidebar"] .stSelectbox,
    section[data-testid="stSidebar"] .stSlider {
        margin-bottom: 0.4rem;
    }
    section[data-testid="stSidebar"] a[data-testid="stBaseLinkButton-secondary"] {
        background: #ffffff;
        border: 1px solid #cbd5e1;
        color: #0b2545;
        font-weight: 600;
        font-size: 0.8rem;
    }
    section[data-testid="stSidebar"] a[data-testid="stBaseLinkButton-secondary"]:hover {
        background: #0b2545;
        color: #ffffff;
        border-color: #0b2545;
    }
    div[data-testid="stDataFrame"] {
        border-radius: 0.5rem;
        overflow: hidden;
    }
</style>
""",
    unsafe_allow_html=True,
)

DASHBOARD_URL = (
    "https://dbc-f9439112-5585.cloud.databricks.com/sql/dashboardsv3/"
    "01f1390c1f7c1322ab547a2f9ae2fd12"
)
GENIE_URL = (
    "https://dbc-f9439112-5585.cloud.databricks.com/genie/rooms/"
    "01f1390bc24d1c8486fae7013fc1a25d"
)


WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")


@st.cache_resource
def get_connection():
    cfg = Config()
    host = (cfg.host or "").replace("https://", "").replace("http://", "").rstrip("/")
    if not host:
        raise RuntimeError("DATABRICKS_HOST is not set in the app environment.")
    if not WAREHOUSE_ID:
        raise RuntimeError(
            "DATABRICKS_WAREHOUSE_ID is not set. Check app.yaml valueFrom binding "
            "and that the sql-warehouse resource is attached to the app."
        )
    return sql.connect(
        server_hostname=host,
        http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
        credentials_provider=lambda: cfg.authenticate,
        use_cloud_fetch=False,
    )


@st.cache_data(ttl=600, show_spinner="Loading deal screener from Unity Catalog...")
def load_data() -> pd.DataFrame:
    query = """
        SELECT
          well_id,
          facility_id,
          facility_name,
          operator_baid,
          operator_name,
          formation,
          field_display_name,
          pool_name,
          region,
          operator_size_bucket,
          CAST(latitude AS DOUBLE)  AS latitude,
          CAST(longitude AS DOUBLE) AS longitude,
          cum_boe,
          recent_boe_per_day,
          liquids_cut,
          water_cut,
          production_trend_pct,
          netback_cad_per_boe,
          emissions_intensity,
          operator_well_count,
          deal_score
        FROM shm.petrinex.gold_deal_screener
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    """
    with get_connection().cursor() as cur:
        cur.execute(query)
        return cur.fetchall_arrow().to_pandas()


def score_to_rgba(score: float) -> list:
    """Red -> orange -> yellow -> green gradient for deal_score 0-100."""
    if score is None or np.isnan(score):
        return [156, 163, 175, 140]
    if score >= 85:
        return [22, 163, 74, 220]
    if score >= 70:
        return [132, 204, 22, 210]
    if score >= 55:
        return [234, 179, 8, 200]
    if score >= 40:
        return [249, 115, 22, 190]
    return [239, 68, 68, 180]


st.markdown('<div class="main-header">Alberta O&amp;G M&amp;A Deal Screener</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="sub-header">Ranked acquisition targets from Petrinex 2024-2025 '
    "production data. Deal score = 35% liquids cut + 25% seller motivation + "
    "20% scale + 20% netback.</div>",
    unsafe_allow_html=True,
)

try:
    df = load_data()
except Exception as exc:
    st.error(f"Failed to load data: {type(exc).__name__}: {exc}")
    import traceback
    st.code(traceback.format_exc(), language="python")
    st.info(
        f"Debug: DATABRICKS_HOST={os.environ.get('DATABRICKS_HOST', '<unset>')}  |  "
        f"DATABRICKS_WAREHOUSE_ID={os.environ.get('DATABRICKS_WAREHOUSE_ID', '<unset>')}  |  "
        f"DATABRICKS_CLIENT_ID={'<set>' if os.environ.get('DATABRICKS_CLIENT_ID') else '<unset>'}"
    )
    st.stop()

with st.sidebar:
    st.markdown("**Quick links**")
    st.link_button(
        "AI/BI Dashboard",
        DASHBOARD_URL,
        use_container_width=True,
    )
    st.link_button(
        "Ask Genie",
        GENIE_URL,
        use_container_width=True,
    )
    st.markdown("---")
    st.header("Screening Criteria")

    formations = ["All"] + sorted(df["formation"].dropna().unique().tolist())
    sel_formation = st.selectbox("Formation", formations)

    regions = ["All"] + sorted(df["region"].dropna().unique().tolist())
    sel_region = st.selectbox("Region", regions)

    sizes = ["All"] + sorted(df["operator_size_bucket"].dropna().unique().tolist())
    sel_size = st.selectbox("Operator size", sizes, index=0)

    min_score = st.slider("Minimum deal score", 0, 100, 50, step=5)

    with st.expander("Advanced filters"):
        min_liquids = st.slider("Minimum liquids cut (%)", 0, 100, 0, step=5)
        min_boe = st.slider("Minimum BOE/day", 0, 500, 0, step=10)
        top_n = st.slider("Shortlist size", 10, 500, 100, step=10)

filt = df.copy()
if sel_formation != "All":
    filt = filt[filt["formation"] == sel_formation]
if sel_region != "All":
    filt = filt[filt["region"] == sel_region]
if sel_size != "All":
    filt = filt[filt["operator_size_bucket"] == sel_size]
filt = filt[filt["deal_score"] >= min_score]
filt = filt[(filt["liquids_cut"].fillna(0) * 100) >= min_liquids]
filt = filt[filt["recent_boe_per_day"].fillna(0) >= min_boe]
filt = filt.sort_values("deal_score", ascending=False).head(top_n).reset_index(drop=True)

k1, k2, k3, k4 = st.columns(4)
k1.metric("Wells in shortlist", f"{len(filt):,}")
k2.metric("Operators", f"{filt['operator_baid'].nunique():,}")
k3.metric(
    "Avg deal score",
    f"{filt['deal_score'].mean():.1f}" if len(filt) else "--",
)
k4.metric(
    "Total BOE/day",
    f"{filt['recent_boe_per_day'].sum():,.0f}" if len(filt) else "--",
)

st.markdown("### Geographic distribution")

if len(filt) == 0:
    st.info("No wells match the current filters. Relax the criteria in the sidebar.")
else:
    map_df = filt.copy()
    map_df["radius"] = np.clip(
        np.sqrt(map_df["recent_boe_per_day"].fillna(0).clip(lower=0)) * 300,
        800,
        9000,
    )
    map_df["color"] = map_df["deal_score"].apply(score_to_rgba)
    map_df["liquids_pct_label"] = (map_df["liquids_cut"].fillna(0) * 100).round(0).astype(int)
    map_df["trend_pct_label"] = (map_df["production_trend_pct"].fillna(0) * 100).round(0).astype(int)
    map_df["deal_score_label"] = map_df["deal_score"].round(1)
    map_df["recent_boe_label"] = map_df["recent_boe_per_day"].fillna(0).round(0).astype(int)
    map_df["netback_label"] = map_df["netback_cad_per_boe"].fillna(0).round(1)

    lat_center = float(map_df["latitude"].mean())
    lon_center = float(map_df["longitude"].mean())

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=map_df,
        get_position="[longitude, latitude]",
        get_radius="radius",
        get_fill_color="color",
        pickable=True,
        opacity=0.85,
        stroked=True,
        filled=True,
        radius_min_pixels=3,
        radius_max_pixels=35,
        line_width_min_pixels=0.5,
        get_line_color=[255, 255, 255, 180],
    )

    view_state = pdk.ViewState(
        latitude=lat_center,
        longitude=lon_center,
        zoom=5.2,
        pitch=30,
        bearing=0,
    )

    tooltip = {
        "html": (
            "<div style='font-family:Inter,system-ui;font-size:12px;min-width:220px'>"
            "<div style='font-weight:700;font-size:13px;margin-bottom:4px'>{operator_name}</div>"
            "<div style='color:#94a3b8;margin-bottom:6px'>{formation} &middot; {region}</div>"
            "<div><b>Deal score:</b> {deal_score_label}</div>"
            "<div><b>BOE/day:</b> {recent_boe_label}</div>"
            "<div><b>Liquids cut:</b> {liquids_pct_label}%</div>"
            "<div><b>Trend:</b> {trend_pct_label}%</div>"
            "<div><b>Netback:</b> ${netback_label} CAD/boe</div>"
            "</div>"
        ),
        "style": {
            "backgroundColor": "#0b2545",
            "color": "white",
            "padding": "10px",
            "borderRadius": "6px",
        },
    }

    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip=tooltip,
        map_style="light",
    )

    st.pydeck_chart(deck, use_container_width=True)

    legend_cols = st.columns(5)
    legend_cols[0].markdown("**Deal score color key**")
    legend_cols[1].markdown(
        "<span style='color:#16a34a'>&#9679;</span> 85+",
        unsafe_allow_html=True,
    )
    legend_cols[2].markdown(
        "<span style='color:#84cc16'>&#9679;</span> 70-84",
        unsafe_allow_html=True,
    )
    legend_cols[3].markdown(
        "<span style='color:#eab308'>&#9679;</span> 55-69",
        unsafe_allow_html=True,
    )
    legend_cols[4].markdown(
        "<span style='color:#f97316'>&#9679;</span> 40-54",
        unsafe_allow_html=True,
    )

st.markdown("### Ranked shortlist")

if len(filt) > 0:
    display_df = filt[
        [
            "deal_score",
            "operator_name",
            "operator_size_bucket",
            "formation",
            "region",
            "recent_boe_per_day",
            "liquids_cut",
            "production_trend_pct",
            "netback_cad_per_boe",
        ]
    ].rename(
        columns={
            "deal_score": "Score",
            "operator_name": "Operator",
            "operator_size_bucket": "Size",
            "formation": "Formation",
            "region": "Region",
            "recent_boe_per_day": "BOE/day",
            "liquids_cut": "Liquids",
            "production_trend_pct": "Trend",
            "netback_cad_per_boe": "Netback",
        }
    )

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        height=520,
        column_config={
            "Score": st.column_config.ProgressColumn(
                "Deal score",
                min_value=0,
                max_value=100,
                format="%.1f",
                help="0-100 composite: liquids + seller motivation + scale + netback",
            ),
            "BOE/day": st.column_config.NumberColumn(format="%.0f"),
            "Liquids": st.column_config.NumberColumn(
                "Liquids cut",
                format="%.0f%%",
                help="Condensate + oil share of BOE (0-1)",
            ),
            "Trend": st.column_config.NumberColumn(
                "Prod trend",
                format="%+.0f%%",
                help="Recent 6mo avg vs first 6mo avg",
            ),
            "Netback": st.column_config.NumberColumn(
                "Netback CAD/boe",
                format="$%.1f",
            ),
        },
    )

    csv = display_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download shortlist as CSV",
        data=csv,
        file_name="ma_shortlist.csv",
        mime="text/csv",
    )
