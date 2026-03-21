"""
Phase 1 Insights Dashboard
SEC 13F Holdings Analytics — Streamlit App

Run from the project root:
    streamlit run Phase1_Insights/app.py
"""

import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

# ── config ──────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SEC 13F Insights",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

DB_PATH = Path(__file__).parent.parent / "edgar_13f.db"

# ── db ──────────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_conn():
    if not DB_PATH.exists():
        st.error(f"Database not found: {DB_PATH}\nRun `python pipeline.py` first.")
        st.stop()
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    return conn


def qdf(sql: str, params=()):
    """Execute SQL and return a DataFrame."""
    return pd.read_sql_query(sql, get_conn(), params=list(params))


# ── cached reference data ────────────────────────────────────────────────────────
@st.cache_data(ttl=600)
def load_managers():
    return qdf("SELECT DISTINCT cik, manager_name FROM filers ORDER BY manager_name")


@st.cache_data(ttl=600)
def load_quarters():
    return qdf("SELECT DISTINCT report_date FROM holdings ORDER BY report_date DESC")


managers_df  = load_managers()
quarters_df  = load_quarters()
managers_map = dict(zip(managers_df["manager_name"], managers_df["cik"]))  # name → cik
quarters     = quarters_df["report_date"].tolist()

# ── sidebar ──────────────────────────────────────────────────────────────────────
st.sidebar.title("📊 SEC 13F Insights")

# ── check if security_master exists ─────────────────────────────────────────────
@st.cache_data(ttl=300)
def has_security_master():
    try:
        n = pd.read_sql_query(
            "SELECT COUNT(*) AS n FROM security_master", get_conn()
        ).iloc[0]["n"]
        return n > 0
    except Exception:
        return False

_has_sm = has_security_master()

P1_PAGES = [
    "ℹ️ About",
    "📖 How to Use",
    "📊 Dataset Overview",
    "🏦 Manager Holdings",
    "📈 Stock Tracker",
    "🎯 Consensus Signals",
    "🗂️ Manager Panel",
    "🔗 Portfolio Overlap",
    "📉 Sector Rotation",
    "🆕 New & Exited Positions",
]

P2_PAGES = [
    "🔬 Security Universe",
    "📂 Classification Explorer",
    "💡 Security Type Signals",
] if _has_sm else []

# Track which radio group was last touched
if "nav_group" not in st.session_state:
    st.session_state.nav_group = "p1"

def _on_p1():
    st.session_state.nav_group = "p1"

def _on_p2():
    st.session_state.nav_group = "p2"

st.sidebar.caption("Phase 1 — Holdings Intelligence")
p1_sel = st.sidebar.radio(
    "Navigate", P1_PAGES, key="p1_nav", on_change=_on_p1, label_visibility="collapsed"
)

if P2_PAGES:
    st.sidebar.divider()
    st.sidebar.caption("Phase 2 — Security Classification")
    p2_sel = st.sidebar.radio(
        "Navigate ", P2_PAGES, key="p2_nav", on_change=_on_p2, label_visibility="collapsed"
    )
    page = p2_sel if st.session_state.nav_group == "p2" else p1_sel
else:
    if not _has_sm:
        st.sidebar.divider()
        st.sidebar.info("Run `python phase2.py` to unlock Phase 2 insights.")
    page = p1_sel

st.sidebar.divider()
st.sidebar.caption(f"DB: `{DB_PATH.name}`")
st.sidebar.caption(f"Quarters: **{len(quarters)}**")
st.sidebar.caption(f"Managers: **{len(managers_df)}**")

# ══════════════════════════════════════════════════════════════════════════════════
# PAGE: About
# ══════════════════════════════════════════════════════════════════════════════════
if page == "ℹ️ About":
    st.title("ℹ️ About This Dashboard")
    st.markdown("""
    ## What is SEC Form 13F?

    **SEC Form 13F** is a quarterly disclosure report mandated by the U.S. Securities and Exchange
    Commission (SEC) under Section 13(f) of the Securities Exchange Act of 1934.

    ### Who Must File?
    Any **institutional investment manager** that exercises investment discretion over
    **$100 million or more** in qualifying securities is required to file a 13F report
    within **45 days** after the end of each calendar quarter.

    Filers include hedge funds, mutual funds, pension funds, banks, insurance companies,
    and investment advisers — including well-known names like Berkshire Hathaway, Bridgewater,
    Renaissance Technologies, and BlackRock.

    ### What Does It Disclose?
    Each filing lists all **long positions** held at quarter-end across:

    | Field | Description |
    |---|---|
    | **Issuer name** | Name of the company whose securities are held |
    | **CUSIP** | Unique 9-character security identifier |
    | **Security class** | e.g., Common Stock, ADR, Put/Call option |
    | **Shares / principal** | Number of shares or face value held |
    | **Market value** | Fair market value in USD (thousands) |
    | **Put / Call** | Flags whether the position is an options contract |

    ### What It Does **Not** Disclose
    - Short positions or hedges
    - Cash, bonds, or private equity
    - Non-U.S. securities (unless listed on a U.S. exchange)
    - Intraday or intra-quarter trading activity

    ### Why It Matters
    13F filings are the primary public window into the portfolios of large institutional
    investors — often called **"smart money"**. Analysts, researchers, and retail investors
    use them to:
    - Track what top funds are buying and selling each quarter
    - Identify consensus positions across multiple managers
    - Detect emerging sector rotations and thematic trends
    - Understand concentration and diversification strategies

    ---

    ## About This Project

    This dashboard ingests and analyzes 13F filings retrieved directly from the
    [SEC EDGAR API](https://efts.sec.gov/LATEST/search-index?q=%2213F%22&dateRange=custom).

    | | |
    |---|---|
    | **Phase 1** | Holdings intelligence — manager portfolios, consensus signals, sector rotation, overlap analysis |
    | **Phase 2** | Security classification — categorizing every CUSIP into stock, ETF, bond, option, ADR, etc. |

    **Data in this dashboard:**
    """)

    col1, col2, col3 = st.columns(3)
    col1.metric("Managers tracked", len(managers_df))
    col2.metric("Quarters covered", len(quarters))
    if quarters:
        col3.metric("Date range", f"{quarters[-1][:7]} → {quarters[0][:7]}")

    st.markdown("""
    ---
    *Data source: [SEC EDGAR](https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&type=13F)*
    """)

# ══════════════════════════════════════════════════════════════════════════════════
# PAGE: How to Use
# ══════════════════════════════════════════════════════════════════════════════════
elif page == "📖 How to Use":
    st.title("📖 How to Use This Dashboard")
    st.caption("A guide to the questions this data can answer — and where to go to answer them.")

    st.markdown("""
    This dashboard is built on **SEC Form 13F** filings — quarterly snapshots of what large
    institutional investors hold. Below is a map of the analytical questions you can explore
    and which page answers them.
    """)

    st.subheader("🔍 Questions You Can Answer")

    with st.expander("🏦  Who are the biggest institutional investors in this dataset?", expanded=True):
        st.markdown("""
        **Page: Manager Holdings**

        See each manager’s total AUM per quarter, their largest positions by market value,
        and how their portfolio size has evolved over time.
        You can filter by manager and date range to compare side-by-side.
        """)

    with st.expander("📈  How has a specific stock been held across all managers?"):
        st.markdown("""
        **Page: Stock Tracker**

        Search for any stock by ticker or company name.
        See the total number of institutional holders each quarter, aggregate shares and value,
        and which managers entered or exited the position.
        """)

    with st.expander("🎯  Which stocks are the most widely held — the ‘consensus’ picks?"):
        st.markdown("""
        **Page: Consensus Signals**

        Ranks stocks by the number of managers holding them simultaneously.
        High consensus = broad institutional conviction.
        Filter by quarter to see how the consensus basket shifts over time.
        """)

    with st.expander("🗂️  What does a specific manager’s full portfolio look like?"):
        st.markdown("""
        **Page: Manager Panel**

        Drill into a single manager: their complete holdings list, sector breakdown,
        top 10 positions, and changes vs. the prior quarter (new buys, adds, trims, exits).
        """)

    with st.expander("🔗  Which managers share the same positions — who overlaps with whom?"):
        st.markdown("""
        **Page: Portfolio Overlap**

        Pick two managers and see their shared holdings: common stocks, combined value,
        and a Jaccard similarity score.
        Useful for spotting crowded trades or similar investment styles.
        """)

    with st.expander("📉  Are managers rotating into or out of certain sectors over time?"):
        st.markdown("""
        **Page: Sector Rotation**

        Tracks aggregate AUM by GICS sector across all quarters.
        A rising sector share signals institutional accumulation;
        a falling share signals distribution.
        """)

    with st.expander("🆕  Which stocks were newly bought or fully sold in the latest quarter?"):
        st.markdown("""
        **Page: New & Exited Positions**

        Lists all stocks that appeared for the first time (new positions) or
        disappeared entirely (full exits) in a selected quarter.
        A strong signal of high-conviction directional bets.
        """)

    with st.expander("🔬  What types of securities are these managers actually holding? *(Phase 2)*"):
        st.markdown("""
        **Page: Security Universe / Classification Explorer / Security Type Signals**

        Phase 2 classifies every CUSIP in the dataset into a security type:
        common stock, ETF, bond, ADR, option, warrant, preferred, etc.

        This unlocks deeper questions:
        - What fraction of holdings are passive ETFs vs. active stock picks?
        - Which managers use options as a significant part of their strategy?
        - Are there bonds or convertibles buried in these “equity” filings?

        > Run `python phase2.py` to enable Phase 2 pages.
        """)

    st.divider()
    st.subheader("📊 Data Coverage")
    col1, col2, col3 = st.columns(3)
    col1.metric("Managers", len(managers_df))
    col2.metric("Quarters", len(quarters))
    if quarters:
        col3.metric("Range", f"{quarters[-1][:7]} → {quarters[0][:7]}")

    st.info(
        "⚠️ 13F filings only capture **long positions** held at quarter-end. "
        "Short positions, cash, private holdings, and intra-quarter trades are not visible."
    )

# ══════════════════════════════════════════════════════════════════════════════════
# PAGE 1 — Dataset Overview
# ══════════════════════════════════════════════════════════════════════════════════
elif page == "📊 Dataset Overview":
    st.title("📊 Dataset Overview")
    st.caption("High-level statistics across all managers, filings, and quarters.")

    stats = qdf("""
        SELECT
            COUNT(DISTINCT f.cik)                                   AS total_filers,
            COUNT(*)                                                 AS total_filings,
            (SELECT COUNT(*) FROM holdings)                          AS total_holdings,
            (SELECT COUNT(DISTINCT cusip) FROM holdings)             AS unique_cusips,
            (SELECT MIN(report_date) FROM holdings)                  AS earliest,
            (SELECT MAX(report_date) FROM holdings)                  AS latest,
            (SELECT ROUND(SUM(value_thousands)/1e6, 2) FROM holdings) AS aum_bn
        FROM filers f
    """)
    s = stats.iloc[0]

    cols = st.columns(7)
    cols[0].metric("Managers",       f"{int(s.total_filers):,}")
    cols[1].metric("Filings",        f"{int(s.total_filings):,}")
    cols[2].metric("Holdings Rows",  f"{int(s.total_holdings):,}")
    cols[3].metric("Unique CUSIPs",  f"{int(s.unique_cusips):,}")
    cols[4].metric("Total AUM",      f"${s.aum_bn:,.1f}B")
    cols[5].metric("From",           str(s.earliest))
    cols[6].metric("To",             str(s.latest))

    st.divider()

    c1, c2 = st.columns(2)

    with c1:
        aum_q = qdf("""
            SELECT report_date,
                   ROUND(SUM(value_thousands)/1e6, 2) AS aum_bn
            FROM holdings
            GROUP BY report_date ORDER BY report_date
        """)
        fig = px.area(
            aum_q, x="report_date", y="aum_bn",
            title="Total Reported AUM by Quarter ($B)",
            labels={"report_date": "Quarter End", "aum_bn": "AUM ($B)"},
        )
        fig.update_traces(line_color="#2563EB", fillcolor="rgba(37,99,235,0.15)")
        st.plotly_chart(fig, width='stretch')

    with c2:
        fy = qdf("""
            SELECT SUBSTR(report_date,1,4) AS year,
                   COUNT(*) AS filings,
                   COUNT(DISTINCT cik) AS managers
            FROM filers GROUP BY 1 ORDER BY 1
        """)
        fig2 = px.bar(
            fy, x="year", y="filings",
            title="Filings & Active Managers per Year",
            labels={"year": "Year", "filings": "Filings"},
            color="managers", color_continuous_scale="Blues",
            text="managers",
        )
        fig2.update_traces(texttemplate="%{text} mgrs", textposition="outside")
        st.plotly_chart(fig2, width='stretch')

    latest_q = quarters[0] if quarters else None
    if latest_q:
        top_mgrs = qdf("""
            SELECT f.manager_name,
                   ROUND(SUM(h.value_thousands)/1e6, 2) AS aum_bn,
                   COUNT(DISTINCT h.cusip) AS positions
            FROM holdings h
            JOIN filers f ON f.cik=h.cik AND f.report_date=h.report_date
            WHERE h.report_date=?
            GROUP BY f.manager_name
            ORDER BY aum_bn DESC
        """, (latest_q,))
        fig3 = px.bar(
            top_mgrs, x="aum_bn", y="manager_name", orientation="h",
            color="positions", color_continuous_scale="Blues",
            title=f"Manager AUM — {latest_q} ($B)",
            labels={"aum_bn": "AUM ($B)", "manager_name": "Manager", "positions": "# Positions"},
        )
        fig3.update_layout(yaxis={"categoryorder": "total ascending"}, height=400)
        st.plotly_chart(fig3, width='stretch')

    hc = qdf("""
        SELECT report_date,
               COUNT(*) AS holdings_rows,
               COUNT(DISTINCT cusip) AS unique_cusips,
               COUNT(DISTINCT cik) AS active_managers
        FROM holdings GROUP BY report_date ORDER BY report_date
    """)
    fig4 = px.line(
        hc.melt(id_vars="report_date",
                value_vars=["holdings_rows", "unique_cusips", "active_managers"]),
        x="report_date", y="value", color="variable",
        markers=True,
        title="Dataset Breadth Over Time",
        labels={"report_date": "Quarter", "value": "Count", "variable": "Metric"},
    )
    st.plotly_chart(fig4, width='stretch')


# ══════════════════════════════════════════════════════════════════════════════════
# PAGE 2 — Manager Holdings
# ══════════════════════════════════════════════════════════════════════════════════
elif page == "🏦 Manager Holdings":
    st.title("🏦 Manager Holdings")
    st.caption("Top 25 positions for any manager in any quarter, with QoQ change.")

    c1, c2 = st.columns([2, 1])
    with c1:
        mgr_name = st.selectbox("Manager", list(managers_map.keys()))
    with c2:
        quarter  = st.selectbox("Quarter", quarters)

    cik = managers_map[mgr_name]

    df = qdf("""
        SELECT h.issuer_name, h.cusip, h.class_title,
               ROUND(h.value_thousands / 1000.0, 2) AS value_usd_m,
               h.shares_principal,
               h.investment_discretion,
               h.put_call
        FROM holdings h
        WHERE h.cik=? AND h.report_date=?
        ORDER BY h.value_thousands DESC
        LIMIT 25
    """, (cik, quarter))

    if df.empty:
        st.warning("No holdings found for this manager/quarter.")
    else:
        total_aum = df["value_usd_m"].sum()
        df["weight_%"] = (df["value_usd_m"] / total_aum * 100).round(2)

        kc1, kc2, kc3 = st.columns(3)
        kc1.metric("AUM in Top 25 ($M)", f"${total_aum:,.1f}M")
        kc2.metric("Positions Shown", len(df))
        kc3.metric("Top-10 Concentration", f"{df.head(10)['weight_%'].sum():.1f}%")

        col1, col2 = st.columns([3, 2])
        with col1:
            fig = px.bar(
                df, x="value_usd_m", y="issuer_name", orientation="h",
                color="weight_%", color_continuous_scale="Blues",
                title=f"Top 25 Holdings — {mgr_name} [{quarter}]",
                labels={"value_usd_m": "Value ($M)", "issuer_name": "Issuer"},
            )
            fig.update_layout(yaxis={"categoryorder": "total ascending"}, height=500)
            st.plotly_chart(fig, width='stretch')

        with col2:
            class_agg = df.groupby("class_title")["value_usd_m"].sum().reset_index()
            fig2 = px.pie(
                class_agg, names="class_title", values="value_usd_m",
                title="Holdings by Asset Class",
            )
            st.plotly_chart(fig2, width='stretch')

        # QoQ comparison
        prior_q_df = qdf("""
            SELECT report_date FROM filers
            WHERE cik=? AND report_date < ?
            ORDER BY report_date DESC LIMIT 1
        """, (cik, quarter))

        if not prior_q_df.empty:
            prior_q = prior_q_df.iloc[0]["report_date"]
            df_prior = qdf("""
                SELECT cusip,
                       ROUND(value_thousands/1000.0, 2) AS value_prior_m
                FROM holdings WHERE cik=? AND report_date=?
            """, (cik, prior_q))
            df_m = df.merge(df_prior[["cusip", "value_prior_m"]], on="cusip", how="left")
            df_m["qoq_chg_m"]  = (df_m["value_usd_m"] - df_m["value_prior_m"].fillna(0)).round(2)
            df_m["qoq_chg_%"]  = (
                df_m["qoq_chg_m"] /
                df_m["value_prior_m"].replace(0, float("nan")) * 100
            ).round(1)
            st.subheader(f"Quarter-over-Quarter Change vs {prior_q}")
            show_cols = ["issuer_name", "value_usd_m", "value_prior_m", "qoq_chg_m", "qoq_chg_%", "weight_%"]
            st.dataframe(df_m[show_cols], use_container_width=True, hide_index=True)
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════════
# PAGE 3 — Stock Tracker
# ══════════════════════════════════════════════════════════════════════════════════
elif page == "📈 Stock Tracker":
    st.title("📈 Stock Tracker")
    st.caption("Track any security across all quarters and managers.")

    all_cusips = qdf("""
        SELECT DISTINCT cusip, issuer_name FROM holdings ORDER BY issuer_name
    """)
    all_cusips["label"] = all_cusips["issuer_name"] + " (" + all_cusips["cusip"] + ")"
    cusip_map   = dict(zip(all_cusips["label"], all_cusips["cusip"]))
    label_list  = list(cusip_map.keys())
    default_idx = next((i for i, l in enumerate(label_list) if "APPLE" in l.upper()), 0)

    c1, c2 = st.columns([3, 1])
    with c1:
        selected_label = st.selectbox("Search security by name or CUSIP", label_list, index=default_idx)
    with c2:
        mgr_filter = st.multiselect("Filter managers", list(managers_map.keys()))

    cusip = cusip_map[selected_label]

    params: list = [cusip]
    mgr_clause = ""
    if mgr_filter:
        ciks_sel     = [managers_map[m] for m in mgr_filter]
        placeholders = ",".join("?" * len(ciks_sel))
        mgr_clause   = f"AND h.cik IN ({placeholders})"
        params.extend(ciks_sel)

    hist = qdf(f"""
        SELECT f.manager_name, h.report_date,
               ROUND(h.value_thousands/1000.0, 2) AS value_usd_m,
               h.shares_principal,
               h.put_call
        FROM holdings h
        JOIN filers f ON f.cik=h.cik AND f.report_date=h.report_date
        WHERE h.cusip=? {mgr_clause}
        ORDER BY h.report_date, f.manager_name
    """, params)

    if hist.empty:
        st.warning("No data found for this security.")
    else:
        issuer = selected_label.split("(")[0].strip()
        latest_mask = hist["report_date"] == hist["report_date"].max()

        kc1, kc2, kc3 = st.columns(3)
        kc1.metric("Quarters Tracked",    hist["report_date"].nunique())
        kc2.metric("Managers Holding",    hist["manager_name"].nunique())
        kc3.metric("Latest Quarter Value ($M)", f"${hist.loc[latest_mask, 'value_usd_m'].sum():,.1f}M")

        fig = px.line(
            hist, x="report_date", y="value_usd_m", color="manager_name",
            markers=True,
            title=f"Reported Value Over Time — {issuer}",
            labels={"report_date": "Quarter", "value_usd_m": "Value ($M)", "manager_name": "Manager"},
        )
        st.plotly_chart(fig, width='stretch')

        fig2 = px.bar(
            hist, x="report_date", y="shares_principal", color="manager_name",
            barmode="stack",
            title=f"Shares Held Over Time — {issuer}",
            labels={"report_date": "Quarter", "shares_principal": "Shares", "manager_name": "Manager"},
        )
        st.plotly_chart(fig2, width='stretch')

        st.subheader("Raw Position Table")
        st.dataframe(hist, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════════
# PAGE 4 — Consensus Signals
# ══════════════════════════════════════════════════════════════════════════════════
elif page == "🎯 Consensus Signals":
    st.title("🎯 Consensus Signals")
    st.caption("Securities that multiple managers were buying or selling in a given quarter.")

    c1, c2 = st.columns([2, 1])
    with c1:
        quarter  = st.selectbox("Quarter", quarters)
    with c2:
        min_mgrs = st.slider("Min managers", 1, max(len(managers_df), 2), 2)

    _CTE = """
        WITH
        curr AS (
            SELECT cusip, issuer_name, cik,
                   SUM(shares_principal) AS shares,
                   SUM(value_thousands)  AS val
            FROM holdings WHERE report_date=?
            GROUP BY cusip, issuer_name, cik
        ),
        prev AS (
            SELECT cusip, cik, SUM(shares_principal) AS shares_prev
            FROM holdings
            WHERE report_date=(SELECT MAX(report_date) FROM holdings WHERE report_date<?)
            GROUP BY cusip, cik
        ),
        changes AS (
            SELECT c.cusip, c.issuer_name, c.cik,
                   c.shares, c.val,
                   COALESCE(p.shares_prev, 0) AS shares_prev,
                   CASE
                     WHEN p.cik IS NULL            THEN 'NEW'
                     WHEN c.shares > p.shares_prev THEN 'ADD'
                     WHEN c.shares < p.shares_prev THEN 'CUT'
                     ELSE 'HOLD'
                   END AS action
            FROM curr c LEFT JOIN prev p ON p.cusip=c.cusip AND p.cik=c.cik
        )
    """

    buys = qdf(_CTE + """
        SELECT cusip, issuer_name,
               COUNT(*) AS num_managers,
               SUM(CASE WHEN action='NEW' THEN 1 ELSE 0 END) AS new_pos,
               SUM(CASE WHEN action='ADD' THEN 1 ELSE 0 END) AS add_pos,
               ROUND(SUM(val)/1e3, 1) AS total_value_m
        FROM changes
        WHERE action IN ('NEW','ADD')
        GROUP BY cusip, issuer_name
        HAVING num_managers >= ?
        ORDER BY num_managers DESC, total_value_m DESC
        LIMIT 30
    """, (quarter, quarter, min_mgrs))

    sells = qdf(_CTE + """
        SELECT cusip, issuer_name,
               COUNT(*) AS num_managers,
               ROUND(SUM(val)/1e3, 1) AS total_value_m
        FROM changes
        WHERE action='CUT'
        GROUP BY cusip, issuer_name
        HAVING num_managers >= ?
        ORDER BY num_managers DESC, total_value_m DESC
        LIMIT 30
    """, (quarter, quarter, min_mgrs))

    tab1, tab2 = st.tabs(["🟢 Consensus Buys", "🔴 Consensus Sells"])

    with tab1:
        if buys.empty:
            st.info("No consensus buys found.")
        else:
            fig = px.bar(
                buys.head(20), x="issuer_name", y="num_managers",
                color="total_value_m", color_continuous_scale="Greens",
                title=f"Consensus Buys — {quarter}",
                labels={"issuer_name": "Security", "num_managers": "# Managers Buying",
                        "total_value_m": "Total Value ($M)"},
                text="num_managers",
            )
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, width='stretch')
            st.dataframe(buys, use_container_width=True, hide_index=True)

    with tab2:
        if sells.empty:
            st.info("No consensus sells found.")
        else:
            fig2 = px.bar(
                sells.head(20), x="issuer_name", y="num_managers",
                color="total_value_m", color_continuous_scale="Reds",
                title=f"Consensus Sells/Cuts — {quarter}",
                labels={"issuer_name": "Security", "num_managers": "# Managers Cutting",
                        "total_value_m": "Total Value ($M)"},
                text="num_managers",
            )
            fig2.update_xaxes(tickangle=45)
            st.plotly_chart(fig2, width='stretch')
            st.dataframe(sells, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════════
# PAGE 5 — Manager Panel
# ══════════════════════════════════════════════════════════════════════════════════
elif page == "🗂️ Manager Panel":
    st.title("🗂️ Manager Panel")
    st.caption("Full holdings panel for any manager — filterable and downloadable as CSV.")

    c1, c2 = st.columns([2, 1])
    with c1:
        mgr_name   = st.selectbox("Manager", list(managers_map.keys()))
    with c2:
        qtr_filter = st.multiselect("Filter quarters (blank = all)", quarters)

    cik = managers_map[mgr_name]
    params: list = [cik]
    q_clause = ""
    if qtr_filter:
        placeholders = ",".join("?" * len(qtr_filter))
        q_clause     = f"AND h.report_date IN ({placeholders})"
        params.extend(qtr_filter)

    df = qdf(f"""
        SELECT f.manager_name, h.cik, h.report_date, f.filing_date,
               h.issuer_name, h.cusip, h.class_title,
               h.value_thousands,
               h.shares_principal, h.shares_type,
               h.put_call, h.investment_discretion,
               h.voting_sole, h.voting_shared, h.voting_none
        FROM holdings h
        JOIN filers f ON f.cik=h.cik AND f.report_date=h.report_date
        WHERE h.cik=? {q_clause}
        ORDER BY h.report_date DESC, h.value_thousands DESC
    """, params)

    if df.empty:
        st.warning("No data found.")
    else:
        kc1, kc2, kc3 = st.columns(3)
        kc1.metric("Total Rows", f"{len(df):,}")
        kc2.metric("Unique Securities", df["cusip"].nunique())
        kc3.metric("Total Value ($M)", f"${df['value_thousands'].sum()/1e3:,.1f}M")

        st.dataframe(df, use_container_width=True, hide_index=True, height=500)

        csv_bytes = df.to_csv(index=False).encode()
        fname = mgr_name.replace(" ", "_").replace(",", "").lower() + "_panel.csv"
        st.download_button(
            label="⬇️ Download as CSV",
            data=csv_bytes,
            file_name=fname,
            mime="text/csv",
        )


# ══════════════════════════════════════════════════════════════════════════════════
# PAGE 6 — Portfolio Overlap
# ══════════════════════════════════════════════════════════════════════════════════
elif page == "🔗 Portfolio Overlap":
    st.title("🔗 Portfolio Overlap")
    st.caption("Jaccard similarity (shared CUSIPs ÷ union CUSIPs × 100) between managers.")

    c1, c2 = st.columns([2, 1])
    with c1:
        quarter = st.selectbox("Quarter", quarters)
    with c2:
        top_n = st.slider("Top N managers by position count", 2, len(managers_df), min(6, len(managers_df)))

    qholdings = qdf("""
        SELECT h.cik, f.manager_name, h.cusip
        FROM holdings h
        JOIN filers f ON f.cik=h.cik AND f.report_date=h.report_date
        WHERE h.report_date=?
    """, (quarter,))

    if qholdings.empty:
        st.warning("No holdings data for this quarter.")
    else:
        top_mgrs = (
            qholdings.groupby(["cik", "manager_name"])["cusip"]
            .nunique().reset_index()
            .sort_values("cusip", ascending=False)
            .head(top_n)
        )
        selected_ciks  = top_mgrs["cik"].tolist()
        selected_names = top_mgrs["manager_name"].tolist()
        mgr_cik_map    = dict(zip(selected_names, selected_ciks))

        # Build CUSIP sets per manager
        cusip_sets: dict = {}
        for cik, name in zip(selected_ciks, selected_names):
            cusip_sets[name] = set(qholdings.loc[qholdings["cik"] == cik, "cusip"])

        # Jaccard matrix
        short = [n[:22] for n in selected_names]
        matrix = []
        for name_a in selected_names:
            row = []
            for name_b in selected_names:
                if name_a == name_b:
                    row.append(100.0)
                else:
                    inter = len(cusip_sets[name_a] & cusip_sets[name_b])
                    union = len(cusip_sets[name_a] | cusip_sets[name_b])
                    row.append(round(inter / union * 100, 1) if union else 0.0)
            matrix.append(row)

        matrix_df = pd.DataFrame(matrix, index=short, columns=short)
        fig = px.imshow(
            matrix_df,
            text_auto=True,
            color_continuous_scale="Blues",
            title=f"Portfolio Overlap Matrix (Jaccard %) — {quarter}",
            zmin=0, zmax=100,
        )
        fig.update_layout(height=520)
        st.plotly_chart(fig, width='stretch')

        # Shared holdings drilldown
        st.subheader("Shared Holdings Drilldown")
        col_a, col_b = st.columns(2)
        with col_a:
            mgr_a = st.selectbox("Manager A", selected_names, key="ov_a")
        with col_b:
            opts_b = [n for n in selected_names if n != mgr_a]
            mgr_b  = st.selectbox("Manager B", opts_b, key="ov_b")

        shared = cusip_sets[mgr_a] & cusip_sets[mgr_b]
        if shared:
            cik_a = mgr_cik_map[mgr_a]
            cik_b = mgr_cik_map[mgr_b]
            placeholders = ",".join("?" * len(shared))
            shared_df = qdf(f"""
                SELECT issuer_name, cusip,
                       ROUND(SUM(value_thousands)/1000.0, 2) AS value_usd_m,
                       SUM(shares_principal) AS total_shares
                FROM holdings
                WHERE cusip IN ({placeholders})
                  AND report_date=?
                  AND cik IN (?,?)
                GROUP BY issuer_name, cusip
                ORDER BY value_usd_m DESC
            """, list(shared) + [quarter, cik_a, cik_b])
            st.caption(f"**{len(shared)}** shared securities between {mgr_a} and {mgr_b}")
            st.dataframe(shared_df, use_container_width=True, hide_index=True)
        else:
            st.info("No shared holdings between these two managers.")


# ══════════════════════════════════════════════════════════════════════════════════
# PAGE 7 — Sector Rotation
# ══════════════════════════════════════════════════════════════════════════════════
elif page == "📉 Sector Rotation":
    st.title("📉 Sector Rotation")
    st.caption("How asset-class composition of holdings changed across quarters.")

    view = st.radio("View", ["Single Manager", "All Managers Aggregate"], horizontal=True)

    if view == "Single Manager":
        mgr_name = st.selectbox("Manager", list(managers_map.keys()))
        cik = managers_map[mgr_name]
        df = qdf("""
            SELECT report_date,
                   COALESCE(NULLIF(TRIM(class_title), ''), 'Unknown') AS class_title,
                   ROUND(SUM(value_thousands)/1e3, 2) AS value_m
            FROM holdings
            WHERE cik=?
            GROUP BY report_date, class_title
            ORDER BY report_date
        """, (cik,))
        title = f"Asset Class Mix — {mgr_name}"
    else:
        df = qdf("""
            SELECT report_date,
                   COALESCE(NULLIF(TRIM(class_title), ''), 'Unknown') AS class_title,
                   ROUND(SUM(value_thousands)/1e3, 2) AS value_m
            FROM holdings
            GROUP BY report_date, class_title
            ORDER BY report_date
        """)
        title = "Asset Class Mix — All Managers Aggregate"

    if df.empty:
        st.warning("No data.")
    else:
        fig = px.bar(
            df, x="report_date", y="value_m", color="class_title",
            barmode="stack",
            title=title + " ($M)",
            labels={"report_date": "Quarter", "value_m": "Value ($M)", "class_title": "Class"},
        )
        st.plotly_chart(fig, width='stretch')

        # Percentage view
        pivot   = df.pivot_table(index="report_date", columns="class_title",
                                 values="value_m", aggfunc="sum", fill_value=0)
        pct     = pivot.div(pivot.sum(axis=1), axis=0) * 100
        melted  = pct.reset_index().melt(id_vars="report_date",
                                         var_name="class_title", value_name="pct")
        fig2 = px.bar(
            melted, x="report_date", y="pct", color="class_title",
            barmode="stack",
            title=title + " — % Share",
            labels={"report_date": "Quarter", "pct": "% of AUM", "class_title": "Class"},
        )
        st.plotly_chart(fig2, width='stretch')

        # Top class titles table
        class_summary = (
            df.groupby("class_title")["value_m"]
            .sum().reset_index()
            .sort_values("value_m", ascending=False)
            .rename(columns={"value_m": "total_value_m"})
        )
        class_summary["share_%"] = (
            class_summary["total_value_m"] / class_summary["total_value_m"].sum() * 100
        ).round(1)
        st.subheader("Class Title Breakdown (All Quarters)")
        st.dataframe(class_summary, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════════
# PAGE 8 — New & Exited Positions
# ══════════════════════════════════════════════════════════════════════════════════
elif page == "🆕 New & Exited Positions":
    st.title("🆕 New & Exited Positions")
    st.caption("Securities newly initiated or completely exited compared to the prior quarter.")

    c1, c2 = st.columns([2, 1])
    with c1:
        quarter = st.selectbox("Quarter", quarters)
    with c2:
        mgr_choice = st.selectbox("Manager", ["All Managers"] + list(managers_map.keys()))

    cik_clause_new  = ""
    cik_clause_exit = ""
    base_params_new  = [quarter, quarter]
    base_params_exit = [quarter, quarter]

    if mgr_choice != "All Managers":
        sel_cik = managers_map[mgr_choice]
        cik_clause_new   = "AND curr.cik = ?"
        cik_clause_exit  = "AND prev.cik = ?"
        base_params_new.append(sel_cik)
        base_params_exit.append(sel_cik)

    new_pos = qdf(f"""
        WITH
        curr AS (
            SELECT h.cusip, h.issuer_name, h.cik, f.manager_name,
                   ROUND(h.value_thousands/1000.0, 2) AS value_m,
                   h.shares_principal, h.class_title
            FROM holdings h
            JOIN filers f ON f.cik=h.cik AND f.report_date=h.report_date
            WHERE h.report_date=?
        ),
        prev_cusips AS (
            SELECT cusip, cik FROM holdings
            WHERE report_date=(SELECT MAX(report_date) FROM holdings WHERE report_date<?)
        )
        SELECT curr.manager_name, curr.issuer_name, curr.cusip,
               curr.class_title, curr.value_m, curr.shares_principal
        FROM curr
        LEFT JOIN prev_cusips ON prev_cusips.cusip=curr.cusip AND prev_cusips.cik=curr.cik
        WHERE prev_cusips.cusip IS NULL
        {cik_clause_new}
        ORDER BY curr.value_m DESC
    """, base_params_new)

    exited = qdf(f"""
        WITH
        prev AS (
            SELECT h.cusip, h.issuer_name, h.cik, f.manager_name,
                   ROUND(h.value_thousands/1000.0, 2) AS value_m,
                   h.shares_principal, h.class_title
            FROM holdings h
            JOIN filers f ON f.cik=h.cik AND f.report_date=h.report_date
            WHERE h.report_date=(SELECT MAX(report_date) FROM holdings WHERE report_date<?)
        ),
        curr_cusips AS (
            SELECT cusip, cik FROM holdings WHERE report_date=?
        )
        SELECT prev.manager_name, prev.issuer_name, prev.cusip,
               prev.class_title, prev.value_m AS prev_value_m, prev.shares_principal
        FROM prev
        LEFT JOIN curr_cusips ON curr_cusips.cusip=prev.cusip AND curr_cusips.cik=prev.cik
        WHERE curr_cusips.cusip IS NULL
        {cik_clause_exit}
        ORDER BY prev.value_m DESC
    """, base_params_exit)

    tab1, tab2 = st.tabs(["🆕 New Positions", "🚪 Exited Positions"])

    with tab1:
        if new_pos.empty:
            st.info("No new positions found for the selected quarter/manager.")
        else:
            st.metric("New Positions", len(new_pos))
            fig = px.bar(
                new_pos.head(20), x="value_m", y="issuer_name", orientation="h",
                color="manager_name",
                title=f"Top New Positions — {quarter}",
                labels={"value_m": "Value ($M)", "issuer_name": "Issuer", "manager_name": "Manager"},
            )
            fig.update_layout(yaxis={"categoryorder": "total ascending"}, height=460)
            st.plotly_chart(fig, width='stretch')
            st.dataframe(new_pos, use_container_width=True, hide_index=True)

    with tab2:
        if exited.empty:
            st.info("No exited positions found for the selected quarter/manager.")
        else:
            st.metric("Exited Positions", len(exited))
            fig2 = px.bar(
                exited.head(20), x="prev_value_m", y="issuer_name", orientation="h",
                color="manager_name",
                title=f"Top Exited Positions — {quarter}",
                labels={"prev_value_m": "Prior Value ($M)", "issuer_name": "Issuer",
                        "manager_name": "Manager"},
            )
            fig2.update_layout(yaxis={"categoryorder": "total ascending"}, height=460)
            st.plotly_chart(fig2, width='stretch')
            st.dataframe(exited, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════════
# PAGE 9 — Security Universe  [Phase 2]
# ══════════════════════════════════════════════════════════════════════════════════
elif page == "🔬 Security Universe":
    st.title("🔬 Security Universe")
    st.caption("Phase 2 · Classification of all unique securities in the dataset.")

    sm = qdf("""
        SELECT classification, confidence_score, classification_source,
               cusip, issuer_name, class_title_raw,
               first_seen_quarter, last_seen_quarter, times_held, manual_override
        FROM security_master
        ORDER BY times_held DESC
    """)

    total = len(sm)
    classified = (sm["classification"] != "other").sum()
    avg_conf = sm["confidence_score"].mean()

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total Securities",    f"{total:,}")
    k2.metric("Classified (non-other)", f"{classified:,} ({classified/total*100:.1f}%)")
    k3.metric("Avg Confidence",      f"{avg_conf*100:.1f}%")
    k4.metric("Manual Overrides",    int(sm["manual_override"].sum()))

    st.divider()
    c1, c2 = st.columns(2)

    with c1:
        # By classification — count
        by_cls = (
            sm.groupby("classification")
            .agg(securities=("cusip", "count"),
                 total_holdings=("times_held", "sum"),
                 avg_conf=("confidence_score", "mean"))
            .reset_index()
            .sort_values("securities", ascending=False)
        )
        by_cls["avg_conf_%"] = (by_cls["avg_conf"] * 100).round(1)
        fig = px.bar(
            by_cls, x="classification", y="securities",
            color="avg_conf_%", color_continuous_scale="Blues",
            title="Securities by Classification",
            labels={"classification": "Type", "securities": "# Securities",
                    "avg_conf_%": "Avg Conf %"},
            text="securities",
        )
        fig.update_traces(textposition="outside")
        st.plotly_chart(fig, width='stretch')

    with c2:
        fig2 = px.pie(
            by_cls[by_cls["classification"] != "other"],
            names="classification", values="total_holdings",
            title="Holdings Distribution by Security Type (excl. other)",
            hole=0.35,
        )
        st.plotly_chart(fig2, width='stretch')

    # Confidence distribution
    bins = [0, 0.65, 0.80, 0.95, 1.01]
    labels = ["Weak (<0.65)", "Low (0.65–0.79)", "Medium (0.80–0.94)", "High (≥0.95)"]
    sm["conf_band"] = pd.cut(sm["confidence_score"], bins=bins, labels=labels, right=False)
    conf_dist = sm["conf_band"].value_counts().reset_index()
    conf_dist.columns = ["band", "count"]
    conf_order = ["High (≥0.95)", "Medium (0.80–0.94)", "Low (0.65–0.79)", "Weak (<0.65)"]
    conf_dist["band"] = pd.Categorical(conf_dist["band"], categories=conf_order, ordered=True)
    conf_dist = conf_dist.sort_values("band")
    fig3 = px.bar(
        conf_dist, x="count", y="band", orientation="h",
        color="band",
        color_discrete_map={
            "High (≥0.95)": "#1d4ed8",
            "Medium (0.80–0.94)": "#60a5fa",
            "Low (0.65–0.79)": "#fbbf24",
            "Weak (<0.65)": "#f87171",
        },
        title="Confidence Score Distribution",
        labels={"count": "# Securities", "band": "Confidence Band"},
        text="count",
    )
    fig3.update_traces(textposition="outside")
    fig3.update_layout(showlegend=False, height=300)
    st.plotly_chart(fig3, width='stretch')

    # Source breakdown
    src = (sm.groupby("classification_source")["cusip"]
           .count().reset_index()
           .rename(columns={"cusip": "count"})
           .sort_values("count", ascending=False))
    with st.expander("Classification source breakdown"):
        st.dataframe(src, use_container_width=True, hide_index=True)

    # Full table with filter
    st.subheader("Explore Security Master")
    cls_filter = st.multiselect(
        "Filter by classification",
        sorted(sm["classification"].unique()),
        default=[]
    )
    disp = sm if not cls_filter else sm[sm["classification"].isin(cls_filter)]
    st.caption(f"Showing {len(disp):,} of {total:,} securities")
    st.dataframe(
        disp[["cusip", "issuer_name", "class_title_raw", "classification",
              "confidence_score", "classification_source",
              "first_seen_quarter", "last_seen_quarter", "times_held"]],
        use_container_width=True, hide_index=True, height=450
    )
    csv_bytes = disp.to_csv(index=False).encode()
    st.download_button("⬇️ Download security_master.csv", csv_bytes,
                       "security_master.csv", "text/csv")


# ══════════════════════════════════════════════════════════════════════════════════
# PAGE 10 — Classification Explorer  [Phase 2]
# ══════════════════════════════════════════════════════════════════════════════════
elif page == "📂 Classification Explorer":
    st.title("📂 Classification Explorer")
    st.caption("Phase 2 · Browse holdings enriched with security classification.")

    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        quarter = st.selectbox("Quarter", quarters)
    with c2:
        mgr_name = st.selectbox("Manager", ["All Managers"] + list(managers_map.keys()))
    with c3:
        cls_choices = qdf(
            "SELECT DISTINCT classification FROM security_master ORDER BY classification"
        )["classification"].tolist()
        cls_filter = st.multiselect("Security type", cls_choices)

    # Build query
    params: list = [quarter]
    mgr_clause = ""
    cls_clause = ""

    if mgr_name != "All Managers":
        mgr_clause = "AND h.cik = ?"
        params.append(managers_map[mgr_name])
    if cls_filter:
        placeholders = ",".join("?" * len(cls_filter))
        cls_clause   = f"AND sm.classification IN ({placeholders})"
        params.extend(cls_filter)

    df = qdf(f"""
        SELECT f.manager_name,
               h.issuer_name, h.cusip,
               sm.classification,
               ROUND(sm.confidence_score * 100, 1)       AS conf_pct,
               sm.classification_source,
               h.class_title,
               ROUND(h.value_thousands / 1000.0, 2)       AS value_usd_m,
               h.shares_principal, h.put_call
        FROM holdings h
        JOIN filers f  ON f.cik = h.cik AND f.report_date = h.report_date
        LEFT JOIN security_master sm ON sm.cusip = h.cusip
        WHERE h.report_date = ?
        {mgr_clause}
        {cls_clause}
        ORDER BY h.value_thousands DESC
    """, params)

    if df.empty:
        st.warning("No data for current filters.")
    else:
        # KPIs
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Holdings Rows",    f"{len(df):,}")
        k2.metric("Unique Securities", df["cusip"].nunique())
        k3.metric("Total Value ($M)", f"${df['value_usd_m'].sum():,.1f}M")
        k4.metric("Avg Confidence",   f"{df['conf_pct'].mean():.1f}%")

        # AUM by type
        by_type = (
            df.groupby("classification")["value_usd_m"]
            .sum().reset_index()
            .sort_values("value_usd_m", ascending=False)
        )
        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(
                by_type, x="classification", y="value_usd_m",
                title=f"AUM by Security Type — {quarter}",
                labels={"classification": "Type", "value_usd_m": "Value ($M)"},
                color="classification",
            )
            st.plotly_chart(fig, width='stretch')
        with col2:
            fig2 = px.pie(
                by_type, names="classification", values="value_usd_m",
                title="AUM Share by Security Type",
                hole=0.3,
            )
            st.plotly_chart(fig2, width='stretch')

        # AUM by type over time (all quarters, same manager/cls filters)
        trend_params: list = []
        trend_mgr = ""
        trend_cls = ""
        if mgr_name != "All Managers":
            trend_mgr = "AND h.cik = ?"
            trend_params.append(managers_map[mgr_name])
        if cls_filter:
            placeholders = ",".join("?" * len(cls_filter))
            trend_cls    = f"AND sm.classification IN ({placeholders})"
            trend_params.extend(cls_filter)

        trend = qdf(f"""
            SELECT h.report_date,
                   COALESCE(sm.classification, 'unclassified') AS classification,
                   ROUND(SUM(h.value_thousands)/1e3, 2) AS value_m
            FROM holdings h
            LEFT JOIN security_master sm ON sm.cusip = h.cusip
            WHERE 1=1 {trend_mgr} {trend_cls}
            GROUP BY h.report_date, classification
            ORDER BY h.report_date
        """, trend_params)

        fig3 = px.bar(
            trend, x="report_date", y="value_m", color="classification",
            barmode="stack",
            title="AUM by Security Type Over Time",
            labels={"report_date": "Quarter", "value_m": "Value ($M)",
                    "classification": "Type"},
        )
        st.plotly_chart(fig3, width='stretch')

        st.subheader("Holdings Detail")
        st.dataframe(df, use_container_width=True, hide_index=True, height=450)
        st.download_button(
            "⬇️ Download CSV", df.to_csv(index=False).encode(),
            f"classified_holdings_{quarter}.csv", "text/csv"
        )


# ══════════════════════════════════════════════════════════════════════════════════
# PAGE 11 — Security Type Signals  [Phase 2]
# ══════════════════════════════════════════════════════════════════════════════════
elif page == "💡 Security Type Signals":
    st.title("💡 Security Type Signals")
    st.caption("Phase 2 · Options activity, ETF flows, ADR exposure, and "
               "bond allocation trends derived from classified security types.")

    quarter = st.selectbox("Reference quarter", quarters)

    # ── Options activity ─────────────────────────────────────────────────────────
    st.subheader("Options Activity")
    opts = qdf("""
        SELECT f.manager_name, h.report_date,
               SUM(CASE WHEN sm.classification='option_call' THEN h.value_thousands ELSE 0 END) AS calls_k,
               SUM(CASE WHEN sm.classification='option_put'  THEN h.value_thousands ELSE 0 END) AS puts_k,
               COUNT(CASE WHEN sm.classification='option_call' THEN 1 END) AS n_calls,
               COUNT(CASE WHEN sm.classification='option_put'  THEN 1 END) AS n_puts
        FROM holdings h
        JOIN filers f ON f.cik=h.cik AND f.report_date=h.report_date
        LEFT JOIN security_master sm ON sm.cusip=h.cusip
        WHERE h.report_date=?
        GROUP BY f.manager_name, h.report_date
        HAVING (calls_k + puts_k) > 0
        ORDER BY (calls_k + puts_k) DESC
    """, (quarter,))

    if opts.empty:
        st.info("No options positions found for this quarter.")
    else:
        opts["call_value_m"] = (opts["calls_k"] / 1e3).round(2)
        opts["put_value_m"]  = (opts["puts_k"]  / 1e3).round(2)
        opts["put_call_ratio"] = (
            opts["puts_k"] / opts["calls_k"].replace(0, float("nan"))
        ).round(2)
        fig = px.bar(
            opts.melt(id_vars="manager_name",
                      value_vars=["call_value_m", "put_value_m"],
                      var_name="type", value_name="value_m"),
            x="manager_name", y="value_m", color="type",
            barmode="group",
            title=f"Options Exposure by Manager — {quarter}",
            labels={"manager_name": "Manager", "value_m": "Value ($M)", "type": "Type"},
            color_discrete_map={"call_value_m": "#22c55e", "put_value_m": "#ef4444"},
        )
        st.plotly_chart(fig, width='stretch')
        st.dataframe(opts[["manager_name", "call_value_m", "put_value_m",
                            "n_calls", "n_puts", "put_call_ratio"]],
                     use_container_width=True, hide_index=True)

    # ── ETF flows ─────────────────────────────────────────────────────────────────
    st.divider()
    st.subheader("ETF Allocation Over Time")

    etf_trend = qdf("""
        SELECT h.report_date,
               ROUND(SUM(CASE WHEN sm.classification='etf' THEN h.value_thousands ELSE 0 END)/1e3, 2) AS etf_m,
               ROUND(SUM(h.value_thousands)/1e3, 2) AS total_m
        FROM holdings h
        LEFT JOIN security_master sm ON sm.cusip=h.cusip
        GROUP BY h.report_date ORDER BY h.report_date
    """)
    etf_trend["etf_pct"] = (etf_trend["etf_m"] / etf_trend["total_m"] * 100).round(2)

    col1, col2 = st.columns(2)
    with col1:
        fig2 = px.bar(
            etf_trend, x="report_date", y="etf_m",
            title="Total ETF Holdings ($M) Over Time",
            labels={"report_date": "Quarter", "etf_m": "ETF Value ($M)"},
            color_discrete_sequence=["#3b82f6"],
        )
        st.plotly_chart(fig2, width='stretch')
    with col2:
        fig3 = px.line(
            etf_trend, x="report_date", y="etf_pct",
            markers=True,
            title="ETF % of Total AUM Over Time",
            labels={"report_date": "Quarter", "etf_pct": "ETF %"},
        )
        fig3.add_hline(y=etf_trend["etf_pct"].mean(), line_dash="dot",
                       annotation_text="avg", line_color="gray")
        st.plotly_chart(fig3, width='stretch')

    # ── ADR Exposure ──────────────────────────────────────────────────────────────
    st.divider()
    st.subheader("ADR / International Depositary Receipt Exposure")

    adr_qtr = qdf("""
        SELECT f.manager_name,
               ROUND(SUM(CASE WHEN sm.classification='adr' THEN h.value_thousands ELSE 0 END)/1e3, 2) AS adr_m,
               ROUND(SUM(h.value_thousands)/1e3, 2) AS total_m
        FROM holdings h
        JOIN filers f ON f.cik=h.cik AND f.report_date=h.report_date
        LEFT JOIN security_master sm ON sm.cusip=h.cusip
        WHERE h.report_date=?
        GROUP BY f.manager_name
        ORDER BY adr_m DESC
    """, (quarter,))
    adr_qtr["adr_pct"] = (adr_qtr["adr_m"] / adr_qtr["total_m"] * 100).round(1)

    fig4 = px.bar(
        adr_qtr, x="manager_name", y="adr_pct",
        title=f"ADR Allocation % by Manager — {quarter}",
        labels={"manager_name": "Manager", "adr_pct": "ADR %"},
        color="adr_m", color_continuous_scale="Oranges",
        text=adr_qtr["adr_pct"].map(lambda x: f"{x:.1f}%"),
    )
    fig4.update_traces(textposition="outside")
    st.plotly_chart(fig4, width='stretch')

    # Top ADR names across all managers in quarter
    top_adrs = qdf("""
        SELECT h.issuer_name, h.cusip,
               COUNT(DISTINCT h.cik) AS managers_holding,
               ROUND(SUM(h.value_thousands)/1e3, 2) AS total_value_m
        FROM holdings h
        LEFT JOIN security_master sm ON sm.cusip=h.cusip
        WHERE h.report_date=? AND sm.classification='adr'
        GROUP BY h.issuer_name, h.cusip
        ORDER BY total_value_m DESC
        LIMIT 20
    """, (quarter,))

    if not top_adrs.empty:
        with st.expander("Top ADR positions this quarter"):
            st.dataframe(top_adrs, use_container_width=True, hide_index=True)

    # ── Bond / Fixed Income ───────────────────────────────────────────────────────
    st.divider()
    st.subheader("Fixed Income (Bond) Allocation")

    bond_trend = qdf("""
        SELECT h.report_date,
               ROUND(SUM(CASE WHEN sm.classification='bond' THEN h.value_thousands ELSE 0 END)/1e3, 2) AS bond_m,
               ROUND(SUM(h.value_thousands)/1e3, 2) AS total_m
        FROM holdings h
        LEFT JOIN security_master sm ON sm.cusip=h.cusip
        GROUP BY h.report_date ORDER BY h.report_date
    """)
    bond_trend["bond_pct"] = (
        bond_trend["bond_m"] / bond_trend["total_m"] * 100
    ).round(2)

    fig5 = px.line(
        bond_trend, x="report_date", y="bond_pct",
        markers=True,
        title="Bond Allocation % Over Time",
        labels={"report_date": "Quarter", "bond_pct": "Bond %"},
        color_discrete_sequence=["#f59e0b"],
    )
    st.plotly_chart(fig5, width='stretch')

    # Preferred stock tracker
    st.divider()
    st.subheader("Preferred Stock Holdings")
    pref = qdf("""
        SELECT h.issuer_name, h.cusip,
               h.report_date,
               COUNT(DISTINCT h.cik) AS managers,
               ROUND(SUM(h.value_thousands)/1e3, 2) AS value_m
        FROM holdings h
        LEFT JOIN security_master sm ON sm.cusip=h.cusip
        WHERE sm.classification='preferred'
        GROUP BY h.issuer_name, h.cusip, h.report_date
        ORDER BY h.report_date DESC, value_m DESC
        LIMIT 30
    """)
    if pref.empty:
        st.info("No preferred stock positions found.")
    else:
        st.dataframe(pref, use_container_width=True, hide_index=True)

