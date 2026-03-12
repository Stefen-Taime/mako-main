import os
import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
import urllib.request


# ─── Page config ───
st.set_page_config(
    page_title="NYC TLC Analytics",
    page_icon="🚕",
    layout="wide",
)

# ─── Snowflake connection (credentials from Vault) ───
@st.cache_resource
def get_connection():
    try:
        vault_addr = os.environ.get("VAULT_ADDR", "http://localhost:8200")
        vault_token = os.environ.get("VAULT_TOKEN", "")
        req = urllib.request.Request(f"{vault_addr}/v1/secret/data/mako/snowflake")
        req.add_header("X-Vault-Token", vault_token)
        resp = urllib.request.urlopen(req)
        creds = json.loads(resp.read())["data"]["data"]
    except Exception:
        creds = {
            "account": st.secrets.get("SNOWFLAKE_ACCOUNT", ""),
            "user": st.secrets.get("SNOWFLAKE_USER", ""),
            "password": st.secrets.get("SNOWFLAKE_PASSWORD", ""),
            "warehouse": st.secrets.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            "role": st.secrets.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
            "database": st.secrets.get("SNOWFLAKE_DATABASE", "ANALYTICS"),
        }

    return snowflake.connector.connect(
        account=creds["account"],
        user=creds["user"],
        password=creds["password"],
        warehouse=creds.get("warehouse", "COMPUTE_WH"),
        role=creds.get("role", "ACCOUNTADMIN"),
        database=creds.get("database", "ANALYTICS"),
        schema="DW",
    )


@st.cache_data(ttl=300)
def run_query(query: str) -> pd.DataFrame:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    cur.close()
    return pd.DataFrame(rows, columns=cols)


# ─── Sidebar ───
st.sidebar.title("🚕 NYC TLC Analytics")
st.sidebar.markdown("Dashboard base sur le star schema Snowflake `ANALYTICS.DW`")

section = st.sidebar.radio(
    "Section",
    [
        "Vue d'ensemble",
        "Analyse Temporelle",
        "Analyse Geographique",
        "Pourboires",
        "Categories de Trajet",
        "Vendeurs",
        "Surcharges & Frais",
        "Tarification",
        "Anomalies (WASM)",
    ],
)

st.sidebar.markdown("---")
st.sidebar.caption("Mako Pipeline | Snowflake | Vault")

# ═══════════════════════════════════════════
# 1. VUE D'ENSEMBLE
# ═══════════════════════════════════════════
if section == "Vue d'ensemble":
    st.title("📊 Vue d'ensemble")

    kpi = run_query("""
        SELECT
            COUNT(*) AS "total_trips",
            ROUND(SUM("total_amount"), 2) AS "total_revenue",
            ROUND(AVG("total_amount"), 2) AS "avg_fare",
            ROUND(AVG("trip_distance"), 2) AS "avg_distance",
            ROUND(AVG("tip_amount"), 2) AS "avg_tip",
            ROUND(AVG("trip_duration_minutes"), 1) AS "avg_duration"
        FROM FACT_TRIPS
    """)

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Total Courses", f"{kpi['total_trips'].iloc[0]:,}")
    c2.metric("Revenue Total", f"${kpi['total_revenue'].iloc[0]:,.2f}")
    c3.metric("Tarif Moyen", f"${kpi['avg_fare'].iloc[0]:.2f}")
    c4.metric("Distance Moy.", f"{kpi['avg_distance'].iloc[0]:.2f} mi")
    c5.metric("Pourboire Moy.", f"${kpi['avg_tip'].iloc[0]:.2f}")
    c6.metric("Duree Moy.", f"{kpi['avg_duration'].iloc[0]:.1f} min")

    st.markdown("---")

    daily = run_query("""
        SELECT "full_date", "day_name", "total_trips", "total_revenue",
               "avg_fare", "is_weekend"
        FROM AGG_DAILY_SUMMARY
        ORDER BY "full_date"
    """)

    if not daily.empty:
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Revenue par jour")
            fig = px.bar(
                daily, x="full_date", y="total_revenue",
                color="is_weekend",
                color_discrete_map={0: "#636EFA", 1: "#EF553B"},
                labels={"total_revenue": "Revenue ($)", "full_date": "Date", "is_weekend": "Weekend"},
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("Courses par jour")
            fig = px.bar(
                daily, x="full_date", y="total_trips",
                color="is_weekend",
                color_discrete_map={0: "#636EFA", 1: "#EF553B"},
                labels={"total_trips": "Courses", "full_date": "Date", "is_weekend": "Weekend"},
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

    st.subheader("Weekend vs Semaine")
    we = run_query("""
        SELECT
            CASE WHEN "is_weekend" = 1 THEN 'Weekend' ELSE 'Semaine' END AS "period",
            SUM("total_trips") AS "trips",
            ROUND(AVG("avg_fare"), 2) AS "avg_fare",
            ROUND(AVG("avg_tip_pct"), 2) AS "avg_tip_pct",
            ROUND(AVG("avg_distance"), 2) AS "avg_distance"
        FROM AGG_DAILY_SUMMARY
        GROUP BY "is_weekend"
    """)

    if not we.empty:
        col1, col2 = st.columns(2)
        with col1:
            fig = px.pie(we, names="period", values="trips", title="Repartition des courses")
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            fig = px.bar(
                we, x="period", y=["avg_fare", "avg_tip_pct", "avg_distance"],
                barmode="group", title="Comparaison Weekend vs Semaine",
            )
            st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════
# 2. ANALYSE TEMPORELLE
# ═══════════════════════════════════════════
elif section == "Analyse Temporelle":
    st.title("🕐 Analyse Temporelle")

    hourly = run_query("""
        SELECT
            t."hour_of_day",
            t."time_period",
            t."is_rush_hour",
            COUNT(*) AS "trips",
            ROUND(AVG(f."total_amount"), 2) AS "avg_fare",
            ROUND(AVG(f."trip_duration_minutes"), 1) AS "avg_duration",
            ROUND(AVG(f."trip_distance"), 2) AS "avg_distance"
        FROM FACT_TRIPS f
        JOIN DIM_TIME t ON f."pickup_time_key" = t."hour_key"
        GROUP BY t."hour_of_day", t."time_period", t."is_rush_hour"
        ORDER BY t."hour_of_day"
    """)

    if not hourly.empty:
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Courses par heure")
            fig = px.bar(
                hourly, x="hour_of_day", y="trips",
                color="time_period",
                labels={"hour_of_day": "Heure", "trips": "Courses"},
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("Tarif moyen par heure")
            fig = px.line(
                hourly, x="hour_of_day", y="avg_fare",
                markers=True,
                labels={"hour_of_day": "Heure", "avg_fare": "Tarif moyen ($)"},
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Resume par periode de la journee")
        period_summary = hourly.groupby("time_period").agg(
            {"trips": "sum", "avg_fare": "mean", "avg_duration": "mean"}
        ).round(2).reset_index()
        period_summary.columns = ["Periode", "Total Courses", "Tarif Moyen ($)", "Duree Moyenne (min)"]
        st.dataframe(period_summary, use_container_width=True, hide_index=True)


# ═══════════════════════════════════════════
# 3. ANALYSE GEOGRAPHIQUE
# ═══════════════════════════════════════════
elif section == "Analyse Geographique":
    st.title("📍 Analyse Geographique")

    st.subheader("Top 10 zones de depart par revenue")
    top_zones = run_query("""
        SELECT
            l."location_id", l."zone_name", l."borough", l."service_zone",
            COUNT(*) AS "trips",
            ROUND(SUM(f."total_amount"), 2) AS "total_revenue",
            ROUND(AVG(f."trip_distance"), 2) AS "avg_distance"
        FROM FACT_TRIPS f
        JOIN DIM_LOCATION l ON f."pickup_location_id" = l."location_id"
        GROUP BY l."location_id", l."zone_name", l."borough", l."service_zone"
        ORDER BY "total_revenue" DESC
        LIMIT 10
    """)

    if not top_zones.empty:
        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(
                top_zones, x="zone_name", y="total_revenue",
                color="borough",
                labels={"total_revenue": "Revenue ($)", "zone_name": "Zone"},
            )
            fig.update_layout(height=400, xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.bar(
                top_zones, x="zone_name", y="trips",
                color="borough",
                labels={"trips": "Courses", "zone_name": "Zone"},
            )
            fig.update_layout(height=400, xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

    st.subheader("Matrice Origine-Destination par borough")
    od_matrix = run_query("""
        SELECT
            pl."borough" AS "origin",
            dl."borough" AS "destination",
            COUNT(*) AS "trips",
            ROUND(AVG(f."total_amount"), 2) AS "avg_fare"
        FROM FACT_TRIPS f
        JOIN DIM_LOCATION pl ON f."pickup_location_id" = pl."location_id"
        JOIN DIM_LOCATION dl ON f."dropoff_location_id" = dl."location_id"
        GROUP BY pl."borough", dl."borough"
        ORDER BY "trips" DESC
    """)

    if not od_matrix.empty:
        pivot = od_matrix.pivot_table(index="origin", columns="destination", values="trips", fill_value=0)
        fig = px.imshow(
            pivot, text_auto=True,
            labels=dict(x="Destination", y="Origine", color="Courses"),
            color_continuous_scale="Blues",
            title="Nombre de courses entre boroughs",
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Revenue par zone de service")
    svc_zones = run_query("""
        SELECT
            l."service_zone",
            COUNT(*) AS "trips",
            ROUND(SUM(f."total_amount"), 2) AS "total_revenue",
            ROUND(AVG(f."total_amount"), 2) AS "avg_fare"
        FROM FACT_TRIPS f
        JOIN DIM_LOCATION l ON f."pickup_location_id" = l."location_id"
        GROUP BY l."service_zone"
        ORDER BY "total_revenue" DESC
    """)

    if not svc_zones.empty:
        col1, col2 = st.columns(2)
        with col1:
            fig = px.pie(
                svc_zones, names="service_zone", values="total_revenue",
                title="Revenue par zone de service",
            )
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            fig = px.bar(
                svc_zones, x="service_zone", y="avg_fare",
                color="service_zone", title="Tarif moyen par zone de service",
                labels={"avg_fare": "Tarif moyen ($)", "service_zone": "Zone de service"},
            )
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════
# 4. POURBOIRES
# ═══════════════════════════════════════════
elif section == "Pourboires":
    st.title("💰 Analyse des Pourboires")

    tips = run_query("""
        SELECT
            p."payment_type_name",
            COUNT(*) AS "trips",
            ROUND(AVG(f."tip_amount"), 2) AS "avg_tip",
            ROUND(SUM(f."tip_amount"), 2) AS "total_tips",
            ROUND(AVG(CASE WHEN f."fare_amount" > 0
                THEN f."tip_amount" / f."fare_amount" * 100 ELSE 0 END), 2) AS "tip_pct"
        FROM FACT_TRIPS f
        JOIN DIM_PAYMENT_TYPE p ON f."payment_type_id" = p."payment_type_id"
        GROUP BY p."payment_type_name"
        ORDER BY "avg_tip" DESC
    """)

    if not tips.empty:
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Pourboire moyen par type de paiement")
            fig = px.bar(
                tips, x="payment_type_name", y="avg_tip",
                color="payment_type_name",
                labels={"avg_tip": "Pourboire moyen ($)", "payment_type_name": "Paiement"},
            )
            fig.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("% Pourboire / Tarif")
            fig = px.bar(
                tips, x="payment_type_name", y="tip_pct",
                color="payment_type_name",
                labels={"tip_pct": "Pourboire (%)", "payment_type_name": "Paiement"},
            )
            fig.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Detail par type de paiement")
        display_tips = tips.copy()
        display_tips.columns = ["Type de Paiement", "Courses", "Pourboire Moy ($)", "Total Pourboires ($)", "Tip %"]
        st.dataframe(display_tips, use_container_width=True, hide_index=True)


# ═══════════════════════════════════════════
# 5. CATEGORIES DE TRAJET
# ═══════════════════════════════════════════
elif section == "Categories de Trajet":
    st.title("🛣️ Categories de Trajet")

    cats = run_query("""
        SELECT
            "trip_category",
            COUNT(*) AS "trips",
            ROUND(AVG("total_amount"), 2) AS "avg_fare",
            ROUND(AVG("trip_distance"), 2) AS "avg_distance",
            ROUND(AVG("cost_per_mile"), 2) AS "avg_cost_per_mile",
            ROUND(AVG("trip_duration_minutes"), 1) AS "avg_duration",
            ROUND(SUM("total_amount"), 2) AS "total_revenue"
        FROM FACT_TRIPS
        GROUP BY "trip_category"
        ORDER BY "trips" DESC
    """)

    if not cats.empty:
        col1, col2, col3 = st.columns(3)

        with col1:
            fig = px.pie(
                cats, names="trip_category", values="trips",
                title="Repartition des courses",
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.bar(
                cats, x="trip_category", y="avg_fare",
                color="trip_category", title="Tarif moyen par categorie",
                labels={"avg_fare": "Tarif moyen ($)", "trip_category": "Categorie"},
            )
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        with col3:
            fig = px.bar(
                cats, x="trip_category", y="avg_cost_per_mile",
                color="trip_category", title="Cout par mile",
                labels={"avg_cost_per_mile": "$/mile", "trip_category": "Categorie"},
            )
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Detail par categorie")
        display = cats.copy()
        display.columns = ["Categorie", "Courses", "Tarif Moy ($)", "Distance Moy (mi)", "$/mile", "Duree Moy (min)", "Revenue Total ($)"]
        st.dataframe(display, use_container_width=True, hide_index=True)


# ═══════════════════════════════════════════
# 6. VENDEURS
# ═══════════════════════════════════════════
elif section == "Vendeurs":
    st.title("🏢 Performance par Vendeur")

    vendors = run_query("""
        SELECT
            v."vendor_name",
            COUNT(*) AS "trips",
            ROUND(SUM(f."total_amount"), 2) AS "total_revenue",
            ROUND(AVG(f."total_amount"), 2) AS "avg_fare",
            ROUND(AVG(f."tip_amount"), 2) AS "avg_tip",
            ROUND(AVG(f."trip_distance"), 2) AS "avg_distance",
            ROUND(AVG(f."trip_duration_minutes"), 1) AS "avg_duration"
        FROM FACT_TRIPS f
        JOIN DIM_VENDOR v ON f."vendor_id" = v."vendor_id"
        GROUP BY v."vendor_name"
        ORDER BY "total_revenue" DESC
    """)

    if not vendors.empty:
        col1, col2 = st.columns(2)

        with col1:
            fig = px.pie(
                vendors, names="vendor_name", values="total_revenue",
                title="Part de marche (revenue)",
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.bar(
                vendors, x="vendor_name",
                y=["avg_fare", "avg_tip"],
                barmode="group",
                title="Tarif et pourboire moyens",
                labels={"value": "$", "vendor_name": "Vendeur"},
            )
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Tableau comparatif")
        display = vendors.copy()
        display.columns = ["Vendeur", "Courses", "Revenue Total ($)", "Tarif Moy ($)", "Tip Moy ($)", "Distance Moy (mi)", "Duree Moy (min)"]
        st.dataframe(display, use_container_width=True, hide_index=True)


# ═══════════════════════════════════════════
# 7. SURCHARGES & FRAIS
# ═══════════════════════════════════════════
elif section == "Surcharges & Frais":
    st.title("💸 Surcharges & Frais")

    surcharges = run_query("""
        SELECT
            ROUND(SUM("congestion_surcharge"), 2) AS "total_congestion",
            ROUND(SUM("airport_fee"), 2) AS "total_airport",
            COALESCE(ROUND(SUM(TRY_TO_DOUBLE("cbd_congestion_fee")), 2), 0) AS "total_cbd",
            ROUND(SUM("extra"), 2) AS "total_extra",
            ROUND(SUM("mta_tax"), 2) AS "total_mta",
            ROUND(SUM("improvement_surcharge"), 2) AS "total_improvement",
            ROUND(SUM("total_surcharges"), 2) AS "total_surcharges",
            ROUND(SUM("total_amount"), 2) AS "total_amount",
            ROUND(AVG(CASE WHEN "total_amount" > 0
                THEN "total_surcharges" / "total_amount" * 100 ELSE 0 END), 2) AS "surcharge_pct"
        FROM FACT_TRIPS
    """)

    if not surcharges.empty:
        row = surcharges.iloc[0]

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Total Surcharges", f"${row['total_surcharges']:,.2f}")
        c2.metric("% du Montant Total", f"{row['surcharge_pct']:.1f}%")
        c3.metric("Congestion Fees", f"${row['total_congestion']:,.2f}")
        c4.metric("Airport Fees", f"${row['total_airport']:,.2f}")
        c5.metric("CBD Congestion", f"${row['total_cbd']:,.2f}")

        st.markdown("---")

        breakdown = pd.DataFrame({
            "Type": ["Congestion", "Airport", "CBD Congestion", "Extra", "MTA Tax", "Improvement"],
            "Montant ($)": [
                row["total_congestion"], row["total_airport"], row["total_cbd"],
                row["total_extra"], row["total_mta"], row["total_improvement"],
            ],
        })

        col1, col2 = st.columns(2)
        with col1:
            fig = px.pie(
                breakdown, names="Type", values="Montant ($)",
                title="Repartition des surcharges",
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.bar(
                breakdown, x="Type", y="Montant ($)",
                color="Type", title="Montant par type de surcharge",
            )
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════
# 8. TARIFICATION
# ═══════════════════════════════════════════
elif section == "Tarification":
    st.title("🏷️ Analyse par Type de Tarif")

    rates = run_query("""
        SELECT
            r."rate_code_name",
            COUNT(*) AS "trips",
            ROUND(AVG(f."total_amount"), 2) AS "avg_fare",
            ROUND(AVG(f."trip_distance"), 2) AS "avg_distance",
            ROUND(SUM(f."total_amount"), 2) AS "total_revenue",
            ROUND(AVG(f."tip_amount"), 2) AS "avg_tip",
            ROUND(AVG(f."trip_duration_minutes"), 1) AS "avg_duration"
        FROM FACT_TRIPS f
        JOIN DIM_RATE_CODE r ON f."rate_code_id" = r."rate_code_id"
        GROUP BY r."rate_code_name"
        ORDER BY "trips" DESC
    """)

    if not rates.empty:
        col1, col2 = st.columns(2)

        with col1:
            fig = px.pie(
                rates, names="rate_code_name", values="trips",
                title="Repartition par type de tarif",
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.bar(
                rates, x="rate_code_name", y="avg_fare",
                color="rate_code_name",
                title="Tarif moyen par type",
                labels={"avg_fare": "Tarif moyen ($)", "rate_code_name": "Tarif"},
            )
            fig.update_layout(showlegend=False, xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Distance vs Tarif par type")
        fig = px.scatter(
            rates, x="avg_distance", y="avg_fare",
            size="trips", color="rate_code_name",
            labels={"avg_distance": "Distance moy (mi)", "avg_fare": "Tarif moy ($)"},
            hover_data=["trips", "total_revenue"],
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Detail par tarif")
        display = rates.copy()
        display.columns = ["Tarif", "Courses", "Tarif Moy ($)", "Distance Moy (mi)", "Revenue Total ($)", "Tip Moy ($)", "Duree Moy (min)"]
        st.dataframe(display, use_container_width=True, hide_index=True)


# ═══════════════════════════════════════════
# 9. ANOMALIES (WASM Rust Plugin)
# ═══════════════════════════════════════════
elif section == "Anomalies (WASM)":
    st.title("🔍 Detection d'Anomalies (WASM Rust)")
    st.markdown("""
    Chaque trip est analyse par un plugin **WebAssembly** ecrit en Rust
    qui calcule un **score d'anomalie** (0-100) base sur 8 regles de detection.
    Les trips avec un score >= 50 sont marques comme anomalies.
    """)

    # KPIs
    anomaly_kpi = run_query("""
        SELECT
            COUNT(*) AS "total_trips",
            SUM(CASE WHEN "is_anomaly" = 'true' THEN 1 ELSE 0 END) AS "anomaly_count",
            ROUND(AVG("anomaly_score"), 2) AS "avg_score",
            MAX("anomaly_score") AS "max_score"
        FROM FACT_TRIPS
    """)

    if not anomaly_kpi.empty:
        row = anomaly_kpi.iloc[0]
        total = row["total_trips"]
        anomalies = row["anomaly_count"]
        pct = round(anomalies / total * 100, 2) if total > 0 else 0

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Trips", f"{total:,}")
        c2.metric("Anomalies Detectees", f"{anomalies:,}")
        c3.metric("% Anomalies", f"{pct:.2f}%")
        c4.metric("Score Moyen", f"{row['avg_score']:.1f}")

    st.markdown("---")

    # Score distribution
    score_dist = run_query("""
        SELECT
            CASE
                WHEN "anomaly_score" = 0 THEN '0 (clean)'
                WHEN "anomaly_score" BETWEEN 1 AND 19 THEN '1-19 (low)'
                WHEN "anomaly_score" BETWEEN 20 AND 49 THEN '20-49 (medium)'
                WHEN "anomaly_score" BETWEEN 50 AND 79 THEN '50-79 (high)'
                ELSE '80-100 (critical)'
            END AS "score_range",
            COUNT(*) AS "trips"
        FROM FACT_TRIPS
        GROUP BY "score_range"
        ORDER BY "score_range"
    """)

    if not score_dist.empty:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Distribution des scores")
            fig = px.bar(
                score_dist, x="score_range", y="trips",
                color="score_range",
                color_discrete_map={
                    "0 (clean)": "#2ecc71",
                    "1-19 (low)": "#27ae60",
                    "20-49 (medium)": "#f39c12",
                    "50-79 (high)": "#e74c3c",
                    "80-100 (critical)": "#c0392b",
                },
                labels={"trips": "Courses", "score_range": "Score"},
            )
            fig.update_layout(showlegend=False, height=400)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("Repartition anomalies vs normal")
            normal_count = total - anomalies if not anomaly_kpi.empty else 0
            fig = px.pie(
                names=["Normal (score < 50)", "Anomalie (score >= 50)"],
                values=[normal_count, anomalies],
                color_discrete_sequence=["#2ecc71", "#e74c3c"],
            )
            st.plotly_chart(fig, use_container_width=True)

    # Top anomaly flags
    st.subheader("Regles les plus declenchees")
    st.markdown("""
    | Regle | Points | Condition |
    |---|---|---|
    | `speed_impossible` | +40 | Vitesse > 100 mph |
    | `speed_suspicious` | +15 | Vitesse > 60 mph |
    | `tip_excessive` | +20 | Pourboire > 50% du fare |
    | `fare_zero_dist` | +30 | Fare > $50, distance = 0 |
    | `duration_extreme` | +25 | Trajet > 6 heures |
    | `cost_per_mile_high` | +20 | Cout/mile > $50 |
    | `negative_amounts` | +30 | Montants negatifs |
    | `passenger_extreme` | +15 | > 6 passagers |
    """)

    # Top anomalous trips
    st.subheader("Top 20 trips les plus anomaux")
    top_anomalies = run_query("""
        SELECT
            "trip_id", "anomaly_score", "anomaly_flags",
            "trip_distance", "trip_duration_minutes",
            "fare_amount", "tip_amount", "total_amount",
            "cost_per_mile", "passenger_count"
        FROM FACT_TRIPS
        WHERE "is_anomaly" = 'true'
        ORDER BY "anomaly_score" DESC
        LIMIT 20
    """)

    if not top_anomalies.empty:
        st.dataframe(top_anomalies, use_container_width=True, hide_index=True)
    else:
        st.info("Aucune anomalie detectee dans les donnees.")
