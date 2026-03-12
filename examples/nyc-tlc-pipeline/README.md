# NYC TLC Analytics Pipeline

End-to-end data pipeline that ingests NYC Taxi & Limousine Commission (TLC) trip data into a Snowflake star schema, with a Streamlit dashboard for analytics.

## Architecture

```
NYC TLC Open Data (Parquet)
        |
        v
  [DuckDB Source] --- HTTP download --->  [GCS Sink]
        |                                     |
        |  (Yellow, Green, FHV, HVFHV)        |
        v                                     v
  [DuckDB Source] --- GCS Parquet --->  [Snowflake Staging]
        |
        v
  +------------------+
  |   Dimensions     |   (6 tables, parallel)
  |  DIM_VENDOR      |   - static lookup
  |  DIM_PAYMENT_TYPE|   - static lookup
  |  DIM_RATE_CODE   |   - static lookup
  |  DIM_DATE        |   - generated calendar
  |  DIM_TIME        |   - generated 24h slots
  |  DIM_LOCATION    |   - NYC TLC Taxi Zone CSV
  +------------------+
        |
        v
  +------------------+
  |   FACT_TRIPS     |   DuckDB -> DQ checks -> WASM anomaly detector -> Snowflake
  +------------------+
        |
        v
  +------------------+
  | AGG_DAILY_SUMMARY|   Pre-aggregated daily metrics
  +------------------+
        |
        v
  [Streamlit Dashboard]   9 analysis sections
```

## Star Schema (Snowflake `ANALYTICS.DW`)

| Table | Type | Rows | Description |
|-------|------|------|-------------|
| `FACT_TRIPS` | Fact | ~5,000 | Trip-level data with measures and dimension keys |
| `AGG_DAILY_SUMMARY` | Aggregate | ~31 | Pre-computed daily KPIs |
| `DIM_LOCATION` | Dimension | 265 | NYC TLC Taxi Zones |
| `DIM_DATE` | Dimension | 366 | Calendar dimension (2024) |
| `DIM_TIME` | Dimension | 24 | Hour-of-day with time periods |
| `DIM_VENDOR` | Dimension | 3 | Taxi vendors |
| `DIM_PAYMENT_TYPE` | Dimension | 4 | Payment methods |
| `DIM_RATE_CODE` | Dimension | 6 | Rate codes (standard, JFK, etc.) |

## Pipelines

| Pipeline | Source | Sink | Description |
|----------|--------|------|-------------|
| `pipeline-tlc-to-gcs.yaml` | NYC TLC HTTP (Parquet) | GCS | Extract Yellow Taxi data |
| `pipeline-tlc-green-to-gcs.yaml` | NYC TLC HTTP | GCS | Extract Green Taxi data |
| `pipeline-tlc-fhv-to-gcs.yaml` | NYC TLC HTTP | GCS | Extract FHV data |
| `pipeline-tlc-hvfhv-to-gcs.yaml` | NYC TLC HTTP | GCS | Extract HVFHV data |
| `pipeline-gcs-to-snowflake-staging.yaml` | GCS (Parquet) | Snowflake | Load Yellow into staging |
| `pipeline-gcs-green-to-snowflake-staging.yaml` | GCS | Snowflake | Load Green into staging |
| `pipeline-gcs-hvfhv-to-snowflake-staging.yaml` | GCS | Snowflake | Load HVFHV into staging |
| `pipeline-dim-*.yaml` | DuckDB / HTTP CSV | Snowflake | Build dimension tables |
| `pipeline-fact-trips.yaml` | GCS (Parquet) | Snowflake | Build fact table (DQ + WASM) |
| `pipeline-agg-daily.yaml` | GCS (Parquet) | Snowflake | Build daily aggregation |

## WASM Anomaly Detector

A Rust WebAssembly plugin (`wasm-anomaly/`) scores each trip on 8 rules:

| Rule | Points | Condition |
|------|--------|-----------|
| `speed_impossible` | +40 | Average speed > 100 mph |
| `speed_suspicious` | +15 | Average speed > 60 mph |
| `fare_zero_dist` | +30 | Fare > $50, distance = 0 |
| `duration_extreme` | +25 | Trip > 6 hours |
| `negative_amounts` | +30 | Negative financial fields |
| `tip_excessive` | +20 | Tip > 50% of fare |
| `cost_per_mile_high` | +20 | Cost/mile > $50 |
| `passenger_extreme` | +15 | > 6 passengers |

Trips with score >= 50 are flagged as anomalies.

```bash
# Build the WASM plugin
cd wasm-anomaly
rustup target add wasm32-wasip1
cargo build --target wasm32-wasip1 --release
cp target/wasm32-wasip1/release/mako_anomaly_detector.wasm anomaly.wasm
```

## Prerequisites

- **Vault** running locally with secrets:
  - `secret/data/mako/snowflake` (account, user, password, warehouse, role, database)
  - `secret/data/mako/gcs` (hmac_access_id, hmac_secret, bucket, project)
- **Snowflake** account with database `ANALYTICS` and schema `DW`
- **GCS bucket** for staging Parquet files

## Quick Start

```bash
# Set Vault environment
export VAULT_ADDR=http://localhost:8200
export VAULT_TOKEN=<your-token>

# Run the full workflow (DAG orchestration)
mako workflow examples/nyc-tlc-pipeline/workflow.yaml

# Or run individual pipelines
mako run examples/nyc-tlc-pipeline/pipeline-dim-location.yaml
mako run examples/nyc-tlc-pipeline/pipeline-fact-trips.yaml

# Launch the dashboard
pip install streamlit snowflake-connector-python plotly pandas
streamlit run examples/nyc-tlc-pipeline/dashboard.py
```

## Dashboard Sections

1. **Vue d'ensemble** - KPIs, daily revenue/trips, weekend vs weekday
2. **Analyse Temporelle** - Hourly patterns, rush hours, time periods
3. **Analyse Geographique** - Top zones, origin-destination matrix, service zones
4. **Pourboires** - Tip analysis by payment type
5. **Categories de Trajet** - Short/medium/long haul breakdown
6. **Vendeurs** - Vendor performance comparison
7. **Surcharges & Frais** - Congestion, airport, MTA fees
8. **Tarification** - Rate code analysis (standard, JFK, negotiated)
9. **Anomalies (WASM)** - Score distribution, top anomalous trips

## Mako Features Demonstrated

- **DuckDB source** with GCS Parquet via HMAC auth
- **Snowflake sink** with `flatten: true` (auto-creates typed columns)
- **Vault integration** for secrets management
- **Data quality checks** (`dq_check` transform)
- **WASM plugins** (Rust anomaly detector)
- **Workflow DAG** with dependencies and parallel execution
