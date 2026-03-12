# Mako Examples

Complete catalog of example pipelines organized by category.

## Directory Structure

```text
examples/
  sources/          Data ingestion (where data comes from)
    http/           REST APIs with auth (Bearer, Basic, API Key, OAuth2)
    file/           Local/remote files (JSON, CSV, Parquet, gzip)
    kafka/          Apache Kafka consumer
    postgres-cdc/   PostgreSQL Change Data Capture
    duckdb/         DuckDB embedded queries + cloud storage

  sinks/            Data destinations (where data goes)
    postgres/       PostgreSQL with auto-flatten + Vault
    snowflake/      Snowflake data warehouse
    duckdb/         DuckDB embedded + Parquet export
    gcs/            Google Cloud Storage (Parquet, CSV)
    kafka/          Kafka producer + Schema Registry
    stdout/         Console output (debugging)

  transforms/       Data transformations (how data is processed)
    sql/            SQL enrichment (CASE WHEN, computed fields)
    wasm/           WebAssembly plugins (Go, Rust)
    schema/         Schema Registry validation
    dq-check/       Data quality checks (not_null, range, regex)
    pii/            PII hashing and masking
    filter/         Conditional filtering

  workflows/        DAG orchestration (multi-pipeline jobs)
    nyc-tlc-star-schema/   Star schema from NYC taxi data (9 steps + quality gate)
    etl-demo/              Simple sequential ETL
    multi-source-demo/     Parallel ingestion from multiple sources

  nyc-tlc-pipeline/   Complete end-to-end example (see below)
```

## Featured Example: NYC TLC Analytics Pipeline

The [`nyc-tlc-pipeline/`](nyc-tlc-pipeline/) directory contains a complete, production-style data pipeline:

- **15 pipelines** orchestrated as a DAG workflow
- **Star schema** in Snowflake (6 dimensions + fact + aggregation)
- **WASM Rust plugin** for real-time anomaly detection
- **Streamlit dashboard** with 9 analysis sections
- **Vault** for secrets, **GCS** for staging, **DuckDB** for transforms

See [`nyc-tlc-pipeline/README.md`](nyc-tlc-pipeline/README.md) for full documentation.

## Quick Start

```bash
# Validate a pipeline
mako validate examples/sources/http/pipeline-beer-noauth.yaml

# Run a single pipeline
mako run examples/sources/http/pipeline-beer-noauth.yaml

# Run a workflow (DAG of pipelines)
mako workflow examples/workflows/nyc-tlc-star-schema/workflow.yaml
```

## By Connector

A connector may appear as both a source and a sink:

| Connector | As Source | As Sink |
|-----------|----------|---------|
| PostgreSQL | [postgres-cdc/](sources/postgres-cdc/) | [postgres/](sinks/postgres/) |
| DuckDB | [duckdb/](sources/duckdb/) | [duckdb/](sinks/duckdb/) |
| Kafka | [kafka/](sources/kafka/) | [kafka/](sinks/kafka/) |
| GCS | - | [gcs/](sinks/gcs/) |
| Snowflake | - | [snowflake/](sinks/snowflake/) |
| HTTP / REST API | [http/](sources/http/) | - |
| File | [file/](sources/file/) | - |
