# PostgreSQL CDC Source Examples

Pipelines that read data from PostgreSQL using Change Data Capture.

## Pipelines

| Pipeline | Mode | Tables | Sink | Description |
|----------|------|--------|------|-------------|
| [pipeline-postgres-to-gcs.yaml](pipeline-postgres-to-gcs.yaml) | snapshot | beer_wasm_rust | GCS (Parquet) | Snapshot PostgreSQL table to GCS |

## CDC Modes

- **snapshot** -- Full table scan (ordered batch reads)
- **cdc** -- Logical replication (real-time WAL streaming)
- **snapshot+cdc** -- Initial snapshot followed by continuous CDC

## Key Config Options

```yaml
source:
  type: postgres_cdc
  config:
    host: localhost
    port: "5432"
    user: mako
    password: ${PG_PASSWORD}
    database: mako
    schema: public
    tables: [my_table]
    mode: snapshot                # snapshot | cdc | snapshot+cdc
    snapshot_batch_size: 500
    snapshot_order_by: created_at
```

## Prerequisites

- PostgreSQL with logical replication enabled (for CDC mode)
- User with `REPLICATION` privilege (for CDC mode)

## Run

```bash
mako validate examples/sources/postgres-cdc/pipeline-postgres-to-gcs.yaml
mako run examples/sources/postgres-cdc/pipeline-postgres-to-gcs.yaml
```
