# Sinks

## PostgreSQL (pgx)

High-performance sink using [pgx](https://github.com/jackc/pgx) with connection pooling and COPY protocol.

```yaml
sink:
  type: postgres
  database: mako
  schema: analytics
  table: pipeline_events
  config:
    host: localhost
    port: "5432"
    user: mako
    password: mako
    # Or use a full DSN:
    # dsn: postgres://mako:mako@localhost:5432/mako
```

Features: `pgxpool` connection pool, bulk `COPY FROM` with batch INSERT fallback, automatic JSONB serialization.

### Flatten mode

With `flatten: true`, each top-level key becomes its own typed column instead of being stored in a single JSONB column. The table schema is auto-detected from the first batch.

```yaml
sink:
  type: postgres
  database: mako
  schema: analytics
  table: users
  flatten: true
  config:
    host: localhost
    port: "5432"
    user: mako
    password: mako
```

Type mapping:

| Go type | PostgreSQL type |
|---|---|
| `string` | `TEXT` |
| `float64` | `NUMERIC` |
| `bool` | `BOOLEAN` |
| `time.Time`-like string | `TIMESTAMPTZ` |
| `map` / `array` (nested) | `JSONB` |

Schema evolution is handled automatically: new keys trigger `ALTER TABLE ADD COLUMN IF NOT EXISTS`.

## Snowflake (gosnowflake)

Production sink using the official [gosnowflake](https://github.com/snowflakedb/gosnowflake) driver.

```yaml
sink:
  type: snowflake
  database: ANALYTICS
  schema: RAW
  table: ORDER_EVENTS
  config:
    account: xy12345.us-east-1
    user: MAKO_USER
    password: ${SNOWFLAKE_PASSWORD}
    warehouse: COMPUTE_WH
    role: MAKO_ROLE
    # Or use a full DSN:
    # dsn: user:password@account/database/schema?warehouse=WH
```

By default, events are stored as `VARIANT` (JSON) via `PARSE_JSON()`. Target table is auto-created:

```sql
CREATE TABLE ORDER_EVENTS (
    event_data    VARIANT NOT NULL,
    event_key     VARCHAR,
    event_topic   VARCHAR,
    event_offset  NUMBER,
    loaded_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### Flatten mode

With `flatten: true`, each top-level key in the event JSON becomes its own typed column instead of being stored in a single VARIANT column. The table schema is auto-detected from the first batch of events.

```yaml
sink:
  type: snowflake
  database: ANALYTICS
  schema: RAW
  table: USERS
  flatten: true
  config:
    account: xy12345.us-east-1
    user: MAKO_USER
    warehouse: COMPUTE_WH
    role: MAKO_ROLE
```

Type mapping:

| Go type | Snowflake type |
|---|---|
| `string` | `VARCHAR` |
| `float64` | `FLOAT` |
| `bool` | `BOOLEAN` |
| `map` / `array` (nested) | `VARIANT` |

The generated table looks like:

```sql
CREATE TABLE USERS (
    city        VARCHAR,
    email       VARCHAR,
    first_name  VARCHAR,
    popularity  FLOAT,
    address     VARIANT,
    loaded_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

Schema evolution is handled automatically: if new keys appear in later batches, columns are added via `ALTER TABLE ADD COLUMN IF NOT EXISTS`.

Nested objects (like `address`) are inserted using `PARSE_JSON()` and stored as `VARIANT`, so they remain queryable with Snowflake's semi-structured data syntax (`address:city`, `address:zip`).

## BigQuery (streaming inserter)

Streaming sink using `cloud.google.com/go/bigquery` with deduplication.

```yaml
sink:
  type: bigquery
  schema: raw_events        # BigQuery dataset
  table: events
  config:
    project: my-gcp-project
```

Authentication via Application Default Credentials (ADC). Set `GOOGLE_APPLICATION_CREDENTIALS` or run `gcloud auth application-default login`.

Features: streaming inserter with per-row `InsertID` dedup, `PutMultiError` handling, JSON-column alternative mode.

### Flatten mode

With `flatten: true`, each top-level key becomes its own typed column. The dataset table is auto-created from the first batch.

```yaml
sink:
  type: bigquery
  schema: raw_events
  table: users
  flatten: true
  config:
    project: my-gcp-project
```

Type mapping:

| Go type | BigQuery type |
|---|---|
| `string` | `STRING` |
| `float64` | `FLOAT64` |
| `int` / `int64` | `INT64` |
| `bool` | `BOOLEAN` |
| `time.Time`-like string | `TIMESTAMP` |
| `map` / `array` (nested) | `JSON` |

Schema evolution is handled automatically via `TableMetadataToUpdate` when new keys appear.

## S3 (AWS SDK v2)

Object storage sink using [AWS SDK for Go v2](https://github.com/aws/aws-sdk-go-v2). Events are buffered and flushed as time-partitioned objects.

```yaml
sink:
  type: s3
  bucket: my-data-lake
  prefix: raw/events
  format: jsonl                     # jsonl | json | parquet | csv
  config:
    region: us-east-1
    # Optional: explicit credentials (defaults to AWS SDK credential chain)
    access_key_id: AKIA...
    secret_access_key: ...
    # Optional: custom endpoint for MinIO / LocalStack
    endpoint: http://localhost:9000
    # Parquet options
    compression: snappy             # snappy (default) | zstd | gzip | none
    # CSV options
    csv_delimiter: ","              # default: ","
```

Object key pattern: `<prefix>/year=YYYY/month=MM/day=DD/hour=HH/<timestamp>_<count>.<ext>`

Each pipeline batch produces one S3 object. With `batch.size: 5000`, a dataset of 500K events produces ~100 properly partitioned objects instead of a single monolithic file.

Features: Hive-style time partitioning, per-batch object creation, AWS credential chain (IAM, env, config), MinIO/LocalStack compatible via custom endpoint, buffered writes with retry on failure.

### Output formats

| Format | Extension | Content-Type | Description |
|---|---|---|---|
| `jsonl` | `.jsonl` | `application/x-ndjson` | One JSON object per line (default) |
| `json` | `.json` | `application/json` | JSON array of objects |
| `parquet` | `.parquet` | `application/octet-stream` | Columnar format, auto-schema from events. Compression: snappy, zstd, gzip, none |
| `csv` | `.csv` | `text/csv` | RFC 4180 with header row, union-of-keys columns, configurable delimiter |

## GCS (cloud.google.com/go/storage)

Object storage sink using the official [Google Cloud Storage](https://pkg.go.dev/cloud.google.com/go/storage) client.

```yaml
sink:
  type: gcs
  bucket: my-data-lake
  prefix: raw/events
  format: jsonl                     # jsonl | json | parquet | csv
  config:
    project: my-gcp-project
    # Parquet options
    compression: snappy             # snappy (default) | zstd | gzip | none
    # CSV options
    csv_delimiter: ","              # default: ","
```

Authentication via Application Default Credentials (ADC). Set `GOOGLE_APPLICATION_CREDENTIALS` or run `gcloud auth application-default login`.

Object name pattern: `<prefix>/year=YYYY/month=MM/day=DD/hour=HH/<timestamp>_<count>.<ext>`

Each pipeline batch produces one GCS object. With `batch.size: 5000`, a dataset of 500K events produces ~100 properly partitioned objects instead of a single monolithic file.

Features: Hive-style time partitioning, per-batch object creation, ADC authentication, buffered writes with retry on failure. Supports the same four output formats as S3 (jsonl, json, parquet, csv).

## ClickHouse (clickhouse-go v2)

High-performance OLAP sink using the official [clickhouse-go](https://github.com/ClickHouse/clickhouse-go) native protocol driver with LZ4 compression and batch inserts.

```yaml
sink:
  type: clickhouse
  database: analytics
  table: pipeline_events
  config:
    host: localhost
    port: "9000"
    user: default
    password: ""
    secure: "false"          # set "true" for TLS
    # Or use a full DSN:
    # dsn: clickhouse://default:@localhost:9000/analytics
```

Target table schema:

```sql
CREATE TABLE analytics.pipeline_events (
    event_date    Date DEFAULT toDate(event_ts),
    event_ts      DateTime64(3) DEFAULT now64(),
    event_key     String,
    event_data    String,
    topic         String,
    partition_id  UInt32,
    offset_id     UInt64
) ENGINE = MergeTree()
ORDER BY (event_date, event_ts)
PARTITION BY toYYYYMM(event_date);
```

Features: native TCP protocol, LZ4 compression, batch inserts via `PrepareBatch`, automatic date partitioning.

### Flatten mode

With `flatten: true`, each top-level key becomes its own typed column. The table is auto-created with a `MergeTree()` engine.

```yaml
sink:
  type: clickhouse
  database: analytics
  table: users
  flatten: true
  config:
    host: localhost
    port: "9000"
    user: default
```

Type mapping:

| Go type | ClickHouse type |
|---|---|
| `string` | `String` |
| `float64` | `Float64` |
| `int` / `int64` | `Int64` |
| `bool` | `Bool` |
| `time.Time`-like string | `DateTime64(3)` |

Schema evolution is handled automatically via `ALTER TABLE ADD COLUMN IF NOT EXISTS`.

## DuckDB (embedded)

Embedded analytical sink using [go-duckdb](https://github.com/marcboeker/go-duckdb) (CGO, embeds DuckDB). Supports auto-table creation with typed columns, schema evolution, and native export to Parquet/CSV/JSON via DuckDB's `COPY` command.

```yaml
sink:
  type: duckdb
  database: /data/output.duckdb    # ":memory:" by default
  table: events
  config:
    create_table: true              # auto-create table (default: true)
```

### Configuration

| Key | Default | Description |
|---|---|---|
| `database` (YAML field) | — | DuckDB database path (also accepts `config.database`) |
| `table` (YAML field) | — | Target table name (required) |
| `create_table` | `true` | Auto-create table from first batch with typed columns |
| `export_path` | — | Path to export data at pipeline close (local, `s3://`, `gs://`) |
| `export_format` | `parquet` | Export format: `parquet`, `csv`, `json` |
| `export_partition_by` | — | List of columns for Hive-style partitioned export |

### Auto-table creation

With `create_table: true` (default), the table is created from the first batch of events. Each top-level key becomes a typed column:

| Go type | DuckDB type |
|---|---|
| `string` | `VARCHAR` |
| `float64` | `DOUBLE` |
| `int` / `int64` | `BIGINT` |
| `bool` | `BOOLEAN` |
| `time.Time`-like string | `TIMESTAMP` |
| `map` / `array` (nested) | `JSON` |

A `loaded_at TIMESTAMP DEFAULT current_timestamp` column is added automatically.

Schema evolution is handled automatically: new keys in later batches trigger `ALTER TABLE ADD COLUMN IF NOT EXISTS`.

### Export at close

When `export_path` is configured, the sink runs a `COPY <table> TO '<path>'` command at pipeline close. This is useful for ETL patterns where you load into an in-memory DuckDB, then export the result.

```yaml
sink:
  type: duckdb
  database: ":memory:"
  table: export_staging
  config:
    create_table: true
    export_path: /data/output/events/
    export_format: parquet
    export_partition_by: [year, month]
```

The generated SQL:
```sql
COPY "export_staging" TO '/data/output/events/' (FORMAT PARQUET, PARTITION_BY (year, month))
```

### Cloud export (S3 / GCS / Azure)

For remote export paths (`s3://`, `gs://`, `az://`), the `httpfs` extension is loaded automatically at `Open()` and cloud credentials are configured from the YAML config or environment variables.

```yaml
sink:
  type: duckdb
  database: ":memory:"
  table: staging
  config:
    create_table: true
    export_path: s3://my-bucket/output/events.parquet
    export_format: parquet

    # S3 credentials (or use AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env vars)
    s3_access_key_id: AKIAIOSFODNN7EXAMPLE
    s3_secret_access_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
    s3_region: eu-west-1
```

| Config key | Env var | Description |
|---|---|---|
| `s3_access_key_id` | `AWS_ACCESS_KEY_ID` | S3 access key |
| `s3_secret_access_key` | `AWS_SECRET_ACCESS_KEY` | S3 secret key |
| `s3_region` | `AWS_REGION` | S3 region (default: `us-east-1`) |
| `s3_endpoint` | `AWS_ENDPOINT_URL` | Custom S3 endpoint (MinIO, LocalStack) |
| `gcs_service_account_key` | `GOOGLE_APPLICATION_CREDENTIALS` | Path to GCS service account JSON key |
| `azure_account_name` | `AZURE_STORAGE_ACCOUNT` | Azure storage account name |
| `azure_account_key` | `AZURE_STORAGE_KEY` | Azure storage account key |
| `azure_connection_string` | `AZURE_STORAGE_CONNECTION_STRING` | Azure full connection string |

### Example: DuckDB query to partitioned Parquet

```yaml
pipeline:
  name: duckdb-export-parquet
  source:
    type: duckdb
    config:
      database: /data/analytics.duckdb
      query: "SELECT *, year(created_at) as year, month(created_at) as month FROM events"
  sink:
    type: duckdb
    database: ":memory:"
    table: export_staging
    config:
      create_table: true
      export_path: /data/output/events/
      export_format: parquet
      export_partition_by: [year, month]
```

## Kafka (franz-go)

Sink using [franz-go](https://github.com/twmb/franz-go) (pure Go, zero CGO) to produce events to a Kafka topic. Uses the same producer implementation as the DLQ sink.

```yaml
sink:
  type: kafka
  topic: enriched-orders
  config:
    brokers: localhost:9092
```

Or as a secondary sink in multi-sink mode:

```yaml
sinks:
  - name: kafka-output
    type: kafka
    topic: events.enriched
    config:
      brokers: ${KAFKA_BROKERS:-localhost:9092}
```

Features:
- Async produce with `AllISRAcks` for durability
- 1MB max batch size, 100ms linger for throughput
- Headers propagated from source events
- Automatic flush on pipeline shutdown

### Configuration

| Key | Default | Description |
|---|---|---|
| `topic` (YAML field) | -- | Target Kafka topic (required) |
| `brokers` (in `config`) | `localhost:9092` | Comma-separated broker addresses |

## Secret Resolution (Vault)

All sinks resolve credentials through a 4-step chain:

1. **YAML config map** — value from `config:` in the pipeline YAML
2. **Environment variable** — e.g., `SNOWFLAKE_PASSWORD`, `POSTGRES_PASSWORD`
3. **HashiCorp Vault** — if `VAULT_ADDR` is set (optional)
4. **Default value** — hardcoded fallback

This is completely backwards compatible. If Vault is not configured, the chain is just config -> env -> default.

### Vault configuration

```yaml
pipeline:
  name: order-events
  vault:
    path: secret/data/mako         # Vault KV path prefix
    ttl: 5m                        # cache TTL (default: 5m)
  sink:
    type: snowflake
    config:
      account: xy12345.us-east-1
      user: MAKO_USER
      vault_path: secret/data/mako/snowflake   # per-sink Vault path
```

Authentication via standard Vault environment variables:

| Method | Environment variables |
|---|---|
| Token | `VAULT_ADDR` + `VAULT_TOKEN` |
| AppRole | `VAULT_ADDR` + `VAULT_ROLE_ID` + `VAULT_SECRET_ID` |
| Kubernetes | `VAULT_ADDR` + `VAULT_K8S_ROLE` |

Supports KV v1 and v2 engines with automatic detection. Secrets are cached in-memory with configurable TTL.

## Multi-Sink

Send events to multiple destinations simultaneously:

```yaml
sink:
  type: snowflake
  database: ANALYTICS
  schema: RAW
  table: ORDER_EVENTS

sinks:
  - name: bigquery-mirror
    type: bigquery
    schema: raw_events
    table: events
    config:
      project: my-project

  - name: downstream
    type: kafka
    topic: events.orders.enriched
```

**All sinks:** stdout, file, PostgreSQL, Snowflake, BigQuery, ClickHouse, DuckDB, S3, GCS, Kafka.
