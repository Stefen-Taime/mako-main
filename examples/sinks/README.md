# Sinks

Data sinks (destinations) supported by Mako. Each subfolder contains example pipelines for a specific sink type.

| Sink | Type | Description | Examples |
|------|------|-------------|----------|
| [PostgreSQL](postgres/) | `postgres` | PostgreSQL with auto-flatten, COPY protocol, Vault integration | 2 |
| [Snowflake](snowflake/) | `snowflake` | Snowflake data warehouse with auto-DDL and flatten mode | 2 |
| [DuckDB](duckdb/) | `duckdb` | Embedded DuckDB with auto-table, schema evolution, Parquet export | 2 |
| [GCS](gcs/) | `gcs` | Google Cloud Storage (Parquet, CSV) with Snappy compression | 3 |
| [Kafka](kafka/) | `kafka` | Kafka producer with Schema Registry validation and DLQ | 1 |
| [Stdout](stdout/) | `stdout` | Console output for debugging and development | 1 |

Additional sinks available (not yet in examples): BigQuery, ClickHouse, S3.

See [docs/sinks.md](../../docs/sinks.md) for full documentation.
