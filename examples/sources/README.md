# Sources

Data sources supported by Mako. Each subfolder contains example pipelines for a specific source type.

| Source | Type | Description | Examples |
|--------|------|-------------|----------|
| [HTTP / REST API](http/) | `http` | REST APIs with pagination, auth (None, Bearer, Basic, API Key, OAuth2) | 8 |
| [File](file/) | `file` | Local or remote files (JSON, CSV, Parquet, gzip) | 5 |
| [Kafka](kafka/) | `kafka` | Apache Kafka consumer (franz-go) | 2 |
| [PostgreSQL CDC](postgres-cdc/) | `postgres_cdc` | Change Data Capture (snapshot, CDC, snapshot+CDC) | 1 |
| [DuckDB](duckdb/) | `duckdb` | Embedded SQL queries, native Parquet/CSV/JSON reading | 3 |

See [docs/sources.md](../../docs/sources.md) for full documentation.
