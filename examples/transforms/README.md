# Transforms

Data transformations available in Mako. Each subfolder contains examples of a specific transform category.

| Transform | Type(s) | Description | Examples |
|-----------|---------|-------------|----------|
| [SQL](sql/) | `sql` | In-memory SQL enrichment (CASE WHEN, computed fields, DuckDB functions) | 2 |
| [WASM Plugins](wasm/) | `plugin` | Custom logic via WebAssembly (Go, Rust) | 2 + source code |
| [Schema Validation](schema/) | `schema` | Confluent Schema Registry enforcement (log, reject, DLQ) | 3 |
| [Data Quality](dq-check/) | `dq_check` | Row-level quality checks (not_null, range, in_set, regex, type) | 2 |
| [PII Masking](pii/) | `hash_fields`, `mask_fields` | Hash or mask personally identifiable information | 2 |
| [Filter](filter/) | `filter` | Keep/discard events by condition | 1 |

Additional built-in transforms (used across examples): `rename_fields`, `drop_fields`, `cast_fields`, `flatten`, `default_values`, `deduplicate`.

See [docs/transforms.md](../../docs/transforms.md) for full documentation.
