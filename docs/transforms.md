# Transforms

Write YAML, not code. Each transform is a pure function chained in sequence.

## Data Governance

```yaml
transforms:
  # SHA-256 hash with salt (PII compliance)
  - name: pii_hash
    type: hash_fields
    fields: [email, phone, ssn, credit_card_number]

  # Partial masking: john@email.com -> j***@email.com
  - name: mask_display
    type: mask_fields
    fields: [email_display, card_display]

  # Remove internal/debug fields
  - name: cleanup
    type: drop_fields
    fields: [internal_trace_id, debug_payload]
```

## Filtering & Enrichment

```yaml
transforms:
  # SQL WHERE-style filter (supports AND, OR, IS NULL, IS NOT NULL)
  - name: prod_only
    type: filter
    condition: "environment = production AND status != test"

  # Rename fields for warehouse convention
  - name: standardize
    type: rename_fields
    mapping:
      amt: amount
      ts: event_timestamp
      cust_id: customer_id

  # Deduplication by key
  - name: dedupe
    type: deduplicate
    fields: [event_id]
```

## SQL Transform

Row-level SQL projections with support for field references, arithmetic expressions, `CASE WHEN` blocks, and aliasing.

```yaml
transforms:
  - name: reshape
    type: sql
    query: >
      SELECT
        *,
        amount * 1.2 AS amount_with_tax,
        CASE WHEN status = 'active' THEN 'yes' ELSE 'no' END AS is_active
      FROM events
```

Supported features:
- `SELECT *` — pass through all fields
- `field AS alias` — rename on the fly
- Arithmetic expressions (`amount * 1.2`, `price + tax`)
- `CASE WHEN condition THEN value ELSE value END`
- `FROM` clause is optional (ignored — operates on the current event)

## Type Casting

Cast field values to a target type.

```yaml
transforms:
  - name: fix_types
    type: cast_fields
    mapping:
      age: int
      price: float
      active: bool
      name: string
```

| Target type | Aliases | Description |
|---|---|---|
| `string` | — | Convert to string |
| `int` | `integer` | Parse as 64-bit integer |
| `float` | `double` | Parse as 64-bit float |
| `bool` | `boolean` | `"true"` / `"1"` → true, everything else → false |

## Flatten

Flatten nested maps into dot-notation keys. Useful before writing to flat sinks (PostgreSQL, Snowflake).

```yaml
transforms:
  - name: flatten_nested
    type: flatten
```

**Example:**

Input: `{"user": {"name": "John", "address": {"city": "Paris"}}}`

Output: `{"user.name": "John", "user.address.city": "Paris"}`

## Default Values

Fill null or missing fields with default values.

```yaml
transforms:
  - name: fill_defaults
    type: default_values
    config:
      status: "unknown"
      priority: 0
      region: "us-east-1"
```

Only fills fields that are absent from the event — existing values (including empty strings) are preserved.

## Aggregate

Window-based aggregation (handled at the pipeline level). Declare the transform to enable windowed processing.

```yaml
transforms:
  - name: window_agg
    type: aggregate
```

> **Note:** Aggregation configuration (window size, grouping, functions) is set at the pipeline level. The transform declaration enables the aggregation pass-through.

## WASM Plugins

Extend Mako with custom transforms written in any language that compiles to WebAssembly. Powered by [wazero](https://github.com/tetratelabs/wazero) (pure Go, zero CGO).

```yaml
transforms:
  - name: custom_enrich
    type: plugin
    config:
      path: ./plugins/enrich.wasm
      function: transform     # optional, default "transform"
```

Your WASM module must export three functions:

| Export | Signature | Description |
|--------|-----------|-------------|
| `alloc` | `(size: u32) -> ptr: u32` | Allocate memory for input |
| `dealloc` | `(ptr: u32, size: u32)` | Free memory (can be no-op) |
| `transform` | `(ptr: u32, len: u32) -> u64` | Process event, return `(result_ptr << 32 \| result_len)`. Return `0` to drop. |

Events are passed as JSON. WASM plugins can be chained with built-in transforms in any order.

### Supported languages

| Language | Status | Notes |
|---|---|---|
| **Rust** | Recommended | `cdylib` crate, no `_start` — works out of the box |
| **TinyGo** | Supported | Benign `_start`, works when TinyGo version is compatible |
| **Go standard** | Not supported | `_start` exits the module after `main()` returns — architectural limitation |

> **Note:** Mako skips `_start` when loading plugins (`WithStartFunctions()` in wazero). Plugins are libraries, not programs. Go standard modules require `_start` for runtime initialization, which then exits the module — making them incompatible.

### Rust example (recommended)

```bash
cd examples/wasm-plugin-rust
rustup target add wasm32-wasip1
cargo build --target wasm32-wasip1 --release
cp target/wasm32-wasip1/release/mako_plugin_rust.wasm plugin.wasm
```

```yaml
transforms:
  - name: rust_enrich
    type: plugin
    config:
      path: ./examples/wasm-plugin-rust/plugin.wasm
```

See `examples/wasm-plugin-rust/` for the full source (adds `_enriched: true` and `_plugin_lang: "rust"` to every event).

### TinyGo example

```bash
cd examples/wasm-plugin
tinygo build -o plugin.wasm -target=wasi -no-debug main.go
```

See `examples/wasm-plugin/` for the full source (adds `_enriched: true` and `_processed_at` timestamp).

## Data Quality (dq_check)

Inline row-level data quality checks. Each event is validated against a set of rules; the action on failure is configurable.

```yaml
transforms:
  - name: quality_checks
    type: dq_check
    on_failure: tag          # tag (default) | drop | fail
    checks:
      - column: email
        rule: not_null

      - column: age
        rule: range
        min: 0
        max: 150

      - column: status
        rule: in_set
        values: [active, inactive, pending]

      - column: email
        rule: regex
        pattern: "^[^@]+@[^@]+\\.[^@]+$"

      - column: name
        rule: min_length
        length: 2

      - column: bio
        rule: max_length
        length: 500

      - column: amount
        rule: type
        type_expected: number
```

### Supported rules

| Rule | Parameters | Description |
|---|---|---|
| `not_null` | — | Field must exist and not be `""` or `nil` |
| `range` | `min`, `max` | Numeric value must be within `[min, max]` |
| `in_set` | `values` | Value must be one of the allowed values |
| `regex` | `pattern` | String must match the regular expression |
| `min_length` | `length` | String length must be >= `length` |
| `max_length` | `length` | String length must be <= `length` |
| `type` | `type_expected` | Value must be of type: `string`, `number`, `bool` |

### Failure modes

| Mode | Behavior |
|---|---|
| `tag` (default) | Event passes through with `_dq_valid: false` and `_dq_errors: [...]` fields added |
| `drop` | Event is silently dropped from the pipeline |
| `fail` | Event is routed to the DLQ (Dead Letter Queue) with error details |

### Example: tag mode output

Input:
```json
{"email": "", "age": 200, "status": "unknown"}
```

Output (with `on_failure: tag`):
```json
{
  "email": "",
  "age": 200,
  "status": "unknown",
  "_dq_valid": false,
  "_dq_errors": [
    "email: not_null check failed",
    "age: range check failed (value 200 not in [0, 150])",
    "status: in_set check failed (value \"unknown\" not in [active inactive pending])"
  ]
}
```

Downstream transforms or the sink can filter on `_dq_valid` to separate clean from dirty data.
