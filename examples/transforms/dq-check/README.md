# Data Quality Check Examples

Pipelines that use the `dq_check` transform for row-level data quality validation.

## Pipelines

| Pipeline | Checks | On Failure | Description |
|----------|--------|------------|-------------|
| [pipeline-tlc-to-gcs-dq.yaml](pipeline-tlc-to-gcs-dq.yaml) | not_null, range | drop | NYC TLC taxi data quality shield before GCS write |
| [pipeline-fact-trips-dq.yaml](pipeline-fact-trips-dq.yaml) | not_null, range | drop | Quality checks for star schema fact table |

## Supported Rules

| Rule | Parameters | Description |
|------|------------|-------------|
| `not_null` | - | Field must not be null or empty |
| `range` | `min`, `max` | Numeric value within bounds |
| `in_set` | `values` | Value must be in allowed set |
| `regex` | `pattern` | Value must match regex pattern |
| `type` | `expected` | Value must be of expected type |

## Key Config Options

```yaml
transforms:
  - name: quality-gate
    type: dq_check
    on_failure: drop              # drop | log | reject
    checks:
      - column: VendorID
        rule: not_null
      - column: trip_distance
        rule: range
        min: 0
      - column: passenger_count
        rule: range
        min: 0
        max: 9
```

## Run

```bash
mako validate examples/transforms/dq-check/pipeline-tlc-to-gcs-dq.yaml
mako run examples/transforms/dq-check/pipeline-tlc-to-gcs-dq.yaml
```
