# Filter Transform Examples

Pipelines that use the `filter` transform to keep or discard events based on conditions.

## Pipelines

| Pipeline | Condition | Description |
|----------|-----------|-------------|
| [pipeline-filter-example.yaml](pipeline-filter-example.yaml) | Various | Filter events by field values |

## Key Config Options

```yaml
transforms:
  - name: prod_only
    type: filter
    condition: "environment = production"
```

## Supported Operators

- `=` -- Equality: `status = active`
- `!=` -- Not equal: `status != test`
- `>`, `<`, `>=`, `<=` -- Numeric comparison: `amount > 100`
- Conditions work on string and numeric fields

## Run

```bash
mako validate examples/transforms/filter/pipeline-filter-example.yaml
mako run examples/transforms/filter/pipeline-filter-example.yaml
```
