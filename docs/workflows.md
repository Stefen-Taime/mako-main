# Workflows

Workflows orchestrate multiple pipelines as a **DAG (Directed Acyclic Graph)**. Independent steps run in parallel, and dependent steps wait for their dependencies to complete before starting.

## Quick start

```bash
mako workflow workflow.yaml
```

## YAML spec

```yaml
apiVersion: mako/v1
kind: Workflow

workflow:
  name: multi-sink-ingest
  description: "Ingest data through multiple sources and sinks"
  owner: data-team
  onFailure: stop              # stop | continue | retry (default: stop)
  maxRetries: 2                # only used when onFailure: retry

  steps:
    - name: ingest-users
      pipeline: pipeline-users.yaml

    - name: users-to-postgres
      pipeline: pipeline-users-to-postgres.yaml
      depends_on: [ingest-users]

    - name: ingest-restaurants
      pipeline: pipeline-restaurants.yaml
```

Pipeline file paths are resolved **relative to the workflow YAML directory**.

## DAG execution

The engine resolves the dependency graph and executes steps in topological order:

```text
  ingest-users ──────────> users-to-postgres
       │
       │ (parallel)
       │
  ingest-restaurants
```

- Steps with no dependencies start immediately (in parallel)
- Steps with `depends_on` wait for all dependencies to complete
- Each step runs in its own goroutine

## Step configuration

### Pipeline steps (default)

| Field | Required | Description |
|---|---|---|
| `name` | yes | Unique step identifier |
| `pipeline` | yes | Path to pipeline YAML (relative to workflow directory) |
| `depends_on` | no | List of step names that must complete before this step starts |

### Quality gate steps

Quality gates run SQL assertions against a DuckDB database to validate data after ingestion. Use them to enforce data contracts between pipeline stages.

| Field | Required | Description |
|---|---|---|
| `name` | yes | Unique step identifier |
| `type` | yes | Must be `quality_gate` |
| `database` | yes | Path to DuckDB database file |
| `depends_on` | no | List of step names that must complete before this step starts |
| `checks` | yes | List of SQL assertions to run |
| `checks[].name` | no | Human-readable check name (auto-generated if omitted) |
| `checks[].sql` | yes | SQL query returning a single numeric value |
| `checks[].expect` | yes | Assertion expression (see below) |

**Assertion expressions:**

| Expression | Example | Description |
|---|---|---|
| `= N` / `== N` | `= 0` | Value must equal N |
| `!= N` | `!= 0` | Value must not equal N |
| `> N` / `>= N` | `>= 100` | Value must be greater than (or equal to) N |
| `< N` / `<= N` | `< 10` | Value must be less than (or equal to) N |
| `BETWEEN N AND M` | `BETWEEN 50 AND 500` | Value must be in range [N, M] |

**Example:**

```yaml
workflow:
  name: ingest-with-quality
  onFailure: stop

  steps:
    - name: ingest-users
      pipeline: pipeline-users.yaml

    - name: validate-users
      type: quality_gate
      database: ./output/users.duckdb
      depends_on: [ingest-users]
      checks:
        - name: row_count
          sql: "SELECT count(*) FROM users"
          expect: ">= 100"

        - name: no_null_emails
          sql: "SELECT count(*) FROM users WHERE email IS NULL"
          expect: "= 0"

        - name: unique_ids
          sql: "SELECT count(*) - count(DISTINCT id) FROM users"
          expect: "= 0"

        - name: avg_age_reasonable
          sql: "SELECT avg(age) FROM users"
          expect: "BETWEEN 18 AND 80"

    - name: export-to-postgres
      pipeline: pipeline-users-to-postgres.yaml
      depends_on: [validate-users]
```

This workflow ingests data, validates it with SQL assertions, and only exports to Postgres if all quality checks pass.

## Failure policies

| Policy | Description |
|---|---|
| `stop` (default) | Stop the entire workflow when any step fails. Dependent steps are skipped. |
| `continue` | Continue running other steps even if one fails. Report failures at the end. |
| `retry` | Retry failed steps up to `maxRetries` times before giving up. |

```yaml
workflow:
  name: resilient-ingest
  onFailure: retry
  maxRetries: 3
  steps:
    - name: fetch-data
      pipeline: fetch.yaml
    - name: transform-data
      pipeline: transform.yaml
      depends_on: [fetch-data]
```

## Validation

`mako validate` auto-detects workflow files (via `kind: Workflow`) and validates:

- All step names are unique
- All `depends_on` references point to existing steps
- Pipeline files exist (resolved relative to workflow directory)
- No circular dependencies (DAG cycle detection via DFS)

```bash
mako validate workflow.yaml
```

## Output

The workflow engine prints real-time progress with status indicators:

```text
[workflow] multi-sink-ingest: 3 steps, 0 max-depth
[workflow] starting step: ingest-users
[workflow] starting step: ingest-restaurants
[workflow] step completed: ingest-restaurants (45.2s)
[workflow] step completed: ingest-users (52.1s)
[workflow] starting step: users-to-postgres
[workflow] step completed: users-to-postgres (12.3s)

========================================
  Workflow: multi-sink-ingest
  Status:   completed
  Steps:    3/3 completed, 0 failed
  Duration: 64.4s
========================================
```

## Graceful shutdown

Pressing `Ctrl+C` (SIGINT/SIGTERM) cancels all running steps via context cancellation. Each pipeline respects the cancellation and shuts down gracefully (flushing sinks, committing offsets, etc.).

## Example: multi-sink data pipeline

See `examples/workflow-demo/` for a complete working example:

```yaml
# workflow.yaml
apiVersion: mako/v1
kind: Workflow

workflow:
  name: multi-sink-ingest
  onFailure: stop

  steps:
    # Step 1: HTTP source -> DuckDB + Parquet export
    - name: ingest-users
      pipeline: pipeline-users.yaml

    # Step 2: DuckDB/Parquet source -> PostgreSQL (waits for step 1)
    - name: users-to-postgres
      pipeline: pipeline-users-to-postgres.yaml
      depends_on: [ingest-users]

    # Step 3: CSV source -> DuckDB + Parquet export (parallel with step 1)
    - name: ingest-restaurants
      pipeline: pipeline-restaurants.yaml
```

This workflow demonstrates:
- **Diverse sources**: HTTP API, DuckDB (Parquet), CSV file
- **Diverse sinks**: DuckDB (with Parquet export), PostgreSQL (flatten mode)
- **Parallel execution**: steps 1 and 3 run concurrently
- **Dependencies**: step 2 waits for step 1 to finish
