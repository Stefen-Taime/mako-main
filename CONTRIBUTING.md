# Contributing to Mako

Thanks for your interest in contributing to Mako! This guide explains how to report issues, suggest features, and submit pull requests.

## Getting Started

```bash
# Fork and clone
git clone https://github.com/<your-username>/mako.git
cd mako

# Build
go build -o bin/mako .

# Generate a starter pipeline (stdout, zero dependencies)
./bin/mako init

# Or generate a full reference template with all connectors
./bin/mako init --full pipeline-full.yaml

# Run tests
go test -v -race ./...

# Validate an example pipeline
./bin/mako validate examples/sources/kafka/pipeline-order-events.yaml
```

**Requirements:** Go 1.23+, Docker (for local infra and integration tests).

## Reporting Issues

Use [GitHub Issues](https://github.com/Stefen-Taime/mako/issues) to report bugs or request features. There are templates to help you provide the right information:

- **Bug report** -- something broken or unexpected
- **Feature request** -- a new capability or improvement

Search existing issues before opening a new one to avoid duplicates.

## Pull Request Process

### 1. Create a branch

```bash
git checkout -b feat/my-feature    # or fix/my-bug, docs/my-change
```

Branch naming conventions:

| Prefix | Use for |
|---|---|
| `feat/` | New features |
| `fix/` | Bug fixes |
| `docs/` | Documentation changes |
| `refactor/` | Code restructuring (no behavior change) |
| `test/` | Adding or updating tests |

### 2. Make your changes

- Follow the existing code style (gofmt, no unused imports)
- Add tests for new functionality
- Update documentation if you add or change user-facing behavior
- Keep PRs focused -- one feature or fix per PR

### 3. Test locally

```bash
# Unit tests (required to pass)
go test -v -race ./...

# Build check
go build -o bin/mako .

# Lint (if you have golangci-lint installed)
golangci-lint run ./...

# Validate examples still work
./bin/mako validate examples/sources/kafka/pipeline-order-events.yaml
./bin/mako validate examples/sources/kafka/pipeline-payment-features.yaml
```

### 4. Commit

Use [Conventional Commits](https://www.conventionalcommits.org/) format:

```
feat: add Redis sink
fix: handle nil pointer in HTTP source pagination
docs: add Redis sink documentation
refactor: extract shared encoding logic
test: add BigQuery flatten mode tests
```

- Keep the subject line under 72 characters
- Use the imperative mood ("add" not "added")
- Add a body if the change needs explanation

### 5. Open a Pull Request

- Push your branch and open a PR against `main`
- Fill in the PR template (summary, test plan)
- CI will run automatically (unit tests, build, integration tests)
- All checks must pass before merge

## Project Structure

```
mako/
├── api/v1/types.go          # Pipeline YAML spec (the DSL model)
├── main.go                  # CLI entry point
├── main_test.go             # All tests
├── pkg/
│   ├── source/              # Sources: file, postgres_cdc, http, duckdb
│   ├── sink/                # Sinks: postgres, snowflake, bigquery, clickhouse, duckdb, s3, gcs
│   ├── pipeline/            # Runtime engine
│   ├── transform/           # Transform implementations + WASM
│   ├── kafka/               # Kafka source + sink
│   ├── vault/               # HashiCorp Vault client
│   ├── schema/              # Schema Registry
│   └── observability/       # Prometheus metrics + health
├── docker/                  # Local infra (docker-compose)
├── docs/                    # Documentation
├── examples/                # Pipeline catalog
│   ├── sources/             # HTTP, File, Kafka, PostgreSQL CDC, DuckDB
│   ├── sinks/               # PostgreSQL, Snowflake, DuckDB, GCS, Kafka, Stdout
│   ├── transforms/          # SQL, WASM, Schema, DQ Check, PII, Filter
│   └── workflows/           # NYC TLC Star Schema, ETL Demo, Multi-Source
```

## Adding a New Source

1. Create `pkg/source/<name>.go` implementing the `pipeline.Source` interface:
   ```go
   type Source interface {
       Open(ctx context.Context) error
       Read(ctx context.Context) (<-chan *pipeline.Event, error)
       Close() error
       Lag() int64
   }
   ```
2. Wire it in `main.go` in the `cmdRun` function's source switch
3. Add the source type constant in `api/v1/types.go`
4. Add tests in `main_test.go`
5. Document in `docs/sources.md` and update `README.md`

## Adding a New Sink

1. Create `pkg/sink/<name>.go` implementing the `pipeline.Sink` interface:
   ```go
   type Sink interface {
       Open(ctx context.Context) error
       Write(ctx context.Context, events []*pipeline.Event) error
       Flush(ctx context.Context) error
       Close() error
       Name() string
   }
   ```
2. Add a constructor call in `pkg/sink/sink.go` `BuildFromSpec()`
3. Add tests in `main_test.go`
4. Document in `docs/sinks.md` and update `README.md`

## Adding a New Transform

1. Add the transform logic in `pkg/transform/transform.go`
2. Register it in the transform builder switch
3. Add tests in `main_test.go`
4. Document in `docs/transforms.md`

## Code Style

- Run `gofmt` (enforced by CI)
- No unused imports or variables
- Use `sink.Resolve()` for credential resolution (config -> env -> Vault -> default)
- Use `context.Context` for cancellation and timeouts
- Log to `os.Stderr` with `[component]` prefix (e.g., `[snowflake]`, `[http]`)
- Errors should wrap with `fmt.Errorf("component action: %w", err)`

## Local Infrastructure

For integration testing with real services:

```bash
cd docker/
docker compose up -d
```

This starts Kafka, PostgreSQL, Schema Registry, Kafka UI, and Adminer. See [docs/local-infra.md](docs/local-infra.md) for details.

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
