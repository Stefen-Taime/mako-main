# Local Infrastructure

The `docker/` directory contains a full local stack for development.

## Quick start

```bash
cd docker/
docker compose up -d           # Start all services
./create-topics.sh             # Create Kafka topics
./produce-sample.sh            # Produce test events
```

> **Tip:** Use `mako init --full pipeline.yaml` to generate a reference template with all available sources and sinks pre-configured for these local services (PostgreSQL, Kafka, etc.).

## Services

| Service | Port | Description |
|---|---|---|
| Kafka (KRaft) | `localhost:9092` | Event broker |
| Schema Registry | `localhost:8081` | Confluent Schema Registry |
| PostgreSQL | `localhost:5432` | Sink database (user: `mako`, pass: `mako`) |
| Vault | `localhost:8200` | Secret management (token: `mako-root-token`) |
| Flink SQL | `localhost:8082` | Stream processing dashboard |
| Prometheus | `localhost:9091` | Metrics collection (scrapes Mako /metrics) |
| Grafana | `localhost:3000` | Dashboards (admin / `mako`) |
| Kafka UI | `localhost:8080` | Web UI for topics and messages |
| Adminer | `localhost:8083` | Database UI for PostgreSQL |

## Vault

Vault starts in **dev mode** with pre-loaded secrets for all services.
See [docs/vault.md](vault.md) for full configuration details.

```bash
export VAULT_ADDR=http://localhost:8200
export VAULT_TOKEN=mako-root-token

vault kv list secret/mako/       # List all secret paths
vault kv get secret/mako/postgres # Read PostgreSQL credentials
```
