# Mako — Local Infrastructure

Stack Docker autonome pour developper et tester Mako contre de vrais services.

## Services

| Service | Port | Description |
|---|---|---|
| **Kafka** | `localhost:9092` | Broker KRaft (sans Zookeeper) |
| **Schema Registry** | `localhost:8081` | Confluent Schema Registry |
| **PostgreSQL** | `localhost:5432` | Base de donnees (user: `mako`, pass: `mako`, db: `mako`) |
| **Prometheus** | `localhost:9091` | Metrics collection (scrapes Mako /metrics) |
| **Grafana** | `localhost:3000` | Dashboards (admin / mako) |
| **Flink JobManager** | `localhost:8082` | Flink Dashboard |
| **Flink TaskManager** | — | Worker (4 task slots) |
| **Kafka UI** | `localhost:8080` | Interface web pour Kafka + Schema Registry |
| **Adminer** | `localhost:8083` | Interface web pour PostgreSQL |

## Demarrage rapide

```bash
cd docker/

# Lancer toute l'infra
docker compose up -d

# Verifier que tout est healthy
docker compose ps

# Creer les topics Kafka pour les exemples
./create-topics.sh

# Produire des events de test
./produce-sample.sh              # 10 events sur events.orders
./produce-sample.sh events.payments 20  # 20 events sur events.payments
```

## Utilisation avec Mako

```bash
# Depuis la racine du projet :

# Valider un pipeline
./bin/mako validate examples/simple/pipeline.yaml

# Dry-run (stdin)
cat test/fixtures/events.jsonl | ./bin/mako dry-run examples/simple/pipeline.yaml

# Run reel contre Kafka local (quand le runtime sera implemente)
KAFKA_BROKERS=localhost:9092 ./bin/mako run examples/simple/pipeline.yaml
```

## PostgreSQL

Le schema est initialise automatiquement au premier demarrage (`postgres/init/001-schema.sql`).

Schemas crees :
- `mako.*` — tables internes (pipelines, metrics, DLQ)
- `analytics.*` — tables sink pour les exemples (order_events, payment_events)

```bash
# Se connecter
psql -h localhost -U mako -d mako

# Verifier les tables
\dt mako.*
\dt analytics.*
```

## Schema Registry

```bash
# Lister les subjects
curl http://localhost:8081/subjects

# Enregistrer un schema (JSON Schema)
curl -X POST http://localhost:8081/subjects/events.orders-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schemaType": "JSON",
    "schema": "{\"type\":\"object\",\"properties\":{\"event_id\":{\"type\":\"string\"},\"amount\":{\"type\":\"number\"},\"status\":{\"type\":\"string\"}},\"required\":[\"event_id\"]}"
  }'

# Verifier
curl http://localhost:8081/subjects/events.orders-value/versions/latest
```

## Flink SQL Client

```bash
# Lancer le SQL client interactif
docker compose --profile sql run --rm flink-sql-client

# Dans le SQL client :
# CREATE TABLE orders (
#   event_id STRING,
#   amount DOUBLE,
#   status STRING,
#   event_time TIMESTAMP(3)
# ) WITH (
#   'connector' = 'kafka',
#   'topic' = 'events.orders',
#   'properties.bootstrap.servers' = 'kafka:29092',
#   'format' = 'json',
#   'scan.startup.mode' = 'earliest-offset'
# );
#
# SELECT * FROM orders;
```

## Monitoring (Prometheus + Grafana)

Prometheus scrape automatiquement les metriques du pipeline Mako sur `host.docker.internal:9090`.

```bash
# Verifier que Prometheus scrape bien
curl http://localhost:9091/api/v1/targets

# Ouvrir Grafana (admin / mako)
open http://localhost:3000
```

Un dashboard Mako est provisionne automatiquement dans Grafana (dossier "Mako").

Pour monitorer un pipeline sur un port different, editer `prometheus/prometheus.yml` et ajouter le target.

## Kafka UI

Ouvrir http://localhost:8080 pour :
- Voir les topics et messages
- Inspecter les consumer groups
- Parcourir le Schema Registry

## Arreter

```bash
docker compose down       # Arrete les containers (garde les volumes)
docker compose down -v    # Arrete et supprime les volumes (reset complet)
```

## Ressources

- Kafka: ~500MB RAM
- Flink: ~2GB RAM (jobmanager + taskmanager)
- PostgreSQL: ~100MB RAM
- Schema Registry: ~300MB RAM
- Prometheus: ~100MB RAM
- Grafana: ~100MB RAM
- **Total: ~3.2GB RAM minimum**
