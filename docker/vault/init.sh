#!/bin/sh
set -e

export VAULT_ADDR=http://127.0.0.1:8200
export VAULT_TOKEN=mako-root-token

echo "[vault] loading secrets..."

# ── PostgreSQL ──────────────────────────────────────────
vault kv put secret/mako/postgres \
    host=postgres \
    port=5432 \
    user=mako \
    password=mako \
    database=mako

# ── Snowflake ───────────────────────────────────────────
vault kv put secret/mako/snowflake \
    account="${SNOWFLAKE_ACCOUNT:-your-account.snowflakecomputing.com}" \
    user="${SNOWFLAKE_USER:-mako_user}" \
    password="${SNOWFLAKE_PASSWORD:-changeme}" \
    warehouse="${SNOWFLAKE_WAREHOUSE:-COMPUTE_WH}" \
    role="${SNOWFLAKE_ROLE:-MAKO_ROLE}" \
    database="${SNOWFLAKE_DATABASE:-ANALYTICS}"

# ── Kafka ───────────────────────────────────────────────
vault kv put secret/mako/kafka \
    brokers=kafka:29092 \
    schema_registry=http://schema-registry:8081

# ── HTTP API credentials ────────────────────────────────
vault kv put secret/mako/api/bearer \
    token=super-secret-token-123

vault kv put secret/mako/api/basic \
    username=admin \
    password=admin123

vault kv put secret/mako/api/apikey \
    header=X-API-Key \
    value=my-api-key-456

vault kv put secret/mako/api/oauth2 \
    client_id=my-client-id \
    client_secret=my-client-secret \
    token_url=http://localhost:8000/oauth2/token

# ── Slack ───────────────────────────────────────────────
vault kv put secret/mako/slack \
    webhook_url="${SLACK_WEBHOOK_URL:-https://hooks.slack.com/services/CHANGE_ME/CHANGE_ME/CHANGE_ME}"

# ── AWS / S3 ────────────────────────────────────────────
vault kv put secret/mako/aws \
    access_key_id="${AWS_ACCESS_KEY_ID:-}" \
    secret_access_key="${AWS_SECRET_ACCESS_KEY:-}" \
    region="${AWS_REGION:-us-east-1}"

# ── GCP / GCS ──────────────────────────────────────────
vault kv put secret/mako/gcs \
    project="${GCP_PROJECT:-uber-324807}" \
    bucket="${GCS_BUCKET:-mako-pipeline-data}"

# ── ClickHouse ──────────────────────────────────────────
vault kv put secret/mako/clickhouse \
    host="${CLICKHOUSE_HOST:-localhost}" \
    port="${CLICKHOUSE_PORT:-9000}" \
    user="${CLICKHOUSE_USER:-default}" \
    password="${CLICKHOUSE_PASSWORD:-}" \
    database="${CLICKHOUSE_DB:-default}"

echo "[vault] done — secrets loaded at secret/mako/*"
