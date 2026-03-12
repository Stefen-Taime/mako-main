# Vault Integration

Mako resolves secrets from HashiCorp Vault using a priority chain:

1. Explicit YAML config value
2. Environment variable
3. **Vault secret** (if `vault_path` is set in the sink config)
4. Default value

Vault is **completely optional**. If `VAULT_ADDR` is not set, Vault is
ignored and secrets are resolved from env vars or YAML as before.

## Local Development (Docker)

Vault is included in the docker-compose stack:

```bash
cd docker/
docker compose up -d
```

Vault starts in dev mode at http://localhost:8200 with pre-loaded
secrets for all services (PostgreSQL, Snowflake, Kafka, APIs, etc.).

```bash
export VAULT_ADDR=http://localhost:8200
export VAULT_TOKEN=mako-root-token

# List all secrets
vault kv list secret/mako/

# Read PostgreSQL credentials
vault kv get secret/mako/postgres

# Update a secret
vault kv put secret/mako/snowflake \
    account=my-real-account.snowflakecomputing.com \
    user=my_user \
    password=my_password \
    warehouse=MY_WH \
    role=MY_ROLE \
    database=MY_DB
```

### Pre-loaded secret paths

| Path                   | Keys                                               |
|------------------------|-----------------------------------------------------|
| secret/mako/postgres   | host, port, user, password, database               |
| secret/mako/snowflake  | account, user, password, warehouse, role, database |
| secret/mako/kafka      | brokers, schema_registry                           |
| secret/mako/api/bearer | token                                              |
| secret/mako/api/basic  | username, password                                 |
| secret/mako/api/apikey | header, value                                      |
| secret/mako/api/oauth2 | client_id, client_secret, token_url                |
| secret/mako/slack      | webhook_url                                        |
| secret/mako/aws        | access_key_id, secret_access_key, region           |
| secret/mako/gcs        | project, bucket                                    |
| secret/mako/clickhouse | host, port, user, password, database               |

## HCP Vault (HashiCorp Cloud)

```bash
export VAULT_ADDR=https://my-cluster.vault.hashicorp.cloud:8200
export VAULT_TOKEN=hvs.XXXXXXXXXXXXXXXXXXXXXXXX
export VAULT_NAMESPACE=admin
```

Then load your secrets:

```bash
vault kv put secret/mako/postgres \
    host=my-rds-instance.amazonaws.com \
    port=5432 \
    user=prod_user \
    password=prod_password \
    database=analytics

vault kv put secret/mako/snowflake \
    account=myorg-myaccount \
    user=MAKO_SVC \
    password=strong-password \
    warehouse=MAKO_WH \
    role=MAKO_ROLE \
    database=PROD
```

## Production -- AppRole Auth

For CI/CD and automated pipelines, use AppRole instead of static tokens:

```bash
# On the Vault server: create the AppRole
vault auth enable approle
vault write auth/approle/role/mako-pipeline \
    token_ttl=1h \
    token_max_ttl=4h \
    secret_id_ttl=24h \
    policies=mako-read

# Create a policy that allows reading secrets
vault policy write mako-read - <<EOF
path "secret/data/mako/*" {
  capabilities = ["read", "list"]
}
EOF

# Get the role_id and secret_id
vault read auth/approle/role/mako-pipeline/role-id
vault write -f auth/approle/role/mako-pipeline/secret-id
```

Then on the client:

```bash
export VAULT_ADDR=https://vault.internal:8200
export VAULT_ROLE_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
export VAULT_SECRET_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
```

## Production -- Kubernetes Auth

For pipelines running in Kubernetes:

```bash
# On the Vault server: enable Kubernetes auth
vault auth enable kubernetes
vault write auth/kubernetes/config \
    kubernetes_host=https://kubernetes.default.svc

vault write auth/kubernetes/role/mako-pipeline \
    bound_service_account_names=mako \
    bound_service_account_namespaces=mako \
    policies=mako-read \
    ttl=1h
```

Then set:

```bash
export VAULT_ADDR=http://vault.vault.svc:8200
export VAULT_K8S_ROLE=mako-pipeline
```

The service account JWT is read automatically from
`/var/run/secrets/kubernetes.io/serviceaccount/token`.

## Pipeline YAML Configuration

Add `vault:` at the pipeline level and `vault_path:` in sink config:

```yaml
pipeline:
  name: my-pipeline
  vault:
    path: secret/data/mako    # base path (optional)
    ttl: 5m                   # cache TTL (default: 5m)
  sink:
    type: postgres
    table: my_table
    config:
      vault_path: secret/data/mako/postgres  # Vault reads host, port, user, password from here
```

No need to put credentials in the YAML or in env vars. Vault
resolves them at runtime.

## Supported Environment Variables

| Variable        | Description                        | Auth Method |
|-----------------|------------------------------------|-------------|
| VAULT_ADDR      | Vault server URL (required)        | All         |
| VAULT_TOKEN     | Static token                       | Token       |
| VAULT_ROLE_ID   | AppRole role ID                    | AppRole     |
| VAULT_SECRET_ID | AppRole secret ID                  | AppRole     |
| VAULT_K8S_ROLE  | Kubernetes auth role name          | Kubernetes  |
| VAULT_NAMESPACE | Vault namespace (Enterprise/HCP)   | All         |
| VAULT_CACERT    | Custom CA certificate path for TLS | All         |
