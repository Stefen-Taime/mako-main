# HTTP Source Examples

Pipelines that ingest data from REST APIs using the `http` source type.

## Pipelines

| Pipeline | Auth | Pagination | Sink | Description |
|----------|------|------------|------|-------------|
| [pipeline-beer-noauth.yaml](pipeline-beer-noauth.yaml) | None | Offset | PostgreSQL | Beer API with no authentication |
| [pipeline-restaurant-bearer.yaml](pipeline-restaurant-bearer.yaml) | Bearer | Offset | PostgreSQL | Restaurant API with Bearer token |
| [pipeline-restaurant-basic.yaml](pipeline-restaurant-basic.yaml) | Basic | Offset | PostgreSQL | Restaurant API with Basic auth |
| [pipeline-restaurant-apikey.yaml](pipeline-restaurant-apikey.yaml) | API Key | Offset | PostgreSQL | Restaurant API with API key header |
| [pipeline-movies-oauth2.yaml](pipeline-movies-oauth2.yaml) | OAuth2 | Offset | PostgreSQL | Movies API with OAuth2 client credentials |
| [pipeline-github-repos.yaml](pipeline-github-repos.yaml) | None | - | PostgreSQL | GitHub public repos API |
| [pipeline-http-products.yaml](pipeline-http-products.yaml) | None | - | PostgreSQL | DummyJSON products API |
| [pipeline-multi-source-join.yaml](pipeline-multi-source-join.yaml) | Mixed | Offset | GCS + Kafka | Multi-source join (3 HTTP APIs + PostgreSQL CDC) |

## Auth Methods

Mako supports all common HTTP authentication patterns:

- **none** -- No authentication required
- **bearer** -- `Authorization: Bearer <token>` header
- **basic** -- `Authorization: Basic <base64>` header
- **apikey** -- Custom header (e.g. `X-API-Key`)
- **oauth2** -- Client credentials flow with automatic token refresh

## Key Config Options

```yaml
source:
  type: http
  config:
    url: https://api.example.com/data
    method: GET
    auth_type: bearer            # none | bearer | basic | apikey | oauth2
    auth_token: ${API_TOKEN}
    response_type: json
    data_path: data              # JSON path to array of records
    pagination_type: offset      # offset | cursor | none
    pagination_limit: 100
    rate_limit_rps: 10
    timeout: 30s
```

## Run

```bash
mako validate examples/sources/http/pipeline-beer-noauth.yaml
mako run examples/sources/http/pipeline-beer-noauth.yaml
```
