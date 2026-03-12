---
name: Bug Report
about: Report a bug or unexpected behavior
title: "[bug] "
labels: bug
assignees: ''
---

## Description

A clear and concise description of the bug.

## Steps to Reproduce

1. Pipeline YAML used (or a minimal example):

```yaml
pipeline:
  name: ...
  source:
    type: ...
  sink:
    type: ...
```

2. Command run:

```bash
mako run pipeline.yaml
```

3. What happened (error message, logs, unexpected output):

```
paste error/logs here
```

## Expected Behavior

What you expected to happen instead.

## Environment

- **OS:** (e.g., macOS 14.5, Ubuntu 24.04)
- **Go version:** (output of `go version`)
- **Mako version/commit:** (output of `mako version` or git commit hash)
- **Source type:** (kafka, file, postgres_cdc, http)
- **Sink type:** (postgres, snowflake, bigquery, clickhouse, s3, gcs)

## Additional Context

- Docker compose services running (if applicable)
- Related configuration (env vars, Vault, etc.)
- Screenshots or full log output
