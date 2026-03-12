# PII Masking Examples

Pipelines that hash or mask personally identifiable information (PII) before loading to the sink.

## Pipelines

| Pipeline | Masked Fields | Description |
|----------|---------------|-------------|
| [pipeline-stripe-masking.yaml](pipeline-stripe-masking.yaml) | credit card numbers, CCV | Stripe payment data with card masking |
| [pipeline-users-pii.yaml](pipeline-users-pii.yaml) | email, phone, SSN | User data with PII hashing |

## Transform Types

**hash_fields** -- One-way SHA-256 hash (irreversible):
```yaml
transforms:
  - name: pii_mask
    type: hash_fields
    fields: [email, phone, ssn]
```

**mask_fields** -- Partial masking (preserves format):
```yaml
transforms:
  - name: card_mask
    type: mask_fields
    fields: [credit_card]
```

## Run

```bash
mako validate examples/transforms/pii/pipeline-stripe-masking.yaml
mako run examples/transforms/pii/pipeline-stripe-masking.yaml
```
