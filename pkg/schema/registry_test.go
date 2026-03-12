package schema

import (
	"context"
	"testing"
)

// ═══════════════════════════════════════════
// JSON Schema validation tests
// ═══════════════════════════════════════════

func TestValidateJSON_Valid(t *testing.T) {
	v := NewValidator("http://fake:8081", "test-subject", true, "reject")

	// Simulate a cached JSON schema
	v.schemaOnce.Do(func() {})
	v.schema = &SchemaInfo{
		ID:         1,
		Version:    1,
		Subject:    "test-subject",
		SchemaType: "JSON",
		Schema: `{
			"type": "object",
			"properties": {
				"name": {"type": "string"},
				"age":  {"type": "integer"}
			},
			"required": ["name"]
		}`,
	}

	event := map[string]any{
		"name": "Alice",
		"age":  float64(30),
	}

	result, err := v.Validate(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Valid {
		t.Fatalf("expected valid, got errors: %v", result.Errors)
	}
}

func TestValidateJSON_MissingRequired(t *testing.T) {
	v := NewValidator("http://fake:8081", "test-subject", true, "reject")

	v.schemaOnce.Do(func() {})
	v.schema = &SchemaInfo{
		ID:         1,
		Version:    1,
		Subject:    "test-subject",
		SchemaType: "JSON",
		Schema: `{
			"type": "object",
			"properties": {
				"name": {"type": "string"},
				"age":  {"type": "integer"}
			},
			"required": ["name"]
		}`,
	}

	event := map[string]any{
		"age": float64(30),
	}

	result, err := v.Validate(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Valid {
		t.Fatal("expected invalid, got valid")
	}
	if len(result.Errors) == 0 {
		t.Fatal("expected errors, got none")
	}
}

func TestValidateJSON_WrongType(t *testing.T) {
	v := NewValidator("http://fake:8081", "test-subject", true, "reject")

	v.schemaOnce.Do(func() {})
	v.schema = &SchemaInfo{
		ID:         1,
		Version:    1,
		Subject:    "test-subject",
		SchemaType: "JSON",
		Schema: `{
			"type": "object",
			"properties": {
				"name": {"type": "string"},
				"age":  {"type": "integer"}
			},
			"required": ["name"]
		}`,
	}

	event := map[string]any{
		"name": 123, // should be string
		"age":  float64(30),
	}

	result, err := v.Validate(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Valid {
		t.Fatal("expected invalid, got valid")
	}
}

func TestValidateJSON_EnforceDisabled(t *testing.T) {
	v := NewValidator("http://fake:8081", "test-subject", false, "reject")

	// No schema needed when enforce=false
	event := map[string]any{"anything": "goes"}

	result, err := v.Validate(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Valid {
		t.Fatal("expected valid when enforce=false")
	}
}

// ═══════════════════════════════════════════
// Avro schema validation tests
// ═══════════════════════════════════════════

const testAvroSchema = `{
	"type": "record",
	"name": "OrderEvent",
	"namespace": "com.mako.test",
	"fields": [
		{"name": "order_id", "type": "string"},
		{"name": "amount", "type": "double"},
		{"name": "quantity", "type": "int"},
		{"name": "active", "type": "boolean"}
	]
}`

func TestValidateAvro_Valid(t *testing.T) {
	v := NewValidator("http://fake:8081", "test-subject", true, "reject")

	v.schemaOnce.Do(func() {})
	v.schema = &SchemaInfo{
		ID:         1,
		Version:    1,
		Subject:    "test-subject",
		SchemaType: "AVRO",
		Schema:     testAvroSchema,
	}

	event := map[string]any{
		"order_id": "ORD-001",
		"amount":   float64(99.99),
		"quantity": float64(3),
		"active":   true,
	}

	result, err := v.Validate(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Valid {
		t.Fatalf("expected valid, got errors: %v", result.Errors)
	}
}

func TestValidateAvro_WrongType(t *testing.T) {
	v := NewValidator("http://fake:8081", "test-subject", true, "reject")

	v.schemaOnce.Do(func() {})
	v.schema = &SchemaInfo{
		ID:         1,
		Version:    1,
		Subject:    "test-subject",
		SchemaType: "AVRO",
		Schema:     testAvroSchema,
	}

	event := map[string]any{
		"order_id": 12345,          // should be string
		"amount":   float64(99.99),
		"quantity": float64(3),
		"active":   true,
	}

	result, err := v.Validate(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Valid {
		t.Fatal("expected invalid for wrong type, got valid")
	}
}

func TestValidateAvro_MissingField(t *testing.T) {
	v := NewValidator("http://fake:8081", "test-subject", true, "reject")

	v.schemaOnce.Do(func() {})
	v.schema = &SchemaInfo{
		ID:         1,
		Version:    1,
		Subject:    "test-subject",
		SchemaType: "AVRO",
		Schema:     testAvroSchema,
	}

	// Missing "active" field — Avro records require all fields without defaults
	event := map[string]any{
		"order_id": "ORD-001",
		"amount":   float64(99.99),
		"quantity": float64(3),
	}

	result, err := v.Validate(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Valid {
		t.Fatal("expected invalid for missing required field, got valid")
	}
}

const testAvroSchemaWithDefaults = `{
	"type": "record",
	"name": "UserEvent",
	"namespace": "com.mako.test",
	"fields": [
		{"name": "user_id", "type": "string"},
		{"name": "role", "type": "string", "default": "viewer"}
	]
}`

func TestValidateAvro_WithDefaults(t *testing.T) {
	v := NewValidator("http://fake:8081", "test-subject", true, "reject")

	v.schemaOnce.Do(func() {})
	v.schema = &SchemaInfo{
		ID:         1,
		Version:    1,
		Subject:    "test-subject",
		SchemaType: "AVRO",
		Schema:     testAvroSchemaWithDefaults,
	}

	// "role" is omitted but has a default — should be valid
	event := map[string]any{
		"user_id": "USR-001",
	}

	result, err := v.Validate(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Valid {
		t.Fatalf("expected valid (field has default), got errors: %v", result.Errors)
	}
}

const testAvroSchemaWithUnion = `{
	"type": "record",
	"name": "NullableEvent",
	"namespace": "com.mako.test",
	"fields": [
		{"name": "id", "type": "string"},
		{"name": "email", "type": ["null", "string"], "default": null}
	]
}`

func TestValidateAvro_NullableField(t *testing.T) {
	v := NewValidator("http://fake:8081", "test-subject", true, "reject")

	v.schemaOnce.Do(func() {})
	v.schema = &SchemaInfo{
		ID:         1,
		Version:    1,
		Subject:    "test-subject",
		SchemaType: "AVRO",
		Schema:     testAvroSchemaWithUnion,
	}

	// email is null (valid for union ["null", "string"])
	event := map[string]any{
		"id":    "EVT-001",
		"email": nil,
	}

	result, err := v.Validate(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Valid {
		t.Fatalf("expected valid for null union, got errors: %v", result.Errors)
	}
}

func TestValidateAvro_NullableFieldWithValue(t *testing.T) {
	v := NewValidator("http://fake:8081", "test-subject", true, "reject")

	v.schemaOnce.Do(func() {})
	v.schema = &SchemaInfo{
		ID:         1,
		Version:    1,
		Subject:    "test-subject",
		SchemaType: "AVRO",
		Schema:     testAvroSchemaWithUnion,
	}

	// email has a string value (valid for union ["null", "string"])
	event := map[string]any{
		"id":    "EVT-001",
		"email": map[string]any{"string": "alice@test.com"},
	}

	result, err := v.Validate(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Valid {
		t.Fatalf("expected valid for union with string value, got errors: %v", result.Errors)
	}
}

func TestValidateAvro_InvalidSchema(t *testing.T) {
	v := NewValidator("http://fake:8081", "test-subject", true, "reject")

	v.schemaOnce.Do(func() {})
	v.schema = &SchemaInfo{
		ID:         1,
		Version:    1,
		Subject:    "test-subject",
		SchemaType: "AVRO",
		Schema:     `{invalid json`,
	}

	event := map[string]any{"foo": "bar"}

	result, err := v.Validate(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Valid {
		t.Fatal("expected invalid for bad schema, got valid")
	}
}

const testAvroSchemaWithNested = `{
	"type": "record",
	"name": "OrderWithAddress",
	"namespace": "com.mako.test",
	"fields": [
		{"name": "order_id", "type": "string"},
		{"name": "address", "type": {
			"type": "record",
			"name": "Address",
			"fields": [
				{"name": "street", "type": "string"},
				{"name": "zip", "type": "int"}
			]
		}}
	]
}`

func TestValidateAvro_NestedRecord(t *testing.T) {
	v := NewValidator("http://fake:8081", "test-subject", true, "reject")

	v.schemaOnce.Do(func() {})
	v.schema = &SchemaInfo{
		ID:         1,
		Version:    1,
		Subject:    "test-subject",
		SchemaType: "AVRO",
		Schema:     testAvroSchemaWithNested,
	}

	event := map[string]any{
		"order_id": "ORD-001",
		"address": map[string]any{
			"street": "123 Main St",
			"zip":    float64(10001),
		},
	}

	result, err := v.Validate(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Valid {
		t.Fatalf("expected valid for nested record, got errors: %v", result.Errors)
	}
}

func TestValidateAvro_IntCoercion(t *testing.T) {
	v := NewValidator("http://fake:8081", "test-subject", true, "reject")

	v.schemaOnce.Do(func() {})
	v.schema = &SchemaInfo{
		ID:         1,
		Version:    1,
		Subject:    "test-subject",
		SchemaType: "AVRO",
		Schema:     testAvroSchema,
	}

	// JSON decodes all numbers as float64. Avro "int" fields should still
	// validate correctly thanks to coercion.
	event := map[string]any{
		"order_id": "ORD-002",
		"amount":   float64(50.0),
		"quantity": float64(10), // float64 but represents an int
		"active":   true,
	}

	result, err := v.Validate(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Valid {
		t.Fatalf("expected valid after int coercion, got errors: %v", result.Errors)
	}
}
