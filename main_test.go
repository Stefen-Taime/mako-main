package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Stefen-Taime/mako/api/v1"
	"github.com/Stefen-Taime/mako/pkg/config"
	"github.com/Stefen-Taime/mako/pkg/pipeline"
	"github.com/Stefen-Taime/mako/pkg/sink"
	"github.com/Stefen-Taime/mako/pkg/source"
	"github.com/Stefen-Taime/mako/pkg/transform"
	"github.com/Stefen-Taime/mako/pkg/vault"
)

// ═══════════════════════════════════════════
// Config / Validation Tests
// ═══════════════════════════════════════════

func TestParseSimplePipeline(t *testing.T) {
	yaml := `
apiVersion: mako/v1
kind: Pipeline
pipeline:
  name: test-events
  source:
    type: kafka
    topic: events.test
  sink:
    type: stdout
`
	spec, err := config.Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	if spec.Pipeline.Name != "test-events" {
		t.Errorf("expected name 'test-events', got %q", spec.Pipeline.Name)
	}
	if spec.Pipeline.Source.Type != v1.SourceKafka {
		t.Errorf("expected source type kafka, got %q", spec.Pipeline.Source.Type)
	}
	if spec.Pipeline.Source.ConsumerGroup != "mako-test-events" {
		t.Errorf("expected auto consumer group, got %q", spec.Pipeline.Source.ConsumerGroup)
	}
}

func TestValidateMinimalPipeline(t *testing.T) {
	spec := &v1.PipelineSpec{
		Pipeline: v1.Pipeline{
			Name:   "test",
			Source: v1.Source{Type: v1.SourceKafka, Topic: "t"},
			Sink:   v1.Sink{Type: v1.SinkStdout},
		},
	}

	result := config.Validate(spec)
	if !result.IsValid() {
		t.Errorf("minimal pipeline should be valid, got errors: %v", result.Errors)
	}
}

func TestValidateRejectsEmptyName(t *testing.T) {
	spec := &v1.PipelineSpec{
		Pipeline: v1.Pipeline{
			Source: v1.Source{Type: v1.SourceKafka, Topic: "t"},
			Sink:   v1.Sink{Type: v1.SinkStdout},
		},
	}

	result := config.Validate(spec)
	if result.IsValid() {
		t.Error("should reject empty pipeline name")
	}
}

func TestValidateRejectsBadName(t *testing.T) {
	cases := []string{"My Pipeline", "UPPER", "has_underscore", "-leading", "trailing-"}
	for _, name := range cases {
		spec := &v1.PipelineSpec{
			Pipeline: v1.Pipeline{
				Name:   name,
				Source: v1.Source{Type: v1.SourceKafka, Topic: "t"},
				Sink:   v1.Sink{Type: v1.SinkStdout},
			},
		}
		result := config.Validate(spec)
		if result.IsValid() {
			t.Errorf("should reject name %q", name)
		}
	}
}

func TestValidateAcceptsGoodNames(t *testing.T) {
	cases := []string{"order-events", "payment-v2", "a", "test123"}
	for _, name := range cases {
		spec := &v1.PipelineSpec{
			Pipeline: v1.Pipeline{
				Name:   name,
				Source: v1.Source{Type: v1.SourceKafka, Topic: "t"},
				Sink:   v1.Sink{Type: v1.SinkStdout},
			},
		}
		result := config.Validate(spec)
		if !result.IsValid() {
			t.Errorf("should accept name %q, got errors: %v", name, result.Errors)
		}
	}
}

func TestValidateKafkaSourceRequiresTopic(t *testing.T) {
	spec := &v1.PipelineSpec{
		Pipeline: v1.Pipeline{
			Name:   "test",
			Source: v1.Source{Type: v1.SourceKafka},
			Sink:   v1.Sink{Type: v1.SinkStdout},
		},
	}

	result := config.Validate(spec)
	if result.IsValid() {
		t.Error("kafka source without topic should be invalid")
	}
}

func TestValidateTransformHashRequiresFields(t *testing.T) {
	spec := &v1.PipelineSpec{
		Pipeline: v1.Pipeline{
			Name:   "test",
			Source: v1.Source{Type: v1.SourceKafka, Topic: "t"},
			Transforms: []v1.Transform{
				{Name: "hash", Type: v1.TransformHashFields},
			},
			Sink: v1.Sink{Type: v1.SinkStdout},
		},
	}

	result := config.Validate(spec)
	if result.IsValid() {
		t.Error("hash_fields without fields should be invalid")
	}
}

func TestValidateWarnsOnMissingOwner(t *testing.T) {
	spec := &v1.PipelineSpec{
		Pipeline: v1.Pipeline{
			Name:   "test",
			Source: v1.Source{Type: v1.SourceKafka, Topic: "t"},
			Sink:   v1.Sink{Type: v1.SinkStdout},
		},
	}

	result := config.Validate(spec)
	if len(result.Warnings) == 0 {
		t.Error("should warn about missing owner")
	}
}

func TestLoadExampleSimple(t *testing.T) {
	spec, err := config.Load("examples/sources/kafka/pipeline-order-events.yaml")
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}
	if spec.Pipeline.Name != "order-events" {
		t.Errorf("expected 'order-events', got %q", spec.Pipeline.Name)
	}
	result := config.Validate(spec)
	if !result.IsValid() {
		t.Errorf("simple example should be valid: %v", result.Errors)
	}
}

func TestLoadExampleAdvanced(t *testing.T) {
	spec, err := config.Load("examples/sources/kafka/pipeline-payment-features.yaml")
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}
	if spec.Pipeline.Name != "payment-features" {
		t.Errorf("expected 'payment-features', got %q", spec.Pipeline.Name)
	}
	if len(spec.Pipeline.Transforms) != 8 {
		t.Errorf("expected 8 transforms, got %d", len(spec.Pipeline.Transforms))
	}
	if len(spec.Pipeline.Sinks) != 2 {
		t.Errorf("expected 2 additional sinks, got %d", len(spec.Pipeline.Sinks))
	}
}

// ═══════════════════════════════════════════
// Transform Tests
// ═══════════════════════════════════════════

func TestHashFieldsTransform(t *testing.T) {
	specs := []v1.Transform{
		{Name: "hash", Type: v1.TransformHashFields, Fields: []string{"email", "phone"}},
	}
	chain, err := transform.NewChain(specs, transform.WithPIISalt("test-salt"))
	if err != nil {
		t.Fatalf("build chain: %v", err)
	}

	event := map[string]any{
		"email":  "john@example.com",
		"phone":  "+15551234567",
		"amount": 99.99,
	}

	result, err := chain.Apply(event)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}

	// Email should be hashed (not original)
	if result["email"] == "john@example.com" {
		t.Error("email should be hashed")
	}
	// Phone should be hashed
	if result["phone"] == "+15551234567" {
		t.Error("phone should be hashed")
	}
	// Amount should be unchanged
	if result["amount"] != 99.99 {
		t.Errorf("amount should be unchanged, got %v", result["amount"])
	}
	// PII processed flag
	if result["_pii_processed"] != true {
		t.Error("should set _pii_processed flag")
	}
	// Hash should be deterministic
	result2, _ := chain.Apply(map[string]any{"email": "john@example.com", "phone": "+15551234567", "amount": 99.99})
	if result["email"] != result2["email"] {
		t.Error("hash should be deterministic")
	}
}

func TestMaskFieldsTransform(t *testing.T) {
	specs := []v1.Transform{
		{Name: "mask", Type: v1.TransformMaskFields, Fields: []string{"email", "card"}},
	}
	chain, _ := transform.NewChain(specs)

	event := map[string]any{
		"email": "john@example.com",
		"card":  "4111222233334444",
		"name":  "John",
	}

	result, _ := chain.Apply(event)

	email, _ := result["email"].(string)
	if !strings.HasPrefix(email, "j") || !strings.Contains(email, "***") {
		t.Errorf("email mask incorrect: %s", email)
	}

	card, _ := result["card"].(string)
	if !strings.HasSuffix(card, "4444") || !strings.Contains(card, "****") {
		t.Errorf("card mask incorrect: %s", card)
	}

	if result["name"] != "John" {
		t.Error("non-masked fields should be unchanged")
	}
}

func TestDropFieldsTransform(t *testing.T) {
	specs := []v1.Transform{
		{Name: "drop", Type: v1.TransformDropFields, Fields: []string{"internal_id", "debug"}},
	}
	chain, _ := transform.NewChain(specs)

	event := map[string]any{
		"email":       "john@example.com",
		"internal_id": "abc123",
		"debug":       true,
	}

	result, _ := chain.Apply(event)

	if _, exists := result["internal_id"]; exists {
		t.Error("internal_id should be dropped")
	}
	if _, exists := result["debug"]; exists {
		t.Error("debug should be dropped")
	}
	if result["email"] != "john@example.com" {
		t.Error("email should remain")
	}
}

func TestFilterTransform(t *testing.T) {
	specs := []v1.Transform{
		{Name: "filter", Type: v1.TransformFilter, Condition: "status = completed"},
	}
	chain, _ := transform.NewChain(specs)

	// Should pass
	result, _ := chain.Apply(map[string]any{"status": "completed", "amount": 100})
	if result == nil {
		t.Error("completed event should pass filter")
	}

	// Should be filtered out
	result, _ = chain.Apply(map[string]any{"status": "test", "amount": 1})
	if result != nil {
		t.Error("test event should be filtered out")
	}
}

func TestFilterTransformAND(t *testing.T) {
	specs := []v1.Transform{
		{Name: "filter", Type: v1.TransformFilter, Condition: "status = completed AND environment = production"},
	}
	chain, _ := transform.NewChain(specs)

	result, _ := chain.Apply(map[string]any{"status": "completed", "environment": "production"})
	if result == nil {
		t.Error("should pass both conditions")
	}

	result, _ = chain.Apply(map[string]any{"status": "completed", "environment": "staging"})
	if result != nil {
		t.Error("should fail second condition")
	}
}

func TestFilterTransformOR(t *testing.T) {
	specs := []v1.Transform{
		{Name: "filter", Type: v1.TransformFilter, Condition: "status = completed OR status = pending"},
	}
	chain, _ := transform.NewChain(specs)

	result, _ := chain.Apply(map[string]any{"status": "completed"})
	if result == nil {
		t.Error("completed should pass")
	}

	result, _ = chain.Apply(map[string]any{"status": "pending"})
	if result == nil {
		t.Error("pending should pass")
	}

	result, _ = chain.Apply(map[string]any{"status": "failed"})
	if result != nil {
		t.Error("failed should not pass")
	}
}

func TestFilterIsNotNull(t *testing.T) {
	specs := []v1.Transform{
		{Name: "filter", Type: v1.TransformFilter, Condition: "email IS NOT NULL"},
	}
	chain, _ := transform.NewChain(specs)

	result, _ := chain.Apply(map[string]any{"email": "test@test.com"})
	if result == nil {
		t.Error("non-null email should pass")
	}

	result, _ = chain.Apply(map[string]any{"email": nil})
	if result != nil {
		t.Error("null email should be filtered")
	}

	result, _ = chain.Apply(map[string]any{"name": "test"})
	if result != nil {
		t.Error("missing email should be filtered")
	}
}

func TestRenameFieldsTransform(t *testing.T) {
	specs := []v1.Transform{
		{Name: "rename", Type: v1.TransformRename, Mapping: map[string]string{"amt": "amount", "ts": "timestamp"}},
	}
	chain, _ := transform.NewChain(specs)

	result, _ := chain.Apply(map[string]any{"amt": 100.0, "ts": "2024-01-01", "name": "test"})

	if _, exists := result["amt"]; exists {
		t.Error("old field 'amt' should be removed")
	}
	if result["amount"] != 100.0 {
		t.Error("'amount' should have the value")
	}
	if result["name"] != "test" {
		t.Error("unrenamed fields should remain")
	}
}

func TestTransformChaining(t *testing.T) {
	specs := []v1.Transform{
		{Name: "hash", Type: v1.TransformHashFields, Fields: []string{"email"}},
		{Name: "drop", Type: v1.TransformDropFields, Fields: []string{"internal_id"}},
		{Name: "filter", Type: v1.TransformFilter, Condition: "status = completed"},
	}
	chain, _ := transform.NewChain(specs)

	// Event that passes all transforms
	event := map[string]any{
		"email":       "john@example.com",
		"internal_id": "abc",
		"status":      "completed",
		"amount":      100,
	}

	result, _ := chain.Apply(event)
	if result == nil {
		t.Fatal("event should pass all transforms")
	}

	// Email should be hashed
	if result["email"] == "john@example.com" {
		t.Error("email should be hashed")
	}
	// internal_id should be dropped
	if _, exists := result["internal_id"]; exists {
		t.Error("internal_id should be dropped")
	}

	// Event that gets filtered
	event2 := map[string]any{"email": "test@test.com", "status": "test"}
	result2, _ := chain.Apply(event2)
	if result2 != nil {
		t.Error("test event should be filtered out")
	}
}

func TestEmptyChain(t *testing.T) {
	chain, _ := transform.NewChain(nil)
	event := map[string]any{"key": "value"}
	result, _ := chain.Apply(event)
	if result["key"] != "value" {
		t.Error("empty chain should pass through")
	}
}

func TestDeduplicateTransform(t *testing.T) {
	specs := []v1.Transform{
		{Name: "dedupe", Type: v1.TransformDedupe, Fields: []string{"event_id"}},
	}
	chain, _ := transform.NewChain(specs)

	// First time: pass
	result, _ := chain.Apply(map[string]any{"event_id": "e1", "data": "first"})
	if result == nil {
		t.Error("first occurrence should pass")
	}

	// Duplicate: filter
	result, _ = chain.Apply(map[string]any{"event_id": "e1", "data": "duplicate"})
	if result != nil {
		t.Error("duplicate should be filtered")
	}

	// Different ID: pass
	result, _ = chain.Apply(map[string]any{"event_id": "e2", "data": "second"})
	if result == nil {
		t.Error("different ID should pass")
	}
}

// ═══════════════════════════════════════════
// ═══════════════════════════════════════════
// Integration: dry-run with fixture data
// ═══════════════════════════════════════════

func TestDryRunWithFixtures(t *testing.T) {
	data, err := os.ReadFile("test/fixtures/events.jsonl")
	if err != nil {
		t.Skip("fixtures not found")
	}

	specs := []v1.Transform{
		{Name: "hash", Type: v1.TransformHashFields, Fields: []string{"email", "phone"}},
		{Name: "drop", Type: v1.TransformDropFields, Fields: []string{"credit_card_number"}},
		{Name: "filter", Type: v1.TransformFilter, Condition: "status != test"},
	}

	chain, err := transform.NewChain(specs, transform.WithPIISalt("fixture-salt"))
	if err != nil {
		t.Fatalf("build chain: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	passed, filtered := 0, 0

	for _, line := range lines {
		var event map[string]any
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatalf("parse fixture: %v", err)
		}

		result, err := chain.Apply(event)
		if err != nil {
			t.Fatalf("transform error: %v", err)
		}

		if result == nil {
			filtered++
			continue
		}
		passed++

		// Verify PII is hashed
		if result["email"] == event["email"] {
			t.Error("email should be hashed in output")
		}
		// Verify credit_card is dropped
		if _, exists := result["credit_card_number"]; exists {
			t.Error("credit_card_number should be dropped")
		}
	}

	if passed == 0 {
		t.Error("expected some events to pass")
	}
	if filtered == 0 {
		t.Error("expected some events to be filtered")
	}

	t.Logf("Fixtures: %d total → %d passed, %d filtered", len(lines), passed, filtered)
}

// ═══════════════════════════════════════════
// Benchmark
// ═══════════════════════════════════════════

func BenchmarkTransformChain(b *testing.B) {
	specs := []v1.Transform{
		{Name: "hash", Type: v1.TransformHashFields, Fields: []string{"email", "phone"}},
		{Name: "drop", Type: v1.TransformDropFields, Fields: []string{"internal_id"}},
		{Name: "filter", Type: v1.TransformFilter, Condition: "status = completed"},
	}
	chain, _ := transform.NewChain(specs)

	event := map[string]any{
		"email":       "john@example.com",
		"phone":       "+15551234567",
		"internal_id": "abc",
		"status":      "completed",
		"amount":      99.99,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		chain.Apply(event)
	}
}

func BenchmarkHashField(b *testing.B) {
	specs := []v1.Transform{
		{Name: "hash", Type: v1.TransformHashFields, Fields: []string{"email"}},
	}
	chain, _ := transform.NewChain(specs)
	event := map[string]any{"email": "john@example.com"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		chain.Apply(event)
	}
}

// ═══════════════════════════════════════════
// Postgres Flatten Tests
// ═══════════════════════════════════════════

func TestPgColumnTypeInference(t *testing.T) {
	cases := []struct {
		name     string
		value    any
		expected string
	}{
		{"string", "hello", "TEXT"},
		{"float64", 42.5, "NUMERIC"},
		{"int", 42, "NUMERIC"},
		{"bool_true", true, "BOOLEAN"},
		{"bool_false", false, "BOOLEAN"},
		{"nested_object", map[string]any{"key": "val"}, "JSONB"},
		{"array", []any{1, 2, 3}, "JSONB"},
		{"nil", nil, "TEXT"},
		{"timestamp_rfc3339", "2024-01-15T10:30:00Z", "TIMESTAMPTZ"},
		{"timestamp_space", "2024-01-15 10:30:00+00", "TIMESTAMPTZ"},
		{"not_timestamp", "hello world", "TEXT"},
		{"empty_string", "", "TEXT"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := sink.PgColumnType(tc.value)
			if got != tc.expected {
				t.Errorf("PgColumnType(%v) = %q, want %q", tc.value, got, tc.expected)
			}
		})
	}
}

func TestIsTimestampLike(t *testing.T) {
	positives := []string{
		"2024-01-15T10:30:00Z",
		"2024-01-15T10:30:00.123Z",
		"2024-01-15 10:30:00",
		"2024-01-15T10:30:00+05:30",
		"2023-12-31T23:59:59.999999Z",
	}
	for _, s := range positives {
		if !sink.IsTimestampLike(s) {
			t.Errorf("IsTimestampLike(%q) should be true", s)
		}
	}

	negatives := []string{
		"hello",
		"2024-01-15",
		"10:30:00",
		"",
		"not-a-timestamp",
		"2024/01/15 10:30:00",
	}
	for _, s := range negatives {
		if sink.IsTimestampLike(s) {
			t.Errorf("IsTimestampLike(%q) should be false", s)
		}
	}
}

func TestPostgresFlattenConfigParsing(t *testing.T) {
	yaml := `
apiVersion: mako/v1
kind: Pipeline
pipeline:
  name: flatten-test
  source:
    type: kafka
    topic: events.users
  sink:
    type: postgres
    database: mako
    schema: public
    table: users
    flatten: true
    config:
      host: localhost
      port: "5432"
      user: mako
      password: mako
`
	spec, err := config.Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	if spec.Pipeline.Sink.Type != v1.SinkPostgres {
		t.Errorf("expected sink type postgres, got %q", spec.Pipeline.Sink.Type)
	}
	if !spec.Pipeline.Sink.Flatten {
		t.Error("expected flatten to be true")
	}
	if spec.Pipeline.Sink.Table != "users" {
		t.Errorf("expected table 'users', got %q", spec.Pipeline.Sink.Table)
	}
}

func TestPostgresFlattenNewSinkSignature(t *testing.T) {
	// Verify NewPostgresSink accepts the flatten parameter and stores it.
	cfg := map[string]any{
		"host":     "localhost",
		"port":     "5432",
		"user":     "test",
		"password": "test",
	}

	s := sink.NewPostgresSink("testdb", "public", "users", true, cfg)
	if s == nil {
		t.Fatal("NewPostgresSink returned nil")
	}
	if s.Name() != "postgres:public.users" {
		t.Errorf("unexpected name: %s", s.Name())
	}

	// Non-flatten mode
	s2 := sink.NewPostgresSink("testdb", "public", "events", false, cfg)
	if s2 == nil {
		t.Fatal("NewPostgresSink returned nil for non-flatten")
	}
}

func TestPostgresFlattenBuildFromSpec(t *testing.T) {
	// Verify BuildFromSpec passes flatten flag to postgres sink.
	spec := v1.Sink{
		Type:     v1.SinkPostgres,
		Database: "mako",
		Schema:   "public",
		Table:    "users",
		Flatten:  true,
		Config: map[string]any{
			"host":     "localhost",
			"port":     "5432",
			"user":     "mako",
			"password": "mako",
		},
	}

	s, err := sink.BuildFromSpec(spec)
	if err != nil {
		t.Fatalf("BuildFromSpec failed: %v", err)
	}
	if s == nil {
		t.Fatal("BuildFromSpec returned nil")
	}
	if s.Name() != "postgres:public.users" {
		t.Errorf("unexpected sink name: %s", s.Name())
	}
}

func TestPgColumnTypeMixedEvent(t *testing.T) {
	// Simulate a realistic event and verify type inference for each field.
	event := map[string]any{
		"user_id":    "usr-001",
		"email":      "test@example.com",
		"age":        float64(30),
		"active":     true,
		"created_at": "2024-06-15T10:00:00Z",
		"address":    map[string]any{"city": "Paris", "zip": "75001"},
		"tags":       []any{"admin", "user"},
		"score":      float64(95.5),
	}

	expected := map[string]string{
		"user_id":    "TEXT",
		"email":      "TEXT",
		"age":        "NUMERIC",
		"active":     "BOOLEAN",
		"created_at": "TIMESTAMPTZ",
		"address":    "JSONB",
		"tags":       "JSONB",
		"score":      "NUMERIC",
	}

	for field, expectedType := range expected {
		got := sink.PgColumnType(event[field])
		if got != expectedType {
			t.Errorf("field %q: PgColumnType(%v) = %q, want %q",
				field, event[field], got, expectedType)
		}
	}
}

// ═══════════════════════════════════════════
// BigQuery Flatten Tests
// ═══════════════════════════════════════════

func TestBqColumnTypeInference(t *testing.T) {
	cases := []struct {
		name     string
		value    any
		expected string
	}{
		{"string", "hello", "STRING"},
		{"float64", 42.5, "FLOAT64"},
		{"int", 42, "INT64"},
		{"bool_true", true, "BOOLEAN"},
		{"bool_false", false, "BOOLEAN"},
		{"nested_object", map[string]any{"key": "val"}, "JSON"},
		{"array", []any{1, 2, 3}, "JSON"},
		{"nil", nil, "STRING"},
		{"timestamp_rfc3339", "2024-01-15T10:30:00Z", "TIMESTAMP"},
		{"timestamp_space", "2024-01-15 10:30:00+00", "TIMESTAMP"},
		{"not_timestamp", "hello world", "STRING"},
		{"empty_string", "", "STRING"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := sink.BqColumnType(tc.value)
			if got != tc.expected {
				t.Errorf("BqColumnType(%v) = %q, want %q", tc.value, got, tc.expected)
			}
		})
	}
}

func TestBigQueryFlattenConfigParsing(t *testing.T) {
	yaml := `
apiVersion: mako/v1
kind: Pipeline
pipeline:
  name: bq-flatten-test
  source:
    type: kafka
    topic: events.users
  sink:
    type: bigquery
    schema: raw_events
    table: users
    flatten: true
    config:
      project: my-gcp-project
`
	spec, err := config.Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	if spec.Pipeline.Sink.Type != v1.SinkBigQuery {
		t.Errorf("expected sink type bigquery, got %q", spec.Pipeline.Sink.Type)
	}
	if !spec.Pipeline.Sink.Flatten {
		t.Error("expected flatten to be true")
	}
	if spec.Pipeline.Sink.Table != "users" {
		t.Errorf("expected table 'users', got %q", spec.Pipeline.Sink.Table)
	}
}

func TestBigQueryFlattenNewSinkSignature(t *testing.T) {
	cfg := map[string]any{
		"project": "my-project",
	}

	s := sink.NewBigQuerySink("my-project", "raw_events", "users", true, cfg)
	if s == nil {
		t.Fatal("NewBigQuerySink returned nil")
	}
	if s.Name() != "bigquery:my-project.raw_events.users" {
		t.Errorf("unexpected name: %s", s.Name())
	}

	// Non-flatten mode
	s2 := sink.NewBigQuerySink("my-project", "raw_events", "events", false, cfg)
	if s2 == nil {
		t.Fatal("NewBigQuerySink returned nil for non-flatten")
	}
}

func TestBigQueryFlattenBuildFromSpec(t *testing.T) {
	spec := v1.Sink{
		Type:    v1.SinkBigQuery,
		Schema:  "raw_events",
		Table:   "users",
		Flatten: true,
		Config: map[string]any{
			"project": "my-gcp-project",
		},
	}

	s, err := sink.BuildFromSpec(spec)
	if err != nil {
		t.Fatalf("BuildFromSpec failed: %v", err)
	}
	if s == nil {
		t.Fatal("BuildFromSpec returned nil")
	}
	if s.Name() != "bigquery:my-gcp-project.raw_events.users" {
		t.Errorf("unexpected sink name: %s", s.Name())
	}
}

func TestBqColumnTypeMixedEvent(t *testing.T) {
	event := map[string]any{
		"user_id":    "usr-001",
		"email":      "test@example.com",
		"age":        float64(30),
		"active":     true,
		"created_at": "2024-06-15T10:00:00Z",
		"address":    map[string]any{"city": "Paris", "zip": "75001"},
		"tags":       []any{"admin", "user"},
		"score":      float64(95.5),
	}

	expected := map[string]string{
		"user_id":    "STRING",
		"email":      "STRING",
		"age":        "FLOAT64",
		"active":     "BOOLEAN",
		"created_at": "TIMESTAMP",
		"address":    "JSON",
		"tags":       "JSON",
		"score":      "FLOAT64",
	}

	for field, expectedType := range expected {
		got := sink.BqColumnType(event[field])
		if got != expectedType {
			t.Errorf("field %q: BqColumnType(%v) = %q, want %q",
				field, event[field], got, expectedType)
		}
	}
}

// ═══════════════════════════════════════════
// ClickHouse Flatten Tests
// ═══════════════════════════════════════════

func TestChColumnTypeInference(t *testing.T) {
	cases := []struct {
		name     string
		value    any
		expected string
	}{
		{"string", "hello", "String"},
		{"float64", 42.5, "Float64"},
		{"int", 42, "Int64"},
		{"bool_true", true, "Bool"},
		{"bool_false", false, "Bool"},
		{"nested_object", map[string]any{"key": "val"}, "String"},
		{"array", []any{1, 2, 3}, "String"},
		{"nil", nil, "String"},
		{"timestamp_rfc3339", "2024-01-15T10:30:00Z", "DateTime64(3)"},
		{"timestamp_space", "2024-01-15 10:30:00+00", "DateTime64(3)"},
		{"not_timestamp", "hello world", "String"},
		{"empty_string", "", "String"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := sink.ChColumnType(tc.value)
			if got != tc.expected {
				t.Errorf("ChColumnType(%v) = %q, want %q", tc.value, got, tc.expected)
			}
		})
	}
}

func TestClickHouseFlattenConfigParsing(t *testing.T) {
	yaml := `
apiVersion: mako/v1
kind: Pipeline
pipeline:
  name: ch-flatten-test
  source:
    type: kafka
    topic: events.users
  sink:
    type: clickhouse
    database: analytics
    table: users
    flatten: true
    config:
      host: localhost
      port: "9000"
      user: default
      password: ""
`
	spec, err := config.Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	if spec.Pipeline.Sink.Type != v1.SinkClickHouse {
		t.Errorf("expected sink type clickhouse, got %q", spec.Pipeline.Sink.Type)
	}
	if !spec.Pipeline.Sink.Flatten {
		t.Error("expected flatten to be true")
	}
	if spec.Pipeline.Sink.Table != "users" {
		t.Errorf("expected table 'users', got %q", spec.Pipeline.Sink.Table)
	}
	if spec.Pipeline.Sink.Database != "analytics" {
		t.Errorf("expected database 'analytics', got %q", spec.Pipeline.Sink.Database)
	}
}

func TestClickHouseFlattenNewSinkSignature(t *testing.T) {
	cfg := map[string]any{
		"host":     "localhost",
		"port":     "9000",
		"user":     "default",
		"password": "",
	}

	s := sink.NewClickHouseSink("analytics", "users", true, cfg)
	if s == nil {
		t.Fatal("NewClickHouseSink returned nil")
	}
	if s.Name() != "clickhouse:analytics.users" {
		t.Errorf("unexpected name: %s", s.Name())
	}

	// Non-flatten mode
	s2 := sink.NewClickHouseSink("analytics", "events", false, cfg)
	if s2 == nil {
		t.Fatal("NewClickHouseSink returned nil for non-flatten")
	}
}

func TestClickHouseFlattenBuildFromSpec(t *testing.T) {
	spec := v1.Sink{
		Type:     v1.SinkClickHouse,
		Database: "analytics",
		Table:    "users",
		Flatten:  true,
		Config: map[string]any{
			"host":     "localhost",
			"port":     "9000",
			"user":     "default",
			"password": "",
		},
	}

	s, err := sink.BuildFromSpec(spec)
	if err != nil {
		t.Fatalf("BuildFromSpec failed: %v", err)
	}
	if s == nil {
		t.Fatal("BuildFromSpec returned nil")
	}
	if s.Name() != "clickhouse:analytics.users" {
		t.Errorf("unexpected sink name: %s", s.Name())
	}
}

func TestChColumnTypeMixedEvent(t *testing.T) {
	event := map[string]any{
		"user_id":    "usr-001",
		"email":      "test@example.com",
		"age":        float64(30),
		"active":     true,
		"created_at": "2024-06-15T10:00:00Z",
		"address":    map[string]any{"city": "Paris", "zip": "75001"},
		"tags":       []any{"admin", "user"},
		"score":      float64(95.5),
	}

	expected := map[string]string{
		"user_id":    "String",
		"email":      "String",
		"age":        "Float64",
		"active":     "Bool",
		"created_at": "DateTime64(3)",
		"address":    "String",
		"tags":       "String",
		"score":      "Float64",
	}

	for field, expectedType := range expected {
		got := sink.ChColumnType(event[field])
		if got != expectedType {
			t.Errorf("field %q: ChColumnType(%v) = %q, want %q",
				field, event[field], got, expectedType)
		}
	}
}

// ═══════════════════════════════════════════
// Parquet Encoder Tests
// ═══════════════════════════════════════════

func makeTestEvents() []*pipeline.Event {
	ts := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)
	return []*pipeline.Event{
		{
			Key:       []byte("key1"),
			Value:     map[string]any{"user_id": "u1", "amount": 99.5, "active": true, "address": map[string]any{"city": "Paris"}},
			Timestamp: ts,
			Topic:     "events.orders",
			Partition: 0,
			Offset:    100,
		},
		{
			Key:       []byte("key2"),
			Value:     map[string]any{"user_id": "u2", "amount": 42.0, "active": false, "tags": []any{"vip"}},
			Timestamp: ts,
			Topic:     "events.orders",
			Partition: 0,
			Offset:    101,
		},
	}
}

func TestEncodeParquetBasic(t *testing.T) {
	events := makeTestEvents()
	data, err := sink.EncodeParquet(events, "snappy")
	if err != nil {
		t.Fatalf("EncodeParquet failed: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("EncodeParquet returned empty bytes")
	}
	// Parquet magic bytes: PAR1
	if string(data[:4]) != "PAR1" {
		t.Errorf("expected Parquet magic bytes PAR1, got %q", string(data[:4]))
	}
	// File should also end with PAR1
	if string(data[len(data)-4:]) != "PAR1" {
		t.Errorf("expected Parquet footer magic PAR1, got %q", string(data[len(data)-4:]))
	}
	t.Logf("Parquet output: %d bytes for %d events", len(data), len(events))
}

func TestEncodeParquetTypes(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	events := []*pipeline.Event{
		{
			Value: map[string]any{
				"name":    "Alice",
				"score":   float64(95.5),
				"active":  true,
				"address": map[string]any{"city": "NYC"},
				"tags":    []any{"admin", "user"},
			},
			Timestamp: ts,
			Topic:     "test",
			Offset:    1,
		},
	}
	data, err := sink.EncodeParquet(events, "snappy")
	if err != nil {
		t.Fatalf("EncodeParquet failed: %v", err)
	}
	if len(data) < 12 {
		t.Fatal("Parquet output too small")
	}
	// Verify magic bytes
	if string(data[:4]) != "PAR1" {
		t.Error("missing PAR1 header")
	}
}

func TestEncodeParquetCompression(t *testing.T) {
	events := makeTestEvents()

	compressions := []string{"snappy", "zstd", "gzip", "none"}
	for _, comp := range compressions {
		t.Run(comp, func(t *testing.T) {
			data, err := sink.EncodeParquet(events, comp)
			if err != nil {
				t.Fatalf("EncodeParquet(%s) failed: %v", comp, err)
			}
			if len(data) == 0 {
				t.Fatalf("EncodeParquet(%s) returned empty", comp)
			}
			if string(data[:4]) != "PAR1" {
				t.Errorf("EncodeParquet(%s) missing PAR1 magic", comp)
			}
		})
	}
}

func TestEncodeParquetEmpty(t *testing.T) {
	data, err := sink.EncodeParquet(nil, "snappy")
	if err != nil {
		t.Fatalf("EncodeParquet(nil) should not error: %v", err)
	}
	if data != nil {
		t.Errorf("EncodeParquet(nil) should return nil, got %d bytes", len(data))
	}

	data, err = sink.EncodeParquet([]*pipeline.Event{}, "snappy")
	if err != nil {
		t.Fatalf("EncodeParquet([]) should not error: %v", err)
	}
	if data != nil {
		t.Errorf("EncodeParquet([]) should return nil, got %d bytes", len(data))
	}
}

func TestS3SinkParquetFormat(t *testing.T) {
	cfg := map[string]any{"region": "us-east-1", "compression": "snappy"}
	s := sink.NewS3Sink("my-bucket", "raw/events", "parquet", cfg)
	if s == nil {
		t.Fatal("NewS3Sink returned nil")
	}
	if s.Name() != "s3:my-bucket/raw/events" {
		t.Errorf("unexpected name: %s", s.Name())
	}
	if s.Format() != "parquet" {
		t.Errorf("expected format parquet, got %s", s.Format())
	}
}

func TestGCSSinkParquetFormat(t *testing.T) {
	cfg := map[string]any{"project": "my-project", "compression": "zstd"}
	s := sink.NewGCSSink("my-bucket", "raw/events", "parquet", cfg)
	if s == nil {
		t.Fatal("NewGCSSink returned nil")
	}
	if s.Name() != "gcs:my-bucket/raw/events" {
		t.Errorf("unexpected name: %s", s.Name())
	}
	if s.Format() != "parquet" {
		t.Errorf("expected format parquet, got %s", s.Format())
	}
}

// ═══════════════════════════════════════════
// CSV Encoder Tests
// ═══════════════════════════════════════════

func TestEncodeCSVBasic(t *testing.T) {
	events := makeTestEvents()
	data, err := sink.EncodeCSV(events, ',')
	if err != nil {
		t.Fatalf("EncodeCSV failed: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("EncodeCSV returned empty bytes")
	}

	// Parse CSV output
	r := csv.NewReader(strings.NewReader(string(data)))
	records, err := r.ReadAll()
	if err != nil {
		t.Fatalf("parse CSV output: %v", err)
	}

	// Should have header + 2 data rows
	if len(records) != 3 {
		t.Errorf("expected 3 rows (header + 2 data), got %d", len(records))
	}

	// Header should be sorted
	header := records[0]
	for i := 1; i < len(header); i++ {
		if header[i] < header[i-1] {
			t.Errorf("header not sorted: %v", header)
			break
		}
	}

	t.Logf("CSV output: %d bytes, %d columns, %d rows", len(data), len(header), len(records)-1)
}

func TestEncodeCSVAllKeys(t *testing.T) {
	// Events with different key sets — CSV should have union of all keys.
	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	events := []*pipeline.Event{
		{Value: map[string]any{"a": "1", "b": "2"}, Timestamp: ts, Topic: "t", Offset: 1},
		{Value: map[string]any{"b": "3", "c": "4"}, Timestamp: ts, Topic: "t", Offset: 2},
	}
	data, err := sink.EncodeCSV(events, ',')
	if err != nil {
		t.Fatalf("EncodeCSV failed: %v", err)
	}

	r := csv.NewReader(strings.NewReader(string(data)))
	records, err := r.ReadAll()
	if err != nil {
		t.Fatalf("parse CSV: %v", err)
	}

	header := records[0]
	// Should contain a, b, c plus metadata columns (_offset, _topic, _ts)
	colSet := make(map[string]bool)
	for _, h := range header {
		colSet[h] = true
	}
	for _, expected := range []string{"a", "b", "c", "_offset", "_topic", "_ts"} {
		if !colSet[expected] {
			t.Errorf("missing expected column %q in header %v", expected, header)
		}
	}
}

func TestEncodeCSVNested(t *testing.T) {
	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	events := []*pipeline.Event{
		{
			Value:     map[string]any{"name": "Alice", "addr": map[string]any{"city": "NYC", "zip": "10001"}},
			Timestamp: ts,
			Topic:     "t",
			Offset:    1,
		},
	}
	data, err := sink.EncodeCSV(events, ',')
	if err != nil {
		t.Fatalf("EncodeCSV failed: %v", err)
	}

	r := csv.NewReader(strings.NewReader(string(data)))
	records, err := r.ReadAll()
	if err != nil {
		t.Fatalf("parse CSV: %v", err)
	}

	// Find the addr column
	header := records[0]
	addrIdx := -1
	for i, h := range header {
		if h == "addr" {
			addrIdx = i
			break
		}
	}
	if addrIdx == -1 {
		t.Fatal("addr column not found in header")
	}

	// addr value should be JSON-serialized
	addrVal := records[1][addrIdx]
	var parsed map[string]any
	if err := json.Unmarshal([]byte(addrVal), &parsed); err != nil {
		t.Errorf("addr value should be valid JSON, got %q: %v", addrVal, err)
	}
	if parsed["city"] != "NYC" {
		t.Errorf("expected city=NYC, got %v", parsed["city"])
	}
}

func TestEncodeCSVDelimiter(t *testing.T) {
	events := makeTestEvents()

	cases := []struct {
		name  string
		delim rune
	}{
		{"tab", '\t'},
		{"semicolon", ';'},
		{"pipe", '|'},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := sink.EncodeCSV(events, tc.delim)
			if err != nil {
				t.Fatalf("EncodeCSV(%s) failed: %v", tc.name, err)
			}
			if len(data) == 0 {
				t.Fatal("empty output")
			}

			r := csv.NewReader(strings.NewReader(string(data)))
			r.Comma = tc.delim
			records, err := r.ReadAll()
			if err != nil {
				t.Fatalf("parse CSV with delimiter %q: %v", tc.delim, err)
			}
			if len(records) != 3 {
				t.Errorf("expected 3 rows, got %d", len(records))
			}
		})
	}
}

func TestEncodeCSVEmpty(t *testing.T) {
	data, err := sink.EncodeCSV(nil, ',')
	if err != nil {
		t.Fatalf("EncodeCSV(nil) should not error: %v", err)
	}
	if data != nil {
		t.Errorf("EncodeCSV(nil) should return nil, got %d bytes", len(data))
	}

	data, err = sink.EncodeCSV([]*pipeline.Event{}, ',')
	if err != nil {
		t.Fatalf("EncodeCSV([]) should not error: %v", err)
	}
	if data != nil {
		t.Errorf("EncodeCSV([]) should return nil, got %d bytes", len(data))
	}
}

func TestS3SinkCSVFormat(t *testing.T) {
	cfg := map[string]any{"region": "us-east-1", "csv_delimiter": ";"}
	s := sink.NewS3Sink("my-bucket", "raw/exports", "csv", cfg)
	if s == nil {
		t.Fatal("NewS3Sink returned nil")
	}
	if s.Format() != "csv" {
		t.Errorf("expected format csv, got %s", s.Format())
	}
}

func TestGCSSinkCSVFormat(t *testing.T) {
	cfg := map[string]any{"project": "my-project", "csv_delimiter": "\\t"}
	s := sink.NewGCSSink("my-bucket", "raw/exports", "csv", cfg)
	if s == nil {
		t.Fatal("NewGCSSink returned nil")
	}
	if s.Format() != "csv" {
		t.Errorf("expected format csv, got %s", s.Format())
	}
}

// ═══════════════════════════════════════════
// Format helpers tests
// ═══════════════════════════════════════════

func TestFormatExtension(t *testing.T) {
	cases := map[string]string{
		"json":    "json",
		"jsonl":   "jsonl",
		"parquet": "parquet",
		"csv":     "csv",
		"":        "jsonl",
		"unknown": "jsonl",
	}
	for input, expected := range cases {
		got := sink.FormatExtension(input)
		if got != expected {
			t.Errorf("FormatExtension(%q) = %q, want %q", input, got, expected)
		}
	}
}

func TestFormatContentType(t *testing.T) {
	cases := map[string]string{
		"json":    "application/json",
		"jsonl":   "application/x-ndjson",
		"parquet": "application/octet-stream",
		"csv":     "text/csv",
		"":        "application/x-ndjson",
	}
	for input, expected := range cases {
		got := sink.FormatContentType(input)
		if got != expected {
			t.Errorf("FormatContentType(%q) = %q, want %q", input, got, expected)
		}
	}
}

func TestParseCSVDelimiter(t *testing.T) {
	cases := []struct {
		input    string
		expected rune
	}{
		{",", ','},
		{"", ','},
		{";", ';'},
		{"|", '|'},
		{"\\t", '\t'},
		{"tab", '\t'},
	}
	for _, tc := range cases {
		got := sink.ParseCSVDelimiter(tc.input)
		if got != tc.expected {
			t.Errorf("ParseCSVDelimiter(%q) = %q, want %q", tc.input, got, tc.expected)
		}
	}
}

// ═══════════════════════════════════════════
// Vault Integration Tests
// ═══════════════════════════════════════════

func TestResolveChainNoVault(t *testing.T) {
	// Without Vault, resolution chain is: config -> env -> default
	sink.ResetVault()
	defer sink.ResetVault()

	cfg := map[string]any{"host": "from-config"}

	// 1. Config value takes priority
	got := sink.Resolve(cfg, "host", "MAKO_TEST_HOST_RESOLVE", "default-host")
	if got != "from-config" {
		t.Errorf("expected 'from-config', got %q", got)
	}

	// 2. Env var takes priority over default
	os.Setenv("MAKO_TEST_HOST_RESOLVE", "from-env")
	defer os.Unsetenv("MAKO_TEST_HOST_RESOLVE")
	got = sink.Resolve(nil, "host", "MAKO_TEST_HOST_RESOLVE", "default-host")
	if got != "from-env" {
		t.Errorf("expected 'from-env', got %q", got)
	}

	// 3. Default value when nothing else matches
	got = sink.Resolve(nil, "host", "MAKO_TEST_NONEXISTENT_VAR", "default-host")
	if got != "default-host" {
		t.Errorf("expected 'default-host', got %q", got)
	}

	// 4. Empty config key falls through to env
	cfg2 := map[string]any{"host": ""}
	got = sink.Resolve(cfg2, "host", "MAKO_TEST_HOST_RESOLVE", "default-host")
	if got != "from-env" {
		t.Errorf("expected 'from-env' for empty config, got %q", got)
	}
}

func TestResolveChainWithVaultPath(t *testing.T) {
	// When VAULT_ADDR is not set, New() returns nil — Vault is disabled
	sink.ResetVault()
	defer sink.ResetVault()

	// Simulate: no Vault configured, vault_path in config should be ignored
	cfg := map[string]any{
		"vault_path": "secret/data/mako/postgres",
	}
	got := sink.Resolve(cfg, "password", "", "default-pass")
	if got != "default-pass" {
		t.Errorf("expected 'default-pass' without Vault client, got %q", got)
	}

	// Config value still takes priority even with vault_path set
	cfg["password"] = "config-pass"
	got = sink.Resolve(cfg, "password", "", "default-pass")
	if got != "config-pass" {
		t.Errorf("expected 'config-pass', got %q", got)
	}
}

func TestVaultSpecParsing(t *testing.T) {
	yaml := `
apiVersion: mako/v1
kind: Pipeline
pipeline:
  name: vault-test
  vault:
    path: secret/data/mako
    ttl: 10m
  source:
    type: kafka
    topic: events.test
  sink:
    type: postgres
    database: mako
    table: users
    config:
      vault_path: secret/data/mako/postgres
`
	spec, err := config.Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	if spec.Pipeline.Vault == nil {
		t.Fatal("expected vault spec to be non-nil")
	}
	if spec.Pipeline.Vault.Path != "secret/data/mako" {
		t.Errorf("expected vault path 'secret/data/mako', got %q", spec.Pipeline.Vault.Path)
	}
	if spec.Pipeline.Vault.TTL != "10m" {
		t.Errorf("expected vault TTL '10m', got %q", spec.Pipeline.Vault.TTL)
	}

	// Verify vault_path in sink config
	vp, ok := spec.Pipeline.Sink.Config["vault_path"].(string)
	if !ok || vp != "secret/data/mako/postgres" {
		t.Errorf("expected sink vault_path 'secret/data/mako/postgres', got %q", vp)
	}
}

func TestVaultSpecOmitted(t *testing.T) {
	// When vault is not specified in YAML, it should be nil
	yaml := `
apiVersion: mako/v1
kind: Pipeline
pipeline:
  name: no-vault
  source:
    type: kafka
    topic: events.test
  sink:
    type: stdout
`
	spec, err := config.Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	if spec.Pipeline.Vault != nil {
		t.Errorf("expected vault spec to be nil when not specified, got %+v", spec.Pipeline.Vault)
	}
}

func TestVaultClientNilWhenNotConfigured(t *testing.T) {
	// Ensure VAULT_ADDR is not set
	oldAddr := os.Getenv("VAULT_ADDR")
	os.Unsetenv("VAULT_ADDR")
	defer func() {
		if oldAddr != "" {
			os.Setenv("VAULT_ADDR", oldAddr)
		}
	}()

	client, err := vault.New()
	if err != nil {
		t.Fatalf("vault.New() returned error: %v", err)
	}
	if client != nil {
		t.Error("expected nil client when VAULT_ADDR is not set")
	}
}

func TestVaultCacheTTL(t *testing.T) {
	// Test that the InitVaultWithTTL function parses TTL correctly
	sink.ResetVault()
	defer sink.ResetVault()

	// Without VAULT_ADDR, InitVaultWithTTL should return nil client
	oldAddr := os.Getenv("VAULT_ADDR")
	os.Unsetenv("VAULT_ADDR")
	defer func() {
		if oldAddr != "" {
			os.Setenv("VAULT_ADDR", oldAddr)
		}
	}()

	client, err := sink.InitVaultWithTTL("10m")
	if err != nil {
		t.Fatalf("InitVaultWithTTL returned error: %v", err)
	}
	if client != nil {
		t.Error("expected nil client when VAULT_ADDR not set")
	}
}

func TestResolveBackwardsCompatible(t *testing.T) {
	// Verify that Resolve() behaves identically to the old envOrConfig()
	// when Vault is not configured.
	sink.ResetVault()
	defer sink.ResetVault()

	tests := []struct {
		name       string
		cfg        map[string]any
		key        string
		envKey     string
		envVal     string
		defaultVal string
		expected   string
	}{
		{
			name:       "config value wins",
			cfg:        map[string]any{"host": "cfg-host"},
			key:        "host",
			envKey:     "MAKO_TEST_BC_HOST",
			defaultVal: "default",
			expected:   "cfg-host",
		},
		{
			name:       "env var wins over default",
			cfg:        nil,
			key:        "host",
			envKey:     "MAKO_TEST_BC_HOST2",
			envVal:     "env-host",
			defaultVal: "default",
			expected:   "env-host",
		},
		{
			name:       "default when nothing else",
			cfg:        map[string]any{},
			key:        "host",
			envKey:     "MAKO_TEST_BC_NONE",
			defaultVal: "fallback",
			expected:   "fallback",
		},
		{
			name:       "nil config falls through",
			cfg:        nil,
			key:        "password",
			envKey:     "MAKO_TEST_BC_NOENV",
			defaultVal: "secret",
			expected:   "secret",
		},
		{
			name:       "non-string config value falls through",
			cfg:        map[string]any{"port": 5432},
			key:        "port",
			envKey:     "MAKO_TEST_BC_PORT",
			defaultVal: "3306",
			expected:   "3306",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.envVal != "" {
				os.Setenv(tc.envKey, tc.envVal)
				defer os.Unsetenv(tc.envKey)
			}
			got := sink.Resolve(tc.cfg, tc.key, tc.envKey, tc.defaultVal)
			if got != tc.expected {
				t.Errorf("Resolve() = %q, want %q", got, tc.expected)
			}
		})
	}
}

func TestVaultClientGetNilSafe(t *testing.T) {
	// Calling Get on a nil client should return empty string
	var c *vault.Client
	got := c.Get("secret/data/test", "key")
	if got != "" {
		t.Errorf("expected empty string from nil client, got %q", got)
	}
}

func TestVaultClientTTLNilSafe(t *testing.T) {
	// TTL and AuthType should be safe on nil client
	var c *vault.Client
	if c.TTL() != 0 {
		t.Errorf("expected 0 TTL from nil client")
	}
	if c.AuthType() != "" {
		t.Errorf("expected empty auth type from nil client")
	}
}

// ═══════════════════════════════════════════
// Postgres CDC Source Tests
// ═══════════════════════════════════════════

func TestPostgresCDCSourceConfig(t *testing.T) {
	yaml := `
apiVersion: mako/v1
kind: Pipeline
pipeline:
  name: cdc-test
  source:
    type: postgres_cdc
    config:
      host: db.example.com
      port: "5433"
      user: cdc_user
      password: secret
      database: myapp
      tables: [users, orders, payments]
      schema: app
      mode: snapshot+cdc
      snapshot_batch_size: 5000
      snapshot_order_by: created_at
      slot_name: my_slot
      publication: my_pub
      start_lsn: "0/1234ABCD"
  sink:
    type: stdout
`
	spec, err := config.Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	if spec.Pipeline.Source.Type != v1.SourcePostgres {
		t.Errorf("expected source type postgres_cdc, got %q", spec.Pipeline.Source.Type)
	}

	src := source.NewPostgresCDCSource(spec.Pipeline.Source.Config)
	if src.Mode() != "snapshot+cdc" {
		t.Errorf("expected mode 'snapshot+cdc', got %q", src.Mode())
	}
	if src.Schema() != "app" {
		t.Errorf("expected schema 'app', got %q", src.Schema())
	}
	tables := src.Tables()
	if len(tables) != 3 || tables[0] != "users" || tables[1] != "orders" || tables[2] != "payments" {
		t.Errorf("unexpected tables: %v", tables)
	}
	if src.SlotName() != "my_slot" {
		t.Errorf("expected slot_name 'my_slot', got %q", src.SlotName())
	}
	if src.Publication() != "my_pub" {
		t.Errorf("expected publication 'my_pub', got %q", src.Publication())
	}
}

func TestPostgresCDCSourceModes(t *testing.T) {
	modes := []string{"snapshot", "cdc", "snapshot+cdc"}
	for _, mode := range modes {
		cfg := map[string]any{
			"tables": []any{"test_table"},
			"mode":   mode,
		}
		src := source.NewPostgresCDCSource(cfg)
		if src.Mode() != mode {
			t.Errorf("expected mode %q, got %q", mode, src.Mode())
		}
	}
}

func TestPostgresCDCSourceDSNConstruction(t *testing.T) {
	// Test DSN from individual fields
	cfg := map[string]any{
		"host":     "myhost",
		"port":     "5433",
		"user":     "myuser",
		"password": "mypass",
		"database": "mydb",
		"tables":   []any{"t1"},
	}
	src := source.NewPostgresCDCSource(cfg)
	dsn := src.DSN()
	if !strings.Contains(dsn, "myhost") {
		t.Errorf("DSN should contain host, got %q", dsn)
	}
	if !strings.Contains(dsn, "5433") {
		t.Errorf("DSN should contain port, got %q", dsn)
	}
	if !strings.Contains(dsn, "myuser") {
		t.Errorf("DSN should contain user, got %q", dsn)
	}
	if !strings.Contains(dsn, "mydb") {
		t.Errorf("DSN should contain database, got %q", dsn)
	}

	// Test DSN from explicit dsn config
	cfg2 := map[string]any{
		"dsn":    "postgres://explicit:pass@host:5432/db",
		"tables": []any{"t1"},
	}
	src2 := source.NewPostgresCDCSource(cfg2)
	if src2.DSN() != "postgres://explicit:pass@host:5432/db" {
		t.Errorf("expected explicit DSN, got %q", src2.DSN())
	}
}

func TestPostgresCDCSnapshotEvent(t *testing.T) {
	record := map[string]any{"id": 1, "name": "Alice", "email": "alice@example.com"}
	event := source.SnapshotEvent("users", "public", record, 42)

	if event.Topic != "public.users" {
		t.Errorf("expected topic 'public.users', got %q", event.Topic)
	}
	if event.Offset != 42 {
		t.Errorf("expected offset 42, got %d", event.Offset)
	}
	if event.Metadata["operation"] != "snapshot" {
		t.Errorf("expected operation 'snapshot', got %v", event.Metadata["operation"])
	}
	if event.Metadata["table"] != "users" {
		t.Errorf("expected table 'users', got %v", event.Metadata["table"])
	}
	if event.Value["name"] != "Alice" {
		t.Errorf("expected name 'Alice', got %v", event.Value["name"])
	}
}

func TestPostgresCDCEventFormat(t *testing.T) {
	tests := []struct {
		operation string
		lsn       string
	}{
		{"insert", "0/1234ABCD"},
		{"update", "0/5678EF01"},
		{"delete", "0/9ABC2345"},
	}

	for _, tc := range tests {
		record := map[string]any{"id": 1, "name": "Bob"}
		event := source.CDCEvent("orders", "public", tc.operation, tc.lsn, record, 0)

		if event.Metadata["operation"] != tc.operation {
			t.Errorf("expected operation %q, got %v", tc.operation, event.Metadata["operation"])
		}
		if event.Metadata["lsn"] != tc.lsn {
			t.Errorf("expected lsn %q, got %v", tc.lsn, event.Metadata["lsn"])
		}
		if event.Topic != "public.orders" {
			t.Errorf("expected topic 'public.orders', got %q", event.Topic)
		}
	}
}

// ═══════════════════════════════════════════
// HTTP Source Tests
// ═══════════════════════════════════════════

func TestHTTPSourceConfig(t *testing.T) {
	yaml := `
apiVersion: mako/v1
kind: Pipeline
pipeline:
  name: http-test
  source:
    type: http
    config:
      url: https://api.example.com/v1/users
      method: POST
      auth_type: bearer
      auth_token: my-token
      response_type: json
      data_path: data.results
      pagination_type: offset
      pagination_limit: 50
      poll_interval: 5m
      rate_limit_rps: 5
      timeout: 10s
      max_retries: 5
  sink:
    type: stdout
`
	spec, err := config.Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	if spec.Pipeline.Source.Type != v1.SourceHTTP {
		t.Errorf("expected source type http, got %q", spec.Pipeline.Source.Type)
	}

	src := source.NewHTTPSource(spec.Pipeline.Source.Config)
	if src.URL() != "https://api.example.com/v1/users" {
		t.Errorf("expected url, got %q", src.URL())
	}
	if src.Method() != "POST" {
		t.Errorf("expected POST, got %q", src.Method())
	}
	if src.AuthType() != "bearer" {
		t.Errorf("expected bearer auth, got %q", src.AuthType())
	}
	if src.ResponseType() != "json" {
		t.Errorf("expected json response type, got %q", src.ResponseType())
	}
	if src.DataPath() != "data.results" {
		t.Errorf("expected data path 'data.results', got %q", src.DataPath())
	}
	if src.PaginationType() != "offset" {
		t.Errorf("expected offset pagination, got %q", src.PaginationType())
	}
	if src.PollInterval() != 5*time.Minute {
		t.Errorf("expected 5m poll interval, got %v", src.PollInterval())
	}
}

func TestHTTPSourceParseResponse(t *testing.T) {
	// Test extracting records from nested JSON using data_path
	src := source.NewHTTPSource(map[string]any{
		"url":           "http://test",
		"response_type": "json",
		"data_path":     "data.results",
	})

	body := []byte(`{
		"data": {
			"results": [
				{"id": 1, "name": "Alice"},
				{"id": 2, "name": "Bob"}
			],
			"total": 2
		}
	}`)

	records, fullBody, err := src.ParseResponse(body)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
	if records[0]["name"] != "Alice" {
		t.Errorf("expected Alice, got %v", records[0]["name"])
	}
	if records[1]["name"] != "Bob" {
		t.Errorf("expected Bob, got %v", records[1]["name"])
	}
	// fullBody should contain total
	if fullBody == nil {
		t.Fatal("expected fullBody to be non-nil")
	}
}

func TestHTTPSourceParseResponseFlat(t *testing.T) {
	// Test extracting records from flat JSON array (no data_path)
	src := source.NewHTTPSource(map[string]any{
		"url":           "http://test",
		"response_type": "json",
	})

	body := []byte(`[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]`)

	records, _, err := src.ParseResponse(body)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
}

func TestHTTPSourceParseCSV(t *testing.T) {
	src := source.NewHTTPSource(map[string]any{
		"url":           "http://test",
		"response_type": "csv",
	})

	body := []byte("id,name,email\n1,Alice,alice@test.com\n2,Bob,bob@test.com\n")

	records, _, err := src.ParseResponse(body)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
	if records[0]["name"] != "Alice" {
		t.Errorf("expected Alice, got %v", records[0]["name"])
	}
	if records[1]["email"] != "bob@test.com" {
		t.Errorf("expected bob@test.com, got %v", records[1]["email"])
	}
}

func TestHTTPSourceAuth(t *testing.T) {
	tests := []struct {
		name     string
		cfg      map[string]any
		expected string // expected Authorization header prefix
	}{
		{
			name: "bearer",
			cfg: map[string]any{
				"url":        "http://test",
				"auth_type":  "bearer",
				"auth_token": "my-token-123",
			},
			expected: "Bearer my-token-123",
		},
		{
			name: "basic",
			cfg: map[string]any{
				"url":           "http://test",
				"auth_type":     "basic",
				"auth_user":     "admin",
				"auth_password": "secret",
			},
			expected: "Basic ",
		},
		{
			name: "api_key",
			cfg: map[string]any{
				"url":            "http://test",
				"auth_type":      "api_key",
				"api_key_header": "X-API-Key",
				"api_key_value":  "key-123",
			},
			expected: "", // not in Authorization header
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var receivedHeaders http.Header
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				receivedHeaders = r.Header.Clone()
				w.Write([]byte(`[]`))
			}))
			defer server.Close()

			tc.cfg["url"] = server.URL
			src := source.NewHTTPSource(tc.cfg)
			src.SetClient(server.Client())

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if err := src.Open(ctx); err != nil {
				t.Fatalf("open error: %v", err)
			}

			ch, err := src.Read(ctx)
			if err != nil {
				t.Fatalf("read error: %v", err)
			}

			// Drain events
			for range ch {
			}

			if tc.expected != "" {
				authHeader := receivedHeaders.Get("Authorization")
				if !strings.HasPrefix(authHeader, tc.expected) {
					t.Errorf("expected Authorization starting with %q, got %q", tc.expected, authHeader)
				}
			}

			if tc.name == "api_key" {
				apiKey := receivedHeaders.Get("X-API-Key")
				if apiKey != "key-123" {
					t.Errorf("expected X-API-Key 'key-123', got %q", apiKey)
				}
			}
		})
	}
}

func TestHTTPSourcePaginationOffset(t *testing.T) {
	var requestCount atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		offsetStr := r.URL.Query().Get("offset")
		offset, _ := strconv.Atoi(offsetStr)

		var records []map[string]any
		if offset == 0 {
			records = []map[string]any{{"id": 1}, {"id": 2}}
		} else if offset == 2 {
			records = []map[string]any{{"id": 3}}
		}
		// offset >= 3 returns empty

		resp := map[string]any{
			"data":  records,
			"total": 3,
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	src := source.NewHTTPSource(map[string]any{
		"url":                    server.URL,
		"response_type":          "json",
		"data_path":              "data",
		"pagination_type":        "offset",
		"pagination_limit":       2,
		"pagination_limit_param": "limit",
		"pagination_offset_param": "offset",
		"pagination_total_path":  "total",
	})
	src.SetClient(server.Client())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := src.Open(ctx); err != nil {
		t.Fatalf("open error: %v", err)
	}

	ch, err := src.Read(ctx)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}

	var events []*pipeline.Event
	for event := range ch {
		events = append(events, event)
	}

	if len(events) != 3 {
		t.Errorf("expected 3 events, got %d", len(events))
	}

	reqCount := requestCount.Load()
	if reqCount < 2 {
		t.Errorf("expected at least 2 HTTP requests for pagination, got %d", reqCount)
	}
}

func TestHTTPSourcePaginationCursor(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cursor := r.URL.Query().Get("cursor")

		var resp map[string]any
		if cursor == "" {
			resp = map[string]any{
				"data": []map[string]any{{"id": 1}, {"id": 2}},
				"meta": map[string]any{"next_cursor": "abc123"},
			}
		} else if cursor == "abc123" {
			resp = map[string]any{
				"data": []map[string]any{{"id": 3}},
				"meta": map[string]any{"next_cursor": nil},
			}
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	src := source.NewHTTPSource(map[string]any{
		"url":                     server.URL,
		"response_type":           "json",
		"data_path":               "data",
		"pagination_type":         "cursor",
		"pagination_limit":        10,
		"pagination_cursor_param": "cursor",
		"pagination_cursor_path":  "meta.next_cursor",
	})
	src.SetClient(server.Client())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := src.Open(ctx); err != nil {
		t.Fatalf("open error: %v", err)
	}

	ch, err := src.Read(ctx)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}

	var events []*pipeline.Event
	for event := range ch {
		events = append(events, event)
	}

	if len(events) != 3 {
		t.Errorf("expected 3 events from cursor pagination, got %d", len(events))
	}
}

func TestHTTPSourcePollInterval(t *testing.T) {
	var requestCount atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		json.NewEncoder(w).Encode([]map[string]any{{"id": requestCount.Load()}})
	}))
	defer server.Close()

	src := source.NewHTTPSource(map[string]any{
		"url":           server.URL,
		"response_type": "json",
		"poll_interval": "100ms",
	})
	src.SetClient(server.Client())

	ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
	defer cancel()

	if err := src.Open(ctx); err != nil {
		t.Fatalf("open error: %v", err)
	}

	ch, err := src.Read(ctx)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}

	var events []*pipeline.Event
	for event := range ch {
		events = append(events, event)
	}

	// Should have initial fetch + at least 1-2 polls in 350ms with 100ms interval
	if len(events) < 2 {
		t.Errorf("expected at least 2 events from polling, got %d", len(events))
	}
}

func TestHTTPSourceRateLimiter(t *testing.T) {
	src := source.NewHTTPSource(map[string]any{
		"url":             "http://test",
		"rate_limit_rps":  5,
		"rate_limit_burst": 10,
	})
	limiter := src.RateLimiter()
	if limiter == nil {
		t.Fatal("expected rate limiter to be non-nil")
	}
	if limiter.Limit() != 5 {
		t.Errorf("expected rate limit 5, got %v", limiter.Limit())
	}
	if limiter.Burst() != 10 {
		t.Errorf("expected burst 10, got %d", limiter.Burst())
	}
}

func TestHTTPSourceRetryOn429(t *testing.T) {
	var requestCount atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip HEAD requests from preflight check
		if r.Method == http.MethodHead {
			w.WriteHeader(200)
			return
		}
		count := requestCount.Add(1)
		if count <= 2 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(429)
			w.Write([]byte(`{"error": "rate limited"}`))
			return
		}
		json.NewEncoder(w).Encode([]map[string]any{{"id": 1}})
	}))
	defer server.Close()

	src := source.NewHTTPSource(map[string]any{
		"url":         server.URL,
		"max_retries": 3,
	})
	src.SetClient(server.Client())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := src.Open(ctx); err != nil {
		t.Fatalf("open error: %v", err)
	}

	ch, err := src.Read(ctx)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}

	var events []*pipeline.Event
	for event := range ch {
		events = append(events, event)
	}

	if len(events) != 1 {
		t.Errorf("expected 1 event after retries, got %d", len(events))
	}

	// Should have made 3 requests (2 retries + 1 success)
	if requestCount.Load() < 3 {
		t.Errorf("expected at least 3 requests, got %d", requestCount.Load())
	}
}

func TestHTTPSourceRetryOn5xx(t *testing.T) {
	var requestCount atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip HEAD requests from preflight check
		if r.Method == http.MethodHead {
			w.WriteHeader(200)
			return
		}
		count := requestCount.Add(1)
		if count == 1 {
			w.WriteHeader(503)
			w.Write([]byte(`{"error": "service unavailable"}`))
			return
		}
		json.NewEncoder(w).Encode([]map[string]any{{"id": 1}})
	}))
	defer server.Close()

	src := source.NewHTTPSource(map[string]any{
		"url":         server.URL,
		"max_retries": 3,
	})
	src.SetClient(server.Client())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := src.Open(ctx); err != nil {
		t.Fatalf("open error: %v", err)
	}

	ch, err := src.Read(ctx)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}

	var events []*pipeline.Event
	for event := range ch {
		events = append(events, event)
	}

	if len(events) != 1 {
		t.Errorf("expected 1 event after 5xx retry, got %d", len(events))
	}
}

func TestHTTPSourceNoRetryOn4xx(t *testing.T) {
	statusCodes := []int{400, 401, 403, 404}

	for _, code := range statusCodes {
		t.Run(fmt.Sprintf("status_%d", code), func(t *testing.T) {
			var requestCount atomic.Int32

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Skip HEAD requests from preflight check
				if r.Method == http.MethodHead {
					w.WriteHeader(code)
					return
				}
				requestCount.Add(1)
				w.WriteHeader(code)
				w.Write([]byte(`{"error": "client error"}`))
			}))
			defer server.Close()

			src := source.NewHTTPSource(map[string]any{
				"url":         server.URL,
				"max_retries": 3,
			})
			src.SetClient(server.Client())

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if err := src.Open(ctx); err != nil {
				t.Fatalf("open error: %v", err)
			}

			ch, err := src.Read(ctx)
			if err != nil {
				t.Fatalf("read error: %v", err)
			}

			// Drain events
			for range ch {
			}

			// Should only make 1 request (no retries on 4xx)
			if requestCount.Load() != 1 {
				t.Errorf("expected 1 request (no retry on %d), got %d", code, requestCount.Load())
			}
		})
	}
}

func TestHTTPSourceGetNestedValue(t *testing.T) {
	obj := map[string]any{
		"data": map[string]any{
			"results": []any{
				map[string]any{"id": 1},
			},
			"meta": map[string]any{
				"cursor": "abc",
			},
		},
	}

	// Test nested path
	val, ok := source.GetNestedValue(obj, "data.meta.cursor")
	if !ok {
		t.Fatal("expected to find value at data.meta.cursor")
	}
	if val != "abc" {
		t.Errorf("expected 'abc', got %v", val)
	}

	// Test missing path
	_, ok = source.GetNestedValue(obj, "data.missing.path")
	if ok {
		t.Error("expected not to find value at data.missing.path")
	}

	// Test top-level key
	val, ok = source.GetNestedValue(obj, "data")
	if !ok {
		t.Fatal("expected to find value at 'data'")
	}
	if _, ok := val.(map[string]any); !ok {
		t.Error("expected map at 'data'")
	}
}

func TestMain(m *testing.M) {
	// Change to project root for example loading
	if _, err := os.Stat("examples"); os.IsNotExist(err) {
		os.Chdir("..")
	}
	fmt.Println("🧪 Mako Test Suite")
	os.Exit(m.Run())
}
