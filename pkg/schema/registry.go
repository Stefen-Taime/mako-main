// Package schema implements Schema Registry validation for Mako pipelines.
//
// Supports Confluent Schema Registry HTTP API for JSON Schema and Avro
// validation at runtime. Events that fail validation are routed based on
// the pipeline's onFailure policy (reject, dlq, log).
package schema

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/hamba/avro/v2"
)

// Validator validates events against schemas from a Schema Registry.
type Validator struct {
	registryURL string
	subject     string
	enforce     bool
	onFailure   string // reject|dlq|log

	client     *http.Client
	schemaOnce sync.Once
	schema     *SchemaInfo
	schemaErr  error
	mu         sync.RWMutex

	// Parsed Avro schema (cached after first parse)
	avroSchema avro.Schema
	avroOnce   sync.Once
	avroErr    error
}

// SchemaInfo represents a schema fetched from the registry.
type SchemaInfo struct {
	ID         int    `json:"id"`
	Version    int    `json:"version"`
	Subject    string `json:"subject"`
	Schema     string `json:"schema"`
	SchemaType string `json:"schemaType"` // AVRO, JSON, PROTOBUF
	Fields     map[string]FieldDef
}

// FieldDef represents a field in a JSON schema.
type FieldDef struct {
	Type     string `json:"type"`
	Required bool   `json:"required"`
}

// ValidationResult contains the outcome of validating an event.
type ValidationResult struct {
	Valid   bool
	Errors  []string
	Subject string
	Version int
}

// NewValidator creates a Schema Registry validator.
func NewValidator(registryURL, subject string, enforce bool, onFailure string) *Validator {
	return &Validator{
		registryURL: strings.TrimRight(registryURL, "/"),
		subject:     subject,
		enforce:     enforce,
		onFailure:   onFailure,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// Validate validates an event against the latest schema for the subject.
// If enforcement is disabled, always returns valid.
func (v *Validator) Validate(ctx context.Context, event map[string]any) (*ValidationResult, error) {
	if !v.enforce {
		return &ValidationResult{Valid: true}, nil
	}

	// Fetch schema (cached after first call)
	schema, err := v.getSchema(ctx)
	if err != nil {
		// If we can't reach the registry, decide based on policy
		if v.onFailure == "log" {
			return &ValidationResult{
				Valid:  true,
				Errors: []string{fmt.Sprintf("schema registry unavailable: %v", err)},
			}, nil
		}
		return nil, fmt.Errorf("fetch schema: %w", err)
	}

	result := &ValidationResult{
		Subject: schema.Subject,
		Version: schema.Version,
	}

	// Validate based on schema type
	switch schema.SchemaType {
	case "JSON", "":
		v.validateJSON(event, schema, result)
	case "AVRO":
		v.validateAvro(event, schema, result)
	default:
		// For PROTOBUF and others, pass through with a warning
		result.Valid = true
		result.Errors = append(result.Errors,
			fmt.Sprintf("schema type %s: structural validation not yet supported", schema.SchemaType))
	}

	return result, nil
}

// validateJSON performs JSON Schema validation against the event.
func (v *Validator) validateJSON(event map[string]any, schema *SchemaInfo, result *ValidationResult) {
	result.Valid = true

	// Parse the JSON schema
	var schemaDef map[string]any
	if err := json.Unmarshal([]byte(schema.Schema), &schemaDef); err != nil {
		result.Valid = false
		result.Errors = append(result.Errors, fmt.Sprintf("invalid schema definition: %v", err))
		return
	}

	// Check required fields
	if required, ok := schemaDef["required"].([]any); ok {
		for _, r := range required {
			fieldName, ok := r.(string)
			if !ok {
				continue
			}
			if _, exists := event[fieldName]; !exists {
				result.Valid = false
				result.Errors = append(result.Errors,
					fmt.Sprintf("missing required field: %s", fieldName))
			}
		}
	}

	// Check field types from "properties"
	if props, ok := schemaDef["properties"].(map[string]any); ok {
		for fieldName, propDef := range props {
			val, exists := event[fieldName]
			if !exists {
				continue // Not required, skip
			}

			propMap, ok := propDef.(map[string]any)
			if !ok {
				continue
			}

			expectedType, ok := propMap["type"].(string)
			if !ok {
				continue
			}

			if !checkType(val, expectedType) {
				result.Valid = false
				result.Errors = append(result.Errors,
					fmt.Sprintf("field %s: expected type %s, got %T", fieldName, expectedType, val))
			}
		}
	}

	// Check for additionalProperties: false
	if addProps, ok := schemaDef["additionalProperties"]; ok {
		if addPropsBool, ok := addProps.(bool); ok && !addPropsBool {
			if props, ok := schemaDef["properties"].(map[string]any); ok {
				for fieldName := range event {
					if _, defined := props[fieldName]; !defined {
						result.Valid = false
						result.Errors = append(result.Errors,
							fmt.Sprintf("unexpected field: %s (additionalProperties: false)", fieldName))
					}
				}
			}
		}
	}
}

// validateAvro validates an event against an Avro schema.
// It parses the Avro schema from the Registry (cached after first parse),
// then attempts to marshal the event map to Avro binary format.
// If the event doesn't conform to the schema (wrong types, missing required
// fields, unknown enums, etc.), Marshal returns an error.
func (v *Validator) validateAvro(event map[string]any, schema *SchemaInfo, result *ValidationResult) {
	result.Valid = true

	// Parse the Avro schema (cached)
	parsedSchema, err := v.getAvroSchema(schema.Schema)
	if err != nil {
		result.Valid = false
		result.Errors = append(result.Errors, fmt.Sprintf("invalid avro schema: %v", err))
		return
	}

	// Coerce numeric types to match Avro expectations.
	// JSON unmarshals numbers as float64, but Avro expects int/long for integer fields.
	coerced := coerceForAvro(event, parsedSchema)

	// Marshal the event map to Avro binary. This validates that every field
	// matches the declared Avro type (string, int, long, float, double,
	// boolean, enum, nested records, arrays, maps, unions, etc.).
	_, err = avro.Marshal(parsedSchema, coerced)
	if err != nil {
		result.Valid = false
		result.Errors = append(result.Errors, fmt.Sprintf("avro validation: %v", err))
		return
	}
}

// coerceForAvro converts JSON-decoded values (float64 for all numbers) to the
// types expected by hamba/avro (int for "int", int64 for "long", float32 for
// "float", etc.). Without this, avro.Marshal would reject valid integer values
// because JSON always decodes them as float64.
func coerceForAvro(event map[string]any, schema avro.Schema) map[string]any {
	rec, ok := schema.(*avro.RecordSchema)
	if !ok {
		return event
	}

	result := make(map[string]any, len(event))
	for k, v := range event {
		result[k] = v
	}

	for _, field := range rec.Fields() {
		val, exists := result[field.Name()]
		if !exists {
			continue
		}
		result[field.Name()] = coerceValue(val, field.Type())
	}

	return result
}

// coerceValue converts a single value to match the target Avro type.
func coerceValue(val any, schema avro.Schema) any {
	switch s := schema.(type) {
	case *avro.PrimitiveSchema:
		f, ok := val.(float64)
		if !ok {
			return val
		}
		switch s.Type() {
		case avro.Int:
			return int(f)
		case avro.Long:
			return int64(f)
		case avro.Float:
			return float32(f)
		case avro.Double:
			return f // already float64
		}
	case *avro.UnionSchema:
		// For unions like ["null", "string"], try each non-null type
		if val == nil {
			return nil
		}
		for _, t := range s.Types() {
			if _, ok := t.(*avro.NullSchema); ok {
				continue
			}
			return coerceValue(val, t)
		}
	case *avro.RecordSchema:
		if m, ok := val.(map[string]any); ok {
			return coerceForAvro(m, s)
		}
	case *avro.ArraySchema:
		if arr, ok := val.([]any); ok {
			coerced := make([]any, len(arr))
			for i, item := range arr {
				coerced[i] = coerceValue(item, s.Items())
			}
			return coerced
		}
	}
	return val
}

// getAvroSchema parses and caches the Avro schema string.
func (v *Validator) getAvroSchema(schemaStr string) (avro.Schema, error) {
	v.avroOnce.Do(func() {
		v.avroSchema, v.avroErr = avro.Parse(schemaStr)
	})
	return v.avroSchema, v.avroErr
}

// checkType validates a Go value against a JSON Schema type.
func checkType(val any, expectedType string) bool {
	if val == nil {
		return expectedType == "null"
	}
	switch expectedType {
	case "string":
		_, ok := val.(string)
		return ok
	case "number":
		switch val.(type) {
		case float64, float32, int, int64, int32, json.Number:
			return true
		}
		return false
	case "integer":
		switch v := val.(type) {
		case float64:
			return v == float64(int64(v))
		case int, int64, int32:
			return true
		}
		return false
	case "boolean":
		_, ok := val.(bool)
		return ok
	case "object":
		_, ok := val.(map[string]any)
		return ok
	case "array":
		_, ok := val.([]any)
		return ok
	case "null":
		return val == nil
	}
	return true
}

// getSchema fetches and caches the latest schema for the subject.
func (v *Validator) getSchema(ctx context.Context) (*SchemaInfo, error) {
	v.schemaOnce.Do(func() {
		v.schema, v.schemaErr = v.fetchLatestSchema(ctx)
	})
	return v.schema, v.schemaErr
}

// RefreshSchema forces a re-fetch of the schema on next validation.
func (v *Validator) RefreshSchema() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.schemaOnce = sync.Once{}
	v.schema = nil
	v.schemaErr = nil
	// Also reset the cached Avro schema
	v.avroOnce = sync.Once{}
	v.avroSchema = nil
	v.avroErr = nil
}

// fetchLatestSchema retrieves the latest schema version from the registry.
func (v *Validator) fetchLatestSchema(ctx context.Context) (*SchemaInfo, error) {
	url := fmt.Sprintf("%s/subjects/%s/versions/latest", v.registryURL, v.subject)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.schemaregistry.v1+json")

	resp, err := v.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request schema: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("subject %q not found in registry", v.subject)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("registry error (%d): %s", resp.StatusCode, string(body))
	}

	var schema SchemaInfo
	if err := json.Unmarshal(body, &schema); err != nil {
		return nil, fmt.Errorf("parse schema response: %w", err)
	}

	// Default to JSON if not specified
	if schema.SchemaType == "" {
		schema.SchemaType = "JSON"
	}

	return &schema, nil
}

// CheckCompatibility checks if a schema is compatible with the subject.
func (v *Validator) CheckCompatibility(ctx context.Context, schemaJSON string) (bool, error) {
	url := fmt.Sprintf("%s/compatibility/subjects/%s/versions/latest",
		v.registryURL, v.subject)

	payload := map[string]string{
		"schema":     schemaJSON,
		"schemaType": "JSON",
	}
	data, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	resp, err := v.client.Do(req)
	if err != nil {
		return false, fmt.Errorf("compatibility check: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusNotFound {
		// No existing schema — compatible by default
		return true, nil
	}

	var result struct {
		IsCompatible bool `json:"is_compatible"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return false, fmt.Errorf("parse compatibility response: %w", err)
	}

	return result.IsCompatible, nil
}

// RegisterSchema registers a new schema for the subject.
func (v *Validator) RegisterSchema(ctx context.Context, schemaJSON, schemaType string) (int, error) {
	url := fmt.Sprintf("%s/subjects/%s/versions", v.registryURL, v.subject)

	if schemaType == "" {
		schemaType = "JSON"
	}

	payload := map[string]string{
		"schema":     schemaJSON,
		"schemaType": schemaType,
	}
	data, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	resp, err := v.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("register schema: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("register error (%d): %s", resp.StatusCode, string(body))
	}

	var result struct {
		ID int `json:"id"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return 0, fmt.Errorf("parse register response: %w", err)
	}

	return result.ID, nil
}

// OnFailure returns the configured failure policy.
func (v *Validator) OnFailure() string {
	return v.onFailure
}

// Enforce returns whether schema enforcement is enabled.
func (v *Validator) Enforce() bool {
	return v.enforce
}
