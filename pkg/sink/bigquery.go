package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/Stefen-Taime/mako/pkg/pipeline"
)

// ═══════════════════════════════════════════
// BigQuery Sink (cloud.google.com/go/bigquery)
// ═══════════════════════════════════════════

// BigQuerySink writes events to BigQuery using the Go client library.
//
// When flatten is false (default), events are inserted as JSON rows using the
// streaming inserter with a flexible schema. When flatten is true, each
// top-level key in the event map becomes its own typed column (STRING,
// FLOAT64, INT64, BOOLEAN, TIMESTAMP, or JSON for nested structures). The
// table is auto-created on the first batch, and new columns are added
// dynamically via table.Update() when unseen keys appear.
//
// Authentication: uses Application Default Credentials (ADC).
// Set GOOGLE_APPLICATION_CREDENTIALS or run `gcloud auth application-default login`.
//
// Configuration via YAML:
//
//	sink:
//	  type: bigquery
//	  schema: my_dataset
//	  table: my_table
//	  flatten: true
//	  config:
//	    project: my-gcp-project
type BigQuerySink struct {
	project string
	dataset string
	table   string
	flatten bool
	config  map[string]any

	client   *bigquery.Client
	inserter *bigquery.Inserter
	mu       sync.Mutex

	// flatten mode state
	columns    []string          // ordered column names (set on first batch)
	columnSet  map[string]bool   // quick lookup for known columns
	columnType map[string]string // column name -> BigQuery type (STRING, FLOAT64, INT64, BOOLEAN, TIMESTAMP, JSON)
	tableReady bool              // true once CREATE TABLE has been executed
}

// NewBigQuerySink creates a BigQuery sink.
func NewBigQuerySink(project, dataset, table string, flatten bool, config map[string]any) *BigQuerySink {
	if project == "" && config != nil {
		if p, ok := config["project"].(string); ok {
			project = p
		}
	}
	if project == "" {
		project = os.Getenv("GCP_PROJECT")
	}
	if project == "" {
		project = os.Getenv("GOOGLE_CLOUD_PROJECT")
	}

	return &BigQuerySink{
		project:    project,
		dataset:    dataset,
		table:      table,
		flatten:    flatten,
		config:     config,
		columnSet:  make(map[string]bool),
		columnType: make(map[string]string),
	}
}

// bqEvent implements bigquery.ValueSaver for streaming inserts.
type bqEvent struct {
	InsertID string
	Data     map[string]any
	Topic    string
	Offset   int64
	LoadedAt time.Time
}

// Save implements bigquery.ValueSaver.
func (e *bqEvent) Save() (row map[string]bigquery.Value, insertID string, err error) {
	row = make(map[string]bigquery.Value, len(e.Data)+3)

	// Flatten event data into top-level columns
	for k, v := range e.Data {
		row[k] = v
	}

	// Add metadata columns
	row["_mako_topic"] = e.Topic
	row["_mako_offset"] = e.Offset
	row["_mako_loaded_at"] = e.LoadedAt

	return row, e.InsertID, nil
}

// Open creates the BigQuery client and configures the inserter.
func (s *BigQuerySink) Open(ctx context.Context) error {
	if s.project == "" {
		return fmt.Errorf("bigquery: project not configured (set GCP_PROJECT env or config.project)")
	}
	if s.dataset == "" {
		return fmt.Errorf("bigquery: dataset (schema) not configured")
	}
	if s.table == "" {
		return fmt.Errorf("bigquery: table not configured")
	}

	client, err := bigquery.NewClient(ctx, s.project)
	if err != nil {
		return fmt.Errorf("bigquery client: %w", err)
	}

	s.client = client

	// Configure the inserter
	tableRef := client.Dataset(s.dataset).Table(s.table)
	s.inserter = tableRef.Inserter()

	// Skip invalid rows and allow unknown values for flexibility
	s.inserter.SkipInvalidRows = false
	s.inserter.IgnoreUnknownValues = true

	fmt.Fprintf(os.Stderr, "[bigquery] connected to %s.%s.%s (flatten=%v)\n",
		s.project, s.dataset, s.table, s.flatten)
	return nil
}

// Write streams events to BigQuery using the streaming inserter.
//
// In flatten mode, the table is auto-created from event keys and data is
// inserted into typed columns. In non-flatten mode (default), events are
// inserted using the flexible bqEvent ValueSaver.
func (s *BigQuerySink) Write(ctx context.Context, events []*pipeline.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(events) == 0 {
		return nil
	}

	// Flatten mode: ensure table exists and handle schema evolution.
	if s.flatten {
		if err := s.ensureFlatTable(ctx, events); err != nil {
			return err
		}
		return s.writeFlatBatch(ctx, events)
	}

	// Non-flatten mode: flexible ValueSaver insert.
	rows := make([]*bqEvent, 0, len(events))
	for _, event := range events {
		insertID := fmt.Sprintf("%s-%d-%d", event.Topic, event.Partition, event.Offset)

		rows = append(rows, &bqEvent{
			InsertID: insertID,
			Data:     event.Value,
			Topic:    event.Topic,
			Offset:   event.Offset,
			LoadedAt: time.Now(),
		})
	}

	if err := s.inserter.Put(ctx, rows); err != nil {
		// Check for partial insert errors
		if multiErr, ok := err.(bigquery.PutMultiError); ok {
			var errMsgs []string
			for _, rowErr := range multiErr {
				for _, e := range rowErr.Errors {
					errMsgs = append(errMsgs, e.Error())
				}
			}
			return fmt.Errorf("bigquery partial insert: %s", joinMax(errMsgs, 3))
		}
		return fmt.Errorf("bigquery insert: %w", err)
	}

	return nil
}

// joinMax joins up to maxN strings with "; ".
func joinMax(msgs []string, maxN int) string {
	if len(msgs) <= maxN {
		result := ""
		for i, m := range msgs {
			if i > 0 {
				result += "; "
			}
			result += m
		}
		return result
	}
	result := ""
	for i := 0; i < maxN; i++ {
		if i > 0 {
			result += "; "
		}
		result += msgs[i]
	}
	return fmt.Sprintf("%s (and %d more)", result, len(msgs)-maxN)
}

// ═══════════════════════════════════════════
// Flatten mode
// ═══════════════════════════════════════════

// BqColumnType maps a Go value to a BigQuery column type.
// Exported for testing.
func BqColumnType(v any) string {
	return bqColumnType(v)
}

// bqColumnType maps a Go value to a BigQuery column type.
func bqColumnType(v any) string {
	switch val := v.(type) {
	case float64, float32:
		return "FLOAT64"
	case int, int64:
		return "INT64"
	case bool:
		return "BOOLEAN"
	case string:
		if isTimestampLike(val) {
			return "TIMESTAMP"
		}
		return "STRING"
	case map[string]any, []any:
		return "JSON"
	default:
		return "STRING"
	}
}

// bqFieldType converts a string type name to a bigquery.FieldType.
func bqFieldType(typeName string) bigquery.FieldType {
	switch typeName {
	case "STRING":
		return bigquery.StringFieldType
	case "FLOAT64":
		return bigquery.FloatFieldType
	case "INT64":
		return bigquery.IntegerFieldType
	case "BOOLEAN":
		return bigquery.BooleanFieldType
	case "TIMESTAMP":
		return bigquery.TimestampFieldType
	case "JSON":
		return bigquery.JSONFieldType
	default:
		return bigquery.StringFieldType
	}
}

// ensureFlatTable creates the table on the first call, and adds any new
// columns discovered in subsequent batches via table.Update().
func (s *BigQuerySink) ensureFlatTable(ctx context.Context, events []*pipeline.Event) error {
	newKeys := make(map[string]any)
	for _, event := range events {
		for k, v := range event.Value {
			if !s.columnSet[k] {
				if _, seen := newKeys[k]; !seen {
					newKeys[k] = v
				}
			}
		}
	}

	if len(newKeys) == 0 {
		return nil
	}

	tableRef := s.client.Dataset(s.dataset).Table(s.table)

	if !s.tableReady {
		// First batch: create table with discovered columns.
		var colNames []string
		for k := range newKeys {
			colNames = append(colNames, k)
		}
		sort.Strings(colNames)

		schema := make(bigquery.Schema, 0, len(colNames)+1)
		for _, k := range colNames {
			schema = append(schema, &bigquery.FieldSchema{
				Name: k,
				Type: bqFieldType(bqColumnType(newKeys[k])),
			})
		}
		// Add loaded_at timestamp column
		schema = append(schema, &bigquery.FieldSchema{
			Name: "loaded_at",
			Type: bigquery.TimestampFieldType,
		})

		md := &bigquery.TableMetadata{Schema: schema}
		if err := tableRef.Create(ctx, md); err != nil {
			// If table already exists, that's fine — we'll use it.
			if !isAlreadyExistsError(err) {
				return fmt.Errorf("bigquery create flat table: %w", err)
			}
		}

		s.tableReady = true
		s.columns = colNames
		for _, k := range colNames {
			s.columnSet[k] = true
			s.columnType[k] = bqColumnType(newKeys[k])
		}
		fmt.Fprintf(os.Stderr, "[bigquery] created flat table %s.%s.%s with %d columns\n",
			s.project, s.dataset, s.table, len(colNames))
		return nil
	}

	// Subsequent batches: add new columns via table.Update().
	var added []string
	var newFields bigquery.Schema
	for k := range newKeys {
		colType := bqColumnType(newKeys[k])
		newFields = append(newFields, &bigquery.FieldSchema{
			Name: k,
			Type: bqFieldType(colType),
		})
		s.columns = append(s.columns, k)
		s.columnSet[k] = true
		s.columnType[k] = colType
		added = append(added, k)
	}

	if len(newFields) > 0 {
		// Get existing metadata to extend the schema.
		md, err := tableRef.Metadata(ctx)
		if err != nil {
			return fmt.Errorf("bigquery get table metadata: %w", err)
		}
		updatedSchema := append(md.Schema, newFields...)
		update := bigquery.TableMetadataToUpdate{Schema: updatedSchema}
		if _, err := tableRef.Update(ctx, update, md.ETag); err != nil {
			return fmt.Errorf("bigquery update table schema: %w", err)
		}
		sort.Strings(s.columns)
		fmt.Fprintf(os.Stderr, "[bigquery] added columns: %v\n", added)
	}

	return nil
}

// isAlreadyExistsError checks if the error indicates a resource already exists.
func isAlreadyExistsError(err error) bool {
	if err == nil {
		return false
	}
	// Google API errors contain "Already Exists" or status code 409
	errStr := err.Error()
	return contains(errStr, "Already Exists") || contains(errStr, "409") || contains(errStr, "already exists")
}

// contains is a simple case-sensitive substring check.
func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// bqFlatEvent implements bigquery.ValueSaver for flatten mode streaming inserts.
type bqFlatEvent struct {
	InsertID string
	Columns  []string
	Types    map[string]string
	Values   map[string]any
	LoadedAt time.Time
}

// Save implements bigquery.ValueSaver for flatten mode.
func (e *bqFlatEvent) Save() (row map[string]bigquery.Value, insertID string, err error) {
	row = make(map[string]bigquery.Value, len(e.Columns)+1)
	for _, col := range e.Columns {
		v, ok := e.Values[col]
		if !ok {
			continue
		}
		// JSON-serialize nested objects/arrays
		switch v.(type) {
		case map[string]any, []any:
			b, merr := json.Marshal(v)
			if merr != nil {
				return nil, "", fmt.Errorf("marshal nested field %s: %w", col, merr)
			}
			row[col] = string(b)
		default:
			row[col] = v
		}
	}
	row["loaded_at"] = e.LoadedAt
	return row, e.InsertID, nil
}

// writeFlatBatch inserts events into individually typed columns using the
// BigQuery streaming inserter with typed ValueSavers.
func (s *BigQuerySink) writeFlatBatch(ctx context.Context, events []*pipeline.Event) error {
	rows := make([]*bqFlatEvent, 0, len(events))
	for _, event := range events {
		insertID := fmt.Sprintf("%s-%d-%d", event.Topic, event.Partition, event.Offset)
		rows = append(rows, &bqFlatEvent{
			InsertID: insertID,
			Columns:  s.columns,
			Types:    s.columnType,
			Values:   event.Value,
			LoadedAt: time.Now(),
		})
	}

	if err := s.inserter.Put(ctx, rows); err != nil {
		if multiErr, ok := err.(bigquery.PutMultiError); ok {
			var errMsgs []string
			for _, rowErr := range multiErr {
				for _, e := range rowErr.Errors {
					errMsgs = append(errMsgs, e.Error())
				}
			}
			return fmt.Errorf("bigquery flat insert: %s", joinMax(errMsgs, 3))
		}
		return fmt.Errorf("bigquery flat insert: %w", err)
	}

	return nil
}

// Flush is a no-op (streaming inserts are immediate).
func (s *BigQuerySink) Flush(ctx context.Context) error {
	return nil
}

// Close shuts down the BigQuery client.
func (s *BigQuerySink) Close() error {
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}

// Name returns the sink identifier.
func (s *BigQuerySink) Name() string {
	return fmt.Sprintf("bigquery:%s.%s.%s", s.project, s.dataset, s.table)
}

// bqEventJSON is an alternative ValueSaver that stores the entire event
// as a JSON string in a single column. Useful for schema-flexible tables.
type bqEventJSON struct {
	InsertID  string
	EventJSON string
	EventKey  string
	Topic     string
	Offset    int64
	LoadedAt  time.Time
}

// Save implements bigquery.ValueSaver for JSON-column tables.
func (e *bqEventJSON) Save() (row map[string]bigquery.Value, insertID string, err error) {
	row = map[string]bigquery.Value{
		"event_data":   e.EventJSON,
		"event_key":    e.EventKey,
		"event_topic":  e.Topic,
		"event_offset": e.Offset,
		"loaded_at":    e.LoadedAt,
	}
	return row, e.InsertID, nil
}

// WriteJSON inserts events as JSON strings (for tables with a STRING event_data column).
func (s *BigQuerySink) WriteJSON(ctx context.Context, events []*pipeline.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(events) == 0 {
		return nil
	}

	rows := make([]*bqEventJSON, 0, len(events))
	for _, event := range events {
		data, err := json.Marshal(event.Value)
		if err != nil {
			return fmt.Errorf("marshal event: %w", err)
		}

		insertID := fmt.Sprintf("%s-%d-%d", event.Topic, event.Partition, event.Offset)
		rows = append(rows, &bqEventJSON{
			InsertID:  insertID,
			EventJSON: string(data),
			EventKey:  string(event.Key),
			Topic:     event.Topic,
			Offset:    event.Offset,
			LoadedAt:  time.Now(),
		})
	}

	if err := s.inserter.Put(ctx, rows); err != nil {
		return fmt.Errorf("bigquery json insert: %w", err)
	}

	return nil
}
