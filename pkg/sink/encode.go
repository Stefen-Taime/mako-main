package sink

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/Stefen-Taime/mako/pkg/pipeline"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/compress/gzip"
	"github.com/parquet-go/parquet-go/compress/snappy"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

// ═══════════════════════════════════════════
// Shared encoders for S3 / GCS sinks
// ═══════════════════════════════════════════

// ── Parquet ──────────────────────────────────

// EncodeParquet converts a batch of events into a Parquet byte buffer.
// Schema is auto-discovered from all events (union of all keys).
// Compression options: "snappy" (default), "zstd", "gzip", "none".
func EncodeParquet(events []*pipeline.Event, compression string) ([]byte, error) {
	if len(events) == 0 {
		return nil, nil
	}

	// 1. Build records and discover all columns.
	records := make([]map[string]any, 0, len(events))
	colSet := make(map[string]bool)
	// Track a sample value for each column for type inference.
	colSample := make(map[string]any)

	for _, e := range events {
		rec := buildRecord(e)
		records = append(records, rec)
		for k, v := range rec {
			if !colSet[k] {
				colSet[k] = true
				colSample[k] = v
			}
		}
	}

	// 2. Sort columns for deterministic output.
	columns := make([]string, 0, len(colSet))
	for k := range colSet {
		columns = append(columns, k)
	}
	sort.Strings(columns)

	// 3. Build parquet schema from discovered columns.
	nodes := make([]parquet.Node, len(columns))
	for i, col := range columns {
		nodes[i] = buildParquetNode(col, colSample[col])
	}

	schema := parquet.NewSchema("event", parquet.Group(
		namedNodes(columns, nodes),
	))

	// 4. Select compression codec.
	codec := selectParquetCompression(compression)

	// 5. Write rows to an in-memory buffer.
	var buf bytes.Buffer
	writerOpts := []parquet.WriterOption{
		schema,
	}
	if codec != nil {
		writerOpts = append(writerOpts, parquet.Compression(codec))
	}

	writer := parquet.NewGenericWriter[map[string]any](&buf, writerOpts...)

	// Convert records: flatten nested objects/arrays to JSON strings.
	for _, rec := range records {
		row := make(map[string]any, len(columns))
		for _, col := range columns {
			v, ok := rec[col]
			if !ok {
				row[col] = nil
				continue
			}
			switch v.(type) {
			case map[string]any, []any:
				b, err := json.Marshal(v)
				if err != nil {
					return nil, fmt.Errorf("parquet marshal nested field %s: %w", col, err)
				}
				row[col] = string(b)
			default:
				row[col] = v
			}
		}
		if _, err := writer.Write([]map[string]any{row}); err != nil {
			return nil, fmt.Errorf("parquet write row: %w", err)
		}
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("parquet close: %w", err)
	}

	return buf.Bytes(), nil
}

// buildParquetNode creates a parquet.Node for a given column name and sample value.
func buildParquetNode(name string, sample any) parquet.Node {
	switch sample.(type) {
	case float64, float32:
		return parquet.Optional(parquet.Leaf(parquet.DoubleType))
	case int, int64:
		return parquet.Optional(parquet.Leaf(parquet.Int64Type))
	case bool:
		return parquet.Optional(parquet.Leaf(parquet.BooleanType))
	default:
		// string, nested objects (JSON-serialized), nil, timestamps — all stored as string
		return parquet.Optional(parquet.String())
	}
}

// namedNodes builds a map of column name -> Node for parquet.Group.
func namedNodes(names []string, nodes []parquet.Node) map[string]parquet.Node {
	m := make(map[string]parquet.Node, len(names))
	for i, n := range names {
		m[n] = nodes[i]
	}
	return m
}

// selectParquetCompression returns the compress.Codec for the given name.
func selectParquetCompression(name string) compress.Codec {
	switch name {
	case "zstd":
		return &zstd.Codec{Level: zstd.DefaultLevel}
	case "gzip":
		return &gzip.Codec{}
	case "none", "uncompressed":
		return nil // parquet-go uses no compression when nil
	default:
		// Default: snappy
		return &snappy.Codec{}
	}
}

// ── CSV ──────────────────────────────────────

// EncodeCSV converts a batch of events into RFC 4180 CSV bytes.
// Columns are the union of all keys across all events, sorted alphabetically.
// Nested objects/arrays are JSON-serialized. Nil values become empty strings.
// Delimiter defaults to comma; use '\t' for TSV, ';' for semicolons, etc.
func EncodeCSV(events []*pipeline.Event, delimiter rune) ([]byte, error) {
	if len(events) == 0 {
		return nil, nil
	}

	if delimiter == 0 {
		delimiter = ','
	}

	// 1. Build records and discover all columns.
	records := make([]map[string]any, 0, len(events))
	colSet := make(map[string]bool)
	for _, e := range events {
		rec := buildRecord(e)
		records = append(records, rec)
		for k := range rec {
			colSet[k] = true
		}
	}

	// 2. Sort columns for deterministic output.
	columns := make([]string, 0, len(colSet))
	for k := range colSet {
		columns = append(columns, k)
	}
	sort.Strings(columns)

	// 3. Write CSV to buffer.
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	w.Comma = delimiter

	// Header row
	if err := w.Write(columns); err != nil {
		return nil, fmt.Errorf("csv write header: %w", err)
	}

	// Data rows
	for _, rec := range records {
		row := make([]string, len(columns))
		for i, col := range columns {
			v, ok := rec[col]
			if !ok || v == nil {
				row[i] = ""
				continue
			}
			row[i] = formatCSVValue(v)
		}
		if err := w.Write(row); err != nil {
			return nil, fmt.Errorf("csv write row: %w", err)
		}
	}

	w.Flush()
	if err := w.Error(); err != nil {
		return nil, fmt.Errorf("csv flush: %w", err)
	}

	return buf.Bytes(), nil
}

// formatCSVValue converts a value to its CSV string representation.
func formatCSVValue(v any) string {
	switch val := v.(type) {
	case string:
		return val
	case float64:
		return fmt.Sprintf("%g", val)
	case float32:
		return fmt.Sprintf("%g", val)
	case int:
		return fmt.Sprintf("%d", val)
	case int64:
		return fmt.Sprintf("%d", val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	case time.Time:
		return val.Format(time.RFC3339)
	case map[string]any, []any:
		b, err := json.Marshal(val)
		if err != nil {
			return fmt.Sprintf("%v", val)
		}
		return string(b)
	default:
		return fmt.Sprintf("%v", val)
	}
}

// ── Helpers ──────────────────────────────────

// ParseCSVDelimiter parses the csv_delimiter config value into a rune.
// Supports: ",", ";", "\t", "tab", "|", or any single character.
func ParseCSVDelimiter(s string) rune {
	switch s {
	case "", ",":
		return ','
	case "\\t", "tab", "\t":
		return '\t'
	case ";":
		return ';'
	case "|":
		return '|'
	default:
		runes := []rune(s)
		if len(runes) > 0 {
			return runes[0]
		}
		return ','
	}
}

// FormatExtension returns the file extension for a given format.
func FormatExtension(format string) string {
	switch format {
	case "json":
		return "json"
	case "parquet":
		return "parquet"
	case "csv":
		return "csv"
	default:
		return "jsonl"
	}
}

// FormatContentType returns the HTTP content type for a given format.
func FormatContentType(format string) string {
	switch format {
	case "json":
		return "application/json"
	case "parquet":
		return "application/octet-stream"
	case "csv":
		return "text/csv"
	default:
		return "application/x-ndjson"
	}
}
