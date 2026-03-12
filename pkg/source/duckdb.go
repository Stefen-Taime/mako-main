package source

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/marcboeker/go-duckdb"

	"github.com/Stefen-Taime/mako/pkg/duckdbext"
	"github.com/Stefen-Taime/mako/pkg/pipeline"
	"github.com/Stefen-Taime/mako/pkg/sink"
)

// ═══════════════════════════════════════════
// DuckDB Source
// ═══════════════════════════════════════════

// DuckDBSource reads events from DuckDB via SQL queries.
// DuckDB can natively read Parquet, CSV, and JSON files (local or S3/GCS),
// making this source a versatile adapter for analytical queries.
//
// Configuration via YAML:
//
//	source:
//	  type: duckdb
//	  config:
//	    database: /path/to/analytics.duckdb   # ":memory:" by default
//	    table: events                          # simple SELECT * FROM <table>
//	    query: "SELECT * FROM read_parquet('s3://bucket/*.parquet')"
//	    batch_size: 10000                      # rows per batch (default: 10000)
type DuckDBSource struct {
	dsn       string // database path or ":memory:"
	query     string // SQL query to execute
	table     string // table name (used if query is empty)
	batchSize int
	config    map[string]any

	db      *sql.DB
	eventCh chan *pipeline.Event
	lag     atomic.Int64
	closed  atomic.Bool
	wg      sync.WaitGroup
}

// NewDuckDBSource creates a DuckDB source from config.
func NewDuckDBSource(cfg map[string]any) *DuckDBSource {
	dsn := strFromConfig(cfg, "database", ":memory:")
	query := strFromConfig(cfg, "query", "")
	table := strFromConfig(cfg, "table", "")
	batchSize := intFromConfig(cfg, "batch_size", 10000)

	return &DuckDBSource{
		dsn:       dsn,
		query:     query,
		table:     table,
		batchSize: batchSize,
		config:    cfg,
		eventCh:   make(chan *pipeline.Event, 1000),
	}
}

// Open establishes the DuckDB connection.
// If the query or table references remote paths (s3://, gs://, az://),
// the httpfs extension is automatically loaded and cloud credentials
// are configured from the YAML config or environment variables.
//
// For GCS paths (gs://), if no service account key is configured, Mako
// uses ADC proxy mode: it generates short-lived signed URLs via the Go SDK
// and rewrites the query to use HTTPS URLs that DuckDB httpfs can read
// without any GCS-specific auth.
func (s *DuckDBSource) Open(ctx context.Context) error {
	if s.query == "" && s.table == "" {
		return fmt.Errorf("duckdb source: either query or table is required")
	}

	db, err := sql.Open("duckdb", s.dsn)
	if err != nil {
		return fmt.Errorf("duckdb open %q: %w", s.dsn, err)
	}

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("duckdb ping: %w", err)
	}

	// Auto-detect cloud paths in query/table and load httpfs + credentials.
	queryOrTable := s.query
	if queryOrTable == "" {
		queryOrTable = s.table
	}
	if duckdbext.NeedsHTTPFSQuery(queryOrTable) || duckdbext.NeedsHTTPFS(queryOrTable) {
		// Try to get GCS HMAC credentials from Vault
		vc, _ := sink.InitVault()
		cc := duckdbext.CloudConfigWithVault(s.config, vc)

		// GCS ADC proxy: rewrite gs:// to signed URLs when no HMAC key is configured.
		if s.query != "" && duckdbext.NeedsGCSSignedURLs(s.query, cc) {
			rewritten, err := duckdbext.RewriteGCSToSignedURLs(ctx, s.query)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[duckdb] warning: GCS signed URL rewrite failed: %v (falling back to native auth)\n", err)
			} else {
				s.query = rewritten
			}
		}

		if err := duckdbext.LoadCloudExtensions(ctx, db, cc, "source"); err != nil {
			fmt.Fprintf(os.Stderr, "[duckdb] warning: %v\n", err)
		}
	}

	s.db = db
	fmt.Fprintf(os.Stderr, "[duckdb] connected to %s\n", s.dsn)
	return nil
}

// Read starts the query and returns the event channel.
func (s *DuckDBSource) Read(ctx context.Context) (<-chan *pipeline.Event, error) {
	s.wg.Add(1)
	go s.readLoop(ctx)
	return s.eventCh, nil
}

// readLoop executes the query and pushes rows as events.
func (s *DuckDBSource) readLoop(ctx context.Context) {
	defer s.wg.Done()
	defer func() {
		if s.closed.CompareAndSwap(false, true) {
			close(s.eventCh)
		}
	}()

	query := s.query
	if query == "" {
		query = fmt.Sprintf("SELECT * FROM %s", s.table)
	}

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[duckdb] query error: %v\n", err)
		return
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		fmt.Fprintf(os.Stderr, "[duckdb] columns error: %v\n", err)
		return
	}

	var offset int64
	for rows.Next() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Prepare scan targets
		values := make([]any, len(columns))
		scanArgs := make([]any, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		if err := rows.Scan(scanArgs...); err != nil {
			fmt.Fprintf(os.Stderr, "[duckdb] scan error at row %d: %v\n", offset, err)
			continue
		}

		// Convert row to map[string]any
		record := make(map[string]any, len(columns))
		for i, col := range columns {
			record[col] = convertDuckDBValue(values[i])
		}

		rawJSON, _ := json.Marshal(record)
		event := &pipeline.Event{
			Value:     record,
			RawValue:  rawJSON,
			Timestamp: time.Now(),
			Topic:     "duckdb",
			Partition: 0,
			Offset:    offset,
		}

		select {
		case s.eventCh <- event:
			offset++
			s.lag.Store(int64(len(s.eventCh)))
		case <-ctx.Done():
			return
		}

		if offset > 0 && offset%10000 == 0 {
			fmt.Fprintf(os.Stderr, "[duckdb] read %d rows\n", offset)
		}
	}

	if err := rows.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "[duckdb] rows error: %v\n", err)
	}

	fmt.Fprintf(os.Stderr, "[duckdb] finished: %d rows read\n", offset)
}

// convertDuckDBValue converts a database/sql scanned value to a JSON-friendly Go type.
func convertDuckDBValue(v any) any {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case []byte:
		// Try to parse as JSON (LIST, STRUCT, MAP types)
		var parsed any
		if err := json.Unmarshal(val, &parsed); err == nil {
			return parsed
		}
		return string(val)
	case time.Time:
		return val.Format(time.RFC3339)
	case int8:
		return int64(val)
	case int16:
		return int64(val)
	case int32:
		return int64(val)
	case int64:
		return val
	case uint8:
		return int64(val)
	case uint16:
		return int64(val)
	case uint32:
		return int64(val)
	case uint64:
		return val
	case float32:
		return float64(val)
	case float64:
		return val
	case bool:
		return val
	case string:
		return val
	default:
		// Fallback: serialize to string via JSON
		b, err := json.Marshal(val)
		if err != nil {
			return fmt.Sprintf("%v", val)
		}
		return string(b)
	}
}

// Close shuts down the DuckDB connection.
func (s *DuckDBSource) Close() error {
	s.wg.Wait()
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// Lag returns the channel backlog.
func (s *DuckDBSource) Lag() int64 {
	return s.lag.Load()
}
