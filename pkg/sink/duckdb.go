package sink

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	_ "github.com/marcboeker/go-duckdb"

	"github.com/Stefen-Taime/mako/pkg/duckdbext"
	"github.com/Stefen-Taime/mako/pkg/pipeline"
)

// ═══════════════════════════════════════════
// DuckDB Sink
// ═══════════════════════════════════════════

// DuckDBSink writes events to a DuckDB database. Supports auto-table
// creation with typed columns (like flatten mode) and optional export
// to Parquet/CSV/JSON at close time via DuckDB's native COPY command.
//
// Configuration via YAML:
//
//	sink:
//	  type: duckdb
//	  database: /path/to/output.duckdb
//	  table: events
//	  config:
//	    create_table: true                  # auto-create table (default: true)
//	    export_path: /data/output/events.parquet
//	    export_format: parquet              # parquet|csv|json
//	    export_partition_by: [year, month]  # Hive-style partitioning
type DuckDBSink struct {
	dsn          string
	table        string
	createTable  bool
	exportPath   string
	exportFormat string
	exportPartBy []string
	config       map[string]any

	db         *sql.DB
	mu         sync.Mutex
	columns    []string
	columnSet  map[string]bool
	tableReady bool
}

// NewDuckDBSink creates a DuckDB sink.
func NewDuckDBSink(database, table string, cfg map[string]any) *DuckDBSink {
	dsn := database
	if dsn == "" {
		dsn = Resolve(cfg, "database", "DUCKDB_DATABASE", ":memory:")
	}

	createTable := true
	if cfg != nil {
		if v, ok := cfg["create_table"].(bool); ok {
			createTable = v
		}
	}

	exportPath := ""
	exportFormat := "parquet"
	var exportPartBy []string

	if cfg != nil {
		if v, ok := cfg["export_path"].(string); ok {
			exportPath = v
		}
		if v, ok := cfg["export_format"].(string); ok && v != "" {
			exportFormat = v
		}
		if v, ok := cfg["export_partition_by"].([]any); ok {
			for _, item := range v {
				if s, ok := item.(string); ok {
					exportPartBy = append(exportPartBy, s)
				}
			}
		}
	}

	return &DuckDBSink{
		dsn:          dsn,
		table:        table,
		createTable:  createTable,
		exportPath:   exportPath,
		exportFormat: exportFormat,
		exportPartBy: exportPartBy,
		config:       cfg,
		columnSet:    make(map[string]bool),
	}
}

// Open establishes the DuckDB connection.
func (s *DuckDBSink) Open(ctx context.Context) error {
	db, err := sql.Open("duckdb", s.dsn)
	if err != nil {
		return fmt.Errorf("duckdb sink open %q: %w", s.dsn, err)
	}

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("duckdb sink ping: %w", err)
	}

	// Load httpfs extension + cloud credentials for remote export paths (S3/GCS/Azure)
	if duckdbext.NeedsHTTPFS(s.exportPath) {
		cc := duckdbext.CloudConfigFromMap(s.config)
		if err := duckdbext.LoadCloudExtensions(ctx, db, cc, "sink"); err != nil {
			fmt.Fprintf(os.Stderr, "[duckdb] warning: %v\n", err)
		}
	}

	s.db = db
	fmt.Fprintf(os.Stderr, "[duckdb] sink connected to %s (table=%s)\n", s.dsn, s.table)
	return nil
}

// Write inserts events into DuckDB.
func (s *DuckDBSink) Write(ctx context.Context, events []*pipeline.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(events) == 0 {
		return nil
	}

	// Ensure table exists and handle schema evolution
	if s.createTable {
		if err := s.ensureTable(ctx, events); err != nil {
			return err
		}
	}

	return s.insertBatch(ctx, events)
}

// duckDBColumnType maps a Go value to a DuckDB column type.
func duckDBColumnType(v any) string {
	switch v.(type) {
	case float64, float32:
		return "DOUBLE"
	case int, int64, int32, int16, int8:
		return "BIGINT"
	case bool:
		return "BOOLEAN"
	case string:
		if isTimestampLike(v.(string)) {
			return "TIMESTAMP"
		}
		return "VARCHAR"
	case map[string]any, []any, []string:
		return "JSON"
	default:
		return "VARCHAR"
	}
}

// ensureTable creates the table on first call and adds new columns on subsequent calls.
func (s *DuckDBSink) ensureTable(ctx context.Context, events []*pipeline.Event) error {
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

	quotedTable := quoteIdent(s.table)

	if !s.tableReady {
		// First batch: CREATE TABLE
		var colDefs []string
		var colNames []string
		for k := range newKeys {
			colNames = append(colNames, k)
		}
		sort.Strings(colNames)

		for _, k := range colNames {
			colDefs = append(colDefs, fmt.Sprintf("%s %s", quoteIdent(k), duckDBColumnType(newKeys[k])))
		}
		// Only add loaded_at if the source data doesn't already contain it
		if _, hasLoadedAt := newKeys["loaded_at"]; !hasLoadedAt {
			colDefs = append(colDefs, "loaded_at TIMESTAMP DEFAULT current_timestamp")
		}

		createDDL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n\t%s\n)",
			quotedTable, strings.Join(colDefs, ",\n\t"))
		if _, err := s.db.ExecContext(ctx, createDDL); err != nil {
			return fmt.Errorf("duckdb create table: %w", err)
		}
		s.tableReady = true
		s.columns = colNames
		for _, k := range colNames {
			s.columnSet[k] = true
		}
		// Track loaded_at even if auto-added, so schema evolution skips it
		s.columnSet["loaded_at"] = true
		fmt.Fprintf(os.Stderr, "[duckdb] created table %s with %d columns\n",
			quotedTable, len(colNames))
		return nil
	}

	// Subsequent batches: ALTER TABLE ADD COLUMN
	var added []string
	for k := range newKeys {
		colType := duckDBColumnType(newKeys[k])
		alterDDL := fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s",
			quotedTable, quoteIdent(k), colType)
		if _, err := s.db.ExecContext(ctx, alterDDL); err != nil {
			return fmt.Errorf("duckdb alter table add column %s: %w", k, err)
		}
		s.columns = append(s.columns, k)
		s.columnSet[k] = true
		added = append(added, k)
	}
	if len(added) > 0 {
		sort.Strings(s.columns)
		fmt.Fprintf(os.Stderr, "[duckdb] added columns: %v\n", added)
	}

	return nil
}

// insertBatch inserts events using a prepared statement in a transaction.
func (s *DuckDBSink) insertBatch(ctx context.Context, events []*pipeline.Event) error {
	quotedTable := quoteIdent(s.table)

	var quotedCols []string
	var placeholders []string
	for i, c := range s.columns {
		quotedCols = append(quotedCols, quoteIdent(c))
		_ = i
		placeholders = append(placeholders, "?")
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		quotedTable,
		strings.Join(quotedCols, ", "),
		strings.Join(placeholders, ", "))

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("duckdb begin tx: %w", err)
	}

	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("duckdb prepare: %w", err)
	}
	defer stmt.Close()

	for _, event := range events {
		args := make([]any, len(s.columns))
		for i, col := range s.columns {
			v, ok := event.Value[col]
			if !ok {
				args[i] = nil
				continue
			}
			switch v.(type) {
			case map[string]any, []any, []string:
				b, err := json.Marshal(v)
				if err != nil {
					return fmt.Errorf("duckdb marshal nested field %s: %w", col, err)
				}
				args[i] = string(b)
			default:
				args[i] = v
			}
		}
		if _, err := stmt.ExecContext(ctx, args...); err != nil {
			tx.Rollback()
			return fmt.Errorf("duckdb insert: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("duckdb commit: %w", err)
	}

	return nil
}

// quoteIdent quotes a DuckDB identifier with double quotes.
func quoteIdent(name string) string {
	escaped := strings.ReplaceAll(name, `"`, `""`)
	return `"` + escaped + `"`
}

// Flush is a no-op for DuckDB (writes are synchronous via transactions).
func (s *DuckDBSink) Flush(ctx context.Context) error {
	return nil
}

// Close exports data if configured, then closes the connection.
func (s *DuckDBSink) Close() error {
	if s.db == nil {
		return nil
	}

	// Export to Parquet/CSV/JSON if configured
	if s.exportPath != "" && s.tableReady {
		if err := s.exportData(); err != nil {
			fmt.Fprintf(os.Stderr, "[duckdb] export error: %v\n", err)
		}
	}

	return s.db.Close()
}

// exportData uses DuckDB's native COPY command to export the table.
func (s *DuckDBSink) exportData() error {
	quotedTable := quoteIdent(s.table)

	// Build COPY statement
	format := strings.ToUpper(s.exportFormat)
	copySQL := fmt.Sprintf("COPY %s TO '%s' (FORMAT %s",
		quotedTable, s.exportPath, format)

	if len(s.exportPartBy) > 0 {
		copySQL += fmt.Sprintf(", PARTITION_BY (%s)", strings.Join(s.exportPartBy, ", "))
	}

	copySQL += ")"

	fmt.Fprintf(os.Stderr, "[duckdb] exporting to %s (format=%s)\n", s.exportPath, s.exportFormat)
	if _, err := s.db.Exec(copySQL); err != nil {
		return fmt.Errorf("duckdb export: %w", err)
	}

	fmt.Fprintf(os.Stderr, "[duckdb] export complete: %s\n", s.exportPath)
	return nil
}

// Name returns the sink identifier.
func (s *DuckDBSink) Name() string {
	return fmt.Sprintf("duckdb:%s/%s", s.dsn, s.table)
}
