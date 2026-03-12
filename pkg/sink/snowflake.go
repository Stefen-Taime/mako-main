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
	"time"

	"github.com/Stefen-Taime/mako/pkg/pipeline"

	// Snowflake driver registers itself as "snowflake" for database/sql.
	_ "github.com/snowflakedb/gosnowflake"
)

// ═══════════════════════════════════════════
// Snowflake Sink (gosnowflake)
// ═══════════════════════════════════════════

// SnowflakeSink writes events to Snowflake using the gosnowflake driver.
//
// When flatten is false (default), events are stored as VARIANT (JSON) in a
// single event_data column. When flatten is true, each top-level key in the
// event map becomes its own typed column (VARCHAR, FLOAT, BOOLEAN, or VARIANT
// for nested structures). The table is auto-created on the first batch, and
// new columns are added dynamically via ALTER TABLE when unseen keys appear.
//
// Connection is configured via:
//   - config.dsn  (full DSN string)
//   - SNOWFLAKE_DSN env var
//   - Individual fields: account, user, password, warehouse, role
//
// Default schema (flatten=false):
//
//	CREATE TABLE <db>.<schema>.<table> (
//	    event_data    VARIANT NOT NULL,
//	    event_key     VARCHAR,
//	    event_topic   VARCHAR,
//	    event_offset  NUMBER,
//	    loaded_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
//	);
type SnowflakeSink struct {
	database  string
	schema    string
	table     string
	warehouse string
	flatten   bool
	config    map[string]any
	dsn       string
	db        *sql.DB
	mu        sync.Mutex

	// flatten mode state
	columns    []string          // ordered column names (set on first batch)
	columnSet  map[string]bool   // quick lookup for known columns
	columnType map[string]string // column name → Snowflake type (VARCHAR, FLOAT, BOOLEAN, VARIANT)
	tableReady bool              // true once CREATE TABLE has been executed
}

// NewSnowflakeSink creates a Snowflake sink.
func NewSnowflakeSink(database, schema, table string, flatten bool, config map[string]any) *SnowflakeSink {
	dsn := ""
	warehouse := ""

	if config != nil {
		if d, ok := config["dsn"].(string); ok {
			dsn = d
		}
		if w, ok := config["warehouse"].(string); ok {
			warehouse = w
		}
	}

	if dsn == "" {
		dsn = os.Getenv("SNOWFLAKE_DSN")
	}

	// Build DSN from individual config/env vars if not provided
	if dsn == "" {
		account := Resolve(config, "account", "SNOWFLAKE_ACCOUNT", "")
		user := Resolve(config, "user", "SNOWFLAKE_USER", "")
		password := Resolve(config, "password", "SNOWFLAKE_PASSWORD", "")
		if warehouse == "" {
			warehouse = Resolve(config, "warehouse", "SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
		}
		role := Resolve(config, "role", "SNOWFLAKE_ROLE", "")

		if account != "" && user != "" {
			dsn = fmt.Sprintf("%s:%s@%s/%s/%s",
				user, password, account, database, schema)

			params := []string{}
			if warehouse != "" {
				params = append(params, "warehouse="+warehouse)
			}
			if role != "" {
				params = append(params, "role="+role)
			}
			// gosnowflake driver-level timeouts — without these the driver
			// uses very short defaults that cause context deadline exceeded
			// even when the Go context has plenty of time left.
			params = append(params, "loginTimeout=30")
			params = append(params, "requestTimeout=60")
			params = append(params, "clientTimeout=300")

			dsn += "?" + strings.Join(params, "&")
		}
	}

	return &SnowflakeSink{
		database:  database,
		schema:    schema,
		table:     table,
		warehouse: warehouse,
		flatten:   flatten,
		config:    config,
		dsn:       dsn,
		columnSet:  make(map[string]bool),
		columnType: make(map[string]string),
	}
}

// Open connects to Snowflake.
func (s *SnowflakeSink) Open(ctx context.Context) error {
	if s.dsn == "" {
		return fmt.Errorf("snowflake: no DSN configured (set SNOWFLAKE_DSN or provide account/user/password in config)")
	}

	db, err := sql.Open("snowflake", s.dsn)
	if err != nil {
		return fmt.Errorf("snowflake open: %w", err)
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)

	// Use context.Background() so the DB connection pool is not tied to the
	// pipeline context. When the pipeline ctx is cancelled (e.g. file source
	// EOF + auto-terminate), a ctx-derived context would invalidate the
	// pool and cause Write() to fail with context deadline exceeded.
	// Graceful shutdown is handled by Close() calling db.Close().
	initCtx, initCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer initCancel()

	if err := db.PingContext(initCtx); err != nil {
		db.Close()
		return fmt.Errorf("snowflake ping: %w", err)
	}

	// NOTE: warehouse is set via the DSN query parameter (?warehouse=X).
	// A redundant USE WAREHOUSE command here can corrupt the driver's
	// internal connection state and cause subsequent queries to fail
	// with context deadline exceeded.

	// For non-flatten mode, create the VARIANT table immediately.
	// For flatten mode, table creation is deferred to the first Write()
	// because we need to inspect event keys to determine column types.
	if !s.flatten {
		qualifiedTable := fmt.Sprintf("%s.%s.%s", s.database, s.schema, s.table)
		createDDL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			event_data    VARIANT NOT NULL,
			event_key     VARCHAR,
			event_topic   VARCHAR,
			event_offset  NUMBER,
			loaded_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
		)`, qualifiedTable)
		if _, err := db.ExecContext(initCtx, createDDL); err != nil {
			db.Close()
			return fmt.Errorf("snowflake create table: %w", err)
		}
		s.tableReady = true
	}

	fmt.Fprintf(os.Stderr, "[snowflake] connected to %s.%s.%s (flatten=%v)\n",
		s.database, s.schema, s.table, s.flatten)
	s.db = db
	return nil
}

// Write inserts events into Snowflake.
// Large batches are split into small chunks so each transaction stays well
// within the driver timeout. If a chunk fails, the remaining chunks are
// still attempted and a summary error is returned.
func (s *SnowflakeSink) Write(ctx context.Context, events []*pipeline.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(events) == 0 {
		return nil
	}

	// Flatten mode: ensure table exists and handle schema evolution.
	if s.flatten {
		if err := s.ensureFlatTable(events); err != nil {
			return err
		}
		return s.writeFlatChunked(events)
	}

	// Non-flatten mode: VARIANT insert.
	qualifiedTable := fmt.Sprintf("%s.%s.%s", s.database, s.schema, s.table)
	query := fmt.Sprintf(
		"INSERT INTO %s (event_data, event_key, event_topic, event_offset) SELECT PARSE_JSON(?), ?, ?, ?",
		qualifiedTable,
	)

	const chunkSize = 16
	var chunkErrors int
	var lastErr error
	for i := 0; i < len(events); i += chunkSize {
		end := i + chunkSize
		if end > len(events) {
			end = len(events)
		}
		if err := s.writeChunk(query, events[i:end]); err != nil {
			chunkErrors++
			lastErr = err
			fmt.Fprintf(os.Stderr, "[snowflake] chunk %d-%d failed: %v\n", i, end, err)
		}
	}

	if lastErr != nil {
		return fmt.Errorf("snowflake write: %d chunk(s) failed, last error: %w", chunkErrors, lastErr)
	}
	return nil
}

// writeChunk inserts a single chunk of events inside its own transaction (VARIANT mode).
func (s *SnowflakeSink) writeChunk(query string, events []*pipeline.Event) error {
	writeCtx, writeCancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer writeCancel()

	tx, err := s.db.BeginTx(writeCtx, nil)
	if err != nil {
		return fmt.Errorf("snowflake begin tx: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(writeCtx, query)
	if err != nil {
		return fmt.Errorf("snowflake prepare: %w", err)
	}
	defer stmt.Close()

	for _, event := range events {
		data, err := json.Marshal(event.Value)
		if err != nil {
			return fmt.Errorf("marshal event: %w", err)
		}

		_, err = stmt.ExecContext(writeCtx, string(data), string(event.Key), event.Topic, event.Offset)
		if err != nil {
			return fmt.Errorf("snowflake insert: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("snowflake commit: %w", err)
	}

	return nil
}

// ═══════════════════════════════════════════
// Flatten mode
// ═══════════════════════════════════════════

// sfColumnType maps a Go value to a Snowflake column type.
func sfColumnType(v any) string {
	switch v.(type) {
	case float64, float32, int, int64:
		return "FLOAT"
	case bool:
		return "BOOLEAN"
	case string:
		return "VARCHAR"
	default:
		return "VARIANT"
	}
}

// ensureFlatTable creates the table on the first call, and adds any new
// columns discovered in subsequent batches via ALTER TABLE ADD COLUMN.
func (s *SnowflakeSink) ensureFlatTable(events []*pipeline.Event) error {
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

	ddlCtx, ddlCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer ddlCancel()

	qualifiedTable := fmt.Sprintf("%s.%s.%s", s.database, s.schema, s.table)

	if !s.tableReady {
		var colDefs []string
		var colNames []string
		for k := range newKeys {
			colNames = append(colNames, k)
		}
		sort.Strings(colNames)

		for _, k := range colNames {
			colDefs = append(colDefs, fmt.Sprintf("%s %s", sfQuoteIdent(k), sfColumnType(newKeys[k])))
		}
		colDefs = append(colDefs, "loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()")

		createDDL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n\t%s\n)",
			qualifiedTable, strings.Join(colDefs, ",\n\t"))
		if _, err := s.db.ExecContext(ddlCtx, createDDL); err != nil {
			return fmt.Errorf("snowflake create flat table: %w", err)
		}
		s.tableReady = true
		s.columns = colNames
		for _, k := range colNames {
			s.columnSet[k] = true
			s.columnType[k] = sfColumnType(newKeys[k])
		}
		fmt.Fprintf(os.Stderr, "[snowflake] created flat table %s with %d columns\n",
			qualifiedTable, len(colNames))
		return nil
	}

	var added []string
	for k := range newKeys {
		colType := sfColumnType(newKeys[k])
		alterDDL := fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s",
			qualifiedTable, sfQuoteIdent(k), colType)
		if _, err := s.db.ExecContext(ddlCtx, alterDDL); err != nil {
			return fmt.Errorf("snowflake alter table add column %s: %w", k, err)
		}
		s.columns = append(s.columns, k)
		s.columnSet[k] = true
		s.columnType[k] = colType
		added = append(added, k)
	}
	if len(added) > 0 {
		sort.Strings(s.columns)
		fmt.Fprintf(os.Stderr, "[snowflake] added columns: %v\n", added)
	}

	return nil
}

// writeFlatChunked splits events into chunks and inserts with flat columns.
func (s *SnowflakeSink) writeFlatChunked(events []*pipeline.Event) error {
	const chunkSize = 16
	var chunkErrors int
	var lastErr error

	for i := 0; i < len(events); i += chunkSize {
		end := i + chunkSize
		if end > len(events) {
			end = len(events)
		}
		if err := s.writeFlatChunk(events[i:end]); err != nil {
			chunkErrors++
			lastErr = err
			fmt.Fprintf(os.Stderr, "[snowflake] flat chunk %d-%d failed: %v\n", i, end, err)
		}
	}

	if lastErr != nil {
		return fmt.Errorf("snowflake flat write: %d chunk(s) failed, last error: %w", chunkErrors, lastErr)
	}
	return nil
}

// writeFlatChunk inserts a chunk of events into individually typed columns.
func (s *SnowflakeSink) writeFlatChunk(events []*pipeline.Event) error {
	writeCtx, writeCancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer writeCancel()

	qualifiedTable := fmt.Sprintf("%s.%s.%s", s.database, s.schema, s.table)

	var quotedCols []string
	for _, c := range s.columns {
		quotedCols = append(quotedCols, sfQuoteIdent(c))
	}
	// Snowflake forbids function calls (like PARSE_JSON) inside a VALUES
	// clause of a prepared statement. Instead we use INSERT INTO ... SELECT
	// ... FROM VALUES (?,...) and apply PARSE_JSON in the outer SELECT for
	// VARIANT columns. Snowflake names positional columns column1, column2, etc.
	innerPlaceholders := make([]string, len(s.columns))
	selectExprs := make([]string, len(s.columns))
	for i, c := range s.columns {
		innerPlaceholders[i] = "?"
		colRef := fmt.Sprintf("column%d", i+1)
		if s.columnType[c] == "VARIANT" {
			selectExprs[i] = fmt.Sprintf("PARSE_JSON(%s)", colRef)
		} else {
			selectExprs[i] = colRef
		}
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM VALUES (%s)",
		qualifiedTable,
		strings.Join(quotedCols, ", "),
		strings.Join(selectExprs, ", "),
		strings.Join(innerPlaceholders, ", "))

	tx, err := s.db.BeginTx(writeCtx, nil)
	if err != nil {
		return fmt.Errorf("snowflake begin tx: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(writeCtx, query)
	if err != nil {
		return fmt.Errorf("snowflake prepare: %w", err)
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
			case map[string]any, []any:
				b, err := json.Marshal(v)
				if err != nil {
					return fmt.Errorf("marshal nested field %s: %w", col, err)
				}
				args[i] = string(b)
			default:
				args[i] = v
			}
		}
		if _, err := stmt.ExecContext(writeCtx, args...); err != nil {
			return fmt.Errorf("snowflake flat insert: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("snowflake commit: %w", err)
	}
	return nil
}

// sfQuoteIdent quotes a Snowflake identifier with double quotes.
func sfQuoteIdent(name string) string {
	escaped := strings.ReplaceAll(name, `"`, `""`)
	return `"` + escaped + `"`
}

// Flush is a no-op (writes are synchronous).
func (s *SnowflakeSink) Flush(ctx context.Context) error {
	return nil
}

// Close shuts down the database connection.
func (s *SnowflakeSink) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// Name returns the sink identifier.
func (s *SnowflakeSink) Name() string {
	return fmt.Sprintf("snowflake:%s.%s.%s", s.database, s.schema, s.table)
}
