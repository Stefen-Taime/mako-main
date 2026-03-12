package sink

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Stefen-Taime/mako/pkg/pipeline"
)

// ═══════════════════════════════════════════
// ClickHouse Sink (clickhouse-go v2)
// ═══════════════════════════════════════════

// ClickHouseSink writes events to ClickHouse using the official native protocol driver.
//
// When flatten is false (default), events are stored as JSON strings with
// metadata in a pre-existing table. When flatten is true, each top-level key
// in the event map becomes its own typed column (String, Float64, Int64, Bool,
// DateTime64(3), or String for nested JSON). The table is auto-created on the
// first batch with ENGINE = MergeTree() ORDER BY tuple(), and new columns are
// added dynamically via ALTER TABLE when unseen keys appear.
//
// YAML example:
//
//	sink:
//	  type: clickhouse
//	  database: analytics
//	  table: pipeline_events
//	  flatten: true
//	  config:
//	    host: localhost
//	    port: "9000"
//	    user: default
//	    password: ""
type ClickHouseSink struct {
	dsn      string
	database string
	table    string
	flatten  bool
	config   map[string]any
	conn     clickhouse.Conn
	mu       sync.Mutex

	// flatten mode state
	columns    []string          // ordered column names (set on first batch)
	columnSet  map[string]bool   // quick lookup for known columns
	columnType map[string]string // column name -> ClickHouse type (String, Float64, Int64, Bool, DateTime64(3))
	tableReady bool              // true once CREATE TABLE has been executed
}

// NewClickHouseSink creates a ClickHouse sink.
// The DSN can be provided via config["dsn"], the CLICKHOUSE_DSN env var,
// or constructed from individual fields (host, port, user, password).
func NewClickHouseSink(database, table string, flatten bool, cfg map[string]any) *ClickHouseSink {
	dsn := ""
	if cfg != nil {
		if d, ok := cfg["dsn"].(string); ok {
			dsn = d
		}
	}
	if dsn == "" {
		dsn = os.Getenv("CLICKHOUSE_DSN")
	}

	return &ClickHouseSink{
		dsn:        dsn,
		database:   database,
		table:      table,
		flatten:    flatten,
		config:     cfg,
		columnSet:  make(map[string]bool),
		columnType: make(map[string]string),
	}
}

// Open establishes the connection to ClickHouse.
func (s *ClickHouseSink) Open(ctx context.Context) error {
	var opts *clickhouse.Options
	var err error

	if s.dsn != "" {
		opts, err = clickhouse.ParseDSN(s.dsn)
		if err != nil {
			return fmt.Errorf("clickhouse parse dsn: %w", err)
		}
	} else {
		// Build options from individual config / env vars
		host := Resolve(s.config, "host", "CLICKHOUSE_HOST", "localhost")
		port := Resolve(s.config, "port", "CLICKHOUSE_PORT", "9000")
		user := Resolve(s.config, "user", "CLICKHOUSE_USER", "default")
		pass := Resolve(s.config, "password", "CLICKHOUSE_PASSWORD", "")
		db := s.database
		if db == "" {
			db = Resolve(s.config, "database", "CLICKHOUSE_DB", "default")
		}

		secure := Resolve(s.config, "secure", "CLICKHOUSE_SECURE", "false")
		isSecure, _ := strconv.ParseBool(secure)

		addr := fmt.Sprintf("%s:%s", host, port)

		opts = &clickhouse.Options{
			Addr: []string{addr},
			Auth: clickhouse.Auth{
				Database: db,
				Username: user,
				Password: pass,
			},
			Settings: clickhouse.Settings{
				"max_execution_time": 60,
			},
			DialTimeout: 10 * time.Second,
			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			},
		}

		if isSecure {
			opts.TLS = &tls.Config{
				InsecureSkipVerify: false,
			}
		}
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return fmt.Errorf("clickhouse open: %w", err)
	}

	// Verify connectivity
	if err := conn.Ping(ctx); err != nil {
		return fmt.Errorf("clickhouse ping: %w", err)
	}

	s.conn = conn

	fmt.Fprintf(os.Stderr, "[clickhouse] connected to %s.%s (flatten=%v)\n",
		s.database, s.table, s.flatten)
	return nil
}

// Write inserts events into ClickHouse using a batch insert.
//
// In flatten mode, the table is auto-created from event keys and data is
// inserted into typed columns. In non-flatten mode (default), events are
// stored as JSON strings with metadata.
func (s *ClickHouseSink) Write(ctx context.Context, events []*pipeline.Event) error {
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

	// Non-flatten mode: JSON string insert.
	qualifiedTable := s.table
	if s.database != "" {
		qualifiedTable = s.database + "." + s.table
	}

	batch, err := s.conn.PrepareBatch(ctx, fmt.Sprintf(
		"INSERT INTO %s (event_ts, event_key, event_data, topic, partition_id, offset_id)",
		qualifiedTable,
	))
	if err != nil {
		return fmt.Errorf("clickhouse prepare batch: %w", err)
	}

	for _, event := range events {
		data, err := json.Marshal(event.Value)
		if err != nil {
			return fmt.Errorf("clickhouse marshal event: %w", err)
		}

		ts := event.Timestamp
		if ts.IsZero() {
			ts = time.Now()
		}

		if err := batch.Append(
			ts,
			string(event.Key),
			string(data),
			event.Topic,
			uint32(event.Partition),
			uint64(event.Offset),
		); err != nil {
			return fmt.Errorf("clickhouse append: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("clickhouse send batch: %w", err)
	}

	return nil
}

// ═══════════════════════════════════════════
// Flatten mode
// ═══════════════════════════════════════════

// ChColumnType maps a Go value to a ClickHouse column type.
// Exported for testing.
func ChColumnType(v any) string {
	return chColumnType(v)
}

// chColumnType maps a Go value to a ClickHouse column type.
func chColumnType(v any) string {
	switch val := v.(type) {
	case float64, float32:
		return "Float64"
	case int, int64:
		return "Int64"
	case bool:
		return "Bool"
	case string:
		if isTimestampLike(val) {
			return "DateTime64(3)"
		}
		return "String"
	case map[string]any, []any:
		return "String" // JSON serialized as String in ClickHouse
	default:
		return "String"
	}
}

// chQuoteIdent quotes a ClickHouse identifier with backticks.
func chQuoteIdent(name string) string {
	escaped := strings.ReplaceAll(name, "`", "``")
	return "`" + escaped + "`"
}

// ensureFlatTable creates the table on the first call, and adds any new
// columns discovered in subsequent batches via ALTER TABLE ADD COLUMN.
func (s *ClickHouseSink) ensureFlatTable(ctx context.Context, events []*pipeline.Event) error {
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

	qualifiedTable := s.table
	if s.database != "" {
		qualifiedTable = chQuoteIdent(s.database) + "." + chQuoteIdent(s.table)
	} else {
		qualifiedTable = chQuoteIdent(s.table)
	}

	if !s.tableReady {
		// First batch: CREATE TABLE with all discovered columns.
		var colDefs []string
		var colNames []string
		for k := range newKeys {
			colNames = append(colNames, k)
		}
		sort.Strings(colNames)

		for _, k := range colNames {
			// Use Nullable for all user columns to handle missing keys
			colDefs = append(colDefs, fmt.Sprintf("%s Nullable(%s)",
				chQuoteIdent(k), chColumnType(newKeys[k])))
		}
		colDefs = append(colDefs, "loaded_at DateTime64(3) DEFAULT now64()")

		createDDL := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s (\n\t%s\n) ENGINE = MergeTree() ORDER BY tuple()",
			qualifiedTable, strings.Join(colDefs, ",\n\t"))
		if err := s.conn.Exec(ctx, createDDL); err != nil {
			return fmt.Errorf("clickhouse create flat table: %w", err)
		}

		s.tableReady = true
		s.columns = colNames
		for _, k := range colNames {
			s.columnSet[k] = true
			s.columnType[k] = chColumnType(newKeys[k])
		}
		fmt.Fprintf(os.Stderr, "[clickhouse] created flat table %s with %d columns\n",
			qualifiedTable, len(colNames))
		return nil
	}

	// Subsequent batches: ALTER TABLE ADD COLUMN for new keys.
	var added []string
	for k := range newKeys {
		colType := chColumnType(newKeys[k])
		alterDDL := fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s Nullable(%s)",
			qualifiedTable, chQuoteIdent(k), colType)
		if err := s.conn.Exec(ctx, alterDDL); err != nil {
			return fmt.Errorf("clickhouse alter table add column %s: %w", k, err)
		}
		s.columns = append(s.columns, k)
		s.columnSet[k] = true
		s.columnType[k] = colType
		added = append(added, k)
	}
	if len(added) > 0 {
		sort.Strings(s.columns)
		fmt.Fprintf(os.Stderr, "[clickhouse] added columns: %v\n", added)
	}

	return nil
}

// writeFlatBatch inserts events into individually typed columns using
// a ClickHouse batch insert.
func (s *ClickHouseSink) writeFlatBatch(ctx context.Context, events []*pipeline.Event) error {
	qualifiedTable := s.table
	if s.database != "" {
		qualifiedTable = chQuoteIdent(s.database) + "." + chQuoteIdent(s.table)
	} else {
		qualifiedTable = chQuoteIdent(s.table)
	}

	// Build column list for the INSERT statement.
	var quotedCols []string
	for _, c := range s.columns {
		quotedCols = append(quotedCols, chQuoteIdent(c))
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s (%s)",
		qualifiedTable, strings.Join(quotedCols, ", "))

	batch, err := s.conn.PrepareBatch(ctx, insertSQL)
	if err != nil {
		return fmt.Errorf("clickhouse prepare flat batch: %w", err)
	}

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
				b, merr := json.Marshal(v)
				if merr != nil {
					return fmt.Errorf("clickhouse marshal nested field %s: %w", col, merr)
				}
				s := string(b)
				args[i] = &s
			default:
				args[i] = v
			}
		}
		if err := batch.Append(args...); err != nil {
			return fmt.Errorf("clickhouse flat append: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("clickhouse flat send batch: %w", err)
	}

	return nil
}

// Flush is a no-op for ClickHouse (batch writes are synchronous).
func (s *ClickHouseSink) Flush(ctx context.Context) error {
	return nil
}

// Close shuts down the ClickHouse connection.
func (s *ClickHouseSink) Close() error {
	if s.conn != nil {
		return s.conn.Close()
	}
	return nil
}

// Name returns the sink identifier.
func (s *ClickHouseSink) Name() string {
	return fmt.Sprintf("clickhouse:%s.%s", s.database, s.table)
}
