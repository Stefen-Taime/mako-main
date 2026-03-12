package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/Stefen-Taime/mako/pkg/pipeline"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ═══════════════════════════════════════════
// PostgreSQL Sink (pgx)
// ═══════════════════════════════════════════

// PostgresSink writes events to PostgreSQL using pgx connection pool.
//
// When flatten is false (default), events are stored as JSONB in a single
// event_data column. When flatten is true, each top-level key in the event
// map becomes its own typed column (TEXT, NUMERIC, BOOLEAN, TIMESTAMPTZ,
// or JSONB for nested objects/arrays). The table is auto-created on the
// first batch, and new columns are added dynamically via ALTER TABLE when
// unseen keys appear.
type PostgresSink struct {
	dsn     string
	schema  string
	table   string
	flatten bool
	config  map[string]any
	pool    *pgxpool.Pool
	buffer  []*pipeline.Event
	mu      sync.Mutex

	// flatten mode state
	columns    []string          // ordered column names (set on first batch)
	columnSet  map[string]bool   // quick lookup for known columns
	columnType map[string]string // column name -> PostgreSQL type (TEXT, NUMERIC, BOOLEAN, TIMESTAMPTZ, JSONB)
	tableReady bool              // true once CREATE TABLE has been executed
}

// NewPostgresSink creates a PostgreSQL sink.
// The DSN can be provided via config["dsn"], or via the POSTGRES_DSN
// environment variable, or constructed from individual fields.
func NewPostgresSink(database, schema, table string, flatten bool, cfg map[string]any) *PostgresSink {
	dsn := ""
	if cfg != nil {
		if d, ok := cfg["dsn"].(string); ok {
			dsn = d
		}
	}
	if dsn == "" {
		dsn = os.Getenv("POSTGRES_DSN")
	}
	if dsn == "" {
		// Build DSN from individual config or env vars
		host := Resolve(cfg, "host", "POSTGRES_HOST", "localhost")
		port := Resolve(cfg, "port", "POSTGRES_PORT", "5432")
		user := Resolve(cfg, "user", "POSTGRES_USER", "mako")
		pass := Resolve(cfg, "password", "POSTGRES_PASSWORD", "mako")
		db := database
		if db == "" {
			db = Resolve(cfg, "database", "POSTGRES_DB", "mako")
		}
		dsn = fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
			user, pass, host, port, db)
	}

	if schema == "" {
		schema = "public"
	}

	return &PostgresSink{
		dsn:        dsn,
		schema:     schema,
		table:      table,
		flatten:    flatten,
		config:     cfg,
		columnSet:  make(map[string]bool),
		columnType: make(map[string]string),
	}
}

// Open establishes the connection pool to PostgreSQL.
func (s *PostgresSink) Open(ctx context.Context) error {
	poolCfg, err := pgxpool.ParseConfig(s.dsn)
	if err != nil {
		return fmt.Errorf("postgres parse dsn: %w", err)
	}

	poolCfg.MinConns = 2
	poolCfg.MaxConns = 10

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return fmt.Errorf("postgres connect: %w", err)
	}

	// Verify connectivity
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return fmt.Errorf("postgres ping: %w", err)
	}

	s.pool = pool

	fmt.Fprintf(os.Stderr, "[postgres] connected to %s.%s (flatten=%v)\n",
		s.schema, s.table, s.flatten)
	return nil
}

// Write inserts events into PostgreSQL.
//
// In flatten mode, the table is auto-created from event keys and data is
// inserted into typed columns. In non-flatten mode (default), events are
// stored as JSONB in a single event_data column.
func (s *PostgresSink) Write(ctx context.Context, events []*pipeline.Event) error {
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

	// Non-flatten mode: JSONB insert.
	qualifiedTable := fmt.Sprintf("%s.%s", s.schema, s.table)

	// Use COPY for bulk insert (much faster than individual INSERTs)
	rows := make([][]any, 0, len(events))
	for _, event := range events {
		data, err := json.Marshal(event.Value)
		if err != nil {
			return fmt.Errorf("marshal event: %w", err)
		}

		rows = append(rows, []any{
			string(event.Key),    // event_key
			string(data),         // event_data (jsonb)
			event.Timestamp,      // event_timestamp
			event.Topic,          // topic
			int(event.Partition), // partition
			event.Offset,         // offset
		})
	}

	_, err := s.pool.CopyFrom(
		ctx,
		pgx.Identifier{s.schema, s.table},
		[]string{"event_key", "event_data", "event_timestamp", "topic", "partition", "offset"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		// Fallback to batch INSERT if COPY fails (e.g., table schema mismatch)
		return s.batchInsert(ctx, qualifiedTable, events)
	}

	return nil
}

// batchInsert is a fallback using INSERT ... VALUES for compatibility.
func (s *PostgresSink) batchInsert(ctx context.Context, qualifiedTable string, events []*pipeline.Event) error {
	batch := &pgx.Batch{}
	query := fmt.Sprintf(
		`INSERT INTO %s (event_key, event_data, event_timestamp, topic, partition, "offset")
		 VALUES ($1, $2::jsonb, $3, $4, $5, $6)`,
		qualifiedTable,
	)

	for _, event := range events {
		data, err := json.Marshal(event.Value)
		if err != nil {
			return fmt.Errorf("marshal event: %w", err)
		}
		batch.Queue(query,
			string(event.Key),
			string(data),
			event.Timestamp,
			event.Topic,
			int(event.Partition),
			event.Offset,
		)
	}

	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()

	for range events {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("postgres batch insert: %w", err)
		}
	}

	return nil
}

// ═══════════════════════════════════════════
// Flatten mode
// ═══════════════════════════════════════════

// PgColumnType maps a Go value to a PostgreSQL column type.
// Exported for testing.
func PgColumnType(v any) string {
	return pgColumnType(v)
}

// pgColumnType maps a Go value to a PostgreSQL column type.
func pgColumnType(v any) string {
	switch val := v.(type) {
	case float64, float32, int, int64:
		return "NUMERIC"
	case bool:
		return "BOOLEAN"
	case string:
		if isTimestampLike(val) {
			return "TIMESTAMPTZ"
		}
		return "TEXT"
	case map[string]any, []any:
		return "JSONB"
	default:
		return "TEXT"
	}
}

// timestampRe matches common ISO 8601 / RFC 3339 timestamp patterns.
var timestampRe = regexp.MustCompile(
	`^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}`,
)

// IsTimestampLike checks if a string looks like a timestamp.
// Exported for testing.
func IsTimestampLike(s string) bool {
	return isTimestampLike(s)
}

// isTimestampLike checks if a string looks like a timestamp.
func isTimestampLike(s string) bool {
	return timestampRe.MatchString(s)
}

// pgQuoteIdent quotes a PostgreSQL identifier with double quotes.
func pgQuoteIdent(name string) string {
	escaped := strings.ReplaceAll(name, `"`, `""`)
	return `"` + escaped + `"`
}

// ensureFlatTable creates the table on the first call, and adds any new
// columns discovered in subsequent batches via ALTER TABLE ADD COLUMN.
func (s *PostgresSink) ensureFlatTable(ctx context.Context, events []*pipeline.Event) error {
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

	qualifiedTable := fmt.Sprintf("%s.%s", pgQuoteIdent(s.schema), pgQuoteIdent(s.table))

	if !s.tableReady {
		// First batch: CREATE TABLE with all discovered columns.
		var colDefs []string
		var colNames []string
		for k := range newKeys {
			colNames = append(colNames, k)
		}
		sort.Strings(colNames)

		for _, k := range colNames {
			colDefs = append(colDefs, fmt.Sprintf("%s %s", pgQuoteIdent(k), pgColumnType(newKeys[k])))
		}
		// Only add loaded_at if the source data doesn't already contain it
		if _, hasLoadedAt := newKeys["loaded_at"]; !hasLoadedAt {
			colDefs = append(colDefs, "loaded_at TIMESTAMPTZ DEFAULT NOW()")
		}

		createDDL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n\t%s\n)",
			qualifiedTable, strings.Join(colDefs, ",\n\t"))
		if _, err := s.pool.Exec(ctx, createDDL); err != nil {
			return fmt.Errorf("postgres create flat table: %w", err)
		}
		s.tableReady = true
		s.columns = colNames
		for _, k := range colNames {
			s.columnSet[k] = true
			s.columnType[k] = pgColumnType(newKeys[k])
		}
		fmt.Fprintf(os.Stderr, "[postgres] created flat table %s with %d columns\n",
			qualifiedTable, len(colNames))
		return nil
	}

	// Subsequent batches: ALTER TABLE ADD COLUMN for new keys.
	var added []string
	for k := range newKeys {
		colType := pgColumnType(newKeys[k])
		alterDDL := fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s",
			qualifiedTable, pgQuoteIdent(k), colType)
		if _, err := s.pool.Exec(ctx, alterDDL); err != nil {
			return fmt.Errorf("postgres alter table add column %s: %w", k, err)
		}
		s.columns = append(s.columns, k)
		s.columnSet[k] = true
		s.columnType[k] = colType
		added = append(added, k)
	}
	if len(added) > 0 {
		sort.Strings(s.columns)
		fmt.Fprintf(os.Stderr, "[postgres] added columns: %v\n", added)
	}

	return nil
}

// writeFlatBatch inserts events into individually typed columns using
// a batch of INSERT statements.
func (s *PostgresSink) writeFlatBatch(ctx context.Context, events []*pipeline.Event) error {
	qualifiedTable := fmt.Sprintf("%s.%s", pgQuoteIdent(s.schema), pgQuoteIdent(s.table))

	// Build the INSERT query with positional parameters.
	var quotedCols []string
	var placeholders []string
	for i, c := range s.columns {
		quotedCols = append(quotedCols, pgQuoteIdent(c))
		if s.columnType[c] == "JSONB" {
			placeholders = append(placeholders, fmt.Sprintf("$%d::jsonb", i+1))
		} else {
			placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
		}
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		qualifiedTable,
		strings.Join(quotedCols, ", "),
		strings.Join(placeholders, ", "))

	batch := &pgx.Batch{}
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
		batch.Queue(query, args...)
	}

	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()

	for range events {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("postgres flat insert: %w", err)
		}
	}

	return nil
}

// Flush is a no-op for PostgreSQL (writes are synchronous).
func (s *PostgresSink) Flush(ctx context.Context) error {
	return nil
}

// Close shuts down the connection pool.
func (s *PostgresSink) Close() error {
	if s.pool != nil {
		s.pool.Close()
	}
	return nil
}

// Name returns the sink identifier.
func (s *PostgresSink) Name() string {
	return fmt.Sprintf("postgres:%s.%s", s.schema, s.table)
}
