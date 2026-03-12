package source

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Stefen-Taime/mako/pkg/pipeline"
	"github.com/Stefen-Taime/mako/pkg/sink"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ═══════════════════════════════════════════
// PostgreSQL CDC Source (pgx + pglogrepl)
// ═══════════════════════════════════════════

// PostgresCDCSource reads events from PostgreSQL using three modes:
//   - snapshot: bulk load via SELECT with cursor-based batching
//   - cdc: real-time change capture via logical replication (pglogrepl)
//   - snapshot+cdc: snapshot first, then switch to CDC streaming
//
// YAML configuration:
//
//	source:
//	  type: postgres_cdc
//	  config:
//	    host: localhost
//	    port: "5432"
//	    user: postgres
//	    password: secret
//	    database: myapp
//	    tables: [users, orders, payments]
//	    schema: public
//	    mode: snapshot+cdc
//	    snapshot_batch_size: 10000
//	    snapshot_order_by: id
//	    slot_name: mako_slot
//	    publication: mako_pub
//	    start_lsn: ""
type PostgresCDCSource struct {
	dsn       string
	schema    string
	tables    []string
	mode      string // "snapshot", "cdc", "snapshot+cdc"
	batchSize int
	orderBy   string
	slotName  string
	pubName   string
	startLSN  string
	config    map[string]any

	pool    *pgxpool.Pool
	eventCh chan *pipeline.Event
	lag     atomic.Int64
	closed  atomic.Bool
	wg      sync.WaitGroup

	// Replication connection (separate from pool, uses replication protocol)
	replConn *pgconn.PgConn
}

// NewPostgresCDCSource creates a Postgres CDC source from config.
func NewPostgresCDCSource(cfg map[string]any) *PostgresCDCSource {
	// Build DSN
	dsn := strFromConfig(cfg, "dsn", "")
	if dsn == "" {
		dsn = os.Getenv("POSTGRES_SOURCE_DSN")
	}
	if dsn == "" {
		host := sink.Resolve(cfg, "host", "POSTGRES_SOURCE_HOST", "localhost")
		port := sink.Resolve(cfg, "port", "POSTGRES_SOURCE_PORT", "5432")
		user := sink.Resolve(cfg, "user", "POSTGRES_SOURCE_USER", "postgres")
		pass := sink.Resolve(cfg, "password", "POSTGRES_SOURCE_PASSWORD", "")
		database := sink.Resolve(cfg, "database", "POSTGRES_SOURCE_DB", "postgres")
		dsn = fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
			user, pass, host, port, database)
	}

	// Parse tables list
	tables := strSliceFromConfig(cfg, "tables")
	schema := strFromConfig(cfg, "schema", "public")
	mode := strFromConfig(cfg, "mode", "snapshot+cdc")
	batchSize := intFromConfig(cfg, "snapshot_batch_size", 10000)
	orderBy := strFromConfig(cfg, "snapshot_order_by", "")
	slotName := strFromConfig(cfg, "slot_name", "")
	pubName := strFromConfig(cfg, "publication", "")
	startLSN := strFromConfig(cfg, "start_lsn", "")

	return &PostgresCDCSource{
		dsn:       dsn,
		schema:    schema,
		tables:    tables,
		mode:      mode,
		batchSize: batchSize,
		orderBy:   orderBy,
		slotName:  slotName,
		pubName:   pubName,
		startLSN:  startLSN,
		config:    cfg,
		eventCh:   make(chan *pipeline.Event, 1000),
	}
}

// Mode returns the configured mode (exported for testing).
func (s *PostgresCDCSource) Mode() string { return s.mode }

// Tables returns the configured tables (exported for testing).
func (s *PostgresCDCSource) Tables() []string { return s.tables }

// Schema returns the configured schema (exported for testing).
func (s *PostgresCDCSource) Schema() string { return s.schema }

// DSN returns the configured DSN (exported for testing).
func (s *PostgresCDCSource) DSN() string { return s.dsn }

// SlotName returns the replication slot name (exported for testing).
func (s *PostgresCDCSource) SlotName() string { return s.slotName }

// Publication returns the publication name (exported for testing).
func (s *PostgresCDCSource) Publication() string { return s.pubName }

// Open validates configuration and establishes the connection pool.
func (s *PostgresCDCSource) Open(ctx context.Context) error {
	if len(s.tables) == 0 {
		return fmt.Errorf("postgres_cdc: no tables configured (set config.tables)")
	}

	validModes := map[string]bool{"snapshot": true, "cdc": true, "snapshot+cdc": true}
	if !validModes[s.mode] {
		return fmt.Errorf("postgres_cdc: invalid mode %q (use snapshot, cdc, or snapshot+cdc)", s.mode)
	}

	pool, err := pgxpool.New(ctx, s.dsn)
	if err != nil {
		return fmt.Errorf("postgres_cdc: connect: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return fmt.Errorf("postgres_cdc: ping: %w", err)
	}

	s.pool = pool
	fmt.Fprintf(os.Stderr, "[postgres_cdc] connected (mode=%s, tables=%v)\n", s.mode, s.tables)
	return nil
}

// Read starts the source and returns the event channel.
func (s *PostgresCDCSource) Read(ctx context.Context) (<-chan *pipeline.Event, error) {
	s.wg.Add(1)
	go s.readLoop(ctx)
	return s.eventCh, nil
}

// readLoop orchestrates snapshot and/or CDC based on mode.
func (s *PostgresCDCSource) readLoop(ctx context.Context) {
	defer s.wg.Done()
	defer func() {
		if s.closed.CompareAndSwap(false, true) {
			close(s.eventCh)
		}
	}()

	// Snapshot phase
	if s.mode == "snapshot" || s.mode == "snapshot+cdc" {
		for _, table := range s.tables {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if err := s.snapshotTable(ctx, table); err != nil {
				fmt.Fprintf(os.Stderr, "[postgres_cdc] snapshot error on %s: %v\n", table, err)
				return
			}
		}
		fmt.Fprintf(os.Stderr, "[postgres_cdc] snapshot complete for %d tables\n", len(s.tables))
	}

	// If snapshot-only, we're done
	if s.mode == "snapshot" {
		return
	}

	// CDC phase
	if err := s.startCDC(ctx); err != nil {
		if ctx.Err() == nil {
			fmt.Fprintf(os.Stderr, "[postgres_cdc] cdc error: %v\n", err)
		}
	}
}

// ═══════════════════════════════════════════
// Snapshot mode
// ═══════════════════════════════════════════

// snapshotTable reads an entire table using keyset pagination for memory efficiency.
func (s *PostgresCDCSource) snapshotTable(ctx context.Context, table string) error {
	qualifiedTable := fmt.Sprintf("%s.%s", pgQuoteIdent(s.schema), pgQuoteIdent(table))
	topic := fmt.Sprintf("%s.%s", s.schema, table)

	// Discover primary key if orderBy not specified
	orderBy := s.orderBy
	if orderBy == "" {
		pk, err := s.discoverPrimaryKey(ctx, table)
		if err != nil || pk == "" {
			orderBy = "ctid" // fallback to system column
		} else {
			orderBy = pk
		}
	}

	// Get total row count for progress logging
	var totalRows int64
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", qualifiedTable)
	if err := s.pool.QueryRow(ctx, countQuery).Scan(&totalRows); err != nil {
		fmt.Fprintf(os.Stderr, "[postgres_cdc] count %s: %v (proceeding without progress)\n", table, err)
	}

	fmt.Fprintf(os.Stderr, "[postgres_cdc] snapshot %s: %d rows, batch_size=%d, order_by=%s\n",
		table, totalRows, s.batchSize, orderBy)

	var offset int64
	var lastKeyVal any

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		var query string
		var args []any

		// Use keyset pagination if we have a usable order column (not ctid)
		if orderBy != "ctid" && lastKeyVal != nil {
			query = fmt.Sprintf("SELECT * FROM %s WHERE %s > $1 ORDER BY %s LIMIT %d",
				qualifiedTable, pgQuoteIdent(orderBy), pgQuoteIdent(orderBy), s.batchSize)
			args = []any{lastKeyVal}
		} else {
			query = fmt.Sprintf("SELECT * FROM %s ORDER BY %s LIMIT %d OFFSET %d",
				qualifiedTable, orderBy, s.batchSize, offset)
		}

		rows, err := s.pool.Query(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("snapshot query %s: %w", table, err)
		}

		fieldDescs := rows.FieldDescriptions()
		colNames := make([]string, len(fieldDescs))
		for i, fd := range fieldDescs {
			colNames[i] = fd.Name
		}

		batchCount := 0
		for rows.Next() {
			values, err := rows.Values()
			if err != nil {
				rows.Close()
				return fmt.Errorf("snapshot scan %s: %w", table, err)
			}

			record := make(map[string]any, len(colNames))
			for i, col := range colNames {
				record[col] = convertPgValue(values[i])
			}

			// Track the last key value for keyset pagination
			if orderBy != "ctid" {
				if v, ok := record[orderBy]; ok {
					lastKeyVal = v
				}
			}

			// Extract key
			key := ""
			if v, ok := record[orderBy]; ok && orderBy != "ctid" {
				key = fmt.Sprintf("%v", v)
			}

			event := &pipeline.Event{
				Key:       []byte(key),
				Value:     record,
				Timestamp: time.Now(),
				Topic:     topic,
				Offset:    offset,
				Metadata: map[string]any{
					"operation": "snapshot",
					"table":     table,
					"schema":    s.schema,
				},
			}

			select {
			case s.eventCh <- event:
				offset++
				batchCount++
				s.lag.Store(int64(len(s.eventCh)))
			case <-ctx.Done():
				rows.Close()
				return nil
			}
		}
		rows.Close()

		if err := rows.Err(); err != nil {
			return fmt.Errorf("snapshot rows %s: %w", table, err)
		}

		// Log progress
		if totalRows > 0 {
			pct := float64(offset) / float64(totalRows) * 100
			fmt.Fprintf(os.Stderr, "[postgres_cdc] snapshot %s: %d/%d rows (%.0f%%)\n",
				table, offset, totalRows, pct)
		}

		// If we got fewer rows than batch size, we're done
		if batchCount < s.batchSize {
			break
		}
	}

	fmt.Fprintf(os.Stderr, "[postgres_cdc] snapshot %s complete: %d rows\n", table, offset)
	return nil
}

// discoverPrimaryKey returns the primary key column name for a table.
func (s *PostgresCDCSource) discoverPrimaryKey(ctx context.Context, table string) (string, error) {
	query := `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = $1::regclass AND i.indisprimary
		ORDER BY a.attnum
		LIMIT 1`

	qualifiedTable := fmt.Sprintf("%s.%s", s.schema, table)
	var pkCol string
	err := s.pool.QueryRow(ctx, query, qualifiedTable).Scan(&pkCol)
	if err != nil {
		return "", err
	}
	return pkCol, nil
}

// ═══════════════════════════════════════════
// CDC mode (logical replication)
// ═══════════════════════════════════════════

// startCDC begins logical replication using pglogrepl.
func (s *PostgresCDCSource) startCDC(ctx context.Context) error {
	// Build replication DSN (needs replication=database parameter)
	replDSN := s.dsn
	if strings.Contains(replDSN, "?") {
		replDSN += "&replication=database"
	} else {
		replDSN += "?replication=database"
	}

	conn, err := pgconn.Connect(ctx, replDSN)
	if err != nil {
		return fmt.Errorf("cdc connect: %w", err)
	}
	s.replConn = conn

	// Create publication if not exists
	tableList := make([]string, len(s.tables))
	for i, t := range s.tables {
		tableList[i] = fmt.Sprintf("%s.%s", pgQuoteIdent(s.schema), pgQuoteIdent(t))
	}

	pubName := s.pubName
	if pubName == "" {
		pubName = "mako_pub"
	}

	// Use the pool (not replication conn) for DDL
	createPub := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s",
		pgQuoteIdent(pubName), strings.Join(tableList, ", "))
	if _, err := s.pool.Exec(ctx, createPub); err != nil {
		// Ignore "already exists" errors
		if !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("cdc create publication: %w", err)
		}
	}

	// Create replication slot if not exists
	slotName := s.slotName
	if slotName == "" {
		slotName = "mako_slot"
	}

	var startLSN pglogrepl.LSN
	if s.startLSN != "" {
		startLSN, err = pglogrepl.ParseLSN(s.startLSN)
		if err != nil {
			return fmt.Errorf("cdc parse start_lsn: %w", err)
		}
	}

	if startLSN == 0 {
		// Try to create the slot (will fail if it already exists)
		result, err := pglogrepl.CreateReplicationSlot(ctx, conn, slotName, "pgoutput",
			pglogrepl.CreateReplicationSlotOptions{Temporary: false})
		if err != nil {
			if !strings.Contains(err.Error(), "already exists") {
				return fmt.Errorf("cdc create slot: %w", err)
			}
			// Slot exists — will resume from its confirmed LSN
		} else {
			startLSN, _ = pglogrepl.ParseLSN(result.ConsistentPoint)
		}
	}

	fmt.Fprintf(os.Stderr, "[postgres_cdc] starting CDC (slot=%s, publication=%s, lsn=%s)\n",
		slotName, pubName, startLSN)

	// Start replication
	err = pglogrepl.StartReplication(ctx, conn, slotName, startLSN,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '1'",
				fmt.Sprintf("publication_names '%s'", pubName),
			},
		})
	if err != nil {
		return fmt.Errorf("cdc start replication: %w", err)
	}

	// Build relation map for decoding WAL messages
	relations := make(map[uint32]*pglogrepl.RelationMessage)
	var currentLSN pglogrepl.LSN

	standbyTicker := time.NewTicker(10 * time.Second)
	defer standbyTicker.Stop()

	var offset int64

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-standbyTicker.C:
			// Send standby status update to keep connection alive
			err := pglogrepl.SendStandbyStatusUpdate(ctx, conn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: currentLSN})
			if err != nil {
				return fmt.Errorf("cdc standby update: %w", err)
			}
		default:
		}

		// Receive message with timeout
		receiveCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		rawMsg, err := conn.ReceiveMessage(receiveCtx)
		cancel()

		if err != nil {
			if pgconn.Timeout(err) {
				continue // Timeout is normal — just loop and check ctx
			}
			if ctx.Err() != nil {
				return nil // Graceful shutdown
			}
			return fmt.Errorf("cdc receive: %w", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("cdc error from server: %s", errMsg.Message)
		}

		copyData, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		switch copyData.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("cdc parse keepalive: %w", err)
			}
			if pkm.ReplyRequested {
				err = pglogrepl.SendStandbyStatusUpdate(ctx, conn,
					pglogrepl.StandbyStatusUpdate{WALWritePosition: currentLSN})
				if err != nil {
					return fmt.Errorf("cdc standby reply: %w", err)
				}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("cdc parse xlog: %w", err)
			}
			currentLSN = xld.WALStart + pglogrepl.LSN(len(xld.WALData))

			events, err := s.parseWALMessage(xld.WALData, relations, currentLSN, &offset)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[postgres_cdc] parse WAL: %v\n", err)
				continue
			}

			for _, event := range events {
				select {
				case s.eventCh <- event:
					s.lag.Store(int64(len(s.eventCh)))
				case <-ctx.Done():
					return nil
				}
			}
		}
	}
}

// parseWALMessage parses a single WAL message and returns events.
func (s *PostgresCDCSource) parseWALMessage(
	walData []byte,
	relations map[uint32]*pglogrepl.RelationMessage,
	lsn pglogrepl.LSN,
	offset *int64,
) ([]*pipeline.Event, error) {
	logicalMsg, err := pglogrepl.Parse(walData)
	if err != nil {
		return nil, fmt.Errorf("parse logical message: %w", err)
	}

	switch msg := logicalMsg.(type) {
	case *pglogrepl.RelationMessage:
		relations[msg.RelationID] = msg
		return nil, nil

	case *pglogrepl.InsertMessage:
		rel, ok := relations[msg.RelationID]
		if !ok {
			return nil, nil
		}
		record := tupleToMap(msg.Tuple, rel)
		topic := fmt.Sprintf("%s.%s", rel.Namespace, rel.RelationName)
		key := extractKey(record, rel)

		event := &pipeline.Event{
			Key:       []byte(key),
			Value:     record,
			Timestamp: time.Now(),
			Topic:     topic,
			Offset:    *offset,
			Metadata: map[string]any{
				"operation": "insert",
				"lsn":       lsn.String(),
				"table":     rel.RelationName,
				"schema":    rel.Namespace,
			},
		}
		*offset++
		return []*pipeline.Event{event}, nil

	case *pglogrepl.UpdateMessage:
		rel, ok := relations[msg.RelationID]
		if !ok {
			return nil, nil
		}
		record := tupleToMap(msg.NewTuple, rel)
		topic := fmt.Sprintf("%s.%s", rel.Namespace, rel.RelationName)
		key := extractKey(record, rel)

		meta := map[string]any{
			"operation": "update",
			"lsn":       lsn.String(),
			"table":     rel.RelationName,
			"schema":    rel.Namespace,
		}

		// Include old values if available (REPLICA IDENTITY FULL)
		if msg.OldTuple != nil {
			oldRecord := tupleToMap(msg.OldTuple, rel)
			meta["old_values"] = oldRecord
		}

		event := &pipeline.Event{
			Key:       []byte(key),
			Value:     record,
			Timestamp: time.Now(),
			Topic:     topic,
			Offset:    *offset,
			Metadata:  meta,
		}
		*offset++
		return []*pipeline.Event{event}, nil

	case *pglogrepl.DeleteMessage:
		rel, ok := relations[msg.RelationID]
		if !ok {
			return nil, nil
		}
		record := tupleToMap(msg.OldTuple, rel)
		topic := fmt.Sprintf("%s.%s", rel.Namespace, rel.RelationName)
		key := extractKey(record, rel)

		event := &pipeline.Event{
			Key:       []byte(key),
			Value:     record,
			Timestamp: time.Now(),
			Topic:     topic,
			Offset:    *offset,
			Metadata: map[string]any{
				"operation": "delete",
				"lsn":       lsn.String(),
				"table":     rel.RelationName,
				"schema":    rel.Namespace,
			},
		}
		*offset++
		return []*pipeline.Event{event}, nil

	case *pglogrepl.BeginMessage, *pglogrepl.CommitMessage, *pglogrepl.TypeMessage, *pglogrepl.OriginMessage:
		// Transactional control messages — skip
		return nil, nil
	}

	return nil, nil
}

// tupleToMap converts a pglogrepl tuple into a map[string]any.
func tupleToMap(tuple *pglogrepl.TupleData, rel *pglogrepl.RelationMessage) map[string]any {
	if tuple == nil {
		return nil
	}
	record := make(map[string]any, len(tuple.Columns))
	for i, col := range tuple.Columns {
		if i >= len(rel.Columns) {
			break
		}
		colName := rel.Columns[i].Name
		switch col.DataType {
		case 'n': // null
			record[colName] = nil
		case 'u': // unchanged TOAST
			record[colName] = nil
		case 't': // text
			record[colName] = string(col.Data)
		}
	}
	return record
}

// extractKey returns the first column value as a string key.
func extractKey(record map[string]any, rel *pglogrepl.RelationMessage) string {
	if len(rel.Columns) == 0 {
		return ""
	}
	// Use first column that is part of the replica identity as key
	for _, col := range rel.Columns {
		if col.Flags == 1 { // part of key
			if v, ok := record[col.Name]; ok {
				return fmt.Sprintf("%v", v)
			}
		}
	}
	// Fallback to first column
	if v, ok := record[rel.Columns[0].Name]; ok {
		return fmt.Sprintf("%v", v)
	}
	return ""
}

// Close stops the CDC source and cleans up connections.
func (s *PostgresCDCSource) Close() error {
	if s.replConn != nil {
		_ = s.replConn.Close(context.Background())
	}
	if s.pool != nil {
		s.pool.Close()
	}
	s.wg.Wait()
	return nil
}

// Lag returns the event channel backlog.
func (s *PostgresCDCSource) Lag() int64 {
	return s.lag.Load()
}

// ═══════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════

// pgQuoteIdent quotes a PostgreSQL identifier with double quotes.
func pgQuoteIdent(name string) string {
	escaped := strings.ReplaceAll(name, `"`, `""`)
	return `"` + escaped + `"`
}

// convertPgValue converts pgx values to JSON-friendly types.
func convertPgValue(v any) any {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case time.Time:
		return val.Format(time.RFC3339Nano)
	case []byte:
		// Try to parse as JSON
		var jsonVal any
		if err := json.Unmarshal(val, &jsonVal); err == nil {
			return jsonVal
		}
		return string(val)
	case [16]byte:
		// UUID
		return fmt.Sprintf("%x-%x-%x-%x-%x", val[0:4], val[4:6], val[6:8], val[8:10], val[10:16])
	default:
		return val
	}
}

// strSliceFromConfig extracts a string slice from config.
// Supports both []any (YAML parsed) and []string formats.
func strSliceFromConfig(cfg map[string]any, key string) []string {
	if cfg == nil {
		return nil
	}
	switch v := cfg[key].(type) {
	case []any:
		result := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				result = append(result, s)
			}
		}
		return result
	case []string:
		return v
	case string:
		// Comma-separated string
		if v == "" {
			return nil
		}
		parts := strings.Split(v, ",")
		for i := range parts {
			parts[i] = strings.TrimSpace(parts[i])
		}
		return parts
	}
	return nil
}

// SnapshotEvent creates a snapshot event for testing purposes.
func SnapshotEvent(table, schema string, record map[string]any, offset int64) *pipeline.Event {
	topic := fmt.Sprintf("%s.%s", schema, table)
	return &pipeline.Event{
		Value:     record,
		Timestamp: time.Now(),
		Topic:     topic,
		Offset:    offset,
		Metadata: map[string]any{
			"operation": "snapshot",
			"table":     table,
			"schema":    schema,
		},
	}
}

// CDCEvent creates a CDC event for testing purposes.
func CDCEvent(table, schema, operation, lsn string, record map[string]any, offset int64) *pipeline.Event {
	topic := fmt.Sprintf("%s.%s", schema, table)
	return &pipeline.Event{
		Value:     record,
		Timestamp: time.Now(),
		Topic:     topic,
		Offset:    offset,
		Metadata: map[string]any{
			"operation": operation,
			"lsn":       lsn,
			"table":     table,
			"schema":    schema,
		},
	}
}

// Ensure PostgresCDCSource implements pipeline.Source at compile time.
var _ pipeline.Source = (*PostgresCDCSource)(nil)

// unused import guard (pgx is used for pgxpool which imports pgx)
var _ pgx.Rows = (pgx.Rows)(nil)

// unused import guard for sort
var _ = sort.Strings
