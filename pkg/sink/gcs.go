package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Stefen-Taime/mako/pkg/pipeline"
)

// ═══════════════════════════════════════════
// GCS Sink (cloud.google.com/go/storage)
// ═══════════════════════════════════════════

// GCSSink writes events to Google Cloud Storage as JSON/JSONL/Parquet/CSV files.
// Authentication uses Application Default Credentials (ADC).
// Set GOOGLE_APPLICATION_CREDENTIALS or use `gcloud auth application-default login`.
//
// YAML example:
//
//	sink:
//	  type: gcs
//	  bucket: my-data-lake
//	  prefix: raw/events
//	  format: parquet           # jsonl|json|parquet|csv
//	  config:
//	    project: my-gcp-project
//	    compression: zstd       # parquet only: snappy|zstd|gzip|none
//	    csv_delimiter: ";"      # csv only: "," | ";" | "\t" | "tab" | "|"
type GCSSink struct {
	bucket  string
	prefix  string
	format  string // "jsonl" (default), "json", "parquet", "csv"
	project string
	cfg     map[string]any

	client *storage.Client
	buffer []*pipeline.Event
	mu     sync.Mutex
}

// NewGCSSink creates a GCS sink.
func NewGCSSink(bucket, prefix, format string, cfg map[string]any) *GCSSink {
	if format == "" {
		format = "jsonl"
	}
	project := Resolve(cfg, "project", "GCP_PROJECT", "")

	return &GCSSink{
		bucket:  bucket,
		prefix:  prefix,
		format:  format,
		project: project,
		cfg:     cfg,
	}
}

// Open initialises the GCS client using Application Default Credentials.
func (s *GCSSink) Open(ctx context.Context) error {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("gcs new client: %w", err)
	}
	s.client = client
	return nil
}

// Write buffers events for the next flush.
func (s *GCSSink) Write(ctx context.Context, events []*pipeline.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.buffer = append(s.buffer, events...)
	return nil
}

// Flush writes buffered events to GCS as a single object.
// Object name: <prefix>/year=YYYY/month=MM/day=DD/hour=HH/<timestamp>_<count>.<ext>
func (s *GCSSink) Flush(ctx context.Context) error {
	s.mu.Lock()
	if len(s.buffer) == 0 {
		s.mu.Unlock()
		return nil
	}
	batch := s.buffer
	s.buffer = nil
	s.mu.Unlock()

	now := time.Now().UTC()
	objectName := s.objectName(now, len(batch))

	body, err := s.encode(batch)
	if err != nil {
		return fmt.Errorf("gcs encode: %w", err)
	}

	bkt := s.client.Bucket(s.bucket)
	obj := bkt.Object(objectName)
	w := obj.NewWriter(ctx)

	w.ContentType = FormatContentType(s.format)

	if _, err := w.Write(body); err != nil {
		_ = w.Close()
		// Put events back in buffer for retry
		s.mu.Lock()
		s.buffer = append(batch, s.buffer...)
		s.mu.Unlock()
		return fmt.Errorf("gcs write: %w", err)
	}

	if err := w.Close(); err != nil {
		s.mu.Lock()
		s.buffer = append(batch, s.buffer...)
		s.mu.Unlock()
		return fmt.Errorf("gcs close writer: %w", err)
	}

	return nil
}

// Close flushes remaining events and releases the client.
func (s *GCSSink) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := s.Flush(ctx); err != nil {
		return err
	}

	if s.client != nil {
		return s.client.Close()
	}
	return nil
}

// Name returns the sink identifier.
func (s *GCSSink) Name() string {
	return fmt.Sprintf("gcs:%s/%s", s.bucket, s.prefix)
}

// Format returns the configured output format.
func (s *GCSSink) Format() string {
	return s.format
}

// ── helpers ──

func (s *GCSSink) objectName(t time.Time, count int) string {
	prefix := s.prefix
	if prefix != "" && prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}
	partition := fmt.Sprintf("year=%04d/month=%02d/day=%02d/hour=%02d",
		t.Year(), t.Month(), t.Day(), t.Hour())
	ts := t.Format("20060102T150405Z")
	ext := FormatExtension(s.format)
	return fmt.Sprintf("%s%s/%s_%d.%s", prefix, partition, ts, count, ext)
}

func (s *GCSSink) encode(events []*pipeline.Event) ([]byte, error) {
	switch s.format {
	case "parquet":
		compression := Resolve(s.cfg, "compression", "", "snappy")
		return EncodeParquet(events, compression)

	case "csv":
		delimStr := Resolve(s.cfg, "csv_delimiter", "", ",")
		delimiter := ParseCSVDelimiter(delimStr)
		return EncodeCSV(events, delimiter)

	case "json":
		records := make([]map[string]any, 0, len(events))
		for _, e := range events {
			rec := buildRecord(e)
			records = append(records, rec)
		}
		data, err := json.Marshal(records)
		if err != nil {
			return nil, err
		}
		return data, nil

	default:
		// Default: JSONL
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		for _, e := range events {
			rec := buildRecord(e)
			if err := enc.Encode(rec); err != nil {
				return nil, err
			}
		}
		return buf.Bytes(), nil
	}
}
