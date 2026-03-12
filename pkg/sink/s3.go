package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/Stefen-Taime/mako/pkg/pipeline"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ═══════════════════════════════════════════
// S3 Sink (AWS SDK v2)
// ═══════════════════════════════════════════

// S3Sink writes events to Amazon S3 as JSON/JSONL/Parquet/CSV files.
// Events are buffered and flushed as objects partitioned by time.
//
// YAML example:
//
//	sink:
//	  type: s3
//	  bucket: my-data-lake
//	  prefix: raw/events
//	  format: parquet         # jsonl|json|parquet|csv
//	  config:
//	    region: us-east-1
//	    compression: snappy   # parquet only: snappy|zstd|gzip|none
//	    csv_delimiter: ","    # csv only: "," | ";" | "\t" | "tab" | "|"
//	    # Optional: override credentials (defaults to AWS SDK chain)
//	    access_key_id: AKIA...
//	    secret_access_key: ...
type S3Sink struct {
	bucket string
	prefix string
	format string // "jsonl" (default), "json", "parquet", "csv"
	region string
	cfg    map[string]any

	client *s3.Client
	buffer []*pipeline.Event
	mu     sync.Mutex
}

// NewS3Sink creates an S3 sink.
func NewS3Sink(bucket, prefix, format string, cfg map[string]any) *S3Sink {
	if format == "" {
		format = "jsonl"
	}
	region := Resolve(cfg, "region", "AWS_REGION", "us-east-1")

	return &S3Sink{
		bucket: bucket,
		prefix: prefix,
		format: format,
		region: region,
		cfg:    cfg,
	}
}

// Open initialises the S3 client using the default credential chain.
// Explicit credentials from config or env vars override the chain.
func (s *S3Sink) Open(ctx context.Context) error {
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(s.region),
	}

	// Allow explicit credentials via config
	accessKey := Resolve(s.cfg, "access_key_id", "AWS_ACCESS_KEY_ID", "")
	secretKey := Resolve(s.cfg, "secret_access_key", "AWS_SECRET_ACCESS_KEY", "")
	if accessKey != "" && secretKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		))
	}

	// Optional custom endpoint (e.g. MinIO, LocalStack)
	endpoint := Resolve(s.cfg, "endpoint", "AWS_S3_ENDPOINT", "")

	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return fmt.Errorf("s3 load config: %w", err)
	}

	s3Opts := []func(*s3.Options){}
	if endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true // required for MinIO / LocalStack
		})
	}

	s.client = s3.NewFromConfig(awsCfg, s3Opts...)
	return nil
}

// Write buffers events for the next flush.
func (s *S3Sink) Write(ctx context.Context, events []*pipeline.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.buffer = append(s.buffer, events...)
	return nil
}

// Flush writes buffered events to S3 as a single object.
// Object key: <prefix>/year=YYYY/month=MM/day=DD/hour=HH/<timestamp>_<count>.<ext>
func (s *S3Sink) Flush(ctx context.Context) error {
	s.mu.Lock()
	if len(s.buffer) == 0 {
		s.mu.Unlock()
		return nil
	}
	batch := s.buffer
	s.buffer = nil
	s.mu.Unlock()

	now := time.Now().UTC()
	key := s.objectKey(now, len(batch))

	body, err := s.encode(batch)
	if err != nil {
		return fmt.Errorf("s3 encode: %w", err)
	}

	contentType := FormatContentType(s.format)

	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(body),
		ContentType: aws.String(contentType),
	})
	if err != nil {
		// Put events back in buffer for retry
		s.mu.Lock()
		s.buffer = append(batch, s.buffer...)
		s.mu.Unlock()
		return fmt.Errorf("s3 put object: %w", err)
	}

	return nil
}

// Close flushes remaining events and releases resources.
func (s *S3Sink) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return s.Flush(ctx)
}

// Name returns the sink identifier.
func (s *S3Sink) Name() string {
	return fmt.Sprintf("s3:%s/%s", s.bucket, s.prefix)
}

// Format returns the configured output format.
func (s *S3Sink) Format() string {
	return s.format
}

// ── helpers ──

func (s *S3Sink) objectKey(t time.Time, count int) string {
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

func (s *S3Sink) encode(events []*pipeline.Event) ([]byte, error) {
	switch s.format {
	case "parquet":
		compression := Resolve(s.cfg, "compression", "", "snappy")
		return EncodeParquet(events, compression)

	case "csv":
		delimStr := Resolve(s.cfg, "csv_delimiter", "", ",")
		delimiter := ParseCSVDelimiter(delimStr)
		return EncodeCSV(events, delimiter)

	case "json":
		// JSON array
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
		// Default: JSONL (one JSON object per line)
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

// buildRecord creates the output record for an event, including metadata.
func buildRecord(e *pipeline.Event) map[string]any {
	rec := make(map[string]any, len(e.Value)+4)
	for k, v := range e.Value {
		rec[k] = v
	}
	// Add pipeline metadata (non-overlapping keys)
	if e.Topic != "" {
		rec["_topic"] = e.Topic
	}
	if e.Offset != 0 {
		rec["_offset"] = e.Offset
	}
	if !e.Timestamp.IsZero() {
		rec["_ts"] = e.Timestamp.Format(time.RFC3339)
	}
	return rec
}

// init registers the S3_BUCKET env var check for early feedback.
func init() {
	_ = os.Getenv("AWS_REGION") // no-op, just for documentation
}
