// Package sink implements output adapters for Mako pipelines.
//
// Each sink implements the pipeline.Sink interface.
// Mako supports multiple sinks per pipeline.
package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	v1 "github.com/Stefen-Taime/mako/api/v1"
	"github.com/Stefen-Taime/mako/pkg/kafka"
	"github.com/Stefen-Taime/mako/pkg/pipeline"
)

// ═══════════════════════════════════════════
// Stdout Sink (for debugging / development)
// ═══════════════════════════════════════════

type StdoutSink struct {
	writer io.Writer
	count  int64
	mu     sync.Mutex
}

func NewStdoutSink() *StdoutSink {
	return &StdoutSink{writer: os.Stdout}
}

func (s *StdoutSink) Open(ctx context.Context) error   { return nil }
func (s *StdoutSink) Close() error                      { return nil }
func (s *StdoutSink) Flush(ctx context.Context) error   { return nil }
func (s *StdoutSink) Name() string                      { return "stdout" }

func (s *StdoutSink) Write(ctx context.Context, events []*pipeline.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, event := range events {
		data, err := json.Marshal(event.Value)
		if err != nil {
			return err
		}
		fmt.Fprintf(s.writer, "[%s] %s\n", time.Now().Format(time.RFC3339), string(data))
		s.count++
	}
	return nil
}

// ═══════════════════════════════════════════
// File Sink (for local testing)
// ═══════════════════════════════════════════

type FileSink struct {
	path   string
	format string // json|jsonl
	file   *os.File
	mu     sync.Mutex
}

func NewFileSink(path, format string) *FileSink {
	if format == "" {
		format = "jsonl"
	}
	return &FileSink{path: path, format: format}
}

func (s *FileSink) Open(ctx context.Context) error {
	f, err := os.Create(s.path)
	if err != nil {
		return err
	}
	s.file = f
	return nil
}

func (s *FileSink) Write(ctx context.Context, events []*pipeline.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, event := range events {
		data, err := json.Marshal(event.Value)
		if err != nil {
			return err
		}
		fmt.Fprintf(s.file, "%s\n", data)
	}
	return nil
}

func (s *FileSink) Flush(ctx context.Context) error {
	if s.file != nil {
		return s.file.Sync()
	}
	return nil
}

func (s *FileSink) Close() error {
	if s.file != nil {
		return s.file.Close()
	}
	return nil
}

func (s *FileSink) Name() string { return "file:" + s.path }

// ═══════════════════════════════════════════
// Builder — construct sinks from YAML specs
// ═══════════════════════════════════════════

// BuildFromSpec creates the appropriate sink from a YAML spec.
func BuildFromSpec(spec v1.Sink) (pipeline.Sink, error) {
	switch spec.Type {
	case v1.SinkStdout:
		return NewStdoutSink(), nil
	case v1.SinkSnowflake:
		return NewSnowflakeSink(spec.Database, spec.Schema, spec.Table, spec.Flatten, spec.Config), nil
	case v1.SinkBigQuery:
		project, _ := spec.Config["project"].(string)
		return NewBigQuerySink(project, spec.Schema, spec.Table, spec.Flatten, spec.Config), nil
	case v1.SinkPostgres:
		return NewPostgresSink(spec.Database, spec.Schema, spec.Table, spec.Flatten, spec.Config), nil
	case v1.SinkKafka:
		brokers := ""
		if spec.Config != nil {
			brokers, _ = spec.Config["brokers"].(string)
		}
		return NewKafkaSink(brokers, spec.Topic), nil
	case v1.SinkS3:
		return NewS3Sink(spec.Bucket, spec.Prefix, spec.Format, spec.Config), nil
	case v1.SinkGCS:
		return NewGCSSink(spec.Bucket, spec.Prefix, spec.Format, spec.Config), nil
	case v1.SinkClickHouse:
		return NewClickHouseSink(spec.Database, spec.Table, spec.Flatten, spec.Config), nil
	case v1.SinkDuckDB:
		return NewDuckDBSink(spec.Database, spec.Table, spec.Config), nil
	default:
		return nil, fmt.Errorf("unsupported sink type: %s", spec.Type)
	}
}

// NewKafkaSink creates a Kafka sink that writes events to a Kafka topic
// using the franz-go based implementation in pkg/kafka.
func NewKafkaSink(brokers, topic string) pipeline.Sink {
	return &kafkaSinkAdapter{
		inner: kafka.NewSink(brokers, topic),
		topic: topic,
	}
}

type kafkaSinkAdapter struct {
	inner *kafka.Sink
	topic string
}

func (s *kafkaSinkAdapter) Open(ctx context.Context) error {
	return s.inner.Open(ctx)
}

func (s *kafkaSinkAdapter) Write(ctx context.Context, events []*pipeline.Event) error {
	return s.inner.Write(ctx, events)
}

func (s *kafkaSinkAdapter) Flush(ctx context.Context) error {
	return s.inner.Flush(ctx)
}

func (s *kafkaSinkAdapter) Close() error {
	return s.inner.Close()
}

func (s *kafkaSinkAdapter) Name() string {
	return "kafka:" + s.topic
}
