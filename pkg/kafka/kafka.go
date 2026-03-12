// Package kafka implements the Kafka source and sink for Mako.
//
// Uses franz-go (pure Go, zero CGO) for maximum portability.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Stefen-Taime/mako/pkg/pipeline"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ═══════════════════════════════════════════
// Kafka Source (consumer)
// ═══════════════════════════════════════════

// Source reads events from a Kafka topic using franz-go.
type Source struct {
	brokers       []string
	topic         string
	consumerGroup string
	startOffset   string

	client  *kgo.Client
	eventCh chan *pipeline.Event
	lag     atomic.Int64
	closed  atomic.Bool
	wg      sync.WaitGroup
}

// NewSource creates a Kafka source.
func NewSource(brokers, topic, consumerGroup, startOffset string) *Source {
	return &Source{
		brokers:       strings.Split(brokers, ","),
		topic:         topic,
		consumerGroup: consumerGroup,
		startOffset:   startOffset,
		eventCh:       make(chan *pipeline.Event, 1000),
	}
}

// Open connects to Kafka and initializes the franz-go consumer.
func (s *Source) Open(ctx context.Context) error {
	opts := []kgo.Opt{
		kgo.SeedBrokers(s.brokers...),
		kgo.ConsumeTopics(s.topic),
		kgo.FetchMaxWait(500 * time.Millisecond),
		kgo.FetchMinBytes(1),
	}

	// Consumer group
	if s.consumerGroup != "" {
		opts = append(opts, kgo.ConsumerGroup(s.consumerGroup))
		opts = append(opts, kgo.DisableAutoCommit())
	}

	// Start offset
	switch s.startOffset {
	case "earliest":
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	default: // "latest" or empty
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("kafka connect: %w", err)
	}

	// Verify connectivity
	if err := client.Ping(ctx); err != nil {
		client.Close()
		return fmt.Errorf("kafka ping %s: %w", strings.Join(s.brokers, ","), err)
	}

	s.client = client
	return nil
}

// Read starts consuming and returns the event channel.
func (s *Source) Read(ctx context.Context) (<-chan *pipeline.Event, error) {
	s.wg.Add(1)
	go s.consumeLoop(ctx)
	return s.eventCh, nil
}

// consumeLoop polls Kafka and pushes events to the channel.
func (s *Source) consumeLoop(ctx context.Context) {
	defer s.wg.Done()
	defer func() {
		if s.closed.CompareAndSwap(false, true) {
			close(s.eventCh)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		fetches := s.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			// Log but continue — transient errors are common
			for _, e := range errs {
				fmt.Printf("[kafka] fetch error topic=%s partition=%d: %v\n",
					e.Topic, e.Partition, e.Err)
			}
		}

		fetches.EachRecord(func(record *kgo.Record) {
			// Parse JSON value
			var value map[string]any
			if err := json.Unmarshal(record.Value, &value); err != nil {
				// Non-JSON record: wrap raw bytes
				value = map[string]any{
					"_raw":   string(record.Value),
					"_error": "non-json payload",
				}
			}

			// Convert headers
			headers := make(map[string]string, len(record.Headers))
			for _, h := range record.Headers {
				headers[h.Key] = string(h.Value)
			}

			event := &pipeline.Event{
				Key:       record.Key,
				Value:     value,
				RawValue:  record.Value,
				Timestamp: record.Timestamp,
				Topic:     record.Topic,
				Partition: record.Partition,
				Offset:    record.Offset,
				Headers:   headers,
			}

			select {
			case s.eventCh <- event:
			case <-ctx.Done():
				return
			}
		})

		// Commit offsets for consumer group
		if s.consumerGroup != "" {
			s.client.AllowRebalance()
			if err := s.client.CommitUncommittedOffsets(ctx); err != nil && ctx.Err() == nil {
				fmt.Printf("[kafka] commit error: %v\n", err)
			}
		}

		// Update lag estimate
		s.lag.Store(int64(len(s.eventCh)))
	}
}

// Close shuts down the consumer.
func (s *Source) Close() error {
	if s.client != nil {
		s.client.Close()
	}
	s.wg.Wait()
	return nil
}

// Lag returns the current consumer lag estimate.
func (s *Source) Lag() int64 {
	return s.lag.Load()
}

// ═══════════════════════════════════════════
// Kafka Sink (producer)
// ═══════════════════════════════════════════

// Sink writes events to a Kafka topic using franz-go.
type Sink struct {
	brokers              []string
	topic                string
	allowAutoTopicCreate bool
	client               *kgo.Client
	mu                   sync.Mutex
}

// NewSink creates a Kafka sink.
func NewSink(brokers, topic string) *Sink {
	return &Sink{
		brokers: strings.Split(brokers, ","),
		topic:   topic,
	}
}

// WithAutoTopicCreation enables auto-creation of the target topic
// if it does not exist (requires broker allow.auto.create.topics=true).
func (s *Sink) WithAutoTopicCreation() *Sink {
	s.allowAutoTopicCreate = true
	return s
}

// Open initializes the Kafka producer.
func (s *Sink) Open(ctx context.Context) error {
	opts := []kgo.Opt{
		kgo.SeedBrokers(s.brokers...),
		kgo.DefaultProduceTopic(s.topic),
		kgo.ProducerBatchMaxBytes(1024 * 1024), // 1MB
		kgo.ProducerLinger(100 * time.Millisecond),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	}

	if s.allowAutoTopicCreate {
		opts = append(opts, kgo.AllowAutoTopicCreation())
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("kafka producer connect: %w", err)
	}

	if err := client.Ping(ctx); err != nil {
		client.Close()
		return fmt.Errorf("kafka producer ping: %w", err)
	}

	s.client = client
	return nil
}

// Write produces events to the Kafka topic.
func (s *Sink) Write(ctx context.Context, events []*pipeline.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var wg sync.WaitGroup
	var firstErr error
	var errOnce sync.Once

	for _, event := range events {
		data, err := json.Marshal(event.Value)
		if err != nil {
			return fmt.Errorf("marshal event: %w", err)
		}

		record := &kgo.Record{
			Key:   event.Key,
			Value: data,
			Topic: s.topic,
		}

		// Copy headers
		for k, v := range event.Headers {
			record.Headers = append(record.Headers, kgo.RecordHeader{
				Key:   k,
				Value: []byte(v),
			})
		}

		wg.Add(1)
		s.client.Produce(ctx, record, func(r *kgo.Record, err error) {
			defer wg.Done()
			if err != nil {
				errOnce.Do(func() { firstErr = err })
			}
		})
	}

	wg.Wait()
	return firstErr
}

// Flush ensures all buffered records are sent.
func (s *Sink) Flush(ctx context.Context) error {
	if s.client != nil {
		if err := s.client.Flush(ctx); err != nil {
			return fmt.Errorf("kafka flush: %w", err)
		}
	}
	return nil
}

// Close shuts down the producer.
func (s *Sink) Close() error {
	if s.client != nil {
		s.client.Close()
	}
	return nil
}

// Name returns the sink identifier.
func (s *Sink) Name() string { return "kafka:" + s.topic }
