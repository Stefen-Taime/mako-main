// Package pipeline implements the Mako runtime engine.
//
// It consumes events from sources, applies transforms in sequence,
// and delivers to sinks. Each pipeline runs in isolation
// (one worker per event type).
package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	v1 "github.com/Stefen-Taime/mako/api/v1"
	"github.com/Stefen-Taime/mako/pkg/schema"
	"github.com/Stefen-Taime/mako/pkg/transform"
)

// Event represents a single event flowing through the pipeline.
type Event struct {
	Key       []byte
	Value     map[string]any
	RawValue  []byte
	Timestamp time.Time
	Topic     string
	Partition int32
	Offset    int64
	Headers   map[string]string
	Metadata  map[string]any
}

// EnsureRawValue lazily serializes Value to JSON if RawValue is nil.
// This avoids expensive json.Marshal in hot paths (CSV, JSON streaming)
// when the sink may not need the raw bytes at all.
func (e *Event) EnsureRawValue() []byte {
	if e.RawValue == nil && e.Value != nil {
		e.RawValue, _ = json.Marshal(e.Value)
	}
	return e.RawValue
}

// Source reads events from an external system.
type Source interface {
	Open(ctx context.Context) error
	Read(ctx context.Context) (<-chan *Event, error)
	Close() error
	Lag() int64
}

// Sink writes events to an external system.
type Sink interface {
	Open(ctx context.Context) error
	Write(ctx context.Context, events []*Event) error
	Flush(ctx context.Context) error
	Close() error
	Name() string
}

// Pipeline is the core runtime that connects Source → Transforms → Sink(s).
type Pipeline struct {
	spec            v1.Pipeline
	source          Source
	chain           *transform.Chain
	sinks           []Sink
	dlq             Sink               // Dead Letter Queue sink (optional)
	schemaValidator *schema.Validator   // Schema Registry validator (optional)

	// Metrics
	eventsIn       atomic.Int64
	eventsOut      atomic.Int64
	errors         atomic.Int64
	dlqCount       atomic.Int64
	schemaFails    atomic.Int64
	sinkLatency    atomic.Int64 // last sink write latency in microseconds
	lastEvent      atomic.Value // time.Time
	startedAt      time.Time

	// Control
	state  atomic.Value // v1.PipelineState
	cancel context.CancelFunc
	wg     sync.WaitGroup
	done   chan struct{} // closed when the event loop exits (e.g. file source EOF)
}

// New creates a new pipeline from a spec, source, transforms, and sinks.
func New(spec v1.Pipeline, source Source, chain *transform.Chain, sinks []Sink) *Pipeline {
	p := &Pipeline{
		spec:   spec,
		source: source,
		chain:  chain,
		sinks:  sinks,
		done:   make(chan struct{}),
	}
	p.state.Store(v1.StateStopped)
	return p
}

// SetDLQ configures a Dead Letter Queue sink for failed events.
func (p *Pipeline) SetDLQ(dlq Sink) {
	p.dlq = dlq
}

// SetSchemaValidator configures Schema Registry validation for incoming events.
func (p *Pipeline) SetSchemaValidator(v *schema.Validator) {
	p.schemaValidator = v
}

// Start begins pipeline execution.
func (p *Pipeline) Start(ctx context.Context) error {
	ctx, p.cancel = context.WithCancel(ctx)
	p.startedAt = time.Now()
	p.state.Store(v1.StateStarting)

	// Preflight: open and verify all connections before processing any data
	fmt.Fprintf(os.Stderr, "🔌 Preflight checks...\n")

	// Open source
	if err := p.source.Open(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "   ❌ source — %v\n", err)
		p.state.Store(v1.StateFailed)
		return fmt.Errorf("open source: %w", err)
	}
	fmt.Fprintf(os.Stderr, "   ✅ source — ready\n")

	// Open sinks
	for _, s := range p.sinks {
		if err := s.Open(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "   ❌ %s — %v\n", s.Name(), err)
			p.state.Store(v1.StateFailed)
			return fmt.Errorf("open sink %s: %w", s.Name(), err)
		}
		fmt.Fprintf(os.Stderr, "   ✅ %s — connected\n", s.Name())
	}

	// Start event loop
	eventCh, err := p.source.Read(ctx)
	if err != nil {
		p.state.Store(v1.StateFailed)
		return fmt.Errorf("read source: %w", err)
	}

	p.state.Store(v1.StateRunning)

	// Batch accumulator
	batchSize := 1000
	if p.spec.Sink.Batch != nil && p.spec.Sink.Batch.Size > 0 {
		batchSize = p.spec.Sink.Batch.Size
	}

	flushInterval := 10 * time.Second
	if p.spec.Sink.Batch != nil && p.spec.Sink.Batch.Interval != "" {
		if d, err := time.ParseDuration(p.spec.Sink.Batch.Interval); err == nil {
			flushInterval = d
		}
	}

	workers := 1
	if p.spec.Sink.Batch != nil && p.spec.Sink.Batch.Workers > 0 {
		workers = p.spec.Sink.Batch.Workers
	}

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		if workers > 1 {
			p.parallelEventLoop(ctx, eventCh, batchSize, flushInterval, workers)
		} else {
			p.eventLoop(ctx, eventCh, batchSize, flushInterval)
		}
		close(p.done)
	}()

	return nil
}

// eventLoop is the core processing loop.
func (p *Pipeline) eventLoop(ctx context.Context, eventCh <-chan *Event, batchSize int, flushInterval time.Duration) {
	batch := make([]*Event, 0, batchSize)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	// flush writes the current batch to all sinks and persists them.
	// Write() buffers events; Flush() persists to storage (required for
	// object-storage sinks like GCS/S3 that buffer until Flush is called).
	// Uses the provided writeCtx (which may differ from the loop ctx during shutdown).
	flush := func(writeCtx context.Context) {
		if len(batch) == 0 {
			return
		}
		anyFailed := false
		for _, sink := range p.sinks {
			start := time.Now()
			if err := sink.Write(writeCtx, batch); err != nil {
				fmt.Fprintf(os.Stderr, "❌ [sink:%s] write failed (%d events): %v\n", sink.Name(), len(batch), err)
				p.errors.Add(1)
				p.handleError(writeCtx, err, batch)
				anyFailed = true
				continue
			}
			if err := sink.Flush(writeCtx); err != nil {
				fmt.Fprintf(os.Stderr, "❌ [sink:%s] flush failed (%d events): %v\n", sink.Name(), len(batch), err)
				p.errors.Add(1)
				anyFailed = true
				continue
			}
			p.sinkLatency.Store(time.Since(start).Microseconds())
		}
		if !anyFailed {
			p.eventsOut.Add(int64(len(batch)))
		}
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			// Use a fresh context for the final flush so sinks can complete their writes
			drainCtx, drainCancel := context.WithTimeout(context.Background(), 30*time.Second)
			flush(drainCtx)
			drainCancel()
			return

		case event, ok := <-eventCh:
			if !ok {
				drainCtx, drainCancel := context.WithTimeout(context.Background(), 30*time.Second)
				flush(drainCtx)
				drainCancel()
				return
			}

			p.eventsIn.Add(1)
			p.lastEvent.Store(time.Now())

			// Schema validation (if configured)
			if p.schemaValidator != nil && p.schemaValidator.Enforce() {
				vResult, vErr := p.schemaValidator.Validate(ctx, event.Value)
				if vErr != nil {
					p.errors.Add(1)
					p.schemaFails.Add(1)
					p.handleTransformError(ctx, fmt.Errorf("schema validation: %w", vErr), event)
					continue
				}
				if !vResult.Valid {
					p.schemaFails.Add(1)
					p.handleTransformError(ctx, fmt.Errorf("schema validation failed: %v", vResult.Errors), event)
					continue
				}
			}

			// Apply transform chain
			result, err := p.chain.Apply(event.Value)
			if err != nil {
				p.errors.Add(1)
				p.handleTransformError(ctx, err, event)
				continue
			}

			// Transform might filter out the event
			if result == nil {
				continue
			}

			event.Value = result
			batch = append(batch, event)

			if len(batch) >= batchSize {
				flush(ctx)
			}

		case <-ticker.C:
			flush(ctx)
		}
	}
}

// parallelEventLoop fans out events to N workers for transform processing,
// then collects results into a single batch writer. The transform.Chain is
// stateless (pure functions), so sharing it across goroutines is safe.
func (p *Pipeline) parallelEventLoop(ctx context.Context, eventCh <-chan *Event, batchSize int, flushInterval time.Duration, workers int) {
	// transformed channel collects processed events from all workers
	transformed := make(chan *Event, batchSize)

	var workerWg sync.WaitGroup
	workerWg.Add(workers)

	for i := 0; i < workers; i++ {
		go func() {
			defer workerWg.Done()
			for event := range eventCh {
				p.eventsIn.Add(1)
				p.lastEvent.Store(time.Now())

				// Schema validation (if configured)
				if p.schemaValidator != nil && p.schemaValidator.Enforce() {
					vResult, vErr := p.schemaValidator.Validate(ctx, event.Value)
					if vErr != nil {
						p.errors.Add(1)
						p.schemaFails.Add(1)
						p.handleTransformError(ctx, fmt.Errorf("schema validation: %w", vErr), event)
						continue
					}
					if !vResult.Valid {
						p.schemaFails.Add(1)
						p.handleTransformError(ctx, fmt.Errorf("schema validation failed: %v", vResult.Errors), event)
						continue
					}
				}

				// Apply transform chain (thread-safe: stateless functions)
				result, err := p.chain.Apply(event.Value)
				if err != nil {
					p.errors.Add(1)
					p.handleTransformError(ctx, err, event)
					continue
				}
				if result == nil {
					continue
				}
				event.Value = result

				select {
				case transformed <- event:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	// Close transformed channel when all workers finish
	go func() {
		workerWg.Wait()
		close(transformed)
	}()

	// Single batch writer — collects from transformed channel
	batch := make([]*Event, 0, batchSize)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	flush := func(writeCtx context.Context) {
		if len(batch) == 0 {
			return
		}
		anyFailed := false
		for _, sink := range p.sinks {
			start := time.Now()
			if err := sink.Write(writeCtx, batch); err != nil {
				fmt.Fprintf(os.Stderr, "❌ [sink:%s] write failed (%d events): %v\n", sink.Name(), len(batch), err)
				p.errors.Add(1)
				p.handleError(writeCtx, err, batch)
				anyFailed = true
				continue
			}
			if err := sink.Flush(writeCtx); err != nil {
				fmt.Fprintf(os.Stderr, "❌ [sink:%s] flush failed (%d events): %v\n", sink.Name(), len(batch), err)
				p.errors.Add(1)
				anyFailed = true
				continue
			}
			p.sinkLatency.Store(time.Since(start).Microseconds())
		}
		if !anyFailed {
			p.eventsOut.Add(int64(len(batch)))
		}
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			drainCtx, drainCancel := context.WithTimeout(context.Background(), 30*time.Second)
			flush(drainCtx)
			drainCancel()
			return

		case event, ok := <-transformed:
			if !ok {
				drainCtx, drainCancel := context.WithTimeout(context.Background(), 30*time.Second)
				flush(drainCtx)
				drainCancel()
				return
			}
			batch = append(batch, event)
			if len(batch) >= batchSize {
				flush(ctx)
			}

		case <-ticker.C:
			flush(ctx)
		}
	}
}

// Stop gracefully stops the pipeline.
func (p *Pipeline) Stop() error {
	if p.cancel != nil {
		p.cancel()
	}
	p.wg.Wait()

	// Flush and close sinks
	ctx := context.Background()
	for _, s := range p.sinks {
		_ = s.Flush(ctx)
		_ = s.Close()
	}

	_ = p.source.Close()
	p.state.Store(v1.StateStopped)
	return nil
}

// Status returns the current pipeline status.
func (p *Pipeline) Status() v1.PipelineStatus {
	state, _ := p.state.Load().(v1.PipelineState)
	lastEvt, _ := p.lastEvent.Load().(time.Time)

	elapsed := time.Since(p.startedAt).Seconds()
	eventsIn := p.eventsIn.Load()
	var eps float64
	if elapsed > 0 {
		eps = float64(eventsIn) / elapsed
	}

	status := v1.PipelineStatus{
		Name:      p.spec.Name,
		State:     state,
		EventsIn:  eventsIn,
		EventsOut: p.eventsOut.Load(),
		Errors:    p.errors.Load(),
		Throughput: v1.ThroughputInfo{
			EventsPerSec: eps,
		},
		Source: v1.SourceStatus{
			Connected: state == v1.StateRunning,
			Lag:       p.source.Lag(),
		},
	}

	if !lastEvt.IsZero() {
		status.LastEvent = &lastEvt
	}
	if !p.startedAt.IsZero() {
		status.StartedAt = &p.startedAt
	}

	return status
}

// ═══════════════════════════════════════════
// Public metric accessors
// ═══════════════════════════════════════════

// MetricsEventsIn returns the events-in counter (for observability).
func (p *Pipeline) MetricsEventsIn() *atomic.Int64  { return &p.eventsIn }
// MetricsEventsOut returns the events-out counter.
func (p *Pipeline) MetricsEventsOut() *atomic.Int64 { return &p.eventsOut }
// MetricsErrors returns the errors counter.
func (p *Pipeline) MetricsErrors() *atomic.Int64    { return &p.errors }
// MetricsDLQCount returns the DLQ counter.
func (p *Pipeline) MetricsDLQCount() *atomic.Int64  { return &p.dlqCount }
// MetricsSchemaFails returns the schema failures counter.
func (p *Pipeline) MetricsSchemaFails() *atomic.Int64 { return &p.schemaFails }
// MetricsSinkLatency returns the last sink write latency in microseconds.
func (p *Pipeline) MetricsSinkLatency() *atomic.Int64 { return &p.sinkLatency }
// StartTime returns when the pipeline started.
func (p *Pipeline) StartTime() time.Time { return p.startedAt }

// Done returns a channel that is closed when the pipeline event loop has
// finished processing (e.g. file source reached EOF). For streaming sources
// like Kafka this channel stays open until the pipeline is explicitly stopped.
func (p *Pipeline) Done() <-chan struct{} { return p.done }

// handleError handles sink write errors with retry/DLQ logic.
func (p *Pipeline) handleError(ctx context.Context, err error, batch []*Event) {
	maxRetries := p.spec.Isolation.MaxRetries
	if maxRetries == 0 {
		maxRetries = 3
	}

	backoff := time.Duration(p.spec.Isolation.BackoffMs) * time.Millisecond
	if backoff == 0 {
		backoff = time.Second
	}

	for attempt := 1; attempt <= maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff * time.Duration(attempt)):
		}

		allOk := true
		for _, sink := range p.sinks {
			if writeErr := sink.Write(ctx, batch); writeErr != nil {
				allOk = false
			}
		}
		if allOk {
			return
		}
	}

	// All retries exhausted — write to DLQ if enabled
	if p.spec.Isolation.DLQEnabled && p.dlq != nil {
		if dlqErr := p.dlq.Write(ctx, batch); dlqErr != nil {
			// DLQ write also failed — degrade pipeline
			fmt.Fprintf(os.Stderr, "⚠️  [pipeline] %d events LOST: all %d retries failed and DLQ write also failed\n", len(batch), maxRetries)
			p.state.Store(v1.StateDegraded)
			return
		}
		fmt.Fprintf(os.Stderr, "⚠️  [pipeline] %d events sent to DLQ after %d failed retries\n", len(batch), maxRetries)
		p.dlqCount.Add(int64(len(batch)))
		return
	}

	fmt.Fprintf(os.Stderr, "⚠️  [pipeline] %d events DROPPED: all %d retries exhausted (no DLQ configured)\n", len(batch), maxRetries)
	p.state.Store(v1.StateDegraded)
}

// handleTransformError handles transform failures.
func (p *Pipeline) handleTransformError(ctx context.Context, err error, event *Event) {
	schemaSpec := p.spec.Schema
	if schemaSpec != nil {
		switch schemaSpec.OnFailure {
		case "reject":
			return // drop the event
		case "dlq":
			if p.dlq != nil {
				if dlqErr := p.dlq.Write(ctx, []*Event{event}); dlqErr == nil {
					p.dlqCount.Add(1)
				}
			}
			return
		}
	}
	// Default: log and continue
}
