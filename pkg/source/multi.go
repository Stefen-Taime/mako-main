package source

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/Stefen-Taime/mako/pkg/join"
	"github.com/Stefen-Taime/mako/pkg/pipeline"
)

// MultiSource runs multiple sources in parallel and joins their output.
// It implements the pipeline.Source interface so it can be used as a
// drop-in replacement for a single source in the pipeline engine.
type MultiSource struct {
	sources map[string]pipeline.Source // name → source
	order   []string                  // source names in definition order
	joiner  *join.JoinEngine
	eventCh chan *pipeline.Event
	lag     atomic.Int64
	done    chan struct{}
}

// NewMultiSource creates a MultiSource that reads from multiple named sources
// and joins their events using the provided JoinEngine.
func NewMultiSource(sources map[string]pipeline.Source, order []string, joiner *join.JoinEngine) *MultiSource {
	return &MultiSource{
		sources: sources,
		order:   order,
		joiner:  joiner,
		eventCh: make(chan *pipeline.Event, 1000),
		done:    make(chan struct{}),
	}
}

// Open initializes all underlying sources.
func (m *MultiSource) Open(ctx context.Context) error {
	for _, name := range m.order {
		src := m.sources[name]
		if err := src.Open(ctx); err != nil {
			return fmt.Errorf("open source %q: %w", name, err)
		}
	}
	return nil
}

// Read starts all sources in parallel, joins events, and returns the output channel.
func (m *MultiSource) Read(ctx context.Context) (<-chan *pipeline.Event, error) {
	tagged := make(chan join.TaggedEvent, 1000)

	var wg sync.WaitGroup

	// Start each source in its own goroutine
	for _, name := range m.order {
		src := m.sources[name]
		wg.Add(1)
		go func(name string, src pipeline.Source) {
			defer wg.Done()

			ch, err := src.Read(ctx)
			if err != nil {
				fmt.Printf("[multi-source] read error for %s: %v\n", name, err)
				return
			}

			for event := range ch {
				select {
				case tagged <- join.TaggedEvent{Source: name, Event: event}:
				case <-ctx.Done():
					return
				}
			}
		}(name, src)
	}

	// Close tagged channel when all sources are done
	go func() {
		wg.Wait()
		close(tagged)
	}()

	// Process tagged events through join engine and emit to output channel
	go func() {
		defer close(m.eventCh)
		defer close(m.done)

		for te := range tagged {
			joined := m.joiner.Process(te)
			for _, ev := range joined {
				select {
				case m.eventCh <- ev:
				case <-ctx.Done():
					return
				}
			}
		}

		// Flush unmatched events (for left/full joins)
		for _, ev := range m.joiner.FlushUnmatched() {
			select {
			case m.eventCh <- ev:
			case <-ctx.Done():
				return
			}
		}
	}()

	return m.eventCh, nil
}

// Close shuts down all underlying sources.
func (m *MultiSource) Close() error {
	var firstErr error
	for _, name := range m.order {
		if err := m.sources[name].Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close source %q: %w", name, err)
		}
	}
	return firstErr
}

// Lag returns the sum of lag across all underlying sources.
func (m *MultiSource) Lag() int64 {
	var total int64
	for _, src := range m.sources {
		total += src.Lag()
	}
	return total
}
