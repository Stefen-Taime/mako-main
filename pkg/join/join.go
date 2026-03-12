// Package join implements in-memory hash join for multi-source pipelines.
//
// Events from multiple sources are buffered by join key and merged
// when matching keys are found across all required sources.
package join

import (
	"fmt"
	"strings"
	"sync"
	"time"

	v1 "github.com/Stefen-Taime/mako/api/v1"
	"github.com/Stefen-Taime/mako/pkg/pipeline"
)

// TaggedEvent wraps an event with its source name.
type TaggedEvent struct {
	Source string
	Event  *pipeline.Event
}

// keyMapping maps source name → field name extracted from the ON clause.
type keyMapping struct {
	source string
	field  string
}

// JoinEngine joins events from multiple sources based on a key.
type JoinEngine struct {
	spec     *v1.JoinSpec
	keys     []keyMapping          // one per source mentioned in ON clause
	sources  []string              // all source names (order matters: first = left)
	buffer   map[string]*srcBuffer // source name → buffered events by key value
	window   time.Duration         // 0 = no expiry
	mu       sync.Mutex
}

// srcBuffer holds events for one source, indexed by join key value.
type srcBuffer struct {
	events map[string][]*pipeline.Event // key_value → events
}

// New creates a JoinEngine from a JoinSpec and source names.
//
// The ON clause is parsed as "source1.field = source2.field".
// Source names in the ON clause must match entries in sourceNames.
func New(spec *v1.JoinSpec, sourceNames []string) (*JoinEngine, error) {
	if spec == nil {
		return nil, fmt.Errorf("join spec is nil")
	}

	keys, err := parseOnClause(spec.On, sourceNames)
	if err != nil {
		return nil, fmt.Errorf("parse join ON clause: %w", err)
	}

	var window time.Duration
	if spec.Window != "" {
		window, err = time.ParseDuration(spec.Window)
		if err != nil {
			return nil, fmt.Errorf("parse join window %q: %w", spec.Window, err)
		}
	}

	buf := make(map[string]*srcBuffer, len(sourceNames))
	for _, name := range sourceNames {
		buf[name] = &srcBuffer{events: make(map[string][]*pipeline.Event)}
	}

	return &JoinEngine{
		spec:    spec,
		keys:    keys,
		sources: sourceNames,
		buffer:  buf,
		window:  window,
	}, nil
}

// parseOnClause parses "orders.customer_id = customers.id" into keyMappings.
func parseOnClause(on string, validSources []string) ([]keyMapping, error) {
	valid := make(map[string]bool, len(validSources))
	for _, s := range validSources {
		valid[s] = true
	}

	// Split on "="
	parts := strings.Split(on, "=")
	if len(parts) < 2 {
		return nil, fmt.Errorf("expected 'source.field = source.field', got %q", on)
	}

	var keys []keyMapping
	for _, part := range parts {
		part = strings.TrimSpace(part)
		dot := strings.IndexByte(part, '.')
		if dot < 0 {
			return nil, fmt.Errorf("expected 'source.field', got %q", part)
		}
		srcName := part[:dot]
		fieldName := part[dot+1:]

		if !valid[srcName] {
			return nil, fmt.Errorf("unknown source %q in ON clause (valid: %v)", srcName, validSources)
		}
		if fieldName == "" {
			return nil, fmt.Errorf("empty field name for source %q", srcName)
		}

		keys = append(keys, keyMapping{source: srcName, field: fieldName})
	}

	return keys, nil
}

// Process takes a tagged event and returns joined events if a match is found.
//
// For inner join: emit only when all sources have a matching key.
// For left join: emit when the left (first) source has an event,
// merging right side data if available.
// For right join: mirror of left.
// For full join: emit when any source has an event, merging available data.
func (e *JoinEngine) Process(tagged TaggedEvent) []*pipeline.Event {
	e.mu.Lock()
	defer e.mu.Unlock()

	srcName := tagged.Source
	event := tagged.Event

	// Find the key field for this source
	keyField := e.fieldForSource(srcName)
	if keyField == "" {
		// Source not in ON clause — pass through
		return []*pipeline.Event{event}
	}

	// Extract key value from event
	keyVal := extractField(event.Value, keyField)
	if keyVal == "" {
		// No key value — cannot join, pass through for left/full
		if e.spec.Type == "left" && e.isLeftSource(srcName) {
			return []*pipeline.Event{event}
		}
		if e.spec.Type == "full" {
			return []*pipeline.Event{event}
		}
		return nil
	}

	// Buffer this event
	e.buffer[srcName].events[keyVal] = append(e.buffer[srcName].events[keyVal], event)

	// Check if we can emit joined events
	return e.tryJoin(keyVal)
}

// tryJoin checks if all required sources have events for the given key and emits merged events.
func (e *JoinEngine) tryJoin(keyVal string) []*pipeline.Event {
	switch e.spec.Type {
	case "inner":
		// All sources must have at least one event for this key
		for _, src := range e.sources {
			if len(e.buffer[src].events[keyVal]) == 0 {
				return nil
			}
		}
		return e.emitAndClear(keyVal)

	case "left":
		// Left source (first) must have events
		leftSrc := e.sources[0]
		if len(e.buffer[leftSrc].events[keyVal]) == 0 {
			return nil
		}
		// Check if at least one right source has data (or emit left-only)
		hasRight := false
		for _, src := range e.sources[1:] {
			if len(e.buffer[src].events[keyVal]) > 0 {
				hasRight = true
				break
			}
		}
		if hasRight {
			return e.emitAndClear(keyVal)
		}
		// For left join without window, we wait for right side.
		// If window is set, the Flush method handles emitting unmatched left events.
		return nil

	case "right":
		// Right source (last) must have events
		rightSrc := e.sources[len(e.sources)-1]
		if len(e.buffer[rightSrc].events[keyVal]) == 0 {
			return nil
		}
		hasLeft := false
		for _, src := range e.sources[:len(e.sources)-1] {
			if len(e.buffer[src].events[keyVal]) > 0 {
				hasLeft = true
				break
			}
		}
		if hasLeft {
			return e.emitAndClear(keyVal)
		}
		return nil

	case "full":
		// Emit whenever we have events from any source (at least 2 sources matched)
		count := 0
		for _, src := range e.sources {
			if len(e.buffer[src].events[keyVal]) > 0 {
				count++
			}
		}
		if count >= 2 {
			return e.emitAndClear(keyVal)
		}
		return nil

	default:
		return nil
	}
}

// emitAndClear merges events from all sources for a key and removes them from the buffer.
func (e *JoinEngine) emitAndClear(keyVal string) []*pipeline.Event {
	merged := e.mergeEvents(keyVal)
	// Clear buffers for this key
	for _, src := range e.sources {
		delete(e.buffer[src].events, keyVal)
	}
	if merged != nil {
		return []*pipeline.Event{merged}
	}
	return nil
}

// mergeEvents combines fields from all sources for a given key into a single event.
// Conflicting field names are prefixed with the source name (e.g., orders.name, customers.name).
func (e *JoinEngine) mergeEvents(keyVal string) *pipeline.Event {
	merged := make(map[string]any)
	var baseEvent *pipeline.Event

	// Collect all field names across sources to detect conflicts
	fieldSources := make(map[string][]string) // field → list of source names that have it
	for _, src := range e.sources {
		events := e.buffer[src].events[keyVal]
		if len(events) == 0 {
			continue
		}
		for k := range events[0].Value {
			fieldSources[k] = append(fieldSources[k], src)
		}
	}

	for _, src := range e.sources {
		events := e.buffer[src].events[keyVal]
		if len(events) == 0 {
			continue
		}
		ev := events[0] // Take the first event per source for this key
		if baseEvent == nil {
			baseEvent = ev
		}

		for k, v := range ev.Value {
			if len(fieldSources[k]) > 1 {
				// Conflict: prefix with source name
				merged[src+"."+k] = v
			} else {
				merged[k] = v
			}
		}
	}

	if baseEvent == nil {
		return nil
	}

	return &pipeline.Event{
		Key:       baseEvent.Key,
		Value:     merged,
		Timestamp: baseEvent.Timestamp,
		Topic:     baseEvent.Topic,
		Partition: baseEvent.Partition,
		Offset:    baseEvent.Offset,
		Headers:   baseEvent.Headers,
		Metadata:  map[string]any{"joined": true, "join_key": keyVal},
	}
}

// FlushUnmatched emits unmatched events for left/full joins (called when sources are done).
func (e *JoinEngine) FlushUnmatched() []*pipeline.Event {
	e.mu.Lock()
	defer e.mu.Unlock()

	var results []*pipeline.Event

	switch e.spec.Type {
	case "left":
		// Emit unmatched left-side events
		leftSrc := e.sources[0]
		for keyVal, events := range e.buffer[leftSrc].events {
			for _, ev := range events {
				results = append(results, ev)
			}
			delete(e.buffer[leftSrc].events, keyVal)
		}

	case "full":
		// Emit all unmatched events from all sources
		for _, src := range e.sources {
			for keyVal, events := range e.buffer[src].events {
				for _, ev := range events {
					results = append(results, ev)
				}
				delete(e.buffer[src].events, keyVal)
			}
		}
	}

	return results
}

// fieldForSource returns the join key field name for a given source.
func (e *JoinEngine) fieldForSource(srcName string) string {
	for _, km := range e.keys {
		if km.source == srcName {
			return km.field
		}
	}
	return ""
}

// isLeftSource returns true if srcName is the first (left) source.
func (e *JoinEngine) isLeftSource(srcName string) bool {
	return len(e.sources) > 0 && e.sources[0] == srcName
}

// extractField gets a string representation of a field value from an event map.
func extractField(value map[string]any, field string) string {
	v, ok := value[field]
	if !ok {
		return ""
	}
	return fmt.Sprintf("%v", v)
}
