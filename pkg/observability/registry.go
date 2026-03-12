// Package observability — MetricsRegistry provides a shared, thread-safe
// registry of pipeline metrics so that a single HTTP server can expose
// /metrics for all pipelines running inside a workflow.
package observability

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"
)

// MetricsRegistry holds metrics for multiple pipelines, keyed by name.
type MetricsRegistry struct {
	mu        sync.RWMutex
	pipelines map[string]*PipelineMetrics
	order     []string // insertion order for deterministic output
}

// NewRegistry creates an empty metrics registry.
func NewRegistry() *MetricsRegistry {
	return &MetricsRegistry{
		pipelines: make(map[string]*PipelineMetrics),
	}
}

// Register adds a new pipeline to the registry and returns its metrics handle.
// If the pipeline name already exists, the existing handle is returned.
func (r *MetricsRegistry) Register(name string) *PipelineMetrics {
	r.mu.Lock()
	defer r.mu.Unlock()

	if m, ok := r.pipelines[name]; ok {
		return m
	}
	m := &PipelineMetrics{StartedAt: time.Now()}
	r.pipelines[name] = m
	r.order = append(r.order, name)
	return m
}

// Unregister removes a pipeline from the registry.
func (r *MetricsRegistry) Unregister(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.pipelines, name)
	filtered := r.order[:0]
	for _, n := range r.order {
		if n != name {
			filtered = append(filtered, n)
		}
	}
	r.order = filtered
}

// Names returns all registered pipeline names in insertion order.
func (r *MetricsRegistry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, len(r.order))
	copy(out, r.order)
	return out
}

// Get returns the metrics for a specific pipeline, or nil if not found.
func (r *MetricsRegistry) Get(name string) *PipelineMetrics {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.pipelines[name]
}

// HandleMetrics writes Prometheus text-format metrics for ALL registered
// pipelines to the given ResponseWriter. Each metric carries a pipeline="" label.
func (r *MetricsRegistry) HandleMetrics(w http.ResponseWriter, _ *http.Request) {
	r.mu.RLock()
	// Snapshot names and pointers under read lock
	names := make([]string, len(r.order))
	copy(names, r.order)
	metrics := make(map[string]*PipelineMetrics, len(r.pipelines))
	for k, v := range r.pipelines {
		metrics[k] = v
	}
	r.mu.RUnlock()

	sort.Strings(names) // alphabetical for stable output

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	// Helper: emit one metric family across all pipelines
	type metricFn func(name string, m *PipelineMetrics) string

	families := []struct {
		help    string
		typ     string
		name    string
		valueFn metricFn
	}{
		{
			"Total events consumed from source.",
			"counter", "mako_events_in_total",
			func(n string, m *PipelineMetrics) string {
				return fmt.Sprintf("mako_events_in_total{pipeline=%q} %d", n, m.EventsIn.Load())
			},
		},
		{
			"Total events written to sinks.",
			"counter", "mako_events_out_total",
			func(n string, m *PipelineMetrics) string {
				return fmt.Sprintf("mako_events_out_total{pipeline=%q} %d", n, m.EventsOut.Load())
			},
		},
		{
			"Total processing errors.",
			"counter", "mako_errors_total",
			func(n string, m *PipelineMetrics) string {
				return fmt.Sprintf("mako_errors_total{pipeline=%q} %d", n, m.Errors.Load())
			},
		},
		{
			"Total events sent to DLQ.",
			"counter", "mako_dlq_total",
			func(n string, m *PipelineMetrics) string {
				return fmt.Sprintf("mako_dlq_total{pipeline=%q} %d", n, m.DLQCount.Load())
			},
		},
		{
			"Total schema validation failures.",
			"counter", "mako_schema_failures_total",
			func(n string, m *PipelineMetrics) string {
				return fmt.Sprintf("mako_schema_failures_total{pipeline=%q} %d", n, m.SchemaFails.Load())
			},
		},
		{
			"Current throughput.",
			"gauge", "mako_throughput_events_per_second",
			func(n string, m *PipelineMetrics) string {
				uptime := time.Since(m.StartedAt).Seconds()
				var eps float64
				if uptime > 0 {
					eps = float64(m.EventsIn.Load()) / uptime
				}
				return fmt.Sprintf("mako_throughput_events_per_second{pipeline=%q} %.2f", n, eps)
			},
		},
		{
			"Pipeline uptime in seconds.",
			"gauge", "mako_uptime_seconds",
			func(n string, m *PipelineMetrics) string {
				return fmt.Sprintf("mako_uptime_seconds{pipeline=%q} %.1f", n, time.Since(m.StartedAt).Seconds())
			},
		},
		{
			"Last sink write latency.",
			"gauge", "mako_sink_latency_microseconds",
			func(n string, m *PipelineMetrics) string {
				return fmt.Sprintf("mako_sink_latency_microseconds{pipeline=%q} %d", n, m.sinkLatency.Load())
			},
		},
	}

	for _, f := range families {
		fmt.Fprintf(w, "# HELP %s %s\n", f.name, f.help)
		fmt.Fprintf(w, "# TYPE %s %s\n", f.name, f.typ)
		for _, n := range names {
			if m, ok := metrics[n]; ok {
				fmt.Fprintln(w, f.valueFn(n, m))
			}
		}
	}
}
