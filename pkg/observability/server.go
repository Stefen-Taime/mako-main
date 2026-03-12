// Package observability provides HTTP endpoints for Prometheus metrics,
// health checks, and pipeline status during `mako run`.
//
// Endpoints:
//
//	GET /metrics      — Prometheus-compatible metrics (text/plain)
//	GET /health       — Liveness probe (always 200 when server is up)
//	GET /ready        — Readiness probe (200 when pipeline is running)
//	GET /status       — JSON pipeline status
package observability

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// PipelineMetrics holds runtime counters exposed via /metrics.
type PipelineMetrics struct {
	EventsIn    atomic.Int64
	EventsOut   atomic.Int64
	Errors      atomic.Int64
	DLQCount    atomic.Int64
	SchemaFails atomic.Int64
	StartedAt   time.Time
	mu          sync.RWMutex
	sinkLatency atomic.Int64 // last write latency in microseconds
}

// PipelineStatusFn is a callback to get current pipeline status.
type PipelineStatusFn func() map[string]any

// Server is the HTTP server for observability endpoints.
// It can run in two modes:
//   - Single-pipeline mode: metrics/pipeline are set directly (mako run).
//   - Registry mode: a MetricsRegistry is attached, and /metrics exposes
//     all registered pipelines on a single port (mako workflow).
type Server struct {
	addr     string
	metrics  *PipelineMetrics
	statusFn PipelineStatusFn
	server   *http.Server
	pipeline string
	ready    atomic.Bool
	registry *MetricsRegistry // non-nil in registry (workflow) mode
}

// NewServer creates an observability HTTP server.
func NewServer(addr, pipelineName string) *Server {
	s := &Server{
		addr:     addr,
		pipeline: pipelineName,
		metrics: &PipelineMetrics{
			StartedAt: time.Now(),
		},
	}
	return s
}

// NewRegistryServer creates an observability HTTP server backed by a shared
// MetricsRegistry. All registered pipelines are exposed on a single /metrics
// endpoint. Use this for `mako workflow` where N pipelines share one port.
func NewRegistryServer(addr string, registry *MetricsRegistry) *Server {
	return &Server{
		addr:     addr,
		registry: registry,
		metrics: &PipelineMetrics{
			StartedAt: time.Now(),
		},
	}
}

// Registry returns the shared MetricsRegistry (nil in single-pipeline mode).
func (s *Server) Registry() *MetricsRegistry {
	return s.registry
}

// Metrics returns the metrics struct for recording counters.
func (s *Server) Metrics() *PipelineMetrics {
	return s.metrics
}

// SetSinkLatency updates the last sink write latency (in microseconds).
func (m *PipelineMetrics) SetSinkLatency(us int64) {
	m.sinkLatency.Store(us)
}

// SetReady marks the pipeline as ready (readiness probe will return 200).
func (s *Server) SetReady(ready bool) {
	s.ready.Store(ready)
}

// SetStatusFn sets the callback to get pipeline status JSON.
func (s *Server) SetStatusFn(fn PipelineStatusFn) {
	s.statusFn = fn
}

// Start starts the HTTP server in the background.
func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/ready", s.handleReady)
	mux.HandleFunc("/status", s.handleStatus)

	s.server = &http.Server{
		Addr:              s.addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("[observability] server error: %v\n", err)
		}
	}()

	return nil
}

// Stop gracefully shuts down the HTTP server.
func (s *Server) Stop() error {
	if s.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.server.Shutdown(ctx)
	}
	return nil
}

// ═══════════════════════════════════════════
// /metrics — Prometheus text format
// ═══════════════════════════════════════════

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	// Registry mode: delegate to the shared registry which emits metrics
	// for ALL registered pipelines on this single endpoint.
	if s.registry != nil {
		s.registry.HandleMetrics(w, r)
		return
	}

	// Single-pipeline mode (original behaviour)
	m := s.metrics
	uptime := time.Since(m.StartedAt).Seconds()
	eventsIn := m.EventsIn.Load()
	eventsOut := m.EventsOut.Load()
	errors := m.Errors.Load()
	dlqCount := m.DLQCount.Load()
	schemaFails := m.SchemaFails.Load()
	sinkLatencyUs := m.sinkLatency.Load()

	var eps float64
	if uptime > 0 {
		eps = float64(eventsIn) / uptime
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	fmt.Fprintf(w, "# HELP mako_events_in_total Total events consumed from source.\n")
	fmt.Fprintf(w, "# TYPE mako_events_in_total counter\n")
	fmt.Fprintf(w, "mako_events_in_total{pipeline=%q} %d\n", s.pipeline, eventsIn)

	fmt.Fprintf(w, "# HELP mako_events_out_total Total events written to sinks.\n")
	fmt.Fprintf(w, "# TYPE mako_events_out_total counter\n")
	fmt.Fprintf(w, "mako_events_out_total{pipeline=%q} %d\n", s.pipeline, eventsOut)

	fmt.Fprintf(w, "# HELP mako_errors_total Total processing errors.\n")
	fmt.Fprintf(w, "# TYPE mako_errors_total counter\n")
	fmt.Fprintf(w, "mako_errors_total{pipeline=%q} %d\n", s.pipeline, errors)

	fmt.Fprintf(w, "# HELP mako_dlq_total Total events sent to DLQ.\n")
	fmt.Fprintf(w, "# TYPE mako_dlq_total counter\n")
	fmt.Fprintf(w, "mako_dlq_total{pipeline=%q} %d\n", s.pipeline, dlqCount)

	fmt.Fprintf(w, "# HELP mako_schema_failures_total Total schema validation failures.\n")
	fmt.Fprintf(w, "# TYPE mako_schema_failures_total counter\n")
	fmt.Fprintf(w, "mako_schema_failures_total{pipeline=%q} %d\n", s.pipeline, schemaFails)

	fmt.Fprintf(w, "# HELP mako_throughput_events_per_second Current throughput.\n")
	fmt.Fprintf(w, "# TYPE mako_throughput_events_per_second gauge\n")
	fmt.Fprintf(w, "mako_throughput_events_per_second{pipeline=%q} %.2f\n", s.pipeline, eps)

	fmt.Fprintf(w, "# HELP mako_uptime_seconds Pipeline uptime in seconds.\n")
	fmt.Fprintf(w, "# TYPE mako_uptime_seconds gauge\n")
	fmt.Fprintf(w, "mako_uptime_seconds{pipeline=%q} %.1f\n", s.pipeline, uptime)

	fmt.Fprintf(w, "# HELP mako_sink_latency_microseconds Last sink write latency.\n")
	fmt.Fprintf(w, "# TYPE mako_sink_latency_microseconds gauge\n")
	fmt.Fprintf(w, "mako_sink_latency_microseconds{pipeline=%q} %d\n", s.pipeline, sinkLatencyUs)

	fmt.Fprintf(w, "# HELP mako_pipeline_ready Pipeline readiness (1=ready, 0=not ready).\n")
	fmt.Fprintf(w, "# TYPE mako_pipeline_ready gauge\n")
	ready := 0
	if s.ready.Load() {
		ready = 1
	}
	fmt.Fprintf(w, "mako_pipeline_ready{pipeline=%q} %d\n", s.pipeline, ready)
}

// ═══════════════════════════════════════════
// /health — Liveness probe
// ═══════════════════════════════════════════

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]any{
		"status":   "ok",
		"pipeline": s.pipeline,
		"uptime":   time.Since(s.metrics.StartedAt).String(),
	})
}

// ═══════════════════════════════════════════
// /ready — Readiness probe
// ═══════════════════════════════════════════

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if s.ready.Load() {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]any{
			"ready":    true,
			"pipeline": s.pipeline,
		})
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]any{
			"ready":    false,
			"pipeline": s.pipeline,
		})
	}
}

// ═══════════════════════════════════════════
// /status — Pipeline status JSON
// ═══════════════════════════════════════════

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	m := s.metrics
	uptime := time.Since(m.StartedAt).Seconds()
	eventsIn := m.EventsIn.Load()

	var eps float64
	if uptime > 0 {
		eps = float64(eventsIn) / uptime
	}

	status := map[string]any{
		"pipeline": s.pipeline,
		"ready":    s.ready.Load(),
		"uptime":   time.Since(m.StartedAt).String(),
		"metrics": map[string]any{
			"events_in":    eventsIn,
			"events_out":   m.EventsOut.Load(),
			"errors":       m.Errors.Load(),
			"dlq_count":    m.DLQCount.Load(),
			"schema_fails": m.SchemaFails.Load(),
			"throughput":   fmt.Sprintf("%.1f events/s", eps),
		},
	}

	// Merge custom status if available
	if s.statusFn != nil {
		extra := s.statusFn()
		for k, v := range extra {
			status[k] = v
		}
	}

	json.NewEncoder(w).Encode(status)
}
