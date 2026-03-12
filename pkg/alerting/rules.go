package alerting

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	v1 "github.com/Stefen-Taime/mako/api/v1"
)

// ═══════════════════════════════════════════
// Rule Engine
// ═══════════════════════════════════════════

// MetricsSnapshot contains the pipeline metrics at a point in time
// for rule evaluation.
type MetricsSnapshot struct {
	EventsIn     int64
	EventsOut    int64
	Errors       int64
	LastEventAge time.Duration // time since last event received
	PrevEventsIn int64         // events in from previous evaluation
	EvalInterval time.Duration // time between evaluations
}

// RuleEngine evaluates alert rules periodically against pipeline metrics.
// All public methods are nil-safe: calling them on a nil receiver is a no-op.
type RuleEngine struct {
	rules      []ParsedRule
	alerter    *SlackAlerter
	pipeline   string
	mu         sync.Mutex
	firedRules map[string]time.Time // cooldown tracking per rule name
}

// ParsedRule is an alert rule with pre-parsed thresholds.
type ParsedRule struct {
	Name     string
	Type     string // latency, error_rate, volume
	Severity string
	Channel  string

	// Parsed thresholds (one is active depending on Type)
	LatencyThreshold   time.Duration // for type=latency
	ErrorRateThreshold float64       // for type=error_rate (0.005 = 0.5%)
	VolumeThreshold    float64       // for type=volume (-0.5 = -50%)
}

const ruleCooldown = 5 * time.Minute

// NewRuleEngine creates a RuleEngine from alert rules and a SlackAlerter.
// Returns nil if there are no rules to evaluate.
func NewRuleEngine(rules []v1.AlertRule, alerter *SlackAlerter, pipelineName string) *RuleEngine {
	if len(rules) == 0 {
		return nil
	}

	var parsed []ParsedRule
	for _, r := range rules {
		pr, err := parseRule(r)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[alert] skip rule %q: %v\n", r.Name, err)
			continue
		}
		parsed = append(parsed, pr)
	}

	if len(parsed) == 0 {
		return nil
	}

	return &RuleEngine{
		rules:      parsed,
		alerter:    alerter,
		pipeline:   pipelineName,
		firedRules: make(map[string]time.Time),
	}
}

// parseRule converts an AlertRule from YAML into a ParsedRule with typed thresholds.
func parseRule(r v1.AlertRule) (ParsedRule, error) {
	pr := ParsedRule{
		Name:     r.Name,
		Type:     r.Type,
		Severity: r.Severity,
		Channel:  r.Channel,
	}

	if pr.Severity == "" {
		pr.Severity = "warning"
	}

	switch r.Type {
	case "latency":
		d, err := time.ParseDuration(r.Threshold)
		if err != nil {
			return pr, fmt.Errorf("invalid latency threshold %q: %w", r.Threshold, err)
		}
		pr.LatencyThreshold = d

	case "error_rate":
		pct, err := parsePercent(r.Threshold)
		if err != nil {
			return pr, fmt.Errorf("invalid error_rate threshold %q: %w", r.Threshold, err)
		}
		pr.ErrorRateThreshold = pct

	case "volume":
		pct, err := parsePercent(r.Threshold)
		if err != nil {
			return pr, fmt.Errorf("invalid volume threshold %q: %w", r.Threshold, err)
		}
		pr.VolumeThreshold = pct

	default:
		return pr, fmt.Errorf("unknown rule type %q", r.Type)
	}

	return pr, nil
}

// parsePercent parses percentage strings like "0.5%", "1%", "-50%", "+200%".
// Returns the decimal form: "0.5%" → 0.005, "-50%" → -0.50.
func parsePercent(s string) (float64, error) {
	s = strings.TrimSpace(s)
	if !strings.HasSuffix(s, "%") {
		return 0, fmt.Errorf("expected %% suffix")
	}
	numStr := strings.TrimSuffix(s, "%")
	val, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, err
	}
	return val / 100.0, nil
}

// Evaluate checks all rules against the current metrics snapshot.
// Rules that trigger are sent as alerts via the SlackAlerter.
// Each rule has a 5-minute cooldown to prevent spam.
func (e *RuleEngine) Evaluate(ctx context.Context, snap MetricsSnapshot) {
	if e == nil {
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now()

	for _, rule := range e.rules {
		// Check cooldown
		if lastFired, ok := e.firedRules[rule.Name]; ok {
			if now.Sub(lastFired) < ruleCooldown {
				continue
			}
		}

		triggered, message := e.evaluateRule(rule, snap)
		if !triggered {
			continue
		}

		// Log to stderr
		fmt.Fprintf(os.Stderr, "[alert] rule %q triggered: %s\n", rule.Name, message)

		// Mark cooldown
		e.firedRules[rule.Name] = now

		// Send alert
		e.alerter.SendRuleAlert(ctx,
			rule.Name, rule.Type, rule.Severity, rule.Channel,
			message,
			snap.EventsIn, snap.EventsOut, snap.Errors,
		)
	}
}

// evaluateRule checks a single rule against the snapshot.
// Returns (triggered, message).
func (e *RuleEngine) evaluateRule(rule ParsedRule, snap MetricsSnapshot) (bool, string) {
	switch rule.Type {
	case "latency":
		if snap.LastEventAge > rule.LatencyThreshold {
			return true, fmt.Sprintf("latency %s exceeds threshold %s",
				snap.LastEventAge.Truncate(time.Second), rule.LatencyThreshold)
		}

	case "error_rate":
		if snap.EventsIn == 0 {
			return false, ""
		}
		rate := float64(snap.Errors) / float64(snap.EventsIn)
		if rate > rule.ErrorRateThreshold {
			return true, fmt.Sprintf("error rate %.2f%% exceeds threshold %.2f%% (%d errors / %d events)",
				rate*100, rule.ErrorRateThreshold*100, snap.Errors, snap.EventsIn)
		}

	case "volume":
		if snap.EvalInterval == 0 {
			return false, ""
		}
		// Calculate current and previous throughput rates
		currentDelta := snap.EventsIn - snap.PrevEventsIn
		currentRate := float64(currentDelta) / snap.EvalInterval.Seconds()

		// Need at least one previous evaluation with events to compare
		if snap.PrevEventsIn == 0 {
			return false, ""
		}

		// Compare rate change as a percentage
		// We compare the delta in this interval vs. what we'd expect
		// from the previous total rate
		prevRate := float64(snap.PrevEventsIn) / snap.EvalInterval.Seconds()
		if prevRate == 0 {
			return false, ""
		}

		change := (currentRate - prevRate) / prevRate
		if change < rule.VolumeThreshold {
			return true, fmt.Sprintf("volume change %.1f%% exceeds threshold %.1f%% (current: %.1f evt/s, previous: %.1f evt/s)",
				change*100, rule.VolumeThreshold*100, currentRate, prevRate)
		}
	}

	return false, ""
}
