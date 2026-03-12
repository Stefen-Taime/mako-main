// Package alerting provides notification capabilities for Mako pipelines.
// Currently supports Slack webhooks for error, SLA breach, and completion alerts.
package alerting

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	v1 "github.com/Stefen-Taime/mako/api/v1"
)

// ═══════════════════════════════════════════
// Slack Alerter
// ═══════════════════════════════════════════

// SlackAlerter sends pipeline alerts to a Slack channel via incoming webhook.
// All public methods are nil-safe: calling them on a nil receiver is a no-op.
type SlackAlerter struct {
	WebhookURL string
	Channel    string // from monitoring.alertChannel
	Pipeline   string // pipeline name
	Owner      string // pipeline owner
	OnError    bool
	OnSLA      bool
	OnComplete bool
}

// NewSlackAlerter creates a SlackAlerter from a PipelineSpec.
// Returns nil if no webhook URL is configured (YAML or SLACK_WEBHOOK_URL env).
func NewSlackAlerter(spec *v1.PipelineSpec) *SlackAlerter {
	p := spec.Pipeline
	if p.Monitoring == nil {
		return nil
	}
	m := p.Monitoring

	webhookURL := os.ExpandEnv(m.SlackWebhookURL)
	if webhookURL == "" {
		webhookURL = os.Getenv("SLACK_WEBHOOK_URL")
	}
	if webhookURL == "" {
		return nil
	}

	// Defaults: alertOnError and alertOnSLA default to true.
	// YAML omitempty means unset bools are false, so we apply defaults here:
	// if the user explicitly set the monitoring block with a webhook,
	// errors and SLA alerts are on by default.
	onError := true
	onSLA := true
	onComplete := false

	// If the user explicitly set these fields in YAML they will be true/false.
	// Since Go zero-value for bool is false, we need a heuristic:
	// If slackWebhookURL is set but alertOnError is false and alertOnSLA is false
	// and alertOnComplete is false, assume defaults (all false means "not set").
	// But if the user explicitly set alertOnComplete: true, we know they touched
	// the config. The simplest correct approach: alertOnError and alertOnSLA
	// default to true unless the user explicitly set them to false.
	// With omitempty YAML tags, we can't distinguish "not set" from "false".
	// So: if ANY of the three bools is true, use them as-is.
	// If ALL are false (likely not set), apply defaults (error=true, sla=true).
	if m.AlertOnError || m.AlertOnSLA || m.AlertOnComplete {
		onError = m.AlertOnError
		onSLA = m.AlertOnSLA
		onComplete = m.AlertOnComplete
	}

	return &SlackAlerter{
		WebhookURL: webhookURL,
		Channel:    m.AlertChannel,
		Pipeline:   p.Name,
		Owner:      p.Owner,
		OnError:    onError,
		OnSLA:      onSLA,
		OnComplete: onComplete,
	}
}

// ═══════════════════════════════════════════
// Public methods (all nil-safe)
// ═══════════════════════════════════════════

// SendError sends an error alert to Slack. Runs asynchronously.
func (a *SlackAlerter) SendError(ctx context.Context, pipelineErr error, eventsIn, eventsOut int64) {
	if a == nil || !a.OnError {
		return
	}

	fields := []slackField{
		{Title: "Error", Value: pipelineErr.Error(), Short: false},
		{Title: "Events In", Value: fmt.Sprintf("%d", eventsIn), Short: true},
		{Title: "Events Out", Value: fmt.Sprintf("%d", eventsOut), Short: true},
	}
	if a.Owner != "" {
		fields = append(fields, slackField{Title: "Owner", Value: a.Owner, Short: true})
	}

	msg := slackMessage{
		Channel:  a.Channel,
		Username: "Mako Pipeline",
		Icon:     ":shark:",
		Attachments: []slackAttachment{{
			Color:     colorRed,
			Title:     fmt.Sprintf("Pipeline Error: %s", a.Pipeline),
			Fields:    fields,
			Timestamp: time.Now().Unix(),
		}},
	}

	go a.send(ctx, msg)
}

// SendSLABreach sends a freshness SLA breach alert to Slack. Runs asynchronously.
func (a *SlackAlerter) SendSLABreach(ctx context.Context, sla time.Duration, actual time.Duration) {
	if a == nil || !a.OnSLA {
		return
	}

	fields := []slackField{
		{Title: "Freshness SLA", Value: sla.String(), Short: true},
		{Title: "Actual Delay", Value: actual.Truncate(time.Second).String(), Short: true},
	}
	if a.Owner != "" {
		fields = append(fields, slackField{Title: "Owner", Value: a.Owner, Short: true})
	}

	msg := slackMessage{
		Channel:  a.Channel,
		Username: "Mako Pipeline",
		Icon:     ":shark:",
		Attachments: []slackAttachment{{
			Color:     colorRed,
			Title:     fmt.Sprintf("SLA Breach: %s", a.Pipeline),
			Fields:    fields,
			Timestamp: time.Now().Unix(),
		}},
	}

	go a.send(ctx, msg)
}

// SendComplete sends a pipeline completion summary to Slack. Runs asynchronously.
func (a *SlackAlerter) SendComplete(ctx context.Context, eventsIn, eventsOut, errors int64, duration time.Duration) {
	if a == nil || !a.OnComplete {
		return
	}

	color := colorGreen
	title := fmt.Sprintf("Pipeline Complete: %s", a.Pipeline)
	if errors > 0 {
		color = colorOrange
	}

	fields := []slackField{
		{Title: "Events In", Value: fmt.Sprintf("%d", eventsIn), Short: true},
		{Title: "Events Out", Value: fmt.Sprintf("%d", eventsOut), Short: true},
		{Title: "Errors", Value: fmt.Sprintf("%d", errors), Short: true},
		{Title: "Duration", Value: duration.Truncate(time.Second).String(), Short: true},
	}
	if a.Owner != "" {
		fields = append(fields, slackField{Title: "Owner", Value: a.Owner, Short: true})
	}

	msg := slackMessage{
		Channel:  a.Channel,
		Username: "Mako Pipeline",
		Icon:     ":shark:",
		Attachments: []slackAttachment{{
			Color:     color,
			Title:     title,
			Fields:    fields,
			Timestamp: time.Now().Unix(),
		}},
	}

	go a.send(ctx, msg)
}

// SendRuleAlert sends a threshold-based rule alert to Slack. Runs asynchronously.
// If channel is non-empty, it overrides the default alertChannel.
func (a *SlackAlerter) SendRuleAlert(ctx context.Context, ruleName, ruleType, severity, channel, message string, eventsIn, eventsOut, errors int64) {
	if a == nil {
		return
	}

	ch := a.Channel
	if channel != "" {
		ch = channel
	}

	color := colorForSeverity(severity)

	fields := []slackField{
		{Title: "Type", Value: ruleType, Short: true},
		{Title: "Pipeline", Value: a.Pipeline, Short: true},
		{Title: "Message", Value: message, Short: false},
		{Title: "Events In", Value: fmt.Sprintf("%d", eventsIn), Short: true},
		{Title: "Events Out", Value: fmt.Sprintf("%d", eventsOut), Short: true},
		{Title: "Errors", Value: fmt.Sprintf("%d", errors), Short: true},
	}
	if a.Owner != "" {
		fields = append(fields, slackField{Title: "Owner", Value: a.Owner, Short: true})
	}

	msg := slackMessage{
		Channel:  ch,
		Username: "Mako Pipeline",
		Icon:     ":shark:",
		Attachments: []slackAttachment{{
			Color:     color,
			Title:     fmt.Sprintf("Alert: %s (%s)", ruleName, severity),
			Fields:    fields,
			Timestamp: time.Now().Unix(),
		}},
	}

	go a.send(ctx, msg)
}

// ═══════════════════════════════════════════
// Internal
// ═══════════════════════════════════════════

const (
	colorRed    = "#dc3545"
	colorGreen  = "#28a745"
	colorOrange = "#fd7e14"
	colorBlue   = "#2196F3"
)

// colorForSeverity returns the Slack attachment color for a given severity.
func colorForSeverity(severity string) string {
	switch severity {
	case "critical":
		return colorRed
	case "warning":
		return colorOrange
	case "info":
		return colorBlue
	default:
		return colorOrange
	}
}

// slackMessage is the Slack incoming webhook payload.
type slackMessage struct {
	Channel     string            `json:"channel,omitempty"`
	Username    string            `json:"username,omitempty"`
	Icon        string            `json:"icon_emoji,omitempty"`
	Attachments []slackAttachment `json:"attachments"`
}

type slackAttachment struct {
	Color     string       `json:"color"`
	Title     string       `json:"title"`
	Fields    []slackField `json:"fields"`
	Timestamp int64        `json:"ts"`
}

type slackField struct {
	Title string `json:"title"`
	Value string `json:"value"`
	Short bool   `json:"short"`
}

// send posts the Slack message to the webhook URL with a 5s timeout.
func (a *SlackAlerter) send(ctx context.Context, msg slackMessage) {
	body, err := json.Marshal(msg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[slack] marshal error: %v\n", err)
		return
	}

	// 5s timeout so webhook failures don't block the pipeline
	sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(sendCtx, http.MethodPost, a.WebhookURL, bytes.NewReader(body))
	if err != nil {
		fmt.Fprintf(os.Stderr, "[slack] request error: %v\n", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[slack] send error: %v\n", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "[slack] webhook returned %d\n", resp.StatusCode)
	}
}
