// Package config handles loading, parsing, and validating
// Mako pipeline YAML specifications.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	v1 "github.com/Stefen-Taime/mako/api/v1"
	"gopkg.in/yaml.v3"
)

// KindDetect reads just the "kind" field from a YAML file without fully parsing it.
type KindDetect struct {
	Kind string `yaml:"kind"`
}

// DetectKind returns the kind field from a YAML file (e.g., "Pipeline" or "Workflow").
func DetectKind(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", path, err)
	}
	var kd KindDetect
	if err := yaml.Unmarshal(data, &kd); err != nil {
		return "", fmt.Errorf("parse yaml: %w", err)
	}
	if kd.Kind == "" {
		return "Pipeline", nil // default
	}
	return kd.Kind, nil
}

// Load reads and parses a pipeline YAML file.
func Load(path string) (*v1.PipelineSpec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return Parse(data)
}

// Parse parses raw YAML bytes into a PipelineSpec.
func Parse(data []byte) (*v1.PipelineSpec, error) {
	var spec v1.PipelineSpec
	if err := yaml.Unmarshal(data, &spec); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}

	// Set defaults
	applyDefaults(&spec)

	return &spec, nil
}

// LoadWorkflow reads and parses a workflow YAML file.
func LoadWorkflow(path string) (*v1.WorkflowSpec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return ParseWorkflow(data)
}

// ParseWorkflow parses raw YAML bytes into a WorkflowSpec.
func ParseWorkflow(data []byte) (*v1.WorkflowSpec, error) {
	var spec v1.WorkflowSpec
	if err := yaml.Unmarshal(data, &spec); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}
	if spec.APIVersion == "" {
		spec.APIVersion = "mako/v1"
	}
	if spec.Kind == "" {
		spec.Kind = "Workflow"
	}
	if spec.Workflow.OnFailure == "" {
		spec.Workflow.OnFailure = "stop"
	}
	return &spec, nil
}

// ValidateWorkflow validates a WorkflowSpec for correctness.
// It checks DAG structure (no cycles, valid references) and validates
// that each referenced pipeline file exists and is individually valid.
// baseDir is the directory of the workflow YAML file, used to resolve
// relative pipeline paths.
func ValidateWorkflow(spec *v1.WorkflowSpec, baseDir string) *ValidationResult {
	result := &ValidationResult{}

	// API version
	if spec.APIVersion != "" && spec.APIVersion != "mako/v1" {
		result.addError("apiVersion", fmt.Sprintf("unsupported version %q, expected mako/v1", spec.APIVersion))
	}

	w := spec.Workflow

	// Workflow name
	if w.Name == "" {
		result.addError("workflow.name", "required")
	} else if !isValidName(w.Name) {
		result.addError("workflow.name", "must be lowercase alphanumeric with hyphens")
	}

	// Steps
	if len(w.Steps) == 0 {
		result.addError("workflow.steps", "at least one step is required")
		return result
	}

	// OnFailure validation
	validPolicies := map[string]bool{"stop": true, "continue": true, "retry": true}
	if w.OnFailure != "" && !validPolicies[w.OnFailure] {
		result.addError("workflow.onFailure", fmt.Sprintf("must be stop|continue|retry, got %q", w.OnFailure))
	}

	// Build step name index for reference validation
	stepNames := make(map[string]bool, len(w.Steps))
	for i, step := range w.Steps {
		prefix := fmt.Sprintf("workflow.steps[%d]", i)
		if step.Name == "" {
			result.addError(prefix+".name", "required")
			continue
		}
		if stepNames[step.Name] {
			result.addError(prefix+".name", fmt.Sprintf("duplicate step name %q", step.Name))
		}
		stepNames[step.Name] = true

		switch step.Type {
		case "quality_gate":
			if len(step.Checks) == 0 {
				result.addError(prefix+".checks", "quality_gate step requires at least one check")
			}
			for j, check := range step.Checks {
				checkPrefix := fmt.Sprintf("%s.checks[%d]", prefix, j)
				if check.SQL == "" {
					result.addError(checkPrefix+".sql", "required")
				}
				if check.Expect == "" {
					result.addError(checkPrefix+".expect", "required")
				}
			}
		default: // pipeline step
			if step.Pipeline == "" {
				result.addError(prefix+".pipeline", "required")
			}
		}
	}

	// Validate depends_on references exist
	for i, step := range w.Steps {
		prefix := fmt.Sprintf("workflow.steps[%d]", i)
		for _, dep := range step.DependsOn {
			if !stepNames[dep] {
				result.addError(prefix+".depends_on", fmt.Sprintf("references unknown step %q", dep))
			}
			if dep == step.Name {
				result.addError(prefix+".depends_on", "step cannot depend on itself")
			}
		}
	}

	// Cycle detection using DFS
	if err := detectCycles(w.Steps); err != nil {
		result.addError("workflow.steps", err.Error())
	}

	// Validate each referenced pipeline file
	for i, step := range w.Steps {
		if step.Pipeline == "" {
			continue
		}
		prefix := fmt.Sprintf("workflow.steps[%d]", i)
		pipelinePath := step.Pipeline
		if !filepath.IsAbs(pipelinePath) {
			pipelinePath = filepath.Join(baseDir, pipelinePath)
		}
		if _, err := os.Stat(pipelinePath); err != nil {
			result.addError(prefix+".pipeline", fmt.Sprintf("file not found: %s", pipelinePath))
			continue
		}
		pipeSpec, err := Load(pipelinePath)
		if err != nil {
			result.addError(prefix+".pipeline", fmt.Sprintf("invalid pipeline file: %v", err))
			continue
		}
		pipeResult := Validate(pipeSpec)
		if !pipeResult.IsValid() {
			for _, e := range pipeResult.Errors {
				result.addError(prefix+".pipeline("+step.Pipeline+")", e.Field+": "+e.Message)
			}
		}
	}

	return result
}

// detectCycles checks for cycles in the workflow DAG using DFS.
func detectCycles(steps []v1.WorkflowStep) error {
	// Build adjacency list
	adj := make(map[string][]string)
	for _, step := range steps {
		adj[step.Name] = step.DependsOn
	}

	const (
		white = 0 // unvisited
		gray  = 1 // in current path
		black = 2 // fully processed
	)

	color := make(map[string]int)
	for _, step := range steps {
		color[step.Name] = white
	}

	var dfs func(node string) error
	dfs = func(node string) error {
		color[node] = gray
		for _, dep := range adj[node] {
			switch color[dep] {
			case gray:
				return fmt.Errorf("cycle detected: %s → %s", node, dep)
			case white:
				if err := dfs(dep); err != nil {
					return err
				}
			}
		}
		color[node] = black
		return nil
	}

	for _, step := range steps {
		if color[step.Name] == white {
			if err := dfs(step.Name); err != nil {
				return err
			}
		}
	}
	return nil
}

// SpecResult pairs a parsed spec with its file path.
type SpecResult struct {
	Spec *v1.PipelineSpec
	Path string
}

// LoadAll loads all pipeline YAML files from a directory.
func LoadAll(dir string) ([]*v1.PipelineSpec, error) {
	var specs []*v1.PipelineSpec

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read dir %s: %w", dir, err)
	}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		ext := filepath.Ext(e.Name())
		if ext != ".yaml" && ext != ".yml" {
			continue
		}

		spec, err := Load(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, fmt.Errorf("load %s: %w", e.Name(), err)
		}
		specs = append(specs, spec)
	}

	return specs, nil
}

// ═══════════════════════════════════════════
// Validation
// ═══════════════════════════════════════════

// ValidationError represents a single validation issue.
type ValidationError struct {
	Field   string
	Message string
	Severity string // error|warning
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("[%s] %s: %s", e.Severity, e.Field, e.Message)
}

// ValidationResult contains all validation issues.
type ValidationResult struct {
	Errors   []ValidationError
	Warnings []ValidationError
}

func (r *ValidationResult) IsValid() bool {
	return len(r.Errors) == 0
}

func (r *ValidationResult) addError(field, msg string) {
	r.Errors = append(r.Errors, ValidationError{Field: field, Message: msg, Severity: "error"})
}

func (r *ValidationResult) addWarning(field, msg string) {
	r.Warnings = append(r.Warnings, ValidationError{Field: field, Message: msg, Severity: "warning"})
}

// Validate checks a PipelineSpec for correctness.
func Validate(spec *v1.PipelineSpec) *ValidationResult {
	result := &ValidationResult{}

	// API version
	if spec.APIVersion != "" && spec.APIVersion != "mako/v1" {
		result.addError("apiVersion", fmt.Sprintf("unsupported version %q, expected mako/v1", spec.APIVersion))
	}

	// Pipeline name
	p := spec.Pipeline
	if p.Name == "" {
		result.addError("pipeline.name", "required")
	} else if !isValidName(p.Name) {
		result.addError("pipeline.name", "must be lowercase alphanumeric with hyphens (e.g., order-events)")
	}

	// Sources validation
	hasSources := len(p.Sources) > 0
	hasSource := p.Source.Type != ""

	if hasSources && hasSource {
		result.addError("pipeline", "cannot define both source and sources")
	} else if hasSources {
		validateSources(result, p.Sources, p.Join)
	} else if hasSource {
		validateSource(result, &p.Source)
	} else {
		result.addError("pipeline.source", "required (define source or sources)")
	}

	// Transforms
	for i, t := range p.Transforms {
		validateTransform(result, &t, i)
	}

	// Sink(s)
	if p.Sink.Type == "" && len(p.Sinks) == 0 {
		result.addError("pipeline.sink", "at least one sink is required")
	}
	if p.Sink.Type != "" {
		validateSink(result, &p.Sink, "pipeline.sink")
	}
	for i, s := range p.Sinks {
		validateSink(result, &s, fmt.Sprintf("pipeline.sinks[%d]", i))
	}

	// Schema
	if p.Schema != nil {
		validateSchema(result, p.Schema)
	}

	// Monitoring
	if p.Monitoring != nil {
		validateMonitoring(result, p.Monitoring)
	}

	// Warnings for best practices
	if p.Owner == "" {
		result.addWarning("pipeline.owner", "recommended for governance (who owns this pipeline?)")
	}
	if p.Monitoring == nil {
		result.addWarning("pipeline.monitoring", "recommended: add freshnessSLA and alertChannel")
	}
	if p.Schema == nil {
		result.addWarning("pipeline.schema", "recommended: enable schema enforcement")
	}

	return result
}

func validateSource(r *ValidationResult, s *v1.Source) {
	validateSourceAt(r, s, "source")
}

func validateSourceAt(r *ValidationResult, s *v1.Source, prefix string) {
	validTypes := map[v1.SourceType]bool{
		v1.SourceKafka: true, v1.SourceHTTP: true,
		v1.SourceFile: true, v1.SourcePostgres: true,
		v1.SourceDuckDB: true,
	}

	if s.Type == "" {
		r.addError(prefix+".type", "required")
	} else if !validTypes[s.Type] {
		r.addError(prefix+".type", fmt.Sprintf("unsupported type %q", s.Type))
	}

	if s.Type == v1.SourceKafka && s.Topic == "" {
		r.addError(prefix+".topic", "required for kafka source")
	}
}

func validateSources(r *ValidationResult, sources []v1.Source, join *v1.JoinSpec) {
	if len(sources) == 1 {
		r.addWarning("pipeline.sources", "single source in sources array, use source instead")
	}

	// Each source must have a unique Name
	names := make(map[string]bool, len(sources))
	for i, s := range sources {
		prefix := fmt.Sprintf("pipeline.sources[%d]", i)
		if s.Name == "" {
			r.addError(prefix+".name", "required when using sources array")
		} else if names[s.Name] {
			r.addError(prefix+".name", fmt.Sprintf("duplicate source name %q", s.Name))
		} else {
			names[s.Name] = true
		}
		validateSourceAt(r, &s, prefix)
	}

	// Join is required when 2+ sources
	if len(sources) >= 2 {
		if join == nil {
			r.addError("pipeline.join", "required when multiple sources are defined")
		} else {
			validateJoin(r, join)
		}
	}
}

func validateJoin(r *ValidationResult, j *v1.JoinSpec) {
	validTypes := map[string]bool{
		"inner": true, "left": true, "right": true, "full": true,
	}
	if j.Type == "" {
		r.addError("pipeline.join.type", "required (inner|left|right|full)")
	} else if !validTypes[j.Type] {
		r.addError("pipeline.join.type", fmt.Sprintf("must be inner|left|right|full, got %q", j.Type))
	}
	if j.On == "" {
		r.addError("pipeline.join.on", "required (e.g., \"orders.customer_id = customers.id\")")
	}
}

func validateTransform(r *ValidationResult, t *v1.Transform, idx int) {
	prefix := fmt.Sprintf("transforms[%d]", idx)

	if t.Name == "" {
		r.addError(prefix+".name", "required")
	}
	if t.Type == "" {
		r.addError(prefix+".type", "required")
	}

	// Type-specific validation
	switch t.Type {
	case v1.TransformHashFields, v1.TransformMaskFields, v1.TransformDropFields:
		if len(t.Fields) == 0 {
			r.addError(prefix+".fields", "required for "+string(t.Type))
		}
	case v1.TransformSQL:
		if t.Query == "" {
			r.addError(prefix+".query", "required for sql transform")
		}
	case v1.TransformFilter:
		if t.Condition == "" {
			r.addError(prefix+".condition", "required for filter transform")
		}
	case v1.TransformRename:
		if len(t.Mapping) == 0 {
			r.addError(prefix+".mapping", "required for rename_fields transform")
		}
	case v1.TransformAggregate:
		if t.Window == nil {
			r.addError(prefix+".window", "required for aggregate transform")
		} else {
			validateWindow(r, t.Window, prefix+".window")
		}
	}
}

func validateWindow(r *ValidationResult, w *v1.WindowSpec, prefix string) {
	validTypes := map[string]bool{"tumbling": true, "sliding": true, "session": true}
	if !validTypes[w.Type] {
		r.addError(prefix+".type", fmt.Sprintf("must be tumbling|sliding|session, got %q", w.Type))
	}
	if w.Size == "" {
		r.addError(prefix+".size", "required")
	} else if _, err := ParseDuration(w.Size); err != nil {
		r.addError(prefix+".size", fmt.Sprintf("invalid duration: %s", err))
	}
	if w.Type == "sliding" && w.Slide == "" {
		r.addError(prefix+".slide", "required for sliding window")
	}
	if w.Function == "" {
		r.addError(prefix+".function", "required")
	}
}

func validateSink(r *ValidationResult, s *v1.Sink, prefix string) {
	validTypes := map[v1.SinkType]bool{
		v1.SinkSnowflake: true, v1.SinkBigQuery: true, v1.SinkPostgres: true,
		v1.SinkKafka: true, v1.SinkS3: true, v1.SinkGCS: true,
		v1.SinkClickHouse: true, v1.SinkDuckDB: true, v1.SinkStdout: true,
	}

	if !validTypes[s.Type] {
		r.addError(prefix+".type", fmt.Sprintf("unsupported type %q", s.Type))
	}

	switch s.Type {
	case v1.SinkSnowflake, v1.SinkBigQuery, v1.SinkPostgres, v1.SinkClickHouse, v1.SinkDuckDB:
		if s.Table == "" {
			r.addError(prefix+".table", "required for warehouse sink")
		}
	case v1.SinkKafka:
		if s.Topic == "" {
			r.addError(prefix+".topic", "required for kafka sink")
		}
	case v1.SinkS3, v1.SinkGCS:
		if s.Bucket == "" {
			r.addError(prefix+".bucket", "required for object storage sink")
		}
	}
}

func validateSchema(r *ValidationResult, s *v1.SchemaSpec) {
	if s.Enforce && s.Registry == "" {
		r.addError("schema.registry", "required when enforce is true")
	}
	validCompat := map[string]bool{
		"": true, "BACKWARD": true, "FORWARD": true, "FULL": true, "NONE": true,
	}
	if !validCompat[s.Compatibility] {
		r.addError("schema.compatibility", "must be BACKWARD|FORWARD|FULL|NONE")
	}
}

func validateMonitoring(r *ValidationResult, m *v1.MonitoringSpec) {
	if m.FreshnessSLA != "" {
		if _, err := ParseDuration(m.FreshnessSLA); err != nil {
			r.addError("monitoring.freshnessSLA", fmt.Sprintf("invalid duration: %s", err))
		}
	}

	// Slack alerting validation
	webhookURL := os.ExpandEnv(m.SlackWebhookURL)
	if webhookURL == "" {
		webhookURL = os.Getenv("SLACK_WEBHOOK_URL")
	}

	if m.AlertChannel != "" && webhookURL == "" {
		r.addWarning("monitoring.slackWebhookURL",
			"alertChannel is set but no webhook URL configured (set slackWebhookURL or SLACK_WEBHOOK_URL)")
	}

	if m.SlackWebhookURL != "" {
		resolved := os.ExpandEnv(m.SlackWebhookURL)
		if resolved != "" && !strings.HasPrefix(resolved, "https://") {
			r.addError("monitoring.slackWebhookURL",
				"must start with https:// (e.g., https://hooks.slack.com/services/...)")
		}
	}

	// Alert rules validation
	if len(m.Alerts) > 0 && webhookURL == "" {
		r.addWarning("monitoring.alerts",
			"alert rules are defined but no Slack webhook configured (rules won't trigger notifications)")
	}

	validRuleTypes := map[string]bool{"latency": true, "error_rate": true, "volume": true}
	validSeverities := map[string]bool{"": true, "critical": true, "warning": true, "info": true}

	for i, rule := range m.Alerts {
		prefix := fmt.Sprintf("monitoring.alerts[%d]", i)

		if rule.Name == "" {
			r.addError(prefix+".name", "required")
		}
		if rule.Type == "" {
			r.addError(prefix+".type", "required")
		} else if !validRuleTypes[rule.Type] {
			r.addError(prefix+".type", fmt.Sprintf("must be latency|error_rate|volume, got %q", rule.Type))
		}
		if !validSeverities[rule.Severity] {
			r.addError(prefix+".severity", fmt.Sprintf("must be critical|warning|info, got %q", rule.Severity))
		}

		// Type-specific threshold validation
		if rule.Threshold == "" {
			r.addError(prefix+".threshold", "required")
		} else if rule.Type != "" {
			switch rule.Type {
			case "latency":
				if _, err := time.ParseDuration(rule.Threshold); err != nil {
					r.addError(prefix+".threshold", fmt.Sprintf("invalid duration %q: %v", rule.Threshold, err))
				}
			case "error_rate":
				if !strings.HasSuffix(rule.Threshold, "%") {
					r.addError(prefix+".threshold", fmt.Sprintf("must be a percentage (e.g., \"0.5%%\"), got %q", rule.Threshold))
				} else {
					numStr := strings.TrimSuffix(rule.Threshold, "%")
					if _, err := strconv.ParseFloat(numStr, 64); err != nil {
						r.addError(prefix+".threshold", fmt.Sprintf("invalid percentage %q: %v", rule.Threshold, err))
					}
				}
			case "volume":
				if !strings.HasSuffix(rule.Threshold, "%") {
					r.addError(prefix+".threshold", fmt.Sprintf("must be a percentage (e.g., \"-50%%\"), got %q", rule.Threshold))
				} else {
					numStr := strings.TrimSuffix(rule.Threshold, "%")
					if _, err := strconv.ParseFloat(numStr, 64); err != nil {
						r.addError(prefix+".threshold", fmt.Sprintf("invalid percentage %q: %v", rule.Threshold, err))
					}
				}
			}
		}
	}
}

// ═══════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════

func applyDefaults(spec *v1.PipelineSpec) {
	if spec.APIVersion == "" {
		spec.APIVersion = "mako/v1"
	}
	if spec.Kind == "" {
		spec.Kind = "Pipeline"
	}

	p := &spec.Pipeline

	// Source defaults (single source)
	if p.Source.Type == v1.SourceKafka {
		if p.Source.ConsumerGroup == "" {
			p.Source.ConsumerGroup = "mako-" + p.Name
		}
		if p.Source.StartOffset == "" {
			p.Source.StartOffset = "latest"
		}
	}

	// Sources defaults (multi-source)
	for i := range p.Sources {
		if p.Sources[i].Type == v1.SourceKafka {
			if p.Sources[i].ConsumerGroup == "" {
				suffix := p.Sources[i].Name
				if suffix == "" {
					suffix = fmt.Sprintf("%d", i)
				}
				p.Sources[i].ConsumerGroup = "mako-" + p.Name + "-" + suffix
			}
			if p.Sources[i].StartOffset == "" {
				p.Sources[i].StartOffset = "latest"
			}
		}
	}

	// Isolation defaults
	if p.Isolation.Strategy == "" {
		p.Isolation.Strategy = "per_event_type"
	}
	if p.Isolation.MaxRetries == 0 {
		p.Isolation.MaxRetries = 3
	}
	if p.Isolation.BackoffMs == 0 {
		p.Isolation.BackoffMs = 1000
	}

	// Sink batch defaults
	allSinks := append([]v1.Sink{p.Sink}, p.Sinks...)
	for i := range allSinks {
		if allSinks[i].Batch == nil {
			allSinks[i].Batch = &v1.BatchSpec{
				Size:     1000,
				Interval: "10s",
			}
		}
	}
}

func isValidName(name string) bool {
	if len(name) == 0 || len(name) > 63 {
		return false
	}
	for _, c := range name {
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-') {
			return false
		}
	}
	return name[0] != '-' && name[len(name)-1] != '-'
}

// ParseDuration parses durations like "5m", "1h", "30s", "1d".
func ParseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if strings.HasSuffix(s, "d") {
		s = strings.TrimSuffix(s, "d")
		var days int
		if _, err := fmt.Sscanf(s, "%d", &days); err != nil {
			return 0, fmt.Errorf("invalid days: %s", s)
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	return time.ParseDuration(s)
}
