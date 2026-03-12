// Package transform implements the event transformation engine.
//
// Each transform is a pure function: map[string]any → map[string]any.
// Transforms are chained: the output of one feeds the input of the next.
package transform

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"

	v1 "github.com/Stefen-Taime/mako/api/v1"
)

// Func is a single transform operation.
// Returns nil to filter out the event (drop it).
type Func func(event map[string]any) (map[string]any, error)

// Chain is an ordered sequence of transforms applied to each event.
type Chain struct {
	steps []namedStep
}

type namedStep struct {
	name string
	fn   Func
}

// NewChain builds a Chain from a list of transform specs.
func NewChain(specs []v1.Transform, opts ...Option) (*Chain, error) {
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	chain := &Chain{}
	for _, spec := range specs {
		fn, err := buildTransform(spec, o)
		if err != nil {
			return nil, fmt.Errorf("transform %q: %w", spec.Name, err)
		}
		chain.steps = append(chain.steps, namedStep{name: spec.Name, fn: fn})
	}
	return chain, nil
}

// Apply executes the full transform chain on an event.
// Returns nil if the event is filtered out.
func (c *Chain) Apply(event map[string]any) (map[string]any, error) {
	if c == nil || len(c.steps) == 0 {
		return event, nil
	}

	current := event
	for _, step := range c.steps {
		result, err := step.fn(current)
		if err != nil {
			return nil, fmt.Errorf("step %q: %w", step.name, err)
		}
		if result == nil {
			return nil, nil // Event filtered out
		}
		current = result
	}
	return current, nil
}

// Len returns the number of transforms in the chain.
func (c *Chain) Len() int {
	if c == nil {
		return 0
	}
	return len(c.steps)
}

// ═══════════════════════════════════════════
// Options
// ═══════════════════════════════════════════

type options struct {
	piiSalt string
}

type Option func(*options)

func defaultOptions() *options {
	return &options{piiSalt: "mako-default-salt"}
}

func WithPIISalt(salt string) Option {
	return func(o *options) { o.piiSalt = salt }
}

// ═══════════════════════════════════════════
// Transform builders
// ═══════════════════════════════════════════

func buildTransform(spec v1.Transform, opts *options) (Func, error) {
	switch spec.Type {

	case v1.TransformHashFields:
		return hashFieldsTransform(spec.Fields, opts.piiSalt), nil

	case v1.TransformMaskFields:
		return maskFieldsTransform(spec.Fields), nil

	case v1.TransformDropFields:
		return dropFieldsTransform(spec.Fields), nil

	case v1.TransformFilter:
		return filterTransform(spec.Condition)

	case v1.TransformSQL:
		return sqlTransform(spec.Query)

	case v1.TransformRename:
		return renameFieldsTransform(spec.Mapping), nil

	case v1.TransformCast:
		return castFieldsTransform(spec.Mapping), nil

	case v1.TransformFlatten:
		return flattenTransform(), nil

	case v1.TransformDefault:
		return defaultValuesTransform(spec.Config), nil

	case v1.TransformDedupe:
		return dedupeTransform(spec.Fields), nil

	case v1.TransformAggregate:
		// Aggregation is handled at the pipeline level (windowing)
		return passthroughTransform(), nil

	case v1.TransformDQCheck:
		return dqCheckTransform(spec.Checks, spec.OnFailure)

	case v1.TransformPlugin:
		path, _ := spec.Config["path"].(string)
		if path == "" {
			return nil, fmt.Errorf("plugin transform requires config.path (path to .wasm file)")
		}
		funcName, _ := spec.Config["function"].(string)
		wt, err := NewWASMTransform(path, funcName)
		if err != nil {
			return nil, fmt.Errorf("wasm plugin: %w", err)
		}
		return wt.Func(), nil

	default:
		return nil, fmt.Errorf("unsupported transform type: %s", spec.Type)
	}
}

// ═══════════════════════════════════════════
// Transform implementations
// ═══════════════════════════════════════════

// hashFieldsTransform applies SHA-256 + salt to specified fields.
// This is the PII governance transform (Autodesk/Atlan pattern).
func hashFieldsTransform(fields []string, salt string) Func {
	fieldSet := toSet(fields)
	return func(event map[string]any) (map[string]any, error) {
		result := copyMap(event)
		for k, v := range result {
			if !fieldSet[k] {
				continue
			}
			str := fmt.Sprintf("%v", v)
			h := sha256.Sum256([]byte(salt + str))
			result[k] = hex.EncodeToString(h[:8]) // 16-char hash
		}
		// Mark as PII-processed
		result["_pii_processed"] = true
		return result, nil
	}
}

// maskFieldsTransform partially masks field values.
// "john@email.com" → "j***@email.com", "4111222233334444" → "****4444"
func maskFieldsTransform(fields []string) Func {
	fieldSet := toSet(fields)
	return func(event map[string]any) (map[string]any, error) {
		result := copyMap(event)
		for k, v := range result {
			if !fieldSet[k] {
				continue
			}
			str := fmt.Sprintf("%v", v)
			result[k] = maskValue(str)
		}
		return result, nil
	}
}

// dropFieldsTransform removes specified fields from events.
func dropFieldsTransform(fields []string) Func {
	fieldSet := toSet(fields)
	return func(event map[string]any) (map[string]any, error) {
		result := make(map[string]any, len(event))
		for k, v := range event {
			if !fieldSet[k] {
				result[k] = v
			}
		}
		return result, nil
	}
}

// filterTransform applies a SQL-like WHERE condition.
// Supports: field = value, field > value, field != value, AND, OR
func filterTransform(condition string) (Func, error) {
	matcher, err := parseCondition(condition)
	if err != nil {
		return nil, fmt.Errorf("parse condition %q: %w", condition, err)
	}
	return func(event map[string]any) (map[string]any, error) {
		if matcher(event) {
			return event, nil
		}
		return nil, nil // filtered out
	}, nil
}

// sqlTransform applies a SQL SELECT to the event.
// Supports: SELECT *, field AS alias, field * N as computed,
// and CASE WHEN expressions for basic enrichment.
func sqlTransform(query string) (Func, error) {
	// Parse SELECT ... FROM ...
	q := strings.TrimSpace(query)
	upperQ := strings.ToUpper(q)

	// Extract the SELECT clause (between SELECT and FROM)
	selectStart := 0
	if strings.HasPrefix(upperQ, "SELECT ") {
		selectStart = 7
	} else {
		// If no SELECT keyword, treat entire query as field list
		selectStart = 0
	}

	fromIdx := strings.Index(upperQ, " FROM ")
	selectClause := ""
	if fromIdx > 0 {
		selectClause = strings.TrimSpace(q[selectStart:fromIdx])
	} else {
		selectClause = strings.TrimSpace(q[selectStart:])
	}

	// Parse projections
	projections, err := parseSQLProjections(selectClause)
	if err != nil {
		return nil, err
	}

	return func(event map[string]any) (map[string]any, error) {
		result := make(map[string]any, len(event)+len(projections))

		// Apply projections
		for _, proj := range projections {
			if proj.isStar {
				// SELECT * — copy all fields
				for k, v := range event {
					result[k] = v
				}
			} else if proj.caseExpr != nil {
				// CASE WHEN ... THEN ... ELSE ... END
				val := evaluateCase(proj.caseExpr, event)
				result[proj.alias] = val
			} else if proj.expr != "" {
				// Expression: could be field reference or arithmetic
				val := evaluateExpr(proj.expr, event)
				result[proj.alias] = val
			}
		}

		return result, nil
	}, nil
}

type sqlProjection struct {
	isStar   bool
	expr     string
	alias    string
	caseExpr *caseExpression
}

type caseExpression struct {
	conditions []caseWhen
	elseValue  string
}

type caseWhen struct {
	condition string
	thenValue string
}

func parseSQLProjections(clause string) ([]sqlProjection, error) {
	var projections []sqlProjection

	// Split by comma, but respect parentheses and CASE/END blocks
	parts := splitSQLCommas(clause)

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		upper := strings.ToUpper(part)

		if part == "*" {
			projections = append(projections, sqlProjection{isStar: true})
			continue
		}

		// CASE WHEN expression
		if strings.HasPrefix(upper, "CASE ") {
			proj, err := parseCaseExpression(part)
			if err != nil {
				return nil, err
			}
			projections = append(projections, proj)
			continue
		}

		// expr AS alias
		asIdx := -1
		upperPart := strings.ToUpper(part)
		// Find last " AS " to handle nested expressions
		asIdx = strings.LastIndex(upperPart, " AS ")
		if asIdx > 0 {
			expr := strings.TrimSpace(part[:asIdx])
			alias := strings.TrimSpace(part[asIdx+4:])
			projections = append(projections, sqlProjection{expr: expr, alias: alias})
		} else {
			// Just a field name
			projections = append(projections, sqlProjection{expr: part, alias: part})
		}
	}

	return projections, nil
}

func splitSQLCommas(s string) []string {
	var parts []string
	depth := 0 // parentheses depth
	caseDepth := 0
	current := ""
	upper := strings.ToUpper(s)

	for i := 0; i < len(s); i++ {
		ch := s[i]

		// Track CASE/END
		if i+5 <= len(s) && upper[i:i+5] == "CASE " {
			caseDepth++
		}
		if i+3 <= len(s) && upper[i:i+3] == "END" && caseDepth > 0 {
			caseDepth--
		}

		if ch == '(' {
			depth++
		} else if ch == ')' {
			depth--
		}

		if ch == ',' && depth == 0 && caseDepth == 0 {
			parts = append(parts, current)
			current = ""
		} else {
			current += string(ch)
		}
	}
	if current != "" {
		parts = append(parts, current)
	}
	return parts
}

func parseCaseExpression(expr string) (sqlProjection, error) {
	upper := strings.ToUpper(expr)

	// Extract alias: ... END as alias
	alias := ""
	endIdx := strings.LastIndex(upper, " END")
	if endIdx < 0 {
		return sqlProjection{}, fmt.Errorf("CASE without END: %s", expr)
	}
	afterEnd := strings.TrimSpace(expr[endIdx+4:])
	upperAfter := strings.ToUpper(afterEnd)
	if strings.HasPrefix(upperAfter, "AS ") {
		alias = strings.TrimSpace(afterEnd[3:])
	} else if afterEnd != "" {
		alias = afterEnd
	} else {
		alias = "case_result"
	}

	// Extract body between CASE and END
	body := strings.TrimSpace(expr[5:endIdx])

	ce := &caseExpression{}

	// Parse WHEN ... THEN ... pairs and optional ELSE
	remaining := body
	for {
		upperRem := strings.ToUpper(remaining)
		whenIdx := strings.Index(upperRem, "WHEN ")
		if whenIdx < 0 {
			// Check for ELSE
			elseIdx := strings.Index(upperRem, "ELSE ")
			if elseIdx >= 0 {
				ce.elseValue = strings.TrimSpace(remaining[elseIdx+5:])
				ce.elseValue = strings.Trim(ce.elseValue, "'\"")
			}
			break
		}

		remaining = remaining[whenIdx+5:]
		upperRem = strings.ToUpper(remaining)

		thenIdx := strings.Index(upperRem, " THEN ")
		if thenIdx < 0 {
			break
		}

		condition := strings.TrimSpace(remaining[:thenIdx])

		remaining = remaining[thenIdx+6:]
		upperRem = strings.ToUpper(remaining)

		// Value goes until next WHEN or ELSE or end
		nextWhen := strings.Index(upperRem, " WHEN ")
		nextElse := strings.Index(upperRem, " ELSE ")
		end := len(remaining)
		if nextWhen >= 0 && (nextElse < 0 || nextWhen < nextElse) {
			end = nextWhen
		} else if nextElse >= 0 {
			end = nextElse
		}

		value := strings.TrimSpace(remaining[:end])
		value = strings.Trim(value, "'\"")

		ce.conditions = append(ce.conditions, caseWhen{condition: condition, thenValue: value})
		remaining = remaining[end:]
	}

	return sqlProjection{alias: alias, caseExpr: ce}, nil
}

func evaluateCase(ce *caseExpression, event map[string]any) any {
	for _, cw := range ce.conditions {
		matcher, err := parseCondition(cw.condition)
		if err != nil {
			continue
		}
		if matcher(event) {
			return cw.thenValue
		}
	}
	if ce.elseValue != "" {
		return ce.elseValue
	}
	return nil
}

func evaluateExpr(expr string, event map[string]any) any {
	expr = strings.TrimSpace(expr)

	// Try arithmetic: field * N, field + N, etc.
	for _, op := range []string{" * ", " + ", " - ", " / "} {
		if parts := strings.SplitN(expr, op, 2); len(parts) == 2 {
			left := evaluateExpr(parts[0], event)
			right := evaluateExpr(parts[1], event)
			lf := toFloat(left)
			rf := toFloat(right)
			switch op {
			case " * ":
				return lf * rf
			case " + ":
				return lf + rf
			case " - ":
				return lf - rf
			case " / ":
				if rf != 0 {
					return lf / rf
				}
				return 0.0
			}
		}
	}

	// Try as number literal
	var f float64
	if _, err := fmt.Sscanf(expr, "%f", &f); err == nil {
		return f
	}

	// Try as string literal
	if (strings.HasPrefix(expr, "'") && strings.HasSuffix(expr, "'")) ||
		(strings.HasPrefix(expr, "\"") && strings.HasSuffix(expr, "\"")) {
		return expr[1 : len(expr)-1]
	}

	// Field reference
	if v, ok := event[expr]; ok {
		return v
	}

	return expr
}

func toFloat(v any) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int:
		return float64(val)
	case int64:
		return float64(val)
	case string:
		var f float64
		fmt.Sscanf(val, "%f", &f)
		return f
	default:
		var f float64
		fmt.Sscanf(fmt.Sprintf("%v", v), "%f", &f)
		return f
	}
}

// renameFieldsTransform renames fields according to a mapping.
func renameFieldsTransform(mapping map[string]string) Func {
	return func(event map[string]any) (map[string]any, error) {
		result := copyMap(event)
		for oldName, newName := range mapping {
			if v, ok := result[oldName]; ok {
				result[newName] = v
				delete(result, oldName)
			}
		}
		return result, nil
	}
}

// castFieldsTransform casts fields to specified types.
func castFieldsTransform(mapping map[string]string) Func {
	return func(event map[string]any) (map[string]any, error) {
		result := copyMap(event)
		for field, targetType := range mapping {
			v, ok := result[field]
			if !ok {
				continue
			}
			casted, err := castValue(v, targetType)
			if err != nil {
				return nil, fmt.Errorf("cast %s to %s: %w", field, targetType, err)
			}
			result[field] = casted
		}
		return result, nil
	}
}

// flattenTransform flattens nested maps into dot-notation keys.
// {"user": {"name": "John"}} → {"user.name": "John"}
func flattenTransform() Func {
	return func(event map[string]any) (map[string]any, error) {
		return flattenMap("", event), nil
	}
}

// defaultValuesTransform fills null/missing fields with defaults.
func defaultValuesTransform(defaults map[string]any) Func {
	return func(event map[string]any) (map[string]any, error) {
		result := copyMap(event)
		for k, v := range defaults {
			if _, exists := result[k]; !exists {
				result[k] = v
			}
		}
		return result, nil
	}
}

// dedupeTransform deduplicates by key fields (in-memory, bounded).
func dedupeTransform(keyFields []string) Func {
	seen := make(map[string]struct{}, 10000)
	return func(event map[string]any) (map[string]any, error) {
		key := buildDedupeKey(event, keyFields)
		if _, exists := seen[key]; exists {
			return nil, nil // duplicate, filter out
		}
		seen[key] = struct{}{}
		// Evict old entries if too many
		if len(seen) > 100000 {
			seen = make(map[string]struct{}, 10000)
		}
		return event, nil
	}
}

func passthroughTransform() Func {
	return func(event map[string]any) (map[string]any, error) {
		return event, nil
	}
}

// ═══════════════════════════════════════════
// Data Quality Check transform
// ═══════════════════════════════════════════

// dqCheckTransform runs a list of row-level data quality checks on each event.
//
// on_failure behavior:
//   - "tag" (default): adds _dq_errors field with list of failures, event passes through
//   - "drop": drops the event if any check fails (returns nil)
//   - "fail": returns an error if any check fails (sends to DLQ)
func dqCheckTransform(checks []v1.DQCheck, onFailure string) (Func, error) {
	if len(checks) == 0 {
		return nil, fmt.Errorf("dq_check requires at least one check")
	}

	if onFailure == "" {
		onFailure = "tag"
	}

	// Pre-compile regex patterns
	type compiledCheck struct {
		check v1.DQCheck
		re    *regexp.Regexp // only for regex rule
	}
	compiled := make([]compiledCheck, len(checks))
	for i, c := range checks {
		cc := compiledCheck{check: c}
		if c.Rule == "regex" && c.Pattern != "" {
			re, err := regexp.Compile(c.Pattern)
			if err != nil {
				return nil, fmt.Errorf("dq_check: invalid regex pattern %q for column %q: %w", c.Pattern, c.Column, err)
			}
			cc.re = re
		}
		compiled[i] = cc
	}

	return func(event map[string]any) (map[string]any, error) {
		var failures []string

		for _, cc := range compiled {
			c := cc.check
			v, exists := event[c.Column]

			switch c.Rule {
			case "not_null":
				if !exists || v == nil {
					failures = append(failures, fmt.Sprintf("%s: is null", c.Column))
				}

			case "range":
				if !exists || v == nil {
					break
				}
				fv := toFloat(v)
				if c.Min != nil && fv < *c.Min {
					failures = append(failures, fmt.Sprintf("%s: %.2f < min %.2f", c.Column, fv, *c.Min))
				}
				if c.Max != nil && fv > *c.Max {
					failures = append(failures, fmt.Sprintf("%s: %.2f > max %.2f", c.Column, fv, *c.Max))
				}

			case "in_set":
				if !exists || v == nil {
					failures = append(failures, fmt.Sprintf("%s: is null (expected one of %v)", c.Column, c.Values))
					break
				}
				str := fmt.Sprintf("%v", v)
				found := false
				for _, allowed := range c.Values {
					if str == allowed {
						found = true
						break
					}
				}
				if !found {
					failures = append(failures, fmt.Sprintf("%s: %q not in %v", c.Column, str, c.Values))
				}

			case "regex":
				if !exists || v == nil {
					break
				}
				str := fmt.Sprintf("%v", v)
				if cc.re != nil && !cc.re.MatchString(str) {
					failures = append(failures, fmt.Sprintf("%s: %q does not match /%s/", c.Column, str, c.Pattern))
				}

			case "min_length":
				if !exists || v == nil {
					break
				}
				str := fmt.Sprintf("%v", v)
				if c.Length != nil && len(str) < *c.Length {
					failures = append(failures, fmt.Sprintf("%s: length %d < min %d", c.Column, len(str), *c.Length))
				}

			case "max_length":
				if !exists || v == nil {
					break
				}
				str := fmt.Sprintf("%v", v)
				if c.Length != nil && len(str) > *c.Length {
					failures = append(failures, fmt.Sprintf("%s: length %d > max %d", c.Column, len(str), *c.Length))
				}

			case "type":
				if !exists || v == nil {
					break
				}
				ok := false
				switch c.Type {
				case "string":
					_, ok = v.(string)
				case "int", "integer":
					switch v.(type) {
					case int, int32, int64:
						ok = true
					case float64:
						f := v.(float64)
						ok = f == float64(int64(f)) // whole number
					}
				case "float", "double", "number":
					switch v.(type) {
					case float32, float64, int, int32, int64:
						ok = true
					}
				case "bool", "boolean":
					_, ok = v.(bool)
				}
				if !ok {
					failures = append(failures, fmt.Sprintf("%s: expected type %s, got %T", c.Column, c.Type, v))
				}
			}
		}

		if len(failures) == 0 {
			return event, nil
		}

		switch onFailure {
		case "drop":
			return nil, nil // filtered out
		case "fail":
			return nil, fmt.Errorf("dq_check failed: %s", strings.Join(failures, "; "))
		default: // "tag"
			result := copyMap(event)
			result["_dq_errors"] = failures
			result["_dq_valid"] = false
			return result, nil
		}
	}, nil
}

// ═══════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════

func copyMap(m map[string]any) map[string]any {
	result := make(map[string]any, len(m))
	for k, v := range m {
		result[k] = v
	}
	return result
}

func toSet(fields []string) map[string]bool {
	s := make(map[string]bool, len(fields))
	for _, f := range fields {
		s[f] = true
	}
	return s
}

func maskValue(s string) string {
	if len(s) <= 4 {
		return "****"
	}
	if strings.Contains(s, "@") {
		// Email: j***@email.com
		parts := strings.SplitN(s, "@", 2)
		if len(parts[0]) > 1 {
			return string(parts[0][0]) + "***@" + parts[1]
		}
		return "***@" + parts[1]
	}
	// Default: show last 4
	return strings.Repeat("*", len(s)-4) + s[len(s)-4:]
}

func flattenMap(prefix string, m map[string]any) map[string]any {
	result := make(map[string]any)
	for k, v := range m {
		key := k
		if prefix != "" {
			key = prefix + "." + k
		}
		if nested, ok := v.(map[string]any); ok {
			for nk, nv := range flattenMap(key, nested) {
				result[nk] = nv
			}
		} else {
			result[key] = v
		}
	}
	return result
}

func buildDedupeKey(event map[string]any, fields []string) string {
	parts := make([]string, len(fields))
	for i, f := range fields {
		parts[i] = fmt.Sprintf("%v", event[f])
	}
	return strings.Join(parts, "|")
}

// parseCondition parses simple SQL-like conditions.
func parseCondition(cond string) (func(map[string]any) bool, error) {
	cond = strings.TrimSpace(cond)

	// Handle AND
	if parts := strings.SplitN(cond, " AND ", 2); len(parts) == 2 {
		left, err := parseCondition(parts[0])
		if err != nil {
			return nil, err
		}
		right, err := parseCondition(parts[1])
		if err != nil {
			return nil, err
		}
		return func(e map[string]any) bool { return left(e) && right(e) }, nil
	}

	// Handle OR
	if parts := strings.SplitN(cond, " OR ", 2); len(parts) == 2 {
		left, err := parseCondition(parts[0])
		if err != nil {
			return nil, err
		}
		right, err := parseCondition(parts[1])
		if err != nil {
			return nil, err
		}
		return func(e map[string]any) bool { return left(e) || right(e) }, nil
	}

	// Handle IS NOT NULL
	if strings.HasSuffix(cond, " IS NOT NULL") {
		field := strings.TrimSuffix(cond, " IS NOT NULL")
		field = strings.TrimSpace(field)
		return func(e map[string]any) bool {
			v, exists := e[field]
			return exists && v != nil
		}, nil
	}

	// Handle IS NULL
	if strings.HasSuffix(cond, " IS NULL") {
		field := strings.TrimSuffix(cond, " IS NULL")
		field = strings.TrimSpace(field)
		return func(e map[string]any) bool {
			v, exists := e[field]
			return !exists || v == nil
		}, nil
	}

	// Handle comparisons: =, !=, >, <, >=, <=
	type cmpFuncs struct {
		op      string
		strCmp  func(a, b string) bool
		numCmp  func(a, b float64) bool
	}
	operators := []cmpFuncs{
		{"!=", func(a, b string) bool { return a != b }, func(a, b float64) bool { return a != b }},
		{">=", func(a, b string) bool { return a >= b }, func(a, b float64) bool { return a >= b }},
		{"<=", func(a, b string) bool { return a <= b }, func(a, b float64) bool { return a <= b }},
		{"=", func(a, b string) bool { return a == b }, func(a, b float64) bool { return a == b }},
		{">", func(a, b string) bool { return a > b }, func(a, b float64) bool { return a > b }},
		{"<", func(a, b string) bool { return a < b }, func(a, b float64) bool { return a < b }},
	}

	for _, op := range operators {
		if parts := strings.SplitN(cond, " "+op.op+" ", 2); len(parts) == 2 {
			field := strings.TrimSpace(parts[0])
			value := strings.Trim(strings.TrimSpace(parts[1]), "'\"")
			strFn := op.strCmp
			numFn := op.numCmp
			return func(e map[string]any) bool {
				v, ok := e[field]
				if !ok {
					return false
				}
				// Try numeric comparison first: if both the field value
				// and the comparison operand parse as float64, compare numerically.
				if af, aOk := toFloat64(v); aOk {
					if bf, bOk := parseFloat64(value); bOk {
						return numFn(af, bf)
					}
				}
				return strFn(fmt.Sprintf("%v", v), value)
			}, nil
		}
	}

	return nil, fmt.Errorf("unsupported condition: %s", cond)
}

// toFloat64 attempts to extract a float64 from a Go value.
// Returns (value, true) if the value is numeric or a numeric string.
func toFloat64(v any) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case int32:
		return float64(val), true
	case string:
		return parseFloat64(val)
	default:
		return 0, false
	}
}

// parseFloat64 attempts to parse a string as float64.
func parseFloat64(s string) (float64, bool) {
	s = strings.TrimSpace(s)
	var f float64
	n, err := fmt.Sscanf(s, "%f", &f)
	if err != nil || n != 1 {
		return 0, false
	}
	return f, true
}

func castValue(v any, targetType string) (any, error) {
	str := fmt.Sprintf("%v", v)
	switch targetType {
	case "string":
		return str, nil
	case "int", "integer":
		var i int64
		_, err := fmt.Sscanf(str, "%d", &i)
		return i, err
	case "float", "double":
		var f float64
		_, err := fmt.Sscanf(str, "%f", &f)
		return f, err
	case "bool", "boolean":
		return str == "true" || str == "1", nil
	default:
		return v, nil
	}
}
