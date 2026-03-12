package workflow

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"

	_ "github.com/marcboeker/go-duckdb"

	v1 "github.com/Stefen-Taime/mako/api/v1"
)

// QualityGateResult holds the outcome of a quality gate execution.
type QualityGateResult struct {
	Passed  int
	Failed  int
	Total   int
	Errors  []string // human-readable error messages for failed checks
}

// RunQualityGate executes a quality_gate step: opens a DuckDB database,
// runs each SQL check, and evaluates the assertion expression.
func RunQualityGate(ctx context.Context, step v1.WorkflowStep, baseDir string) (*QualityGateResult, error) {
	if len(step.Checks) == 0 {
		return nil, fmt.Errorf("quality_gate step %q has no checks", step.Name)
	}

	dsn := step.Database
	if dsn == "" {
		dsn = ":memory:"
	}

	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return nil, fmt.Errorf("quality_gate open duckdb %q: %w", dsn, err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("quality_gate ping duckdb: %w", err)
	}

	result := &QualityGateResult{Total: len(step.Checks)}

	for i, check := range step.Checks {
		checkName := check.Name
		if checkName == "" {
			checkName = fmt.Sprintf("check_%d", i+1)
		}

		// Execute the SQL query — must return a single row with a single numeric value
		var value float64
		row := db.QueryRowContext(ctx, check.SQL)
		if err := row.Scan(&value); err != nil {
			result.Failed++
			errMsg := fmt.Sprintf("[%s] SQL error: %v (query: %s)", checkName, err, truncateSQL(check.SQL))
			result.Errors = append(result.Errors, errMsg)
			fmt.Fprintf(os.Stderr, "  ❌ %s\n", errMsg)
			continue
		}

		// Evaluate the assertion
		pass, err := evaluateAssertion(value, check.Expect)
		if err != nil {
			result.Failed++
			errMsg := fmt.Sprintf("[%s] invalid assertion %q: %v", checkName, check.Expect, err)
			result.Errors = append(result.Errors, errMsg)
			fmt.Fprintf(os.Stderr, "  ❌ %s\n", errMsg)
			continue
		}

		if pass {
			result.Passed++
			fmt.Fprintf(os.Stderr, "  ✅ %s: %.4g %s\n", checkName, value, check.Expect)
		} else {
			result.Failed++
			errMsg := fmt.Sprintf("[%s] assertion failed: got %.4g, expected %s", checkName, value, check.Expect)
			result.Errors = append(result.Errors, errMsg)
			fmt.Fprintf(os.Stderr, "  ❌ %s\n", errMsg)
		}
	}

	return result, nil
}

// evaluateAssertion evaluates a value against an assertion expression.
// Supported formats:
//   - "= N", "== N"
//   - "!= N"
//   - "< N", "<= N", "> N", ">= N"
//   - "BETWEEN N AND M"
func evaluateAssertion(value float64, expect string) (bool, error) {
	expect = strings.TrimSpace(expect)
	upper := strings.ToUpper(expect)

	// BETWEEN N AND M
	if strings.HasPrefix(upper, "BETWEEN ") {
		parts := strings.SplitN(upper[8:], " AND ", 2)
		if len(parts) != 2 {
			return false, fmt.Errorf("invalid BETWEEN syntax: %s", expect)
		}
		lo, err := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
		if err != nil {
			return false, fmt.Errorf("invalid lower bound: %s", parts[0])
		}
		hi, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
		if err != nil {
			return false, fmt.Errorf("invalid upper bound: %s", parts[1])
		}
		return value >= lo && value <= hi, nil
	}

	// Comparison operators
	operators := []struct {
		prefix string
		eval   func(v, n float64) bool
	}{
		{">=", func(v, n float64) bool { return v >= n }},
		{"<=", func(v, n float64) bool { return v <= n }},
		{"!=", func(v, n float64) bool { return v != n }},
		{"==", func(v, n float64) bool { return v == n }},
		{"=", func(v, n float64) bool { return v == n }},
		{">", func(v, n float64) bool { return v > n }},
		{"<", func(v, n float64) bool { return v < n }},
	}

	for _, op := range operators {
		if strings.HasPrefix(expect, op.prefix) {
			numStr := strings.TrimSpace(expect[len(op.prefix):])
			n, err := strconv.ParseFloat(numStr, 64)
			if err != nil {
				return false, fmt.Errorf("invalid number %q after %s", numStr, op.prefix)
			}
			return op.eval(value, n), nil
		}
	}

	return false, fmt.Errorf("unsupported assertion format: %s", expect)
}

// truncateSQL shortens a SQL query for display in error messages.
func truncateSQL(sql string) string {
	sql = strings.TrimSpace(sql)
	if len(sql) > 80 {
		return sql[:77] + "..."
	}
	return sql
}
