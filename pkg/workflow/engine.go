// Package workflow implements the Mako workflow engine for DAG-based
// multi-pipeline orchestration.
//
// A workflow is a directed acyclic graph (DAG) of pipeline steps.
// Steps with no dependencies start immediately (in parallel if multiple),
// and when a step completes, any dependents that are now unblocked are triggered.
package workflow

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	v1 "github.com/Stefen-Taime/mako/api/v1"
)

// RunPipelineFunc is the function signature for executing a single pipeline.
// It receives a context and the absolute path to a pipeline YAML file,
// and returns the pipeline's final status or an error.
type RunPipelineFunc func(ctx context.Context, pipelineFile string) (v1.PipelineStatus, error)

// Engine orchestrates a workflow DAG execution.
type Engine struct {
	spec    *v1.WorkflowSpec
	baseDir string // directory of the workflow YAML file
	runFn   RunPipelineFunc

	// Runtime state (protected by mu)
	mu      sync.Mutex
	steps   map[string]*stepState
	order   []string // step names in definition order
	started time.Time
}

// stepState tracks the runtime state of a single workflow step.
type stepState struct {
	step     v1.WorkflowStep
	state    v1.PipelineState
	status   v1.PipelineStatus
	duration time.Duration
	err      error
}

// New creates a new workflow engine.
// baseDir is the directory of the workflow YAML file (used for relative path resolution).
// runFn is the function that executes a single pipeline (typically main.runPipeline).
func New(spec *v1.WorkflowSpec, baseDir string, runFn RunPipelineFunc) *Engine {
	steps := make(map[string]*stepState, len(spec.Workflow.Steps))
	order := make([]string, 0, len(spec.Workflow.Steps))
	for _, s := range spec.Workflow.Steps {
		steps[s.Name] = &stepState{
			step:  s,
			state: v1.StatePending,
		}
		order = append(order, s.Name)
	}
	return &Engine{
		spec:    spec,
		baseDir: baseDir,
		runFn:   runFn,
		steps:   steps,
		order:   order,
	}
}

// Run executes the workflow DAG and returns the final workflow status.
// It respects context cancellation for graceful shutdown.
func (e *Engine) Run(ctx context.Context) v1.WorkflowStatus {
	e.started = time.Now()
	w := e.spec.Workflow
	totalSteps := len(w.Steps)

	fmt.Fprintf(os.Stderr, "🔄 Workflow: %s (%d steps)\n", w.Name, totalSteps)

	// Build reverse dependency map: step -> list of steps that depend on it
	dependents := make(map[string][]string)
	for _, step := range w.Steps {
		for _, dep := range step.DependsOn {
			dependents[dep] = append(dependents[dep], step.Name)
		}
	}

	// Track completed count for progress display
	completedCount := 0
	failedCount := 0

	// Channel to receive step completion notifications
	type stepResult struct {
		name string
	}
	resultCh := make(chan stepResult, totalSteps)

	// WaitGroup for all running step goroutines
	var wg sync.WaitGroup

	// Launch a step in its own goroutine
	launchStep := func(name string) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.executeStep(ctx, name)
			resultCh <- stepResult{name: name}
		}()
	}

	// Find and launch all steps with no dependencies (roots)
	for _, step := range w.Steps {
		if len(step.DependsOn) == 0 {
			launchStep(step.Name)
		}
	}

	// Process results as they arrive
	for completedCount+failedCount < totalSteps {
		select {
		case <-ctx.Done():
			// Context cancelled (SIGINT/SIGTERM) — mark remaining steps as skipped
			e.mu.Lock()
			for _, name := range e.order {
				ss := e.steps[name]
				if ss.state == v1.StatePending {
					ss.state = v1.StateSkipped
				}
			}
			e.mu.Unlock()

			// Wait for any running steps to finish
			wg.Wait()
			return e.buildStatus()

		case res := <-resultCh:
			e.mu.Lock()
			ss := e.steps[res.name]
			stepState := ss.state
			stepIdx := e.stepIndex(res.name)
			e.mu.Unlock()

			if stepState == v1.StateCompleted {
				completedCount++
				fmt.Fprintf(os.Stderr, "✅ [%d/%d] %s — completed (%d in, %d out, %d errors, %s)\n",
					stepIdx+1, totalSteps, res.name,
					ss.status.EventsIn, ss.status.EventsOut, ss.status.Errors,
					formatDuration(ss.duration))

				// Check if any dependents are now unblocked
				for _, depName := range dependents[res.name] {
					if e.isReady(depName) {
						launchStep(depName)
					}
				}
			} else {
				// Step failed
				failedCount++
				errMsg := "unknown error"
				if ss.err != nil {
					errMsg = ss.err.Error()
				}
				fmt.Fprintf(os.Stderr, "❌ [%d/%d] %s — failed (%s)\n",
					stepIdx+1, totalSteps, res.name, errMsg)

				switch w.OnFailure {
				case "stop", "":
					// Abort remaining steps
					e.mu.Lock()
					for _, name := range e.order {
						s := e.steps[name]
						if s.state == v1.StatePending {
							s.state = v1.StateSkipped
							failedCount++ // count skipped as "not completed"
						}
					}
					e.mu.Unlock()

					// Wait for any running steps to finish
					wg.Wait()
					return e.buildStatus()

				case "continue":
					// Mark dependents of the failed step as skipped (transitively)
					e.skipDependents(res.name, dependents, &failedCount)

					// Check if any other steps are now unblocked
					// (steps that don't depend on the failed step)
					for _, depName := range dependents[res.name] {
						// These were already skipped above
						_ = depName
					}

				case "retry":
					// Retry the step up to maxRetries times
					retries := w.MaxRetries
					if retries <= 0 {
						retries = 1
					}
					retried := false
					for attempt := 1; attempt <= retries; attempt++ {
						fmt.Fprintf(os.Stderr, "🔄 [%d/%d] %s — retrying (attempt %d/%d)...\n",
							stepIdx+1, totalSteps, res.name, attempt, retries)

						// Reset state for retry
						e.mu.Lock()
						ss.state = v1.StatePending
						ss.err = nil
						e.mu.Unlock()

						e.executeStep(ctx, res.name)

						e.mu.Lock()
						newState := e.steps[res.name].state
						e.mu.Unlock()

						if newState == v1.StateCompleted {
							retried = true
							// Adjust counts
							failedCount--
							completedCount++
							fmt.Fprintf(os.Stderr, "✅ [%d/%d] %s — completed on retry %d (%d in, %d out, %d errors, %s)\n",
								stepIdx+1, totalSteps, res.name, attempt,
								ss.status.EventsIn, ss.status.EventsOut, ss.status.Errors,
								formatDuration(ss.duration))

							// Unblock dependents
							for _, depName := range dependents[res.name] {
								if e.isReady(depName) {
									launchStep(depName)
								}
							}
							break
						}
					}
					if !retried {
						// All retries exhausted — skip dependents
						e.skipDependents(res.name, dependents, &failedCount)
					}
				}
			}
		}
	}

	// Wait for all goroutines to finish
	wg.Wait()

	return e.buildStatus()
}

// executeStep runs a single step — either a pipeline or a quality gate.
func (e *Engine) executeStep(ctx context.Context, name string) {
	e.mu.Lock()
	ss := e.steps[name]
	ss.state = v1.StateRunning
	stepType := ss.step.Type
	e.mu.Unlock()

	stepIdx := e.stepIndex(name)
	totalSteps := len(e.order)

	if stepType == "quality_gate" {
		fmt.Fprintf(os.Stderr, "🔍 [%d/%d] %s — running quality checks...\n", stepIdx+1, totalSteps, name)
		e.executeQualityGate(ctx, name)
		return
	}

	// Default: pipeline step
	fmt.Fprintf(os.Stderr, "▶️  [%d/%d] %s — running...\n", stepIdx+1, totalSteps, name)

	// Resolve pipeline path relative to workflow file's directory
	pipelinePath := ss.step.Pipeline
	if !filepath.IsAbs(pipelinePath) {
		pipelinePath = filepath.Join(e.baseDir, pipelinePath)
	}

	// Create a derived context for this step
	stepCtx, stepCancel := context.WithCancel(ctx)
	defer stepCancel()

	startTime := time.Now()
	status, err := e.runFn(stepCtx, pipelinePath)
	elapsed := time.Since(startTime)

	e.mu.Lock()
	defer e.mu.Unlock()

	ss.duration = elapsed
	ss.status = status

	if err != nil {
		ss.state = v1.StateFailed
		ss.err = err
		return
	}

	// Check for errors in the pipeline status
	if status.Errors > 0 {
		ss.state = v1.StateFailed
		ss.err = fmt.Errorf("%d pipeline errors", status.Errors)
		return
	}

	ss.state = v1.StateCompleted
}

// executeQualityGate runs SQL-based data quality assertions against a DuckDB database.
func (e *Engine) executeQualityGate(ctx context.Context, name string) {
	e.mu.Lock()
	ss := e.steps[name]
	e.mu.Unlock()

	startTime := time.Now()
	result, err := RunQualityGate(ctx, ss.step, e.baseDir)
	elapsed := time.Since(startTime)

	e.mu.Lock()
	defer e.mu.Unlock()

	ss.duration = elapsed

	if err != nil {
		ss.state = v1.StateFailed
		ss.err = err
		return
	}

	// Store check counts in the status for reporting
	ss.status = v1.PipelineStatus{
		EventsIn:  int64(result.Total),
		EventsOut: int64(result.Passed),
		Errors:    int64(result.Failed),
	}

	if result.Failed > 0 {
		ss.state = v1.StateFailed
		ss.err = fmt.Errorf("%d/%d quality checks failed: %s",
			result.Failed, result.Total, strings.Join(result.Errors, "; "))
		return
	}

	ss.state = v1.StateCompleted
}

// isReady returns true if a step's dependencies are all completed.
func (e *Engine) isReady(name string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	ss := e.steps[name]
	if ss.state != v1.StatePending {
		return false
	}
	for _, dep := range ss.step.DependsOn {
		if e.steps[dep].state != v1.StateCompleted {
			return false
		}
	}
	return true
}

// skipDependents recursively marks all steps that depend on a failed step as skipped.
func (e *Engine) skipDependents(failedName string, dependents map[string][]string, failedCount *int) {
	e.mu.Lock()
	defer e.mu.Unlock()

	var skipRecursive func(name string)
	skipRecursive = func(name string) {
		for _, depName := range dependents[name] {
			ss := e.steps[depName]
			if ss.state == v1.StatePending {
				ss.state = v1.StateSkipped
				*failedCount++
				fmt.Fprintf(os.Stderr, "⏭️  %s — skipped (depends on failed step %s)\n", depName, failedName)
				skipRecursive(depName)
			}
		}
	}
	skipRecursive(failedName)
}

// stepIndex returns the 0-based index of a step in definition order.
func (e *Engine) stepIndex(name string) int {
	for i, n := range e.order {
		if n == name {
			return i
		}
	}
	return -1
}

// buildStatus builds the final WorkflowStatus from the engine's state.
func (e *Engine) buildStatus() v1.WorkflowStatus {
	e.mu.Lock()
	defer e.mu.Unlock()

	totalDuration := time.Since(e.started)
	steps := make([]v1.WorkflowStepStatus, 0, len(e.order))
	allCompleted := true
	anyFailed := false

	for _, name := range e.order {
		ss := e.steps[name]
		steps = append(steps, v1.WorkflowStepStatus{
			Name:      name,
			Pipeline:  ss.step.Pipeline,
			State:     ss.state,
			EventsIn:  ss.status.EventsIn,
			EventsOut: ss.status.EventsOut,
			Errors:    ss.status.Errors,
			Duration:  ss.duration,
		})
		if ss.state != v1.StateCompleted {
			allCompleted = false
		}
		if ss.state == v1.StateFailed {
			anyFailed = true
		}
	}

	workflowState := v1.StateCompleted
	if anyFailed {
		workflowState = v1.StateFailed
	} else if !allCompleted {
		workflowState = v1.StateFailed
	}

	succeeded := 0
	for _, ss := range steps {
		if ss.State == v1.StateCompleted {
			succeeded++
		}
	}

	if workflowState == v1.StateCompleted {
		fmt.Fprintf(os.Stderr, "🎯 Workflow completed: %d/%d steps succeeded (%s total)\n",
			succeeded, len(steps), formatDuration(totalDuration))
	} else {
		fmt.Fprintf(os.Stderr, "💥 Workflow failed: %d/%d steps succeeded (%s total)\n",
			succeeded, len(steps), formatDuration(totalDuration))
	}

	now := e.started
	return v1.WorkflowStatus{
		Name:      e.spec.Workflow.Name,
		State:     workflowState,
		Steps:     steps,
		StartedAt: &now,
		Duration:  totalDuration,
	}
}

// formatDuration formats a duration for human-readable display.
func formatDuration(d time.Duration) string {
	if d < time.Second {
		return d.Round(time.Millisecond).String()
	}
	if d < time.Minute {
		return fmt.Sprintf("%.0fs", d.Seconds())
	}
	minutes := int(d.Minutes())
	seconds := int(d.Seconds()) % 60
	if minutes < 60 {
		return fmt.Sprintf("%dm%02ds", minutes, seconds)
	}
	hours := minutes / 60
	minutes = minutes % 60
	return fmt.Sprintf("%dh%02dm%02ds", hours, minutes, seconds)
}
