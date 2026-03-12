package transform

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

// ═══════════════════════════════════════════
// WASM Plugin Transform
// ═══════════════════════════════════════════
//
// Loads a user-supplied .wasm module and uses it as a transform.
// The WASM module must export the following functions:
//
//	alloc(size u32) -> ptr u32
//	    Allocate `size` bytes in WASM linear memory.
//
//	dealloc(ptr u32, size u32)
//	    Free memory previously allocated by alloc (optional).
//
//	transform(ptr u32, len u32) -> u64
//	    Read a JSON-encoded event from (ptr, len), transform it,
//	    and return a packed u64 = (result_ptr << 32) | result_len.
//	    Return 0 to filter out (drop) the event.
//
// The event is serialized as JSON, passed into WASM memory via alloc,
// processed by transform(), and the result JSON is read back.
//
// YAML example:
//
//	transforms:
//	  - name: custom_enrich
//	    type: plugin
//	    config:
//	      path: ./plugins/enrich.wasm
//	      function: transform     # optional, default "transform"

// WASMTransform holds a compiled WASM module and provides a transform.Func.
type WASMTransform struct {
	runtime  wazero.Runtime
	compiled wazero.CompiledModule
	funcName string

	// Pool of module instances for concurrent use
	pool sync.Pool
	mu   sync.Mutex
}

// wasmInstance wraps a single instantiated WASM module.
type wasmInstance struct {
	mod       api.Module
	alloc     api.Function
	dealloc   api.Function
	transform api.Function
}

// NewWASMTransform compiles a WASM module from the given path.
// The funcName is the name of the exported transform function (default: "transform").
func NewWASMTransform(path, funcName string) (*WASMTransform, error) {
	if funcName == "" {
		funcName = "transform"
	}

	wasmBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("wasm read %s: %w", path, err)
	}

	ctx := context.Background()
	rt := wazero.NewRuntime(ctx)

	// Instantiate WASI for modules that need it (stdio, env, etc.)
	wasi_snapshot_preview1.MustInstantiate(ctx, rt)

	compiled, err := rt.CompileModule(ctx, wasmBytes)
	if err != nil {
		rt.Close(ctx)
		return nil, fmt.Errorf("wasm compile: %w", err)
	}

	// Validate that the module exports the required functions
	exports := compiled.ExportedFunctions()
	if _, ok := exports["alloc"]; !ok {
		rt.Close(ctx)
		return nil, fmt.Errorf("wasm module missing required export: alloc(size) -> ptr")
	}
	if _, ok := exports[funcName]; !ok {
		rt.Close(ctx)
		return nil, fmt.Errorf("wasm module missing required export: %s(ptr, len) -> u64", funcName)
	}

	wt := &WASMTransform{
		runtime:  rt,
		compiled: compiled,
		funcName: funcName,
	}

	return wt, nil
}

// getInstance returns a WASM module instance from the pool, or creates a new one.
func (wt *WASMTransform) getInstance(ctx context.Context) (*wasmInstance, error) {
	if v := wt.pool.Get(); v != nil {
		return v.(*wasmInstance), nil
	}

	// Create a new instance with a unique name.
	// Skip _start — plugins are libraries (Rust cdylib, TinyGo), not programs.
	// Go standard (GOOS=wasip1) is not supported: _start exits the module.
	wt.mu.Lock()
	cfg := wazero.NewModuleConfig().
		WithStdout(os.Stdout).
		WithStderr(os.Stderr).
		WithStartFunctions(). // Skip _start — plugins are libraries, not programs
		WithName("")
	wt.mu.Unlock()

	mod, err := wt.runtime.InstantiateModule(ctx, wt.compiled, cfg)
	if err != nil {
		return nil, fmt.Errorf("wasm instantiate: %w", err)
	}

	inst := &wasmInstance{
		mod:       mod,
		alloc:     mod.ExportedFunction("alloc"),
		dealloc:   mod.ExportedFunction("dealloc"),
		transform: mod.ExportedFunction(wt.funcName),
	}

	return inst, nil
}

// putInstance returns an instance to the pool.
func (wt *WASMTransform) putInstance(inst *wasmInstance) {
	wt.pool.Put(inst)
}

// Func returns a transform.Func that calls the WASM transform function.
func (wt *WASMTransform) Func() Func {
	return func(event map[string]any) (map[string]any, error) {
		ctx := context.Background()

		// Serialize event to JSON
		inputJSON, err := json.Marshal(event)
		if err != nil {
			return nil, fmt.Errorf("wasm marshal input: %w", err)
		}

		// Get a WASM instance
		inst, err := wt.getInstance(ctx)
		if err != nil {
			return nil, err
		}
		defer wt.putInstance(inst)

		// Allocate memory in WASM for the input
		results, err := inst.alloc.Call(ctx, uint64(len(inputJSON)))
		if err != nil {
			return nil, fmt.Errorf("wasm alloc: %w", err)
		}
		inputPtr := uint32(results[0])

		// Write input JSON into WASM memory
		mem := inst.mod.Memory()
		if !mem.Write(inputPtr, inputJSON) {
			return nil, fmt.Errorf("wasm memory write failed: out of bounds (ptr=%d, len=%d)", inputPtr, len(inputJSON))
		}

		// Call the transform function
		results, err = inst.transform.Call(ctx, uint64(inputPtr), uint64(len(inputJSON)))
		if err != nil {
			return nil, fmt.Errorf("wasm transform call: %w", err)
		}

		// Deallocate input memory
		if inst.dealloc != nil {
			_, _ = inst.dealloc.Call(ctx, uint64(inputPtr), uint64(len(inputJSON)))
		}

		packed := results[0]
		if packed == 0 {
			// Transform returned 0 → filter out the event
			return nil, nil
		}

		// Unpack result: high 32 bits = ptr, low 32 bits = len
		resultPtr := uint32(packed >> 32)
		resultLen := uint32(packed & 0xFFFFFFFF)

		// Read result JSON from WASM memory
		outputJSON, ok := mem.Read(resultPtr, resultLen)
		if !ok {
			return nil, fmt.Errorf("wasm memory read failed: out of bounds (ptr=%d, len=%d)", resultPtr, resultLen)
		}

		// Make a copy since the memory could be reused
		outputCopy := make([]byte, len(outputJSON))
		copy(outputCopy, outputJSON)

		// Deallocate result memory
		if inst.dealloc != nil {
			_, _ = inst.dealloc.Call(ctx, uint64(resultPtr), uint64(resultLen))
		}

		// Deserialize result
		var result map[string]any
		if err := json.Unmarshal(outputCopy, &result); err != nil {
			return nil, fmt.Errorf("wasm unmarshal output: %w (raw: %s)", err, string(outputCopy))
		}

		return result, nil
	}
}

// Close releases the WASM runtime and all resources.
func (wt *WASMTransform) Close() error {
	ctx := context.Background()
	return wt.runtime.Close(ctx)
}
