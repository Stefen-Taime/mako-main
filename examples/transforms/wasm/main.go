//go:build tinygo

// ⚠️  STATUS: STANDBY — Go standard (GOOS=wasip1) does not work for WASM plugins.
// The Go runtime requires _start to initialize, but _start exits the module.
// Use Rust (examples/wasm-plugin-rust/) or TinyGo when it supports Go 1.26+.
//
// Example WASM plugin for Mako.
//
// This plugin adds a "_enriched" flag and a "_processed_at" timestamp
// to every event passing through the pipeline.
//
// Build with TinyGo (when compatible):
//
//	tinygo build -o plugin.wasm -target=wasi -no-debug main.go
//
// Usage in pipeline.yaml:
//
//	transforms:
//	  - name: enrich
//	    type: plugin
//	    config:
//	      path: ./examples/wasm-plugin/plugin.wasm
//
// ─── Mako Plugin ABI ───
//
// Your WASM module must export these functions:
//
//	alloc(size uint32) -> ptr uint32
//	    Allocate `size` bytes for Mako to write input JSON.
//
//	dealloc(ptr uint32, size uint32)
//	    Free previously allocated memory (optional, can be no-op).
//
//	transform(ptr uint32, len uint32) -> uint64
//	    Read JSON event from (ptr, len), process it, write result JSON
//	    to a new buffer, and return (result_ptr << 32 | result_len).
//	    Return 0 to filter out (drop) the event.
package main

import (
	"encoding/json"
	"time"
	"unsafe"
)

// Global bump allocator
var allocOffset uint32 = 4096
var allocBuf [1 << 20]byte // 1MB arena

//export alloc
func alloc(size uint32) uint32 {
	ptr := allocOffset
	allocOffset += size
	return ptr
}

//export dealloc
func dealloc(ptr uint32, size uint32) {
	// No-op for bump allocator
}

//export transform
func transform(ptr uint32, length uint32) uint64 {
	// Read input JSON from WASM memory
	input := unsafe.Slice((*byte)(unsafe.Pointer(uintptr(ptr))), length)

	// Parse the event
	var event map[string]any
	if err := json.Unmarshal(input, &event); err != nil {
		return 0 // drop event on parse error
	}

	// ─── Your transform logic here ───
	event["_enriched"] = true
	event["_processed_at"] = time.Now().UTC().Format(time.RFC3339)
	// ─────────────────────────────────

	// Serialize the result
	output, err := json.Marshal(event)
	if err != nil {
		return 0
	}

	// Write output to memory
	outPtr := alloc(uint32(len(output)))
	outSlice := unsafe.Slice((*byte)(unsafe.Pointer(uintptr(outPtr))), len(output))
	copy(outSlice, output)

	// Return packed pointer: (ptr << 32) | len
	return (uint64(outPtr) << 32) | uint64(len(output))
}

func main() {} // required for WASM
