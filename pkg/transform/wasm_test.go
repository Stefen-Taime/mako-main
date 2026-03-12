package transform

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	v1 "github.com/Stefen-Taime/mako/api/v1"
)

// writeTestWASM writes a minimal valid WASM module implementing the Mako plugin ABI.
//
// The strategy is simple: instead of trying to manipulate JSON in WASM bytecode
// (which is complex), we implement a "copy-and-inject" transform:
//   - alloc: bump allocator starting at offset 256
//   - transform: copies input verbatim, then overwrites the last '}' with ,"_wasm":true}
//
// The suffix ,"_wasm":true} is stored in the data segment at offset 0 (14 bytes).
func writeTestWASM(t *testing.T, dir string) string {
	t.Helper()

	// Build the WASM binary section by section
	w := newWasmBuilder()

	// Type section: 3 function signatures
	w.addTypeSection([]funcType{
		{params: []byte{i32}, results: []byte{i32}},       // type 0: alloc
		{params: []byte{i32, i32}, results: nil},           // type 1: dealloc
		{params: []byte{i32, i32}, results: []byte{i64}},   // type 2: transform
	})

	// Function section: map functions to types
	w.addFuncSection([]uint32{0, 1, 2})

	// Memory section: 1 page minimum
	w.addMemorySection(1)

	// Global section: mutable i32, init=256 (bump pointer)
	w.addGlobalSection(i32, 256)

	// Export section
	w.addExportSection([]export{
		{name: "memory", kind: exportMemory, idx: 0},
		{name: "alloc", kind: exportFunc, idx: 0},
		{name: "dealloc", kind: exportFunc, idx: 1},
		{name: "transform", kind: exportFunc, idx: 2},
	})

	// Code section: function bodies
	w.addCodeSection([]funcBody{
		allocBody(),
		deallocBody(),
		transformBody(),
	})

	// Data section: suffix at offset 0
	w.addDataSection(0, []byte(`,"_wasm":true}`))

	wasmBytes := w.build()
	wasmPath := filepath.Join(dir, "test-plugin.wasm")
	if err := os.WriteFile(wasmPath, wasmBytes, 0644); err != nil {
		t.Fatalf("write wasm: %v", err)
	}
	return wasmPath
}

// ═══════════════════════════════════════════
// WASM binary builder
// ═══════════════════════════════════════════

const (
	i32 byte = 0x7F
	i64 byte = 0x7E

	exportFunc   byte = 0x00
	exportMemory byte = 0x02
)

type funcType struct {
	params  []byte
	results []byte
}

type export struct {
	name string
	kind byte
	idx  uint32
}

type funcBody struct {
	locals []localDecl
	code   []byte
}

type localDecl struct {
	count uint32
	typ   byte
}

type wasmBuilder struct {
	sections []section
}

type section struct {
	id   byte
	data []byte
}

func newWasmBuilder() *wasmBuilder {
	return &wasmBuilder{}
}

func (w *wasmBuilder) addTypeSection(types []funcType) {
	var buf []byte
	buf = appendU32(buf, uint32(len(types)))
	for _, ft := range types {
		buf = append(buf, 0x60) // functype
		buf = appendU32(buf, uint32(len(ft.params)))
		buf = append(buf, ft.params...)
		buf = appendU32(buf, uint32(len(ft.results)))
		buf = append(buf, ft.results...)
	}
	w.sections = append(w.sections, section{id: 1, data: buf})
}

func (w *wasmBuilder) addFuncSection(typeIdxs []uint32) {
	var buf []byte
	buf = appendU32(buf, uint32(len(typeIdxs)))
	for _, idx := range typeIdxs {
		buf = appendU32(buf, idx)
	}
	w.sections = append(w.sections, section{id: 3, data: buf})
}

func (w *wasmBuilder) addMemorySection(minPages uint32) {
	var buf []byte
	buf = appendU32(buf, 1)        // 1 memory
	buf = append(buf, 0x00)        // limits: no max
	buf = appendU32(buf, minPages)
	w.sections = append(w.sections, section{id: 5, data: buf})
}

func (w *wasmBuilder) addGlobalSection(typ byte, initVal uint32) {
	var buf []byte
	buf = appendU32(buf, 1)        // 1 global
	buf = append(buf, typ, 0x01)   // type, mutable
	buf = append(buf, 0x41)        // i32.const
	buf = appendU32(buf, initVal)
	buf = append(buf, 0x0B)        // end
	w.sections = append(w.sections, section{id: 6, data: buf})
}

func (w *wasmBuilder) addExportSection(exports []export) {
	var buf []byte
	buf = appendU32(buf, uint32(len(exports)))
	for _, e := range exports {
		buf = appendU32(buf, uint32(len(e.name)))
		buf = append(buf, e.name...)
		buf = append(buf, e.kind)
		buf = appendU32(buf, e.idx)
	}
	w.sections = append(w.sections, section{id: 7, data: buf})
}

func (w *wasmBuilder) addCodeSection(bodies []funcBody) {
	var buf []byte
	buf = appendU32(buf, uint32(len(bodies)))
	for _, body := range bodies {
		var funcBuf []byte
		// Locals
		funcBuf = appendU32(funcBuf, uint32(len(body.locals)))
		for _, l := range body.locals {
			funcBuf = appendU32(funcBuf, l.count)
			funcBuf = append(funcBuf, l.typ)
		}
		// Code + end
		funcBuf = append(funcBuf, body.code...)
		funcBuf = append(funcBuf, 0x0B) // end

		// Prepend body size
		buf = appendU32(buf, uint32(len(funcBuf)))
		buf = append(buf, funcBuf...)
	}
	w.sections = append(w.sections, section{id: 10, data: buf})
}

func (w *wasmBuilder) addDataSection(offset uint32, data []byte) {
	var buf []byte
	buf = appendU32(buf, 1)         // 1 segment
	buf = append(buf, 0x00)         // active, memory 0
	buf = append(buf, 0x41)         // i32.const
	buf = appendU32(buf, offset)
	buf = append(buf, 0x0B)         // end
	buf = appendU32(buf, uint32(len(data)))
	buf = append(buf, data...)
	w.sections = append(w.sections, section{id: 11, data: buf})
}

func (w *wasmBuilder) build() []byte {
	var out []byte
	// Magic + version
	out = append(out, 0x00, 0x61, 0x73, 0x6D)
	out = append(out, 0x01, 0x00, 0x00, 0x00)

	for _, s := range w.sections {
		out = append(out, s.id)
		out = appendU32(out, uint32(len(s.data)))
		out = append(out, s.data...)
	}
	return out
}

// appendU32 appends a ULEB128-encoded uint32.
func appendU32(b []byte, v uint32) []byte {
	for {
		byt := byte(v & 0x7F)
		v >>= 7
		if v != 0 {
			byt |= 0x80
		}
		b = append(b, byt)
		if v == 0 {
			break
		}
	}
	return b
}

// appendS32 appends a signed LEB128-encoded int32.
func appendS32(b []byte, v int32) []byte {
	for {
		byt := byte(v & 0x7F)
		v >>= 7
		// Sign bit of byte matches remaining value: we're done
		if (v == 0 && byt&0x40 == 0) || (v == -1 && byt&0x40 != 0) {
			b = append(b, byt)
			break
		}
		byt |= 0x80
		b = append(b, byt)
	}
	return b
}

// ── Function bodies ──

// alloc(size: i32) -> i32
// ptr = bump; bump += size; return ptr
func allocBody() funcBody {
	// param: $size = local 0
	// local: $ptr  = local 1
	var c []byte
	c = append(c, 0x23, 0x00)       // global.get 0
	c = append(c, 0x21, 0x01)       // local.set 1 ($ptr)
	c = append(c, 0x23, 0x00)       // global.get 0
	c = append(c, 0x20, 0x00)       // local.get 0 ($size)
	c = append(c, 0x6A)             // i32.add
	c = append(c, 0x24, 0x00)       // global.set 0
	c = append(c, 0x20, 0x01)       // local.get 1 ($ptr)
	return funcBody{
		locals: []localDecl{{count: 1, typ: i32}}, // $ptr
		code:   c,
	}
}

// dealloc(ptr: i32, size: i32) -- no-op
func deallocBody() funcBody {
	return funcBody{}
}

// transform(ptr: i32, len: i32) -> i64
//
// Algorithm (no branching that breaks WASM validation):
//  1. Allocate output buffer = input_len + 32
//  2. Use memory.copy to copy input to output
//  3. Scan backwards for '}' using a loop
//  4. If found, overwrite from that position with the suffix from data segment
//  5. Compute output length and return packed (ptr << 32 | len)
//
// To avoid the br-in-typed-block validation issue, we use local variables
// for the result and a single return at the end.
func transformBody() funcBody {
	// params: $ptr=0, $len=1
	// locals: $out_ptr=2, $i=3, $close_pos=4, $result_hi=5(i64), $result_lo=6(i64)
	locals := []localDecl{
		{count: 3, typ: i32}, // $out_ptr, $i, $close_pos
		{count: 2, typ: i64}, // $result_hi, $result_lo
	}

	var c []byte

	// $out_ptr = global.get $bump
	c = append(c, 0x23, 0x00) // global.get 0
	c = append(c, 0x21, 0x02) // local.set 2

	// $bump += $len + 32
	c = append(c, 0x23, 0x00) // global.get 0
	c = append(c, 0x20, 0x01) // local.get 1 ($len)
	c = append(c, 0x41, 0x20) // i32.const 32
	c = append(c, 0x6A)       // i32.add
	c = append(c, 0x6A)       // i32.add
	c = append(c, 0x24, 0x00) // global.set 0

	// memory.copy($out_ptr, $ptr, $len)
	c = append(c, 0x20, 0x02) // local.get 2 ($out_ptr)
	c = append(c, 0x20, 0x00) // local.get 0 ($ptr)
	c = append(c, 0x20, 0x01) // local.get 1 ($len)
	c = append(c, 0xFC, 0x0A, 0x00, 0x00) // memory.copy 0 0

	// $close_pos = $len (default: no '}' found, will use full len)
	c = append(c, 0x20, 0x01) // local.get 1 ($len)
	c = append(c, 0x21, 0x04) // local.set 4 ($close_pos)

	// $i = $len - 1
	c = append(c, 0x20, 0x01) // local.get 1
	c = append(c, 0x41, 0x01) // i32.const 1
	c = append(c, 0x6B)       // i32.sub
	c = append(c, 0x21, 0x03) // local.set 3 ($i)

	// Loop: scan backwards for '}'
	// block $break
	//   loop $continue
	//     if $i < 0: br $break
	//     if mem[$ptr + $i] == '}': $close_pos = $i; br $break
	//     $i--
	//     br $continue
	//   end
	// end
	c = append(c, 0x02, 0x40) // block (void)
	c = append(c, 0x03, 0x40) // loop (void)

	// if $i < 0, break
	c = append(c, 0x20, 0x03)       // local.get 3 ($i)
	c = append(c, 0x41, 0x00)       // i32.const 0
	c = append(c, 0x48)             // i32.lt_s
	c = append(c, 0x0D, 0x01)       // br_if 1 (break)

	// ch = mem[$ptr + $i]
	c = append(c, 0x20, 0x00)       // local.get 0 ($ptr)
	c = append(c, 0x20, 0x03)       // local.get 3 ($i)
	c = append(c, 0x6A)             // i32.add
	c = append(c, 0x2D, 0x00, 0x00) // i32.load8_u offset=0

	// if ch == 125 ('}')
	c = append(c, 0x41, 0xFD, 0x00) // i32.const 125 (signed LEB128)
	c = append(c, 0x46)             // i32.eq
	c = append(c, 0x04, 0x40)       // if (void)
	c = append(c, 0x20, 0x03)       // local.get 3 ($i)
	c = append(c, 0x21, 0x04)       // local.set 4 ($close_pos)
	c = append(c, 0x0C, 0x02)       // br 2 (exit block — if=0, loop=1, block=2)
	c = append(c, 0x0B)             // end if

	// $i--
	c = append(c, 0x20, 0x03)       // local.get 3
	c = append(c, 0x41, 0x01)       // i32.const 1
	c = append(c, 0x6B)             // i32.sub
	c = append(c, 0x21, 0x03)       // local.set 3

	c = append(c, 0x0C, 0x00)       // br 0 (continue loop)
	c = append(c, 0x0B)             // end loop
	c = append(c, 0x0B)             // end block

	// memory.copy($out_ptr + $close_pos, dataSegment@0, 14)
	// This overwrites from the '}' position with ,"_wasm":true}
	c = append(c, 0x20, 0x02)       // local.get 2 ($out_ptr)
	c = append(c, 0x20, 0x04)       // local.get 4 ($close_pos)
	c = append(c, 0x6A)             // i32.add
	c = append(c, 0x41, 0x00)       // i32.const 0 (source: data at offset 0)
	c = append(c, 0x41, 0x0E)       // i32.const 14 (suffix length)
	c = append(c, 0xFC, 0x0A, 0x00, 0x00) // memory.copy

	// result = ($close_pos + 14) as output length
	// $result_lo = i64.extend_i32_u($close_pos + 14)
	c = append(c, 0x20, 0x04)       // local.get 4 ($close_pos)
	c = append(c, 0x41, 0x0E)       // i32.const 14
	c = append(c, 0x6A)             // i32.add
	c = append(c, 0xAD)             // i64.extend_i32_u
	c = append(c, 0x21, 0x06)       // local.set 6 ($result_lo)

	// $result_hi = i64.extend_i32_u($out_ptr) << 32
	c = append(c, 0x20, 0x02)       // local.get 2 ($out_ptr)
	c = append(c, 0xAD)             // i64.extend_i32_u
	c = append(c, 0x42, 0x20)       // i64.const 32
	c = append(c, 0x86)             // i64.shl
	c = append(c, 0x21, 0x05)       // local.set 5 ($result_hi)

	// return $result_hi | $result_lo
	c = append(c, 0x20, 0x05)       // local.get 5
	c = append(c, 0x20, 0x06)       // local.get 6
	c = append(c, 0x84)             // i64.or

	return funcBody{locals: locals, code: c}
}

// ═══════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════

func TestWASMTransformPassthrough(t *testing.T) {
	dir := t.TempDir()
	wasmPath := writeTestWASM(t, dir)

	wt, err := NewWASMTransform(wasmPath, "transform")
	if err != nil {
		t.Fatalf("NewWASMTransform: %v", err)
	}
	defer wt.Close()

	fn := wt.Func()

	input := map[string]any{
		"email":  "alice@test.com",
		"amount": 99.99,
		"status": "completed",
	}

	result, err := fn(input)
	if err != nil {
		t.Fatalf("transform: %v", err)
	}
	if result == nil {
		t.Fatal("expected result, got nil (filtered out)")
	}

	// _wasm field should have been injected
	wasmFlag, ok := result["_wasm"]
	if !ok {
		// Debug: show what we got
		raw, _ := json.Marshal(result)
		t.Fatalf("expected _wasm field, got: %s", raw)
	}
	if wasmFlag != true {
		t.Errorf("_wasm: got %v, want true", wasmFlag)
	}

	// Original fields preserved
	if result["email"] != "alice@test.com" {
		t.Errorf("email: got %v, want alice@test.com", result["email"])
	}
	if result["status"] != "completed" {
		t.Errorf("status: got %v, want completed", result["status"])
	}
}

func TestWASMTransformMultipleEvents(t *testing.T) {
	dir := t.TempDir()
	wasmPath := writeTestWASM(t, dir)

	wt, err := NewWASMTransform(wasmPath, "transform")
	if err != nil {
		t.Fatalf("NewWASMTransform: %v", err)
	}
	defer wt.Close()

	fn := wt.Func()

	events := []map[string]any{
		{"id": 1, "name": "first"},
		{"id": 2, "name": "second"},
		{"id": 3, "name": "third"},
	}

	for i, event := range events {
		result, err := fn(event)
		if err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
		if result == nil {
			t.Fatalf("event %d: unexpected nil", i)
		}
		if result["_wasm"] != true {
			t.Errorf("event %d: _wasm not true", i)
		}
	}
}

func TestWASMTransformViaChain(t *testing.T) {
	dir := t.TempDir()
	wasmPath := writeTestWASM(t, dir)

	specs := []v1.Transform{
		{
			Name: "wasm_enrich",
			Type: v1.TransformPlugin,
			Config: map[string]any{
				"path":     wasmPath,
				"function": "transform",
			},
		},
	}

	chain, err := NewChain(specs)
	if err != nil {
		t.Fatalf("NewChain: %v", err)
	}

	input := map[string]any{"key": "value"}
	result, err := chain.Apply(input)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if result["_wasm"] != true {
		raw, _ := json.Marshal(result)
		t.Errorf("expected _wasm=true in chain result, got: %s", raw)
	}
}

func TestWASMTransformChainedWithBuiltin(t *testing.T) {
	dir := t.TempDir()
	wasmPath := writeTestWASM(t, dir)

	// Chain: hash_fields → wasm plugin
	specs := []v1.Transform{
		{
			Name:   "hash_email",
			Type:   v1.TransformHashFields,
			Fields: []string{"email"},
		},
		{
			Name: "wasm_enrich",
			Type: v1.TransformPlugin,
			Config: map[string]any{
				"path": wasmPath,
			},
		},
	}

	chain, err := NewChain(specs)
	if err != nil {
		t.Fatalf("NewChain: %v", err)
	}

	input := map[string]any{
		"email":  "alice@test.com",
		"amount": 50.0,
	}
	result, err := chain.Apply(input)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// Email should be hashed (not the original)
	if result["email"] == "alice@test.com" {
		t.Error("email should have been hashed")
	}
	// _pii_processed from hash transform
	if result["_pii_processed"] != true {
		t.Error("expected _pii_processed=true from hash transform")
	}
	// _wasm from WASM plugin
	if result["_wasm"] != true {
		t.Error("expected _wasm=true from WASM plugin")
	}
}

func TestWASMTransformMissingExport(t *testing.T) {
	dir := t.TempDir()
	wasmPath := writeTestWASM(t, dir)

	_, err := NewWASMTransform(wasmPath, "nonexistent")
	if err == nil {
		t.Fatal("expected error for missing export")
	}
}

func TestWASMTransformMissingFile(t *testing.T) {
	_, err := NewWASMTransform("/nonexistent/plugin.wasm", "transform")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

// To test with the Rust plugin:
//   cd examples/wasm-plugin-rust
//   cargo build --target wasm32-wasip1 --release
//   cp target/wasm32-wasip1/release/mako_plugin_rust.wasm plugin.wasm
//
// Then use: NewWASMTransform("examples/wasm-plugin-rust/plugin.wasm", "transform")

func TestWASMTransformMissingPath(t *testing.T) {
	specs := []v1.Transform{
		{
			Name:   "bad_plugin",
			Type:   v1.TransformPlugin,
			Config: map[string]any{},
		},
	}

	_, err := NewChain(specs)
	if err == nil {
		t.Fatal("expected error for missing config.path")
	}
}

