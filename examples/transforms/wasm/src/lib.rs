// Mako WASM Plugin — Rust Example
//
// Implements the Mako plugin ABI (alloc / dealloc / transform) in Rust.
// Adds "_enriched": true and "_plugin_lang": "rust" to every event.
//
// Build:
//   rustup target add wasm32-wasip1
//   cargo build --target wasm32-wasip1 --release
//   cp target/wasm32-wasip1/release/mako_plugin_rust.wasm plugin.wasm

use std::alloc::{alloc as rust_alloc, Layout};
use std::slice;

/// Allocate `size` bytes in WASM linear memory and return the pointer.
#[no_mangle]
pub extern "C" fn alloc(size: u32) -> u32 {
    let layout = Layout::from_size_align(size as usize, 1).expect("invalid layout");
    let ptr = unsafe { rust_alloc(layout) };
    ptr as u32
}

/// Free memory previously allocated by `alloc`. No-op — consistent with the
/// Go example; the WASM instance is short-lived and memory is reclaimed when
/// the module is dropped.
#[no_mangle]
pub extern "C" fn dealloc(_ptr: u32, _size: u32) {}

/// Read a JSON event from `(ptr, len)`, enrich it, and return the result as
/// a packed u64: `(result_ptr << 32) | result_len`. Returns 0 to drop the event.
#[no_mangle]
pub extern "C" fn transform(ptr: u32, len: u32) -> u64 {
    // Read input slice from WASM linear memory
    let input = unsafe { slice::from_raw_parts(ptr as *const u8, len as usize) };

    // Parse JSON
    let mut event: serde_json::Value = match serde_json::from_slice(input) {
        Ok(v) => v,
        Err(_) => return 0, // drop event on parse error
    };

    // Enrich the event
    if let Some(obj) = event.as_object_mut() {
        obj.insert("_enriched".to_string(), serde_json::Value::Bool(true));
        obj.insert(
            "_plugin_lang".to_string(),
            serde_json::Value::String("rust".to_string()),
        );
    }

    // Serialize result
    let output = match serde_json::to_vec(&event) {
        Ok(v) => v,
        Err(_) => return 0,
    };

    // Allocate output buffer and copy result
    let out_len = output.len() as u32;
    let out_ptr = alloc(out_len);
    unsafe {
        let dest = slice::from_raw_parts_mut(out_ptr as *mut u8, output.len());
        dest.copy_from_slice(&output);
    }

    // Return packed pointer: (ptr << 32) | len
    ((out_ptr as u64) << 32) | (out_len as u64)
}
