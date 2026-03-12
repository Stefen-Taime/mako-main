// Mako WASM Plugin — NYC TLC Anomaly Detector
//
// Detects anomalies in taxi trip data and enriches each event with:
//   - anomaly_score   (0-100, higher = more anomalous)
//   - anomaly_flags   (comma-separated list of triggered rules)
//   - is_anomaly      (true if score >= 50)
//
// Rules:
//   1. speed_impossible  — avg speed > 100 mph (distance / duration)
//   2. speed_suspicious  — avg speed > 60 mph
//   3. tip_excessive     — tip > 50% of fare
//   4. fare_zero_dist    — fare > $50 but distance = 0
//   5. duration_extreme  — trip > 6 hours
//   6. cost_per_mile_high — cost/mile > $50
//   7. negative_amounts  — any negative financial field
//   8. passenger_extreme — passenger_count > 6
//
// Build:
//   rustup target add wasm32-wasip1
//   cargo build --target wasm32-wasip1 --release
//   cp target/wasm32-wasip1/release/mako_anomaly_detector.wasm anomaly.wasm

use std::alloc::{alloc as rust_alloc, Layout};
use std::slice;

#[no_mangle]
pub extern "C" fn alloc(size: u32) -> u32 {
    let layout = Layout::from_size_align(size as usize, 1).expect("invalid layout");
    let ptr = unsafe { rust_alloc(layout) };
    ptr as u32
}

#[no_mangle]
pub extern "C" fn dealloc(_ptr: u32, _size: u32) {}

/// Helper: extract f64 from a serde_json::Value
fn get_f64(obj: &serde_json::Map<String, serde_json::Value>, key: &str) -> Option<f64> {
    obj.get(key).and_then(|v| v.as_f64())
}

#[no_mangle]
pub extern "C" fn transform(ptr: u32, len: u32) -> u64 {
    let input = unsafe { slice::from_raw_parts(ptr as *const u8, len as usize) };

    let mut event: serde_json::Value = match serde_json::from_slice(input) {
        Ok(v) => v,
        Err(_) => return 0,
    };

    if let Some(obj) = event.as_object_mut() {
        let mut score: u32 = 0;
        let mut flags: Vec<&str> = Vec::new();

        let trip_distance = get_f64(obj, "trip_distance").unwrap_or(0.0);
        let trip_duration = get_f64(obj, "trip_duration_minutes").unwrap_or(0.0);
        let fare_amount = get_f64(obj, "fare_amount").unwrap_or(0.0);
        let tip_amount = get_f64(obj, "tip_amount").unwrap_or(0.0);
        let total_amount = get_f64(obj, "total_amount").unwrap_or(0.0);
        let cost_per_mile = get_f64(obj, "cost_per_mile").unwrap_or(0.0);
        let passenger_count = get_f64(obj, "passenger_count").unwrap_or(1.0);

        // Rule 1 & 2: Speed checks (distance in miles, duration in minutes)
        if trip_duration > 0.0 {
            let speed_mph = (trip_distance / trip_duration) * 60.0;
            if speed_mph > 100.0 {
                score += 40;
                flags.push("speed_impossible");
            } else if speed_mph > 60.0 {
                score += 15;
                flags.push("speed_suspicious");
            }
        }

        // Rule 3: Excessive tip (> 50% of fare)
        if fare_amount > 0.0 && tip_amount > fare_amount * 0.5 {
            score += 20;
            flags.push("tip_excessive");
        }

        // Rule 4: High fare but zero distance
        if fare_amount > 50.0 && trip_distance < 0.01 {
            score += 30;
            flags.push("fare_zero_dist");
        }

        // Rule 5: Extremely long trip (> 360 minutes = 6 hours)
        if trip_duration > 360.0 {
            score += 25;
            flags.push("duration_extreme");
        }

        // Rule 6: Cost per mile > $50
        if cost_per_mile > 50.0 {
            score += 20;
            flags.push("cost_per_mile_high");
        }

        // Rule 7: Negative amounts
        if fare_amount < 0.0 || tip_amount < 0.0 || total_amount < 0.0 {
            score += 30;
            flags.push("negative_amounts");
        }

        // Rule 8: Too many passengers (> 6)
        if passenger_count > 6.0 {
            score += 15;
            flags.push("passenger_extreme");
        }

        // Cap score at 100
        if score > 100 {
            score = 100;
        }

        // Enrich the event
        obj.insert("anomaly_score".to_string(), serde_json::json!(score));
        obj.insert("anomaly_flags".to_string(), serde_json::json!(flags.join(",")));
        obj.insert("is_anomaly".to_string(), serde_json::json!(score >= 50));
    }

    // Serialize result
    let output = match serde_json::to_vec(&event) {
        Ok(v) => v,
        Err(_) => return 0,
    };

    let out_len = output.len() as u32;
    let out_ptr = alloc(out_len);
    unsafe {
        let dest = slice::from_raw_parts_mut(out_ptr as *mut u8, output.len());
        dest.copy_from_slice(&output);
    }

    ((out_ptr as u64) << 32) | (out_len as u64)
}
