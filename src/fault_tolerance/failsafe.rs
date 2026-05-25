//! src/fault_tolerance/failsafe.rs
//!
//! This file is to handle the structs and functions related to fail-safe implementation

use crate::config::{SystemMode, FAIL_SAFE_P99_OVERALL_THRESHOLD_MICRO, LATENCY_HIST_BUCKET_SIZE, PROCESSING_JITTER_LIMIT, REQUIRED_STABLE_COUNT};
use crate::fault_tolerance::latency_histogram::LatencyHistogram;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// The fail-safe struct handles the mode of the system, and records several key information
/// (e.g., number of counts for the system to return to normal)
pub struct FailSafe {
    mode: Arc<Mutex<SystemMode>>,               // Records the current system state
    stable_counter: Mutex<u32>,                 // Current count of stable packets
    required_stable_count: u32,                 // Required count of stable packets that must achieved before returning to normal
    histogram: Mutex<LatencyHistogram>,         // The latency histogram to calculate P99 that triggers fail-safe
}

impl FailSafe {

    /// Initialize the details the fail-safe monitoring
    pub fn new() -> Self {
        Self {
            mode: Arc::new(Mutex::new(SystemMode::Normal)),
            stable_counter: Mutex::new(0),
            required_stable_count: REQUIRED_STABLE_COUNT,
            histogram: Mutex::new(LatencyHistogram::new(
                PROCESSING_JITTER_LIMIT.as_micros() as usize,
                LATENCY_HIST_BUCKET_SIZE,
            )),
        }
    }

    /// A function to update the latency of packets into system
    pub fn record_latency(&self, latency: Duration) {

        // Try to get and update the latency histogram, then calculate P99
        let p99 = {
            let mut hist = self.histogram.lock().unwrap();
            hist.record(latency);
            hist.p99()
        };

        // Perform evaluation to determine if system should enter degrade mode
        self.update_status(latency, p99);
    }

    /// A function to drop packet when the time taken in queue exceeds the threshold
    pub fn report_dropped_packet(&self) {

        // First get the system mode and stable count and make them mutable
        let mut mode = self.mode.lock().unwrap();
        let mut stable = self.stable_counter.lock().unwrap();

        // Then change the system to degraded mode
        *mode = SystemMode::Degraded;
        *stable = 0;

        // Output the trigger
        println!("⚠ FAIL-SAFE TRIGGERED: QUEUE DELAY EXCEEDED. Packets will be dropped.");
    }

    pub fn report_queue_overflow(&self) {

        // First get the system mode and stable count and make them mutable
        let mut mode = self.mode.lock().unwrap();
        let mut stable = self.stable_counter.lock().unwrap();

        // Then change the system to degraded mode
        *mode = SystemMode::Degraded;
        *stable = 0;

        // Output the trigger
        println!("⚠ FAIL-SAFE TRIGGERED: QUEUE OVERFLOW. Oldest packets will be dropped.");
    }

    /// A function to evaluate if the system shall enter degrade mode
    fn update_status(&self, last_latency: Duration, p99: u64) {

        // First retrieve two information: system mode and stable count
        let mut mode = self.mode.lock().unwrap();
        let mut stable = self.stable_counter.lock().unwrap();

        // Perform checking on the latency
        // Case 1: Latency exceeds the 2ms deadline
        if last_latency > PROCESSING_JITTER_LIMIT {

            // Convert the system mode to degraded and reset stable to 0
            *mode = SystemMode::Degraded;
            *stable = 0;

            // Output and end the function
            println!("⚠ FAIL-SAFE TRIGGERED: HARD DEADLINE BREACH");
            return;
        }

        // Case 2: Latency exceeds the P99 tail (from packet perspective)
        if last_latency.as_micros() as u64 > p99 {

            // Also degrade the system and output the trigger
            *mode = SystemMode::Degraded;
            *stable = 0;
            println!("⚠ FAIL-SAFE TRIGGERED: PACKET P99 BREACH. Bot edits will be dropped.");
            return;
        }

        // Case 3 (Additional): If the current P99 exceeds the defined P99 in the system (from system perspective)
        if p99 > FAIL_SAFE_P99_OVERALL_THRESHOLD_MICRO {

            // Overall system performance is degrading, so degrade the system
            *mode = SystemMode::Degraded;
            *stable = 0;
            println!("⚠ FAIL-SAFE TRIGGERED: SYSTEM P99 BREACH. Bot edits will be dropped.");
            return;
        }

        // If no degradation is triggered, latency is normal, so stable can be incremented
        *stable += 1;

        // Check if the stable count has achieved the requirement
        if *mode == SystemMode::Degraded && *stable >= self.required_stable_count {

            // Reset the system to normal
            *mode = SystemMode::Normal;
            *stable = 0;

            // Output status
            println!("✅ FAIL-SAFE: DEGRADED → NORMAL. Bot edits will be resumed.");
        }
    }

    /// A function to check if the system is currently in degraded mode
    pub fn is_degraded(&self) -> bool {
        *self.mode.lock().unwrap() == SystemMode::Degraded
    }

    /// A function to reset system to normal mode
    pub fn reset_to_normal(&self) {
        *self.mode.lock().unwrap() = SystemMode::Normal
    }
}
