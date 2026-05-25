//! src/fault_tolerance/latency_histogram.rs
//!
//! This file is to handle the latency histogram used to calculate P99 for fail-safe implementation

use crate::config::FAIL_SAFE_P99_OVERALL_THRESHOLD_MICRO;
use std::time::Duration;

/// The struct to store details of the latency histogram
pub(crate) struct LatencyHistogram {
    buckets: Vec<u64>,              // Define the number of buckets for the histogram. Example: slow, normal, fast = 3 buckets
    bucket_width: u64,              // Define the value range for each bucket in micro seconds, e.g., slow = 15-20 means the size is 5
    total: u64,                     // The total number of latencies recorded
    overflow: u64,                  // Latency that falls outside the final bucket
}

/// Functions related to the latency histogram
impl LatencyHistogram {

    /// A function to initialize itself
    pub(crate) fn new(total_range: usize, bucket_width: usize) -> Self {
        Self {
            buckets: vec![0; total_range / bucket_width],
            bucket_width: bucket_width as u64,
            total: 0,
            overflow: 0,
        }
    }

    /// A function to record the latency into the histogram
    pub(crate) fn record(&mut self, latency_in_micro: Duration) {

        // Calculate the position of the bin that the latency will fall under
        // Example: If latency = 20 us, and the bucket size is 10, it will fall under 20/10 = 2nd bucket
        let bucket_position = (latency_in_micro.as_micros() as u64 / self.bucket_width) as usize;

        // If the latency does not belong to any bin, it goes to the overflow
        if bucket_position >= self.buckets.len() {
            self.overflow += 1;

        // If it's a normal value, it goes into the corresponding bin
        } else {
            self.buckets[bucket_position] += 1;
        }

        // Update the total count
        self.total += 1;
    }

    /// A function to calculate the P99 of the data (in microseconds)
    pub(crate) fn p99(&self) -> u64 {

        // Have to handle cold-start, if less than 50 records then use a predefined threshold
        if self.total < 50 {
            return FAIL_SAFE_P99_OVERALL_THRESHOLD_MICRO;
        }

        // Calculation: First find the position of P99
        let target = ((self.total as f64) * 0.99).ceil() as u64;

        // Then loop through each bucket from left to right to find the bucket
        let mut cumulative = 0;
        for (bucket_index, bucket_count) in self.buckets.iter().enumerate() {
            cumulative += *bucket_count;

            // Return the lower limit of the bucket
            if cumulative >= target {
                return (bucket_index as u64) * self.bucket_width;
            }
        }

        // If P99 is in the overflow, then return the max of the histogram
        cumulative += self.overflow;
        if cumulative >= target {
            return (self.buckets.len() as u64) * self.bucket_width;
        }

        // In case if calculation error occurs, use back the initial threshold
        FAIL_SAFE_P99_OVERALL_THRESHOLD_MICRO
    }

    /// A function to reset the histogram after system converts back to normal - to eliminate old data from impacting new data
    pub(crate) fn _reset(&mut self) {

        // Reset the bucket counts to 0
        for bucket in &mut self.buckets {
            *bucket = 0;
        }

        // Reset the counters too
        self.total = 0;
        self.overflow = 0;
    }
}
