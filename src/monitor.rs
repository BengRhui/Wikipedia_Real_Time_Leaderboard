//! src/monitor.rs
//!
//! This file contains the functions to calculate memory allocation throughout system execution.

use crate::display::Dashboard;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

/// The struct to monitor RAM usage during program execution
pub struct CountingAllocator;

/// Static variables to record the allocation size and deallocation size
static ALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);
static DEALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);
static ALLOC_BYTES: AtomicUsize = AtomicUsize::new(0);
static DEALLOC_BYTES: AtomicUsize = AtomicUsize::new(0);

/// Implement own behavior to additionally measure byte counts and size allocated and deallocated during program execution
unsafe impl GlobalAlloc for CountingAllocator {

    /// Function for measuring memory allocation
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    /// Function for measuring memory deallocation
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        DEALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        DEALLOC_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
}

/// Helper functions to retrieve the allocation and deallocation counts
pub fn get_alloc_count() -> usize {
    ALLOC_COUNT.load(Ordering::Relaxed)
}
pub fn get_dealloc_count() -> usize {
    DEALLOC_COUNT.load(Ordering::Relaxed)
}
pub fn get_alloc_bytes() -> usize {
    ALLOC_BYTES.load(Ordering::Relaxed)
}
pub fn get_dealloc_bytes() -> usize {
    DEALLOC_BYTES.load(Ordering::Relaxed)
}

/// A function to perform global background monitoring for RAM allocation and deallocation
pub fn run_allocator_monitor(stop_flag: Arc<AtomicBool>, dashboard: Arc<Dashboard>) {
    loop {

        // Check if the thread has to be stopped
        if stop_flag.load(Ordering::Relaxed) {
            break;
        }

        // Get the memory details
        let last_alloc_count = get_alloc_count();
        let last_dealloc_count = get_dealloc_count();
        let last_alloc_size = get_alloc_bytes();
        let last_dealloc_size = get_dealloc_bytes();
        let last_time = Instant::now();

        // Wait for one second
        thread::sleep(Duration::from_secs(1));

        // Measure the updated memory details
        let current_alloc_count = get_alloc_count();
        let current_dealloc_count = get_dealloc_count();
        let current_alloc_size = get_alloc_bytes();
        let current_dealloc_size = get_dealloc_bytes();
        let current_time = Instant::now();

        // Measure the difference in count and bytes
        let delta_alloc_count = current_alloc_count.saturating_sub(last_alloc_count);
        let delta_dealloc_count = current_dealloc_count.saturating_sub(last_dealloc_count);
        let delta_alloc_size = current_alloc_size.saturating_sub(last_alloc_size);
        let delta_dealloc_size = current_dealloc_size.saturating_sub(last_dealloc_size);

        // Measure the time elapsed to calculate the rate
        let elapsed = current_time.duration_since(last_time).as_secs_f64();
        let alloc_rate = delta_alloc_size as f64 / elapsed;
        let dealloc_rate = delta_dealloc_size as f64 / elapsed;
        let alloc_ops_per_sec = delta_alloc_count as f64 / elapsed;
        let dealloc_ops_per_sec = delta_dealloc_count as f64 / elapsed;

        // Update the stats to the dashboard
        dashboard.update_allocator_stats(
            alloc_rate,
            dealloc_rate,
            alloc_ops_per_sec,
            dealloc_ops_per_sec,
            current_alloc_count,
            current_dealloc_count,
            current_alloc_size,
            current_dealloc_size,
        );
    }
}
