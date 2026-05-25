//! src/display.rs
//!
//! This file contains all dashboard features to calculate and render the rankings and statistics in real-time.

use crate::config::{SystemMode, DASHBOARD_LOG_CAPACITY, DASHBOARD_RENDER_INTERVAL_MS, LEADERBOARD_TOP_COUNT};
use crate::leaderboard::Leaderboard;
use chrono::Local;
use std::collections::VecDeque;
use std::io::{self, Write};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// A struct to store the details related to a single event / packet.
#[derive(Clone)]
struct EventLog {
    timestamp: String,
    process_type: &'static str,
    edit_type: &'static str,
    ingest_alloc: usize,
    ingest_bytes: usize,
    scheduling_drift_us: Option<u128>,
    domain: String,
}

/// A struct to store the state of the dashboard to be displayed
struct DashboardState {
    logs: VecDeque<EventLog>,
    last_render: Instant,
    overflow_count: u64,
    processed_count: u64,
    alloc_rate_bps: f64,
    dealloc_rate_bps: f64,
    alloc_ops_per_sec: f64,
    dealloc_ops_per_sec: f64,
    total_alloc_ops: usize,
    total_dealloc_ops: usize,
    total_alloc_bytes: usize,
    total_dealloc_bytes: usize,
    system_mode: SystemMode,
    human_min_drift_us: Option<u128>,
    human_max_drift_us: Option<u128>,
    human_total_drift_us: u128,
    human_drift_samples: u64,
    bot_min_drift_us: Option<u128>,
    bot_max_drift_us: Option<u128>,
    bot_total_drift_us: u128,
    bot_drift_samples: u64,
    total_parse_alloc_count: u64,
}

/// A struct to link the leaderboard and the dashboard stats together.
pub struct Dashboard {
    leaderboard: Arc<dyn Leaderboard>,
    state: Mutex<DashboardState>,
}

impl Dashboard {

    /// A function to create a new dashboard using leaderboard
    pub fn new(leaderboard: Arc<dyn Leaderboard>) -> Arc<Self> {
        Arc::new(Self {
            leaderboard,
            state: Mutex::new(DashboardState {
                logs: VecDeque::with_capacity(DASHBOARD_LOG_CAPACITY),
                last_render: Instant::now() - Duration::from_millis(DASHBOARD_RENDER_INTERVAL_MS),
                overflow_count: 0,
                processed_count: 0,
                alloc_rate_bps: 0.0,
                dealloc_rate_bps: 0.0,
                alloc_ops_per_sec: 0.0,
                dealloc_ops_per_sec: 0.0,
                total_alloc_ops: 0,
                total_dealloc_ops: 0,
                total_alloc_bytes: 0,
                total_dealloc_bytes: 0,
                system_mode: SystemMode::Normal,
                human_min_drift_us: None,
                human_max_drift_us: None,
                human_total_drift_us: 0,
                human_drift_samples: 0,
                bot_min_drift_us: None,
                bot_max_drift_us: None,
                bot_total_drift_us: 0,
                bot_drift_samples: 0,
                total_parse_alloc_count: 0,
            }),
        })
    }

    /// A function to immediately render the dashboard without caring about the time interval
    pub fn render_now(&self) {
        self.render(true);
    }

    /// A function to update the system mode to the dashboard
    pub fn set_system_mode(&self, degraded: bool) {

        // Extract the system mode
        let new_mode = if degraded { SystemMode::Degraded } else { SystemMode::Normal };

        // Check if the system mode has changed
        let changed = {

            // Retrieved the current state
            let mut state = self.state.lock().unwrap();

            // Do nothing if the state remains
            if state.system_mode == new_mode {
                false

            // Update the dashboard state if the state changes
            } else {
                state.system_mode = new_mode;
                true
            }
        };

        // If the state changes, render the dashboard immediately
        if changed {
            self.render_now();
        }
    }

    /// A function to update the statistics of the memory allocator
    pub fn update_allocator_stats(
        &self,
        alloc_rate_bps: f64,
        dealloc_rate_bps: f64,
        alloc_ops_per_sec: f64,
        dealloc_ops_per_sec: f64,
        total_alloc_ops: usize,
        total_dealloc_ops: usize,
        total_alloc_bytes: usize,
        total_dealloc_bytes: usize,
    ) {
        {
            // Lock the states to prevent concurrent updates (racing conditions)
            let mut state = self.state.lock().unwrap();

            // Update the dashboard states
            state.alloc_rate_bps = alloc_rate_bps;
            state.dealloc_rate_bps = dealloc_rate_bps;
            state.alloc_ops_per_sec = alloc_ops_per_sec;
            state.dealloc_ops_per_sec = dealloc_ops_per_sec;
            state.total_alloc_ops = total_alloc_ops;
            state.total_dealloc_ops = total_dealloc_ops;
            state.total_alloc_bytes = total_alloc_bytes;
            state.total_dealloc_bytes = total_dealloc_bytes;
        }

        // Display the stats in the next render
        self.render(false);
    }

    /// A function to record the statistics of the processed events
    pub fn record_processed_event(
        &self,
        domain: &str,
        edit_type: &'static str,
        ingest_alloc: usize,
        ingest_bytes: usize,
        scheduling_drift_us: u128,
    ) {
        {
            // Lock the states to prevent racing conditions
            let mut state = self.state.lock().unwrap();

            // Increment the processed event count by 1 (since one event is processed when the function is called)
            state.processed_count += 1;

            // Check for the event type: If it's bot
            if edit_type == "BOT" {

                // Minimum scheduling drift by comparing with the current drift
                state.bot_min_drift_us = Some(
                    state.bot_min_drift_us
                        .map_or(scheduling_drift_us, |prev| prev.min(scheduling_drift_us)),
                );

                // Maximum scheduling drift by comparing with the current drift
                state.bot_max_drift_us = Some(
                    state
                        .bot_max_drift_us
                        .map_or(scheduling_drift_us, |prev| prev.max(scheduling_drift_us)),
                );

                // Total scheduling drift is by adding previous drift and the current drift
                state.bot_total_drift_us = state.bot_total_drift_us.saturating_add(scheduling_drift_us);

                // Scheduling drift count is an increment of 1
                state.bot_drift_samples = state.bot_drift_samples.saturating_add(1);

            // If it's human-edit
            } else {

                // Minimum scheduling drift by comparing with the current drift
                state.human_min_drift_us = Some(
                    state.human_min_drift_us
                        .map_or(scheduling_drift_us, |prev| prev.min(scheduling_drift_us)),
                );

                // Maximum scheduling drift by comparing with the current drift
                state.human_max_drift_us = Some(
                    state.human_max_drift_us
                        .map_or(scheduling_drift_us, |prev| prev.max(scheduling_drift_us)),
                );

                // Total drift = Previous + Current
                state.human_total_drift_us = state.human_total_drift_us.saturating_add(scheduling_drift_us);

                // Drift count = Previous + 1
                state.human_drift_samples = state.human_drift_samples.saturating_add(1);
            }

            // Add the ingest allocation count with the current one
            state.total_parse_alloc_count = state.total_parse_alloc_count.saturating_add(ingest_alloc as u64);

            // Once done, print the log in the event log table
            push_log(
                &mut state.logs,
                EventLog {
                    timestamp: high_precision_timestamp(),
                    process_type: "PROCESS",
                    edit_type,
                    ingest_alloc,
                    ingest_bytes,
                    scheduling_drift_us: Some(scheduling_drift_us),
                    domain: domain.to_string(),
                },
            );
        }

        // Update the leaderboard and render the new values at the next iteration
        self.leaderboard.update(domain);
        self.render(false);
    }

    /// A function to record the statistics of the events dropped due to full queues
    pub fn record_overflow_event(
        &self,
        queue_name: &'static str,
        dropped_bytes: usize,
        incoming_bytes: usize,
    ) {
        {
            // Lock the states to prevent racing conditions
            let mut state = self.state.lock().unwrap();

            // Increment overflow count by 1
            state.overflow_count += 1;

            // Add the event to a log queue
            push_log(
                &mut state.logs,
                EventLog {
                    timestamp: high_precision_timestamp(),
                    process_type: "OVERFLOW",
                    edit_type: "-",
                    ingest_alloc: 0,
                    ingest_bytes: incoming_bytes,
                    scheduling_drift_us: None,
                    domain: format!(
                        "{queue_name} dropped_oldest={} incoming={}",
                        format_bytes(dropped_bytes),
                        format_bytes(incoming_bytes)
                    ),
                },
            );

            // Also print the log
            println!("QUEUE OVERFLOW: Oldest packet from {queue_name} is dropped.");
        }

        // Immediately update the dashboard to show the overflow event
        self.render(true);
    }

    /// A function to record the statistics of the events dropped due to exceeding the queueing delay threshold
    pub fn record_queue_delay_dropped_event(
        &self,
        queue_name: &'static str,
        scheduling_drift_us: u128,
    ) {
        {
            // Lock the states to prevent racing conditions
            let mut state = self.state.lock().unwrap();

            // Increment overflow count by 1
            state.overflow_count += 1;

            // Add the event to a log queue
            push_log(
                &mut state.logs,
                EventLog {
                    timestamp: high_precision_timestamp(),
                    process_type: "DEADLINE",
                    edit_type: "-",
                    ingest_alloc: 0,
                    ingest_bytes: 0,
                    scheduling_drift_us: Some(scheduling_drift_us),
                    domain: format!(
                        "{queue_name} scheduling_drift={} us",
                        scheduling_drift_us,
                    ),
                },
            );
        }

        // Immediately update the dashboard to show the overflow event
        self.render(true);
    }

    /// A function to show the statistics of the leaderboard and dashboard
    fn render(&self, force: bool) {

        // Retrieve the values to be rendered
        let (
            logs,
            overflow_count,
            processed_count,
            alloc_rate_bps,
            dealloc_rate_bps,
            alloc_ops_per_sec,
            dealloc_ops_per_sec,
            total_alloc_ops,
            total_dealloc_ops,
            total_alloc_bytes,
            total_dealloc_bytes,
            system_mode,
            human_min_drift_us,
            human_max_drift_us,
            human_total_drift_us,
            human_drift_samples,
            bot_min_drift_us,
            bot_max_drift_us,
            bot_total_drift_us,
            bot_drift_samples,
            total_parse_alloc_count,
        ) = {

            // Lock the states to prevent racing conditions
            let mut state = self.state.lock().unwrap();

            // If the render interval has not passed, do nothing
            if !force && state.last_render.elapsed() < Duration::from_millis(DASHBOARD_RENDER_INTERVAL_MS) {
                return;
            }

            // If the render interval is up, update the last render time as now
            state.last_render = Instant::now();

            (
                // Return these values to allow subsequent rendering
                state.logs.iter().cloned().collect::<Vec<_>>(),
                state.overflow_count,
                state.processed_count,
                state.alloc_rate_bps,
                state.dealloc_rate_bps,
                state.alloc_ops_per_sec,
                state.dealloc_ops_per_sec,
                state.total_alloc_ops,
                state.total_dealloc_ops,
                state.total_alloc_bytes,
                state.total_dealloc_bytes,
                state.system_mode,
                state.human_min_drift_us,
                state.human_max_drift_us,
                state.human_total_drift_us,
                state.human_drift_samples,
                state.bot_min_drift_us,
                state.bot_max_drift_us,
                state.bot_total_drift_us,
                state.bot_drift_samples,
                state.total_parse_alloc_count,
            )
        };

        // Retrieve the top domains from the leaderboard
        let top = self.leaderboard.top(LEADERBOARD_TOP_COUNT);

        // Begin printing
        print!("\x1B[2J\x1B[1;1H");
        println!("\n=== WikiRT Live Dashboard ===");

        // First display the processed and overflow events
        println!("Processed Events: {processed_count} | Overflow Events: {overflow_count}");
        println!();

        // Then display the top edited domains
        println!("Top {LEADERBOARD_TOP_COUNT} Edited Domains:");
        for rank in 0..LEADERBOARD_TOP_COUNT {
            if let Some((domain, count)) = top.get(rank) {
                println!("{}. {:<30} {}", rank + 1, domain, count);
            } else {
                println!("{}. {:<30} -", rank + 1, "-");
            }
        }
        println!();

        // Print the current system mode (normal or degraded)
        println!("System Mode: {system_mode}");
        println!();

        // Print the allocator statistics
        println!("Allocator Monitor:");
        println!(
            "alloc_rate = {:.2} B/s | dealloc_rate = {:.2} B/s | alloc_ops/s = {:.2} | dealloc_ops/s = {:.2}",
            alloc_rate_bps, dealloc_rate_bps, alloc_ops_per_sec, dealloc_ops_per_sec
        );
        println!(
            "total_alloc_ops = {} | total_dealloc_ops = {} | total_alloc_bytes = {} | total_dealloc_bytes = {}",
            total_alloc_ops, total_dealloc_ops, total_alloc_bytes, total_dealloc_bytes
        );

        // Calculate the statistics for scheduling drift and ingestion allocation
        let human_avg_drift_us =
            if human_drift_samples == 0 { 0.0 } else { human_total_drift_us as f64 / human_drift_samples as f64 };

        let bot_avg_drift_us =
            if bot_drift_samples == 0 { 0.0 } else { bot_total_drift_us as f64 / bot_drift_samples as f64 };

        let parse_alloc_rate_per_event =
            if processed_count == 0 { 0.0 } else { total_parse_alloc_count as f64 / processed_count as f64 };

        // Print the ingestion allocation stats
        println!(
            "total_parse_alloc_count={} | parse_alloc_rate_per_event={:.2} allocs/event",
            total_parse_alloc_count, parse_alloc_rate_per_event
        );

        // Print the scheduling drift stats
        println!(
            "human_sched_drift_us: min={} | avg={:.2} | max={}",
            human_min_drift_us
                .map(|v| v.to_string())
                .unwrap_or_else(|| "n/a".to_string()),
            human_avg_drift_us,
            human_max_drift_us
                .map(|v| v.to_string())
                .unwrap_or_else(|| "n/a".to_string())
        );
        println!(
            "bot_sched_drift_us:   min={} | avg={:.2} | max={}",
            bot_min_drift_us
                .map(|v| v.to_string())
                .unwrap_or_else(|| "n/a".to_string()),
            bot_avg_drift_us,
            bot_max_drift_us
                .map(|v| v.to_string())
                .unwrap_or_else(|| "n/a".to_string())
        );
        println!();

        // Print the event log table
        println!("Recent Event Logs:\n");
        println!(
            "{:<33}  {:<15}  {:<10}  {:>13}  {:>15}  {:>22}    {}",
            "timestamp_nanoseconds",
            "process_type",
            "edit_type",
            "parse_alloc",
            "parse_bytes",
            "scheduling_drift_us",
            "domain"
        );
        println!("{}", "-".repeat(142));

        for log in logs {
            println!(
                "{:<33}  {:<15}  {:<10}  {:>13}  {:>15}  {:>22}    {}",
                log.timestamp,
                log.process_type,
                log.edit_type,
                log.ingest_alloc,
                log.ingest_bytes,
                log.scheduling_drift_us
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                log.domain
            );
        }
        println!();

        // Force the terminal to show the output
        let _ = io::stdout().flush();
    }
}

/// A function to add the events into a queue for displaying
fn push_log(logs: &mut VecDeque<EventLog>, event: EventLog) {

    // If the queue is full, remove the oldest event
    if logs.len() == DASHBOARD_LOG_CAPACITY {
        logs.pop_front();
    }

    // Add the new event to the queue
    logs.push_back(event);
}

/// A function to get the current high-precision timestamp
fn high_precision_timestamp() -> String {

    // Get the current time up to microseconds
    let now = Local::now();
    now.format("%d %b %Y [%H:%M:%S%.6f]").to_string()
}


/// A function to format bytes into more common formats (e.g., KB, MB)
fn format_bytes(bytes: usize) -> String {

    // If the bytes are too small, leave it as bytes
    if bytes < 1024 {
        return format!("{}B", bytes);
    }

    // If bytes are less than 1024KB, leave it as KB
    let kib = bytes as f64 / 1024.0;
    if kib < 1024.0 {
        return format!("{kib:.1}KB");
    }

    // The remaining will be shown as MB (usually won't but included just in case)
    let mib = kib / 1024.0;
    format!("{mib:.2}MB")
}
