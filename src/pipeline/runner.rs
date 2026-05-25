//! src/pipeline/runner.rs
//!
//! This file consist of the main workflow of the async (Tokio) and the thread-based implementation

use crate::config::{RunningTime, Safety, CHANNEL_CAPACITY, HIGH_PRIORITY_QUOTA, QUEUEING_DELAY_LIMIT};
use crate::display::Dashboard;
use crate::fault_tolerance::failsafe::FailSafe;
use crate::fault_tolerance::{watchdog_async, watchdog_threaded};
use crate::ingest::{sse_async, sse_threaded};
use crate::leaderboard::create_leaderboard;
use crate::monitor::run_allocator_monitor;
use crate::pipeline::queue_async::PriorityQueueAsync;
use crate::pipeline::queue_threaded::PriorityQueueThread;
use futures::FutureExt;
use std::error::Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::sync::Notify;
use crate::processor;

/// The function to execute async-based pipeline
pub async fn run_async_pipeline(safety_handling: Safety, time_constraint: RunningTime) -> Result<(), Box<dyn Error>> {

    // Retrieve the time and the duration that the program will run
    let start = tokio::time::Instant::now();
    let execute_duration = Duration::from_secs(time_constraint.convert_to_secs());

    // Create a leaderboard and dashboard to display the results
    let leaderboard = create_leaderboard(&safety_handling);
    let dashboard = Dashboard::new(Arc::clone(&leaderboard));
    dashboard.render_now();

    // Create a priority queue to handle tasks
    let queue = PriorityQueueAsync::new(
        CHANNEL_CAPACITY,
        HIGH_PRIORITY_QUOTA,
        Arc::clone(&dashboard),
    );

    // Clone the queue to be used in different tasks
    let ingest_queue = Arc::clone(&queue);
    let process_queue = Arc::clone(&queue);

    // Create monitoring objects: fail-safe and heartbeat
    let failsafe = Arc::new(FailSafe::new());
    let system_state = Arc::clone(&failsafe);
    let heartbeat = Arc::new(tokio::sync::Mutex::new(tokio::time::Instant::now()));

    // Once getting the system state, set the system mode accordingly
    dashboard.set_system_mode(system_state.is_degraded());

    // No channel is required for watchdog here - in async watchdog uses notify()

    // Shutdown and restart flags to stop task when everything ends
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let ingest_stop = Arc::new(AtomicBool::new(false));
    let notify_ingest_restart = Arc::new(Notify::new());

    // Watchdog task
    let watchdog_task = {

        // Clone the data to be used in this scope
        let heartbeat = Arc::clone(&heartbeat);
        let notify = Arc::clone(&notify_ingest_restart);
        let shutdown = Arc::clone(&shutdown_flag);

        // Create the watchdog task
        tokio::spawn(async move {

            // When the task is running (when shutdown does not happen yet)
            while !shutdown.load(Ordering::Relaxed) {

                // Let the checking pause for 0.5 seconds (to avoid occupying CPU too much)
                tokio::time::sleep(Duration::from_millis(500)).await;

                // Calculate the time from last received data till now (heartbeat is updated in other tasks subsequently)
                let elapsed = heartbeat.lock().await.elapsed();

                // If the duration exceeds, notify "notify_ingest_restart" in ingestion
                if elapsed >= Duration::from_secs(10) {
                    println!("⚠ Watchdog: No data for 10s → triggering restart");
                    notify.notify_one();
                }
            }
        })
    };

    // Memory allocation monitoring
    let allocation_task = {

        // Copy the shutdown flag and create the Tokio task
        let shutdown = Arc::clone(&shutdown_flag);
        let dashboard = Arc::clone(&dashboard);

        // Create the allocation task
        tokio::spawn(async move {
            run_allocator_monitor(shutdown, dashboard);
        })
    };

    // Ingestion task
    let mut ingest_handle = {

        // Copy the data to be used within the scope
        let queue = Arc::clone(&ingest_queue);
        let heartbeat = Arc::clone(&heartbeat);
        let ingest_stop = Arc::clone(&ingest_stop);
        let system_state = Arc::clone(&system_state);

        // Create the ingestion task
        tokio::spawn(async move {
            let _ = sse_async::run(queue, heartbeat, ingest_stop, &system_state).await;
        })
    };

    // Processing task
    let processing_task = {

        // Clone the data to be used within the scope
        let queue = Arc::clone(&process_queue);
        let failsafe = Arc::clone(&failsafe);
        let shutdown = Arc::clone(&shutdown_flag);
        let dashboard = Arc::clone(&dashboard);

        // Create the processing task
        tokio::spawn(async move {
            loop {

                // When shutdown is triggered
                if shutdown.load(Ordering::Relaxed) {

                    // Clean up everything from the channel first
                    while let Some(event) = queue.recv().await {

                        // Check if the remaining packets are over deadline
                        let scheduling_drift = event.enqueue_time.unwrap().elapsed();
                        if scheduling_drift > QUEUEING_DELAY_LIMIT {

                            // If yes, drop the packet
                            failsafe.report_dropped_packet();

                            // Update the dashboard as well
                            dashboard.set_system_mode(failsafe.is_degraded());
                            dashboard.record_queue_delay_dropped_event(
                                if event.is_bot { "BOT" } else { "HUMAN" },
                                scheduling_drift.as_micros()
                            );
                            continue;
                        }

                        // If not, begin calculating the time the packet dequeue
                        let dequeue_time = tokio::time::Instant::now();

                        // Convert the event into local object with the associated metrics
                        if let Ok(Some((wiki_edit, metrics))) = processor::process_event(&event.payload) {

                            // Extract the key information to be updated into the dashboard
                            let domain = wiki_edit.meta.and_then(|m| m.domain).unwrap_or("unknown");
                            let edit_type = if event.is_bot { "BOT" } else { "HUMAN" };
                            dashboard.record_processed_event(
                                domain,
                                edit_type,
                                metrics.parse_alloc,
                                metrics.parse_bytes,
                                scheduling_drift.as_micros(),
                            );

                            // Record the latency of the event and update the dashboard in case if system is degraded
                            failsafe.record_latency(dequeue_time.elapsed());
                            dashboard.set_system_mode(failsafe.is_degraded());
                        }
                    }

                    // Done and exit
                    break;
                }

                // When an event is received
                if let Some(event) = queue.recv().await {

                    // Fault injection: Randomly introduce sleeping in processing
                    if rand::random_range(1..=500) == 1 {
                        println!("FAULT INJECTION: Simulating 0.5s processing delay...");
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }

                    // First check if the event is over the queueing delay limit
                    let scheduling_drift = event.enqueue_time.unwrap().elapsed();
                    if scheduling_drift > QUEUEING_DELAY_LIMIT {

                        // If the packet is over the limit, drop it
                        failsafe.report_dropped_packet();

                        // Update the dashboard as well
                        dashboard.set_system_mode(failsafe.is_degraded());
                        dashboard.record_queue_delay_dropped_event(
                            if event.is_bot { "BOT" } else { "HUMAN" },
                            scheduling_drift.as_micros()
                        );
                        continue;
                    }

                    // If the time limit is not exceeded, calculate the time when the packet is dequeued
                    let dequeue_time = tokio::time::Instant::now();

                    // Convert the data bytes into local wiki edit objects
                    if let Ok(Some((wiki_edit, metrics))) = processor::process_event(&event.payload) {

                        // Extract the key information to be updated into the dashboard
                        let domain = wiki_edit.meta.and_then(|m| m.domain).unwrap_or("unknown");
                        let edit_type = if event.is_bot { "BOT" } else { "HUMAN" };
                        dashboard.record_processed_event(
                            domain,
                            edit_type,
                            metrics.parse_alloc,
                            metrics.parse_bytes,
                            scheduling_drift.as_micros(),
                        );

                        // Lastly record the latency to determine if system should be degraded
                        failsafe.record_latency(dequeue_time.elapsed());
                        dashboard.set_system_mode(failsafe.is_degraded());
                    }
                }
            }
        })
    };

    // Create a shutdown timer to invoke shut down at last
    let shutdown_timer = tokio::time::sleep_until(start + execute_duration);
    tokio::pin!(shutdown_timer);

    loop {
        tokio::select! {

            // When network reset is triggered (from the watchdog task above)
            _ = notify_ingest_restart.notified() => {

                // End the ingestion task first
                println!("🔄 Restarting ingestion...");
                ingest_stop.store(true, Ordering::Relaxed);
                ingest_handle.abort();
                let _ = ingest_handle.await;

                // Reset heartbeat and system mode for restarting task
                watchdog_async::touch_heartbeat(&heartbeat).await;
                system_state.reset_to_normal();
                dashboard.set_system_mode(false);

                // Mark the stop flag as false and restart the task
                ingest_stop.store(false, Ordering::Relaxed);
                let queue = Arc::clone(&queue);
                let heartbeat = Arc::clone(&heartbeat);
                let ingest_stop = Arc::clone(&ingest_stop);
                let system_state = Arc::clone(&system_state);

                ingest_handle = tokio::spawn(async move {
                    loop {

                        // Shut down the task if flag is marked as true
                        if ingest_stop.load(Ordering::Relaxed) {
                            break;
                        }

                        // Run the task
                        let _ = sse_async::run(
                            Arc::clone(&queue),
                            Arc::clone(&heartbeat),
                            Arc::clone(&ingest_stop),
                            &system_state
                        ).await;

                        // Perform checking every 0.1 seconds to avoid overloading the CPU
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                });

                // Remove any additional notifications sent to notify_ingest_restart
                while notify_ingest_restart.notified().now_or_never().is_some() {}
            }

            // When the execution time ends
            _ = &mut shutdown_timer => {
                println!("Program has been executed for {} seconds. Shutting down the task-based model ...", time_constraint.convert_to_secs());
                shutdown_flag.store(true, Ordering::Relaxed);
                ingest_stop.store(true, Ordering::Relaxed);
                break;
            }
        }
    }

    // Wait for all tasks to complete
    let _ = ingest_handle.await;
    let _ = processing_task.await;
    let _ = allocation_task.await;
    let _ = watchdog_task.await;

    // All tasks ended and can exit pipeline
    println!("Async pipeline stopped cleanly. Thanks for using the system!");
    Ok(())
}


/// The function to execute thread-based pipeline
pub fn run_threaded_pipeline(safety_handling: Safety, time_constraint: RunningTime) {

    // First retrieve the time length that the program will run
    let start = std::time::Instant::now();
    let execute_duration = Duration::from_secs(time_constraint.convert_to_secs());

    // Create a leaderboard and dashboard to display the results
    let leaderboard = create_leaderboard(&safety_handling);
    let dashboard = Dashboard::new(Arc::clone(&leaderboard));
    dashboard.render_now();

    // Create a priority queue to handle threads
    let queue = PriorityQueueThread::new(
        CHANNEL_CAPACITY,
        HIGH_PRIORITY_QUOTA,
        Arc::clone(&dashboard),
    );

    // Clone the queue so that they can be accessed by different functions later
    let ingest_queue = Arc::clone(&queue);
    let process_queue = Arc::clone(&queue);

    // Create monitoring objects: fail-safe and heartbeat
    let failsafe = Arc::new(FailSafe::new());
    let system_status = Arc::clone(&failsafe);
    let heartbeat = Arc::new(std::sync::Mutex::new(std::time::Instant::now()));

    // Set the mode of the dashboard with the system status
    dashboard.set_system_mode(system_status.is_degraded());

    // Create a channel for watchdog
    let (reset_tx, reset_rx) = std::sync::mpsc::sync_channel(1);
    let watchdog = watchdog_threaded::Watchdog::new(Arc::clone(&heartbeat), reset_tx);

    // Create shutdown flags to stop threads when everything ends
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let shutdown_watchdog = Arc::clone(&shutdown_flag);
    let shutdown_allocator = Arc::clone(&shutdown_flag);
    let shutdown_processing = Arc::clone(&shutdown_flag);

    // The ingest stop has to be independent to not affect the other threads when performing network reset
    let mut ingest_stop = Arc::new(AtomicBool::new(false));

    // Run the watchdog function for network reset
    let monitoring_thread = thread::spawn(move || {
        watchdog.run(shutdown_watchdog);
    });

    // Running allocator to measure memory consumption
    let allocator_dashboard = Arc::clone(&dashboard);
    let allocator_thread = thread::spawn(move || {
        run_allocator_monitor(shutdown_allocator, allocator_dashboard);
    });

    // Ingestion thread
    let mut ingest_thread = {

        // Clone the priority queue and heartbeat for own usage
        let queue = Arc::clone(&ingest_queue);
        let hb = Arc::clone(&heartbeat);
        let stop = Arc::clone(&ingest_stop);
        let system_status = Arc::clone(&system_status);

        // Run the ingestion thread
        thread::spawn(move || {
            let _ = sse_threaded::run(queue, hb, stop, &system_status);
        })
    };

    // Processing thread
    let processing_dashboard = Arc::clone(&dashboard);
    let processing_thread = thread::spawn(move || {

        // Clone the dashboard to be updated later
        let dashboard = Arc::clone(&processing_dashboard);
        loop {

            // Shutdown when flag becomes true
            if shutdown_processing.load(Ordering::Relaxed) {

                // Drain the remaining events
                while let Some(event) = process_queue.recv() {

                    // Check for the queueing delay
                    let scheduling_drift = event.enqueue_time.unwrap().elapsed();
                    if scheduling_drift > QUEUEING_DELAY_LIMIT {

                        // If the packet exceeds the limit, drop the packet
                        failsafe.report_dropped_packet();

                        // Update the dashboard as well
                        dashboard.set_system_mode(failsafe.is_degraded());
                        dashboard.record_queue_delay_dropped_event(
                            if event.is_bot { "BOT" } else { "HUMAN" },
                            scheduling_drift.as_micros()
                        );
                        continue;
                    }

                    // If did not exceed queueing delay, start calculating the processing time
                    let dequeue_time = std::time::Instant::now();

                    // Convert the data bytes into local wiki events
                    if let Ok(Some((wiki_edit, metrics))) = processor::process_event(&event.payload) {

                        // Extract the key information to be updated into the dashboard
                        let domain = wiki_edit.meta.and_then(|m| m.domain).unwrap_or("unknown");
                        let edit_type = if event.is_bot { "BOT" } else { "HUMAN" };
                        dashboard.record_processed_event(
                            domain,
                            edit_type,
                            metrics.parse_alloc,
                            metrics.parse_bytes,
                            scheduling_drift.as_micros(),
                        );

                        // Record the latency and determine if the system shall be degraded
                        failsafe.record_latency(dequeue_time.elapsed());
                        dashboard.set_system_mode(failsafe.is_degraded());
                    }
                }

                // Once done, can end the loop and the main thread
                break;
            }

            // If the channel receives a packet
            if let Some(event) = process_queue.recv() {

                // Fault injection: Randomly introduce sleeping in processing
                if rand::random_range(1..=500) == 1 {
                    println!("FAULT INJECTION: Simulating 0.5s processing delay...");
                    thread::sleep(Duration::from_millis(500));
                }

                // Check for the queueing delay
                let scheduling_drift = event.enqueue_time.unwrap().elapsed();
                if scheduling_drift > QUEUEING_DELAY_LIMIT {

                    // If packet exceeds the limit, drop it
                    failsafe.report_dropped_packet();

                    // Update the dashboard as well
                    dashboard.set_system_mode(failsafe.is_degraded());
                    dashboard.record_queue_delay_dropped_event(
                        if event.is_bot { "BOT" } else { "HUMAN" },
                        scheduling_drift.as_micros()
                    );
                    continue;
                }

                // If the packet does not exceed the limit, calculate the processing time
                let dequeue_time = std::time::Instant::now();

                // Convert the data bytes into local wiki events
                if let Ok(Some((wiki_edit, metrics))) = processor::process_event(&event.payload) {

                    // Extract the key information to be updated into the dashboard
                    let domain = wiki_edit.meta.and_then(|m| m.domain).unwrap_or("unknown");
                    let edit_type = if event.is_bot { "BOT" } else { "HUMAN" };
                    dashboard.record_processed_event(
                        domain,
                        edit_type,
                        metrics.parse_alloc,
                        metrics.parse_bytes,
                        scheduling_drift.as_micros(),
                    );

                    // Lastly record the latency and determine if the system shall be degraded
                    failsafe.record_latency(dequeue_time.elapsed());
                    dashboard.set_system_mode(failsafe.is_degraded());
                }
            }
        }
    });

    // Main thread to check for watchdog + determine when to stop
    loop {

        // Check for time. If time's up, perform cleanup
        if start.elapsed() >= execute_duration {

            // Prompt that program ends
            println!("Program has been executed for {} seconds. Shutting down the thread-based model ...", time_constraint.convert_to_secs());

            // Convert the flags to true
            shutdown_flag.store(true, Ordering::Relaxed);
            ingest_stop.store(true, Ordering::Relaxed);

            // Let all the threads end
            let _ = ingest_thread.join();
            let _ = processing_thread.join();
            let _ = monitoring_thread.join();
            let _ = allocator_thread.join();

            // Once all threads end, can exit the pipeline
            break;
        }

        // Check for watchdog by looking at if the watchdog channel sent anything
        if reset_rx.try_recv().is_ok() {

            // End the ingest thread when watchdog is triggered
            ingest_stop.store(true, Ordering::Relaxed);

            // Reset the heartbeat and system mode
            watchdog_threaded::touch_heartbeat(&heartbeat);
            system_status.reset_to_normal();
            dashboard.set_system_mode(false);

            // Restart the thread again
            ingest_stop = Arc::new(AtomicBool::new(false));
            ingest_thread = {
                let queue = Arc::clone(&queue);
                let hb = Arc::clone(&heartbeat);
                let stop = Arc::clone(&ingest_stop);
                let system_status = Arc::clone(&system_status);

                thread::spawn(move || {
                    let _ = sse_threaded::run(queue, hb, stop, &system_status);
                })
            };
        }

        // Let the watchdog checking (in main) stop for a while to avoid it occupying CPU
        thread::sleep(Duration::from_millis(100));
    }

    // All tasks end, pipeline exits
    println!("Thread-based pipeline stopped cleanly. Thanks for using the system!");
}
