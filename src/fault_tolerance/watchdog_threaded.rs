//! src/fault_tolerance/watchdog_threaded.rs
//!
//! This file is to handle watchdog-related functions to perform network reset if there is no incoming packets

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// Create a Heartbeat type to record the time of the last seen packet
pub type Heartbeat = Arc<Mutex<Instant>>;

/// Watchdog struct has two data: last seen time and a transmitter to communicate with main thread
pub struct Watchdog {
    heartbeat: Heartbeat,
    reset_tx: SyncSender<()>,
}

impl Watchdog {

    /// Initialize the watchdog
    pub fn new(heartbeat: Heartbeat, reset_tx: SyncSender<()>) -> Self {
        Self {
            heartbeat,
            reset_tx,
        }
    }

    /// Function to trigger watchdog for constant monitoring
    pub fn run(&self, stop_flag: Arc<AtomicBool>) {
        loop {

            // Check if the thread has to be stopped
            if stop_flag.load(Ordering::Relaxed) {
                break;
            }

            // Check every 0.5 second (to prevent high CPU usage, okay for 10-second timeout)
            thread::sleep(Duration::from_millis(500));

            // Check for elapsed time
            let elapsed = self.heartbeat.lock().unwrap().elapsed();

            // If it is more than 10 seconds
            if elapsed >= Duration::from_secs(10) {

                // Send a trigger to the MPSC channel at the run function
                match self.reset_tx.try_send(()) {

                    // Send a log when trigger is sent
                    Ok(_) => {
                        println!("Watchdog Triggered: Network Reset in progress");
                    }

                    // Also do nothing if the channel is full, means that reset will take place
                    Err(std::sync::mpsc::TrySendError::Full(_)) => {}

                    // Prompt error when the channel cannot be accessed
                    Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                        println!("Failed to send reset signal");
                    }
                }
            }
        }
    }
}

/// Update heartbeat when packet arrives
pub fn touch_heartbeat(heartbeat: &Heartbeat) {
    *heartbeat.lock().unwrap() = Instant::now();
}
