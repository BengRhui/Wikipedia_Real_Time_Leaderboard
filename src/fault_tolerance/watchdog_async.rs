//! src/fault_tolerance/watchdog_async.rs
//!
//! The helper functions for the async-version of watchdogs

use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::Instant;

/// Heartbeat type for async pipeline
pub type Heartbeat = Arc<Mutex<Instant>>;

/// Update heartbeat when packet arrives (async)
pub async fn touch_heartbeat(heartbeat: &Heartbeat) {
    *heartbeat.lock().await = Instant::now();
}
