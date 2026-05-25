//! src/pipeline/queue_async.rs
//!
//! This file contains the structs and functions to handle the priority queue in async models.

use crate::display::Dashboard;
use crate::fault_tolerance::failsafe::FailSafe;
use crate::types::WikiEdit;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::error::TrySendError;

/// The struct of async's priority queue. Two channels will be implemented: high-priority for human, low-priority for bot
pub struct PriorityQueueAsync {
    high_tx: Sender<WikiEdit>,
    high_rx: Arc<Mutex<Receiver<WikiEdit>>>,

    low_tx: Sender<WikiEdit>,
    low_rx: Arc<Mutex<Receiver<WikiEdit>>>,

    // Quota is used to let bot-edits run to prevent starvation
    high_quota: Arc<Mutex<usize>>,
    max_quota: usize,

    // And also linked to a dashboard to record any events taking place
    dashboard: Arc<Dashboard>,
}

impl PriorityQueueAsync {

    /// A function to create a new priority queue for async
    pub fn new(capacity: usize, quota: usize, dashboard: Arc<Dashboard>) -> Arc<Self> {

        // High priority channel created for human-edits
        let (high_tx, high_rx) = tokio::sync::mpsc::channel::<WikiEdit>(capacity);

        // Low priority channel created for bot-edits
        let (low_tx, low_rx) = tokio::sync::mpsc::channel::<WikiEdit>(capacity);

        // Initialize the queue
        Arc::new(Self {
            high_tx,
            low_tx,
            high_rx: Arc::new(Mutex::new(high_rx)),
            low_rx: Arc::new(Mutex::new(low_rx)),
            high_quota: Arc::new(Mutex::new(quota)),
            max_quota: quota,
            dashboard,
        })
    }

    /// A function to add events into channel
    pub async fn send(&self, event: WikiEdit, system_state: &Arc<FailSafe>) {

        // First check if the edit is bot and if the system is in degrade mode
        let is_bot = event.is_bot;
        let degraded = system_state.is_degraded();

        // If system is in degraded mode, we drop the bot edits
        if degraded && is_bot {
            return;
        }

        // If not in degraded mode, we add bot-edits to low priority queue, human-edits to high priority queue
        if is_bot {
            self.send_with_drop(
                &self.low_tx,
                &self.low_rx,
                event,
                "BOT QUEUE (LOW PRIORITY)",
                system_state,
            )
            .await;
        } else {
            self.send_with_drop(
                &self.high_tx,
                &self.high_rx,
                event,
                "HUMAN QUEUE (HIGH PRIORITY)",
                system_state,
            )
            .await;
        }
    }

    /// A function to drop the oldest packet when the queue is full
    async fn send_with_drop(
        &self,
        tx: &Sender<WikiEdit>,
        rx: &Arc<Mutex<Receiver<WikiEdit>>>,
        mut event: WikiEdit,
        queue_name: &'static str,
        system_state: &Arc<FailSafe>,
    ) {

        // Update the enqueue time here. If dropped, the event is gone, so it's okay to declare before send.
        event.update_enqueue_time();
        
        // We attempt to add the event to the channel
        match tx.try_send(event) {

            // Do nothing if sent successfully
            Ok(_) => {}

            // If there is an error - channel is full
            Err(TrySendError::Full(event)) => {
                
                // Trigger degrade mode because we dropped due to full queue
                system_state.report_queue_overflow();

                // We first drop the oldest packet and retrieve its size
                let dropped_bytes = rx.lock().await.recv().await
                    .map(|ev| ev.memory_bytes_estimate())
                    .unwrap_or(0);

                // As well as the size of the new packet and resend to channel
                let incoming_bytes = event.memory_bytes_estimate();
                let _ = tx.try_send(event);

                // Lastly log the case
                self.dashboard.record_overflow_event(queue_name, dropped_bytes, incoming_bytes);
            }

            // When cannot connect to channel, show error
            Err(TrySendError::Closed(_)) => {
                println!("Error: Cannot connect to channel");
            }
        }
    }

    /// A function to retrieve events from channels
    pub async fn recv(&self) -> Option<WikiEdit> {

        // First, lock the high_quota, we will edit it later
        let mut quota = self.high_quota.lock().await;

        // Check if there is quota, if yes, extract human-edits
        if *quota > 0 {
            if let Some(event) = self.try_recv_high().await {
                *quota -= 1;
                return Some(event);
            }
        }

        // If there is no quota and bot-edits exist, extract bot-edits and reset quota
        if let Some(event) = self.try_recv_low().await {
            *quota = self.max_quota;
            return Some(event);
        }

        // If there is no bot-edits, extract human-edits and reset the quota
        if let Some(event) = self.try_recv_high().await {
            *quota = self.max_quota;
            return Some(event);
        }

        // If there is nothing to extract, return none
        None
    }

    /// A helper function to retrieve events from high priority queue
    async fn try_recv_high(&self) -> Option<WikiEdit> {
        let mut rx = self.high_rx.lock().await;
        rx.try_recv().ok()
    }

    /// A helper function to retrieve events from low priority queue
    async fn try_recv_low(&self) -> Option<WikiEdit> {
        let mut rx = self.low_rx.lock().await;
        rx.try_recv().ok()
    }
}
