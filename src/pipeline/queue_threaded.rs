//! src/pipeline/queue_threaded.rs
//!
//! This file contains the details of the priority queue implemented in the thread-based model.

use crate::display::Dashboard;
use crate::fault_tolerance::failsafe::FailSafe;
use crate::types::WikiEdit;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::SyncSender;
use std::sync::mpsc::TrySendError;

/// The struct for thread-based priority queue. The queue contains two channels, one for human and one for bot.
pub struct PriorityQueueThread {
    
    // Human-edit channel
    high_tx: SyncSender<WikiEdit>,
    high_rx: Arc<Mutex<Receiver<WikiEdit>>>,

    // Bot-edit channel
    low_tx: SyncSender<WikiEdit>,
    low_rx: Arc<Mutex<Receiver<WikiEdit>>>,

    // To prevent starvation, a quota is fixed so that bot-edits can be released as well
    high_quota: Mutex<usize>,
    max_quota: usize,
    
    // A dashboard is also linked to record any events taking place
    dashboard: Arc<Dashboard>,
}

impl PriorityQueueThread {
    
    /// The function to create a new priority queue
    pub fn new(capacity: usize, quota: usize, dashboard: Arc<Dashboard>) -> Arc<Self> {
        
        // Create a MPSC channel to store human-edits
        let (high_tx, high_rx) = std::sync::mpsc::sync_channel::<WikiEdit>(capacity);

        // Create a MPSC channel to store bot-edits too
        let (low_tx, low_rx) = std::sync::mpsc::sync_channel::<WikiEdit>(capacity);

        // Initialize the priority queue
        Arc::new(Self {
            high_tx,
            high_rx: Arc::new(Mutex::new(high_rx)),
            low_tx,
            low_rx: Arc::new(Mutex::new(low_rx)),
            high_quota: Mutex::new(quota),
            max_quota: quota,
            dashboard,
        })
    }

    /// A function to send the events to the priority queue
    pub fn send(&self, event: WikiEdit, system_state: &Arc<FailSafe>) {
        
        // First check if it is bot-edits
        let is_bot = event.is_bot;

        // Also check if the system is currently under degraded mode
        let degraded = system_state.is_degraded();

        // If system is in degraded mode, we drop the bot edits
        if degraded && is_bot {
            return;
        }

        // If not in degraded mode, we add bot-edits to low priority queue, human-edits to high priority queue
        if is_bot {
            self.send_with_drop(&self.low_tx, &self.low_rx, event, "BOT QUEUE (LOW PRIORITY)", system_state);
        } else {
            self.send_with_drop(&self.high_tx, &self.high_rx, event, "HUMAN QUEUE (HIGH PRIORITY)", system_state);
        }
    }

    /// A function to drop the oldest packet when the queue is full
    fn send_with_drop(
        &self,
        tx: &SyncSender<WikiEdit>,
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

                // We retrieve the oldest packet and calculate its size
                let dropped_bytes = rx.lock().unwrap().try_recv().ok()
                    .map(|ev| ev.memory_bytes_estimate())
                    .unwrap_or(0);

                // We then get the size of the new packet and resend to channel
                let incoming_bytes = event.memory_bytes_estimate();
                let _ = tx.try_send(event);

                // And lastly log the case
                self.dashboard.record_overflow_event(queue_name, dropped_bytes, incoming_bytes);
            }

            // When cannot connect to channel, show error
            Err(TrySendError::Disconnected(_)) => {
                println!("Error: Cannot connect to channel");
            }
        }
    }

    /// A function to retrieve event / packet from channel
    pub fn recv(&self) -> Option<WikiEdit> {
        
        // We first lock the high_quota coz we'll update it
        let mut quota = self.high_quota.lock().unwrap();

        // Check for the quota. If still got quota we extract human-edit first
        if *quota > 0 && let Ok(event) = self.high_rx.lock().unwrap().try_recv() {
            *quota -= 1;
            return Some(event);
        }

        // If no more quota, we extract the bot-edits one time, then reset the quota
        if let Ok(event) = self.low_rx.lock().unwrap().try_recv() {
            *quota = self.max_quota;
            return Some(event);
        }

        // If no bot-edits and no more quota, then we continue extract human-edits and reset quota
        if let Ok(event) = self.high_rx.lock().unwrap().try_recv() {
            *quota = self.max_quota;
            return Some(event);
        }

        // If really have nothing to extract, then return none
        None
    }
}
