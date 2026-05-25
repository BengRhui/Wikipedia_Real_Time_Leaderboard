//! src/ingest/sse_threaded.rs
//!
//! This is the file to handle the receival of Wikipedia event logs into the system using threads.

use crate::config::WIKIPEDIA_URL;
use crate::fault_tolerance::failsafe::FailSafe;
use crate::fault_tolerance::watchdog_threaded::{Heartbeat, touch_heartbeat};
use crate::pipeline::queue_threaded::PriorityQueueThread;
use crate::types::WikiEdit;
use bytes::Bytes;
use reqwest::blocking::Client;
use reqwest::header::{ACCEPT, CACHE_CONTROL, USER_AGENT};
use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// The function to execute the overall data retrieval process
pub fn run(
    queue: Arc<PriorityQueueThread>,
    heartbeat: Heartbeat,
    stop_flag: Arc<AtomicBool>,
    fail_safe: &Arc<FailSafe>,
) -> Result<(), Box<dyn std::error::Error>> {

    // Create a client to handle connections + set timeout to stop the connection automatically if nothing is received
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(15))
        .build()?;

    // Let the client receive messages (response) from the API
    let mut response = client
        .get(WIKIPEDIA_URL)
        .header(ACCEPT, "text/event-stream")
        .header(CACHE_CONTROL, "no-cache")
        .header(USER_AGENT, "WikiRT")
        .send()?
        .error_for_status()?;

    // Create a buffer to store response from the API
    let mut buffer: Vec<u8> = Vec::with_capacity(128 * 1024);
    let mut chunk = [0u8; 8192];

    // Loop through the responses
    loop {

        // Check if the thread has to be stopped
        if stop_flag.load(Ordering::Relaxed) {
            break;
        }
        
        // Fault injection: Randomly introduce sleeping in ingestion to show network delay
        if rand::random_range(1..=5000) == 1 {
            println!("FAULT INJECTION: Simulating 15 seconds network delay...");
            std::thread::sleep(Duration::from_secs(15));
        }

        // Write response into chunk. If connection closes (size = 0), then loop breaks.
        let response_size = response.read(&mut chunk)?;
        if response_size == 0 {
            break;
        }

        // Update the watchdog with current time
        touch_heartbeat(&heartbeat);

        // Only allocate additional space when capacity is insufficient
        if buffer.len() + response_size > buffer.capacity() {
            buffer.reserve(buffer.len() + response_size);
        }

        // Add the chunk to the buffer
        buffer.extend_from_slice(&chunk[..response_size]);

        // Check for the end of the event with \n\n
        while let Some(end_position) = buffer.windows(2).position(|w| w == b"\n\n") {

            // Retrieve the raw event
            let raw_event = &buffer[..end_position];

            // Quick peek to see if it's a valid edit and get its priority
            if let Some(is_bot) = crate::processor::is_bot_edit(raw_event) {

                // Copy the raw bytes into a payload (reference-based) and create the WikiEdit object
                let payload = Bytes::copy_from_slice(raw_event);
                let queued_event = WikiEdit::new(is_bot, payload);

                // Send the event to the queue
                queue.send(queued_event, fail_safe);
            }

            // Clear the buffer once done
            buffer.drain(..end_position + 2);
        }
    }

    // If everything goes well, return success
    Ok(())
}
