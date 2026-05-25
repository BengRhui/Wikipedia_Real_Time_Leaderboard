//! src/ingest/sse_async.rs
//!
//! This is the file to handle the receival of Wikipedia event logs into the system using Tokio.

use crate::config::WIKIPEDIA_URL;
use crate::fault_tolerance::failsafe::FailSafe;
use crate::fault_tolerance::watchdog_async::{Heartbeat, touch_heartbeat};
use crate::pipeline::queue_async::PriorityQueueAsync;
use crate::processor;
use crate::types::WikiEdit;
use bytes::Bytes;
use reqwest::Client;
use reqwest::header::{ACCEPT, CACHE_CONTROL, USER_AGENT};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio_stream::StreamExt;

/// The function to execute the overall data retrieval process
pub async fn run(
    queue: Arc<PriorityQueueAsync>,
    heartbeat: Heartbeat,
    stop_flag: Arc<AtomicBool>,
    fail_safe: &Arc<FailSafe>,
) -> Result<(), Box<dyn std::error::Error>> {

    // Create a client to handle connections, and add a timeout to make sure error can be returned in case connection fails
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(15))
        .build()?;

    // Let the client receive messages (response) from the API
    let response = client
        .get(WIKIPEDIA_URL)
        .header(ACCEPT, "text/event-stream")        // This is to receive message in SSE format
        .header(CACHE_CONTROL, "no-cache")          // This is to ensure that fresh data is received
        .header(USER_AGENT, "WikiRT")               // Required to avoid 403 errors
        .send()
        .await?                                               // ? means that the function will immediately return when errors occur
        .error_for_status()?;                                           // Converts network errors (4xx and 5xx) into Rust errors

    // Buffer is required: Data comes in chunks, so a buffer is unavoidable to ensure that the event is complete
    // In this case, we use Vec to store data as bytes to minimize string conversion
    let mut buffer = Vec::with_capacity(128 * 1024);

    // The response is converted into bytes to allow data to arrive incrementally
    let mut stream = response.bytes_stream();

    // When an incoming chunk arrives
    while let Some(chunk_result) = stream.next().await {

        // Check if the thread has to be stopped
        if stop_flag.load(Ordering::Relaxed) {
            break;
        }

        // Fault injection: Randomly introduce sleeping in ingestion to show network delay
        if rand::random_range(1..=5000) == 1 {
            println!("FAULT INJECTION: Simulating 15 seconds network delay...");
            tokio::time::sleep(Duration::from_secs(15)).await;
        }

        // Check the chunk and return error immediately if there is any
        let chunk = chunk_result?;

        // Update the watchdog with current time - data received
        touch_heartbeat(&heartbeat).await;

        // Only allocate additional space when capacity is insufficient
        if buffer.len() + chunk.len() > buffer.capacity() {
            buffer.reserve(buffer.len() + chunk.len());
        }

        // Add the chunk to the buffer
        buffer.extend_from_slice(&chunk);

        // When \n\n exists, this means that the event is complete: Check for \n\n
        while let Some(end_position) = buffer.windows(2).position(|w| w == b"\n\n") {

            // Extract the raw event
            let raw_event = &buffer[..end_position];

            // Quick peek to see if it's a valid edit and get its priority
            if let Some(is_bot) = processor::is_bot_edit(raw_event) {

                // Copy the raw bytes into a payload (reference-based) and create the WikiEdit object
                let payload = Bytes::copy_from_slice(raw_event);
                let queued_event = WikiEdit::new(is_bot, payload);

                // Send the event to the queue
                queue.send(queued_event, fail_safe).await;
            }

            // Once done, clear the buffer and move on to the next event
            buffer.drain(..end_position + 2);
        }
    }

    // Lastly return success if everything goes well
    Ok(())
}
