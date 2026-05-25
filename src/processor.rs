//! src/processor.rs
//!
//! This file is used to store the common processes that share between Tokio and thread implementation

use crate::monitor::{get_alloc_bytes, get_alloc_count};
use crate::types::WikiEvent;

/// A struct to store the number of ingestion allocations and bytes
#[derive(Debug, Clone, Copy)]
pub struct ParseMetrics {
    pub parse_alloc: usize,
    pub parse_bytes: usize,
}

/// The function to process Wikipedia API from bytes to WikiModification struct
pub fn process_event<'a>(raw_event: &'a [u8]) -> Result<Option<(WikiEvent<'a>, ParseMetrics)>, serde_json::Error> {
    
    // Separate bytes into lines
    let lines = raw_event.split(|&b| b == b'\n');

    // Loop through each line and find for message with "data" key
    for line in lines {
        
        // Begin memory measurement
        let alloc_before = get_alloc_count();
        let bytes_before = get_alloc_bytes();

        // Retrieve the data that starts with "data:"
        let data = match line.strip_prefix(b"data:") {
            Some(data) => data,
            None => continue,
        };

        // Trim the data to remove any unnecessary blank spaces
        let data = data.trim_ascii();
        if data.is_empty() {
            continue;
        }

        // Use serde_json to extract the fields from the string
        let event = serde_json::from_slice::<WikiEvent<'a>>(data)?;

        // End memory measurement and output
        let alloc_after = get_alloc_count();
        let bytes_after = get_alloc_bytes();

        // Calculate the difference in memory usage
        let metrics = ParseMetrics {
            parse_alloc: alloc_after.saturating_sub(alloc_before),
            parse_bytes: bytes_after.saturating_sub(bytes_before),
        };

        // Return the event if the change type is edit
        return if event.change_type.as_deref() == Some("edit") {
            Ok(Some((event, metrics)))

        // If not, discard it and return nothing
        } else {
            Ok(None)
        };
    }

    // If nothing is found, data does not exist, hence return nothing.
    Ok(None)
}

/// A function to check if an event is a bot by using raw data
pub fn is_bot_edit(raw_event: &[u8]) -> Option<bool> {
    
    // First separate the bytes into lines
    let lines = raw_event.split(|&b| b == b'\n');
    
    // Loop through each line
    for line in lines {
        
        // Find the line that starts with "data:"
        if let Some(data) = line.strip_prefix(b"data:") {
            
            // Continue to loop if the data is empty
            let data = data.trim_ascii();
            if data.is_empty() {
                continue;
            }

            // Use serde_json to parse the values to check if the change type is edit
            if let Ok(event) = serde_json::from_slice::<WikiEvent>(data) && event.change_type.as_deref() == Some("edit") {

                // Return the bot flag
                return Some(event.bot.unwrap_or(false));
            }
        }
    }
    
    // If nothing is found, return None
    None
}
