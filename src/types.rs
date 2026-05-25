//! src/types.rs
//!
//! This file is to define the structs related to the Wikipedia events.
//! A sample format of JSON from the API looks like this:
//!
//  data: {
//   "$schema": "/mediawiki/recentchange/1.0.0",
//   "meta": {
//     "uri": "https://www.wikidata.org/wiki/Q40596369",
//     "request_id": "b8ce4fe0-e739-4e13-90cf-a9725c26fc34",
//     "id": "41b72afe-5612-418a-9026-a163001f6c9d",
//     "domain": "www.wikidata.org",
//     "stream": "mediawiki.recentchange",
//     "dt": "2026-04-24T07:58:46.187Z",
//     "topic": "eqiad.mediawiki.recentchange",
//     "partition": 0,
//     "offset": 6057703263
//   },
//   "id": 2561527017,
//   "type": "edit",
//   "namespace": 0,
//   "title": "Q40596369",
//   "title_url": "https://www.wikidata.org/wiki/Q40596369",
//   "comment": "/* wbeditentity-statements-multiple-properties-update:0||2 */ Author Disambiguator set author [[Q46840757]] ([[:toollabs:editgroups/b/AD/2a9b6ed4|details]])",
//   "timestamp": 1777017525,
//   "user": "GerardM",
//   "bot": false,
//   "notify_url": "https://www.wikidata.org/w/index.php?diff=2484324046&oldid=2411984637&rcid=2561527017",
//   "minor": false,
//   "patrolled": true,
//   "length": {
//     "old": 109745,
//     "new": 110060
//   },
//   "revision": {
//     "old": 2411984637,
//     "new": 2484324046
//   },
//   "server_url": "https://www.wikidata.org",
//   "server_name": "www.wikidata.org",
//   "server_script_path": "/w",
//   "wiki": "wikidatawiki",
//   "parsedcomment": "‎<span dir=\"auto\"><span class=\"autocomment\">Changed statements for 2 properties: </span></span> Author Disambiguator set author <a href=\"/wiki/Q46840757\" title=\"‎Leandro Silveira‎ | ‎researcher‎\"><span class=\"wb-itemlink\"><span class=\"wb-itemlink-label\" lang=\"en\" dir=\"ltr\">Leandro Silveira</span> <span class=\"wb-itemlink-id\">(Q46840757)</span></span></a> (<a href=\"https://iw.toolforge.org/editgroups/b/AD/2a9b6ed4\" class=\"extiw\" title=\"toollabs:editgroups/b/AD/2a9b6ed4\">details</a>)"
// }
//! Only a few attributes will be used: bot, domain, type

use bytes::Bytes;
use serde::Deserialize;
use std::time::Instant;

/// The struct to represent the events received from the Wikipedia API
#[derive(Debug, Deserialize)]
pub struct WikiEvent<'a> {
    
    #[serde(default)]
    pub bot: Option<bool>,

    #[serde(default, borrow)]                       // #[serde(borrow)] means that we try to avoid copying but borrowing the string from the JSON
    pub meta: Option<Meta<'a>>,

    #[serde(default, borrow, rename = "type")]      // Have to use rename coz type is a reserved keyword in Rust
    pub change_type: Option<&'a str>,
}

/// The Meta struct to retrieve domain ("domain" is nested inside "meta" in JSON)
#[derive(Debug, Deserialize)]
pub struct Meta<'a> {
    #[serde(default, borrow)]
    pub domain: Option<&'a str>,
}

/// The struct used to convert events into local objects
#[derive(Debug)]
pub struct WikiEdit {
    pub is_bot: bool,
    pub payload: Bytes,
    pub enqueue_time: Option<Instant>,
}

impl WikiEdit {
    
    /// A function to create a new wiki event
    pub fn new(is_bot: bool, payload: Bytes) -> Self {
        Self {
            is_bot,
            payload,
            enqueue_time: None,
        }
    }

    /// Retrieve the size of the event and current allocated memory in bytes (this is overall memory usage from all threads/tasks)
    pub fn memory_bytes_estimate(&self) -> usize {
        size_of::<Self>() + self.payload.len()
    }

    /// A function to update the enqueue time
    pub fn update_enqueue_time(&mut self) {
        self.enqueue_time = Some(Instant::now());
    }
}
