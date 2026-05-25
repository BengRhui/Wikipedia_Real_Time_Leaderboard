//! src/leaderboard.rs
//!
//! This file is to handle leaderboard-related actions.

use crate::config::Safety;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

/// The parent class - send and sync is to make sure that the leaderboard can be shared and access across threads
pub trait Leaderboard: Send + Sync {
    fn update(&self, domain: &str);
    fn top(&self, size: usize) -> Vec<(Arc<str>, u64)>;
}

/// The function that allows users to specify the leaderboard to be created
/// dyn is used to cater for different types of leaderboard to be returned
pub fn create_leaderboard(mode: &Safety) -> Arc<dyn Leaderboard> {
    match mode {
        Safety::Mutex => Arc::new(MutexLeaderboard::new()),
        Safety::RwLock => Arc::new(RwLeaderboard::new()),
        Safety::Atomic => Arc::new(AtomicLeaderboard::new()),
        Safety::DashMap => Arc::new(DashMapLeaderboard::new()),
    }
}

/// The struct for mutex-based leaderboard. It contains only a HashMap to update data
pub struct MutexLeaderboard {
    inner: Mutex<HashMap<Arc<str>, u64>>,
}

/// The constructor for leaderboard
impl MutexLeaderboard {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }
}

/// Override the existing functions from parent
impl Leaderboard for MutexLeaderboard {
    
    /// Function to update values
    fn update(&self, domain: &str) {
        
        // First lock the map for edit
        let mut map = self.inner.lock().unwrap();

        // Then perform the edit
        let entry = map.entry(Arc::<str>::from(domain));
        *entry.or_insert(0) += 1;
    }

    /// Function to retrieve the top 3 domains
    fn top(&self, size: usize) -> Vec<(Arc<str>, u64)> {
        
        // First get the map
        let map = self.inner.lock().unwrap();

        // Retrieve all items, sort them, then get the top n domains
        let mut items: Vec<(Arc<str>, u64)> = map.iter().map(|(k, v)| (k.clone(), *v)).collect();
        items.sort_by(|a, b| b.1.cmp(&a.1));
        items.truncate(size);

        // Lastly return the truncated list
        items
    }
}

/// RwLock-based leaderboard
pub struct RwLeaderboard {
    inner: RwLock<HashMap<Arc<str>, u64>>,
}

/// Constructor for the RwLock-based leaderboard
impl RwLeaderboard {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }
}

/// Functions to be overridden from parent
impl Leaderboard for RwLeaderboard {
    
    /// A function to update the domain to the hashmap
    fn update(&self, domain: &str) {
        
        // Lock the map
        let mut map = self.inner.write().unwrap();

        // Then perform edit
        let entry = map.entry(Arc::<str>::from(domain));
        *entry.or_insert(0) += 1;
    }

    /// A function to retrieve the top n domains
    fn top(&self, size: usize) -> Vec<(Arc<str>, u64)> {
        
        // Lock the map
        let map = self.inner.read().unwrap();

        // Sort and truncate the map to return top n items
        let mut items: Vec<(Arc<str>, u64)> = map.iter().map(|(k, v)| (k.clone(), *v)).collect();
        items.sort_by(|a, b| b.1.cmp(&a.1));
        items.truncate(size);

        // Return the vector list
        items
    }
}

/// Atomic-based leaderboard (atomic is used to update counter, mutex still used to lock the map)
pub struct AtomicLeaderboard {
    inner: RwLock<HashMap<Arc<str>, AtomicU64>>,
}

/// Constructor for atomic-based leaderboard
impl AtomicLeaderboard {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }
}

/// Functions to be overridden from parent
impl Leaderboard for AtomicLeaderboard {
    
    /// A function to update domains into the hashmap
    fn update(&self, domain: &str) {
        
        // This is to allow different threads to access it simultaneously (for reading only)
        {
            let map = self.inner.read().unwrap();
            if let Some(counter) = map.get(domain) {
                counter.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }

        // When the map is going to be written, first lock the map
        let mut map = self.inner.write().unwrap();

        // Update the counter
        let domain_arc = Arc::<str>::from(domain);
        let counter = map.entry(domain_arc).or_insert_with(|| AtomicU64::new(0));
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// A function to return the top n domains
    fn top(&self, size: usize) -> Vec<(Arc<str>, u64)> {
        
        // Lock the map
        let map = self.inner.read().unwrap();

        // Convert the map into vectors
        let mut items: Vec<(Arc<str>, u64)> = map
            .iter()
            .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed)))
            .collect();

        // Sort the vector and truncate it to top n items
        items.sort_by(|a, b| b.1.cmp(&a.1));
        items.truncate(size);

        // Lastly return the vector list
        items
    }
}

/// An additional comparison: DashMap that supports concurrency with atomic
pub struct DashMapLeaderboard {
    inner: DashMap<Arc<str>, AtomicU64>,
}

/// Constructor for DashMap-based leaderboard
impl DashMapLeaderboard {
    pub fn new() -> Self {
        Self {
            inner: DashMap::new(),
        }
    }
}

/// Functions to be overridden from parent
impl Leaderboard for DashMapLeaderboard {
    
    /// A function to update domains into the dashmap
    fn update(&self, domain: &str) {
        
        // First convert the domain string into arc
        let domain_arc = Arc::<str>::from(domain);

        // Directly insert values, don't have to lock (DashMap handles lock automatically)
        let counter = self
            .inner
            .entry(domain_arc)
            .or_insert_with(|| AtomicU64::new(0));
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// A function to return the top n domains
    fn top(&self, size: usize) -> Vec<(Arc<str>, u64)> {
        
        // Directly perform iteration - DashMap is safe
        let mut items: Vec<(Arc<str>, u64)> = self
            .inner
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().load(Ordering::Relaxed)))
            .collect();

        // Sort the vector and truncate it to top n items
        items.sort_by(|a, b| b.1.cmp(&a.1));
        items.truncate(size);

        // Lastly return the vector list
        items
    }
}
