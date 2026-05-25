//! src/config.rs
//!
//! This file stores all the configuration of the project (e.g., deadline and channel size).
//! This is to avoid these constants from scattering across the project.

use std::fmt;
use std::time::Duration;

pub const WIKIPEDIA_URL: &str = "https://stream.wikimedia.org/v2/stream/recentchange";
pub const CHANNEL_CAPACITY: usize = 20;
pub const HIGH_PRIORITY_QUOTA: usize = 10;
pub const PROCESSING_JITTER_LIMIT: Duration = Duration::from_millis(2);
pub const QUEUEING_DELAY_LIMIT: Duration = Duration::from_millis(2);
pub const REQUIRED_STABLE_COUNT: u32 = 10;
pub const LATENCY_HIST_BUCKET_SIZE: usize = 5;
pub const FAIL_SAFE_P99_OVERALL_THRESHOLD_MICRO: u64 = 1900;
pub const LEADERBOARD_TOP_COUNT: usize = 3;
pub const DASHBOARD_LOG_CAPACITY: usize = 14;
pub const DASHBOARD_RENDER_INTERVAL_MS: u64 = 250;

/// The system mode is divided into two, normal and degraded
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SystemMode {
    Normal,
    Degraded,
}

/// A function to convert enum to string
impl fmt::Display for SystemMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let string = match self {
            SystemMode::Normal => "Normal",
            SystemMode::Degraded => "Degraded",
        };
        write!(f, "{string}")
    }
}

/// Enum to represent the safety model chosen
pub enum Safety {
    Mutex,
    RwLock,
    Atomic,
    DashMap,
}

impl Safety {

    /// A function to convert Safety from text to enum
    pub fn convert_to_enum(input: &str) -> Result<Safety, &'static str> {
        match input.trim() {
            "1" => Ok(Self::Mutex),
            "2" => Ok(Self::RwLock),
            "3" => Ok(Self::Atomic),
            "4" => Ok(Self::DashMap),
            _ => Err("Error in Safety enum conversion!"),
        }
    }
}

/// Enum to represent the time chosen
pub enum RunningTime {
    Quick,
    Normal,
    Extensive,
}

impl RunningTime {

    /// A function to convert RunningTime from string to enum
    pub fn convert_to_enum(input: &str) -> Result<RunningTime, &'static str> {
        match input.trim() {
            "1" => Ok(Self::Quick),
            "2" => Ok(Self::Normal),
            "3" => Ok(Self::Extensive),
            _ => Err("Error in RunningTime enum conversion!"),
        }
    }

    /// A function to convert enum to seconds
    pub fn convert_to_secs(&self) -> u64 {
        match self {
            Self::Quick => 30,
            Self::Normal => 60,
            Self::Extensive => 90,
        }
    }
}
