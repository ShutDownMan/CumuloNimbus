//! Housekeeping functionality
//! 
//! This module is responsible for maintenance tasks.

mod cleanup;

// Re-export public APIs
pub use cleanup::Housekeeper;
