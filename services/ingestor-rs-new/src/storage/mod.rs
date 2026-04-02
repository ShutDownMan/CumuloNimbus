//! Storage functionality
//! 
//! This module is responsible for storing and retrieving data.

mod sqlite;

// Re-export public APIs
pub use sqlite::SqliteStorage;
