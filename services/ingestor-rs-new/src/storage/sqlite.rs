//! SQLite storage implementation
//! 
//! This module implements storage in SQLite.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use sqlx::{migrate::Migrator, sqlite::{SqlitePool, SqlitePoolOptions}, Sqlite};
use tracing::{error, info};

use crate::error::{IngestorError, IngestorResult};

/// Trait for storage operations
#[async_trait]
pub trait Storage: Send + Sync {
    /// Initialize the storage
    async fn initialize(&self) -> IngestorResult<()>;
    
    /// Get the SQLite pool
    fn get_pool(&self) -> Arc<SqlitePool>;
}

/// SQLite-based storage
pub struct SqliteStorage {
    pool: Arc<SqlitePool>,
    migrations_path: String,
}

impl SqliteStorage {
    /// Create a new SQLite storage
    pub fn new(pool: Arc<SqlitePool>, migrations_path: String) -> Self {
        Self {
            pool,
            migrations_path,
        }
    }
    
    /// Create a new SQLite storage with the specified connection parameters
    pub async fn connect(url: &str, migrations_path: &str) -> IngestorResult<Self> {
        // Create the database if it doesn't exist
        info!("Checking if database exists: {}", url);
        if !Sqlite::database_exists(url).await.unwrap_or(false) {
            info!("Creating database: {}", url);
            match Sqlite::create_database(url).await {
                Ok(_) => info!("Database created successfully"),
                Err(e) => return Err(IngestorError::Database(format!("Failed to create database: {}", e))),
            }
        } else {
            info!("Database already exists");
        }
        
        // Connect to the database
        info!("Connecting to database");
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(url)
            .await
            .map_err(|e| IngestorError::Database(format!("Failed to connect to database: {}", e)))?;
            
        Ok(Self::new(Arc::new(pool), migrations_path.to_string()))
    }
}

#[async_trait]
impl Storage for SqliteStorage {
    async fn initialize(&self) -> IngestorResult<()> {
        // Run migrations
        info!("Running migrations from {}", self.migrations_path);
        let migrator = Migrator::new(Path::new(&self.migrations_path))
            .await
            .map_err(|e| IngestorError::Database(format!("Failed to create migrator: {}", e)))?;
            
        migrator.run(&*self.pool)
            .await
            .map_err(|e| IngestorError::Database(format!("Failed to run migrations: {}", e)))?;
            
        info!("Migrations complete");
        
        Ok(())
    }
    
    fn get_pool(&self) -> Arc<SqlitePool> {
        self.pool.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_sqlite_storage_init() {
        // Use an in-memory database for testing
        let pool = SqlitePoolOptions::new()
            .connect("sqlite::memory:")
            .await
            .unwrap();
            
        let storage = SqliteStorage::new(Arc::new(pool), "test_migrations".to_string());
        
        // We're just testing that it doesn't panic
        assert!(storage.get_pool().is_some());
    }
}
