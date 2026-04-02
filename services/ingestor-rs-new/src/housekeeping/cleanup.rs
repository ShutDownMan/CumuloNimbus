//! Cleanup functionality
//!
//! This module implements cleanup tasks.

use std::sync::Arc;
use std::time::{Duration, Instant};
use async_trait::async_trait;
use sqlx::SqlitePool;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use crate::config::HousekeeperConfig;
use crate::error::{IngestorError, IngestorResult};

/// Trait for cleanup operations
#[async_trait]
pub trait CleanupTask: Send + Sync {
    /// Run the cleanup task
    async fn run(&self) -> IngestorResult<()>;
    
    /// Get the name of the task
    fn name(&self) -> &str;
}

/// Enum representing the cleaning strategy
enum CleaningStrategy {
    /// Clean data based on capacity limits
    Capacity {
        max_datapoints: usize,
    },
    /// Clean data based on age
    Time {
        max_age: chrono::Duration,
    },
}

/// Cleanup task for temporary storage
pub struct TempStorageCleaner {
    /// SQLite connection pool
    pool: Arc<SqlitePool>,
    
    /// Cleaning strategy
    strategy: CleaningStrategy,
}

impl TempStorageCleaner {
    /// Create a new temporary storage cleaner with capacity-based strategy
    pub fn with_capacity(pool: Arc<SqlitePool>, max_datapoints: usize) -> Self {
        Self {
            pool,
            strategy: CleaningStrategy::Capacity { max_datapoints },
        }
    }
    
    /// Create a new temporary storage cleaner with time-based strategy
    pub fn with_max_age(pool: Arc<SqlitePool>, max_age: chrono::Duration) -> Self {
        Self {
            pool,
            strategy: CleaningStrategy::Time { max_age },
        }
    }
    
    /// Clean old data points
    async fn clean_by_age(&self, max_age: chrono::Duration) -> IngestorResult<usize> {
        // Calculate the cutoff timestamp
        let cutoff = chrono::Utc::now() - max_age;
        let cutoff_ts = cutoff.timestamp_millis();
        
        // Delete old data points
        let result = sqlx::query!(
            r#"
            DELETE FROM datapoints
            WHERE timestamp < ?
              AND sent = 1
            "#,
            cutoff_ts
        )
        .execute(&*self.pool)
        .await
        .map_err(|e| IngestorError::Database(format!("Failed to clean old data points: {}", e)))?;
        
        Ok(result.rows_affected() as usize)
    }
    
    /// Clean excess data points based on capacity
    async fn clean_by_capacity(&self, max_datapoints: usize) -> IngestorResult<usize> {
        // First, count all data points
        let count = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) as count
            FROM datapoints
            WHERE sent = 1
            "#
        )
        .fetch_one(&*self.pool)
        .await
        .map_err(|e| IngestorError::Database(format!("Failed to count data points: {}", e)))?;
        
        // If we're below the threshold, no need to clean
        if count <= max_datapoints as i64 {
            return Ok(0);
        }
        
        // Calculate how many to delete
        let to_delete = count - max_datapoints as i64;
        
        // Delete oldest data points first
        let result = sqlx::query!(
            r#"
            DELETE FROM datapoints
            WHERE id IN (
                SELECT id FROM datapoints
                WHERE sent = 1
                ORDER BY timestamp ASC
                LIMIT ?
            )
            "#,
            to_delete
        )
        .execute(&*self.pool)
        .await
        .map_err(|e| IngestorError::Database(format!("Failed to clean excess data points: {}", e)))?;
        
        Ok(result.rows_affected() as usize)
    }
}

#[async_trait]
impl CleanupTask for TempStorageCleaner {
    async fn run(&self) -> IngestorResult<()> {
        let count = match &self.strategy {
            CleaningStrategy::Capacity { max_datapoints } => {
                self.clean_by_capacity(*max_datapoints).await?
            },
            CleaningStrategy::Time { max_age } => {
                self.clean_by_age(*max_age).await?
            }
        };
        
        if count > 0 {
            info!("Cleaned {} data points from temporary storage", count);
        }
        
        Ok(())
    }
    
    fn name(&self) -> &str {
        "Temporary Storage Cleaner"
    }
}

/// State of the housekeeper
struct HousekeeperState {
    /// Tasks to run
    tasks: Vec<Box<dyn CleanupTask>>,
    
    /// Current patience level (adaptive scheduling)
    patience: f64,
    
    /// Last time tasks were run
    last_run: Instant,
}

/// Housekeeper for maintenance tasks
pub struct Housekeeper {
    /// State of the housekeeper
    state: Mutex<HousekeeperState>,
    
    /// SQLite connection pool
    pool: Arc<SqlitePool>,
    
    /// Configuration
    config: HousekeeperConfig,
}

impl Housekeeper {
    /// Create a new housekeeper
    pub fn new(pool: Arc<SqlitePool>, config: HousekeeperConfig) -> Self {
        Self {
            state: Mutex::new(HousekeeperState {
                tasks: Vec::new(),
                patience: 1.0,
                last_run: Instant::now(),
            }),
            pool,
            config,
        }
    }
    
    /// Add a cleanup task
    pub async fn add_task<T: CleanupTask + 'static>(&self, task: T) {
        let mut state = self.state.lock().await;
        state.tasks.push(Box::new(task));
    }
    
    /// Start the housekeeper
    pub async fn start(&self) -> IngestorResult<()> {
        info!("Starting housekeeper");
        
        // Add default cleanup tasks
        let cleaner = TempStorageCleaner::with_max_age(
            self.pool.clone(),
            chrono::Duration::minutes(30),
        );
        self.add_task(cleaner).await;
        
        // Run the main loop
        self.run().await
    }
    
    /// Run the housekeeper loop
    async fn run(&self) -> IngestorResult<()> {
        loop {
            // Determine whether to work or idle
            let mut state = self.state.lock().await;
            let elapsed = state.last_run.elapsed();
            
            if elapsed >= self.config.work_interval {
                // Time to work
                state.last_run = Instant::now();
                let tasks = state.tasks.iter().map(|t| t.name()).collect::<Vec<_>>();
                drop(state);
                
                info!("Running {} cleanup tasks", tasks.len());
                self.run_tasks().await?;
                
                // Update patience based on result
                let mut state = self.state.lock().await;
                state.patience -= self.config.patience_falloff_rate;
                if state.patience < self.config.patience_min_threshold {
                    state.patience = self.config.patience_min_threshold;
                }
                drop(state);
            } else {
                // Continue idling
                drop(state);
                
                // Gradually increase patience while idle
                let mut state = self.state.lock().await;
                state.patience += self.config.patience_recovery_rate;
                if state.patience > 1.0 {
                    state.patience = 1.0;
                }
                drop(state);
                
                // Sleep for a while
                let patience_adjusted_idle = Duration::from_secs_f64(
                    self.config.idle_interval.as_secs_f64() * state.patience
                );
                debug!("Housekeeper idling for {:?} (patience: {:.2})", patience_adjusted_idle, state.patience);
                tokio::time::sleep(patience_adjusted_idle).await;
            }
        }
    }
    
    /// Run all cleanup tasks
    async fn run_tasks(&self) -> IngestorResult<()> {
        let state = self.state.lock().await;
        let tasks = state.tasks.clone();
        drop(state);
        
        for task in tasks {
            match task.run().await {
                Ok(_) => debug!("Task {} completed successfully", task.name()),
                Err(e) => error!("Task {} failed: {}", task.name(), e),
            }
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::SqlitePool;
    use std::time::Duration;
    
    struct MockTask {
        name: String,
        run_count: Arc<Mutex<usize>>,
    }
    
    impl MockTask {
        fn new(name: &str) -> Self {
            Self {
                name: name.to_string(),
                run_count: Arc::new(Mutex::new(0)),
            }
        }
        
        async fn get_run_count(&self) -> usize {
            *self.run_count.lock().await
        }
    }
    
    #[async_trait]
    impl CleanupTask for MockTask {
        async fn run(&self) -> IngestorResult<()> {
            let mut count = self.run_count.lock().await;
            *count += 1;
            Ok(())
        }
        
        fn name(&self) -> &str {
            &self.name
        }
    }
    
    #[tokio::test]
    async fn test_adding_tasks() {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        let config = HousekeeperConfig {
            work_interval: Duration::from_millis(100),
            idle_interval: Duration::from_millis(50),
            patience_falloff_rate: 0.1,
            patience_recovery_rate: 0.05,
            patience_min_threshold: 0.1,
        };
        
        let housekeeper = Housekeeper::new(Arc::new(pool), config);
        let task1 = MockTask::new("Task 1");
        let task2 = MockTask::new("Task 2");
        
        housekeeper.add_task(task1).await;
        housekeeper.add_task(task2).await;
        
        let state = housekeeper.state.lock().await;
        assert_eq!(state.tasks.len(), 2);
    }
}
