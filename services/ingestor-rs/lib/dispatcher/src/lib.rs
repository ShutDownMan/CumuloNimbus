use anyhow::Result;
use chrono::prelude::*;
use chrono::Duration;
use sqlx::sqlite::SqlitePool;
use sqlx::Row;
use sqlx::{migrate::MigrateDatabase, migrate::Migrator, Sqlite};
use std::path::Path;
use tracing::{debug, info};
use uuid::Uuid;

const DB_URL: &str = "sqlite://./lib/dispatcher/db/ingestor.db";
// const DB_URL: &str =
//     "sqlite:///home/jedi/workspace/Nimbus/services/ingestor-rs/lib/dispatcher/db/ingestor.db";

#[derive(Clone, Debug)]
pub struct DataPoint {
    pub timestamp: DateTime<Utc>,
    pub value: f64,
}

#[derive(Debug)]
pub struct DataSeries {
    pub dataseries_id: Uuid,
    pub values: Vec<DataPoint>,
}

pub struct Dispatcher {
    sqlite_pool: SqlitePool,
}

#[derive(Clone, Debug)]
pub enum DispatchTriggerType {
    BatchSize,
    Holdoff { holdoff: Duration },
    Interval { interval: Duration },
    Cron { cron: String },
}

#[derive(Clone, Debug)]
pub enum DispatchStrategy {
    Realtime,
    Batched {
        max_batch: usize,
        trigger: DispatchTriggerType,
    },
}

impl Dispatcher {
    pub async fn new() -> Result<Self> {
        // create the database if it doesn't exist
        info!("checking if database exists");
        if !Sqlite::database_exists(DB_URL).await.unwrap_or(false) {
            info!("creating database {}", DB_URL);
            match Sqlite::create_database(DB_URL).await {
                Ok(_) => info!("Create db success"),
                Err(error) => panic!("error: {}", error),
            }
        } else {
            info!("database already exists");
        }

        // initialize the database
        info!("initializing database");
        let sqlite_pool = SqlitePool::connect(DB_URL)
            .await
            .expect("failed to connect to sqlite");

        sqlx::query!(
            r#"
            VACUUM;
            "#
        )
        .execute(&sqlite_pool)
        .await?;

        // run migrations
        info!("running migrations");
        let migrator = Migrator::new(Path::new("./lib/dispatcher/migrations")).await?;
        migrator.run(&sqlite_pool).await?;
        info!("migrations complete");

        // create the dispatcher
        Ok(Dispatcher { sqlite_pool })
    }

    pub async fn dispatch(&self, dataseries: &DataSeries) -> Result<()> {
        debug!(
            "dispatching dataseries of id {:?}",
            dataseries.dataseries_id
        );

        // save the dataseries to the database
        self.save_dataseries(dataseries).await?;

        // send the dataseries to the service_bus
        // self.send_dataseries(dataseries.dataseries_id).await?;

        Ok(())
    }

    async fn save_dataseries(&self, dataseries: &DataSeries) -> Result<()> {
        info!("saving dataseries of id {:?}", dataseries.dataseries_id);

        // save the dataseries to the database
        info!("fetching database connection");
        let mut executor = self.sqlite_pool.begin().await?;
        info!("saving dataseries to database");
        let res = sqlx::query(
            r#"
            INSERT INTO DataSeries (external_id, created_at)
            VALUES (?, DATE('now'))
            ON CONFLICT (external_id) DO UPDATE SET updated_at = DATE('now')
            RETURNING id;
            "#,
        )
        .bind(dataseries.dataseries_id.to_string())
        .fetch_one(&mut *executor)
        .await?;
        let dataseries_id: i32 = res.try_get("id")?;
        debug!("dataseries_id: {:?}", dataseries_id);

        info!("saving datapoints to database");
        for data_point in dataseries.values.iter() {
            debug!("saving datapoint {:?}", data_point);
            sqlx::query(
                r#"
                INSERT INTO DataPoint (dataseries_id, value_timestamp, value, created_at)
                VALUES (?, ?, ?, DATE('now'));
                "#,
            )
            .bind(dataseries_id)
            .bind(data_point.timestamp.timestamp())
            .bind(data_point.value)
            .execute(&mut *executor)
            .await?;
        }
        info!("committing transaction");
        executor.commit().await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
