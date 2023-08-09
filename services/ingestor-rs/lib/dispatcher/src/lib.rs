use anyhow::{Context, Result};
use chrono::prelude::*;
use service_bus::capnp::data;
use sqlx::sqlite::SqlitePool;
use sqlx::Row;
use sqlx::{migrate::MigrateDatabase, migrate::Migrator, Sqlite};
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, info};
use uuid::Uuid;

extern crate service_bus;

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
    service_bus: Arc<service_bus::ServiceBus>,
}

#[derive(Clone, Debug)]
pub enum DispatchTriggerType {
    BatchSize,
    Holdoff { holdoff: chrono::Duration },
    Interval { interval: std::time::Duration },
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

struct DBDataPoint {
    timestamp: DateTime<Utc>,
    value: f64,
}

impl Dispatcher {
    pub async fn new(service_bus: Arc<service_bus::ServiceBus>) -> Result<Self> {
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
        Ok(Dispatcher {
            sqlite_pool,
            service_bus,
        })
    }

    pub async fn dispatch(&self, dataseries: &DataSeries) -> Result<()> {
        debug!(
            "dispatching dataseries of id {:?}",
            dataseries.dataseries_id
        );

        // save the dataseries to the database
        self.save_dataseries(dataseries).await?;

        // send the dataseries to the service_bus
        self.send_dataseries(&dataseries.dataseries_id).await?;

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
                INSERT INTO DataPoint (dataseries_id, timestamp, value, created_at)
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

    async fn send_dataseries(&self, dataseries_id: &Uuid) -> Result<()> {
        info!("sending dataseries of id {:?}", dataseries_id);

        info!("fetching dataseries from database");
        let res = sqlx::query(
            r#"
            SELECT external_id
            FROM DataSeries
            WHERE external_id = ?;
            "#,
        )
        .bind(dataseries_id.to_string())
        .fetch_one(&self.sqlite_pool)
        .await
        .context("failed to fetch dataseries from database")?;
        let dataseries_id: String = res.try_get("external_id")?;
        debug!("dataseries_id: {:?}", dataseries_id);

        info!("fetching datapoints from database");
        let mut rows: Vec<sqlx::sqlite::SqliteRow> = sqlx::query(
            r#"
            SELECT "timestamp", "value"
            FROM DataPoint
            WHERE dataseries_id = (SELECT Id FROM DataSeries WHERE external_id = ?)
            ORDER BY timestamp ASC;
            "#,
        )
        .bind(dataseries_id.clone())
        .fetch_all(&self.sqlite_pool)
        .await
        .context("failed to fetch datapoints from database")?;

        debug!("rows length: {}", rows.len());

        let mut datapoints: Vec<DBDataPoint> = Vec::new();

        for row in rows.drain(..) {
            let timestamp: i64 = row.try_get("timestamp")?;
            let value: f64 = row.try_get("value")?;
            if let Some(timestamp) = Utc.timestamp_opt(timestamp, 0).single() {
                datapoints.push(DBDataPoint { timestamp, value });
            }
        }

        info!("transforming dataseries into bytearray with capnp");
        info!("datapoints length: {}", datapoints.len());
        let buffer: Vec<u8> = {
            let mut message = service_bus::capnp::message::Builder::new_default();
            let mut dataseries =
                message.init_root::<service_bus::schemas::persistor_capnp::data_series::Builder>();
            dataseries.set_id(&dataseries_id.to_string());
            dataseries
                .set_type(service_bus::schemas::persistor_capnp::data_series::DataType::Numerical);
            let mut datapoints_builder = dataseries.init_values(datapoints.len() as u32);
            for (i, datapoint) in datapoints.iter().enumerate() {
                let mut datapoint_builder = datapoints_builder.reborrow().get(i as u32);
                datapoint_builder.set_timestamp(datapoint.timestamp.timestamp() as u64);
                datapoint_builder.init_data().set_numerical(datapoint.value);
            }

            let mut buffer = Vec::new();
            service_bus::capnp::serialize::write_message(&mut buffer, &message)?;

            // vec to bytearray
            buffer
        };
        let buffer: &[u8] = &buffer;

        info!("sending dataseries to service bus");
        self.service_bus
            .capnp_publish_message("ingestor", buffer)
            .await
            .context("failed to send message to service bus")?;

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
