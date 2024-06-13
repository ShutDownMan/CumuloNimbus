use anyhow::{Context, Result};
use chrono::prelude::*;
use sqlx::sqlite::SqlitePool;
use sqlx::Row;
use std::sync::Arc;
use tracing::{debug, error, info};
use uuid::Uuid;

extern crate intercom;

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
    sqlite_pool: Arc<SqlitePool>,
    service_bus: Arc<intercom::ServiceBus>,
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
    // TODO: use a more generic type
    value: f64,
}

impl Dispatcher {
    pub async fn new(
        sqlite_pool: Arc<SqlitePool>,
        service_bus: Arc<intercom::ServiceBus>,
    ) -> Result<Self> {
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
        self.save_dataseries(dataseries)
            .await
            .context("failed to save dataseries")?;

        // send the dataseries to the service_bus
        self.send_dataseries(&dataseries.dataseries_id)
            .await
            .context("failed to send dataseries")?;

        Ok(())
    }

    async fn save_dataseries_old(&self, dataseries: &DataSeries) -> Result<()> {
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
            sqlx::query(r#"
                INSERT INTO DataPoint (dataseries_id, timestamp, value, created_at)
                VALUES (?, ?, ?, STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW'));
            "#,)
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

    async fn save_dataseries(&self, dataseries: &DataSeries) -> Result<()> {
        // this time we will save as much as possible in a single statement
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
        let mut values = String::new();
        for data_point in dataseries.values.iter() {
            debug!("saving datapoint {:?}", data_point);
            // TODO: not use string concatenation and use bind parameters
            values.push_str(&format!(
                "({}, {}, {}, STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')),",
                dataseries_id,
                data_point.timestamp.timestamp_millis(),
                data_point.value
            ));
        }
        values.pop(); // remove the trailing comma
        sqlx::query(&format!(
            r#"
            INSERT INTO DataPoint (dataseries_id, timestamp, value, created_at)
            VALUES {};
            "#,
            values
        ))
        .execute(&mut *executor)
        .await?;
        info!("committing transaction");
        executor.commit().await?;

        Ok(())
    }


    async fn send_dataseries(&self, dataseries_id: &Uuid) -> Result<()> {
        info!("sending dataseries of id {:?}", dataseries_id);

        info!("fetching dataseries from database");
        let mut executor = self.sqlite_pool.begin().await?;
        let res = sqlx::query(
            r#"
                SELECT external_id
                FROM DataSeries
                WHERE external_id = ?;
            "#,
        )
        .bind(dataseries_id.to_string())
        .fetch_one(&mut *executor)
        .await
        .context("failed to fetch dataseries from database")?;
        let dataseries_id: String = res.try_get("external_id")?;
        debug!("dataseries_id: {:?}", dataseries_id);

        info!("fetching datapoints from database");
        let mut rows: Vec<sqlx::sqlite::SqliteRow> = sqlx::query(r#"
            SELECT "timestamp", "value"
            FROM DataPoint
            WHERE dataseries_id = (SELECT Id FROM DataSeries WHERE external_id = ?)
            AND sent_at IS NULL
            ORDER BY timestamp ASC;
        "#,)
        .bind(dataseries_id.clone())
        .fetch_all(&mut *executor)
        .await
        .context("failed to fetch datapoints from database")?;

        debug!("fetched {:?} rows", rows.len());

        let mut datapoints: Vec<DBDataPoint> = Vec::new();

        info!("transforming rows into datapoints");
        for row in rows.drain(..) {
            let timestamp: i64 = row
                .try_get("timestamp")
                .context("failed to get timestamp")?;
            let value: f64 = row.try_get("value").context("failed to get value")?;
            if let Some(timestamp) = Utc.timestamp_opt(timestamp, 0).single() {
                datapoints.push(DBDataPoint { timestamp, value });
            } else {
                error!("failed to parse timestamp");
            }
        }

        info!("transforming dataseries into bytearray with capnp");
        info!("datapoints length: {}", datapoints.len());
        // TODO: break this into a separate function
        let buffer: Vec<u8> = {
            let mut message = intercom::capnp::message::Builder::new_default();
            let mut dataseries =
                message.init_root::<intercom::schemas::persistor_capnp::persist_data_series::Builder>();
            dataseries.set_id(&dataseries_id.to_string());
            dataseries
                .set_type(intercom::schemas::persistor_capnp::persist_data_series::DataType::Numerical);
            let mut datapoints_builder = dataseries.init_values(datapoints.len() as u32);
            for (i, datapoint) in datapoints.iter().enumerate() {
                let mut datapoint_builder = datapoints_builder.reborrow().get(i as u32);
                datapoint_builder.set_timestamp(datapoint.timestamp.timestamp() as u64);
                datapoint_builder.init_data().set_numerical(datapoint.value);
            }

            let mut buffer = Vec::new();
            intercom::capnp::serialize::write_message(&mut buffer, &message)
                .context("failed to serialize message")?;

            buffer
        };

        info!("sending dataseries to service bus");
        self.service_bus
            .capnp_publish_message("amq.direct", "persist-dataseries", &buffer, true)
            .await
            .context("failed to send message to service bus")?;
        info!("dataseries sent to service bus");

        info!("marking datapoints as sent");
        sqlx::query(r#"
            UPDATE DataPoint
            SET sent_at = DATE('now')
            WHERE dataseries_id = (SELECT Id FROM DataSeries WHERE external_id = ?);
        "#,)
        .bind(dataseries_id)
        .execute(&mut *executor)
        .await
        .context("failed to mark datapoints as sent")?;

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
