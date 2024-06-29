use anyhow::Result;
use chrono::prelude::*;
use intercom::MessagePriority;
use sqlx::{sqlite::SqlitePool, Acquire};
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

    pub fn dispatch(&self, dataseries: &DataSeries) -> Result<()> {
        debug!(
            "dispatching dataseries of id {:?}",
            dataseries.dataseries_id
        );

        // save the dataseries to the database
        async_global_executor::block_on(self.save_dataseries(dataseries))?;

        // send the dataseries to the service_bus
        async_global_executor::block_on(self.send_dataseries(&dataseries.dataseries_id))?;

        Ok(())
    }

    async fn save_dataseries(&self, dataseries: &DataSeries) -> Result<()> {
        // this time we will save as much as possible in a single statement
        info!("saving dataseries of id {:?}", dataseries.dataseries_id);

        // save the dataseries to the database
        info!("fetching database connection");
        // block until we get a connection
        let mut executor = self.sqlite_pool.acquire().await?;
        let mut transaction = executor.begin().await?;
        info!("saving dataseries to database");
        let res = sqlx::query(r#"
            INSERT INTO DataSeries (external_id, created_at)
            VALUES (?, DATE('now'))
            ON CONFLICT (external_id) DO UPDATE SET updated_at = DATE('now')
            RETURNING id;
        "#,)
        .bind(dataseries.dataseries_id.to_string())
        .fetch_one(&mut *transaction)
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
        .execute(&mut *transaction)
        .await?;
        info!("committing transaction");
        transaction.commit().await?;

        Ok(())
    }


    async fn send_dataseries(&self, dataseries_id: &Uuid) -> Result<()> {
        info!("sending dataseries of id {:?}", dataseries_id);

        info!("fetching dataseries from database");
        let mut executor = self.sqlite_pool.acquire().await?;
        let mut transaction = executor.begin().await?;
        let res = sqlx::query(r#"
            SELECT external_id
            FROM DataSeries
            WHERE external_id = ?;
        "#,)
        .bind(dataseries_id.to_string())
        .fetch_one(&mut *transaction)
        .await?;
        let dataseries_id: String = res.try_get("external_id")?;
        debug!("dataseries_id: {:?}", dataseries_id);

        info!("fetching datapoints from database");
        let mut rows: Vec<sqlx::sqlite::SqliteRow> = sqlx::query(r#"
            SELECT "timestamp", "value"
            FROM DataPoint
            WHERE dataseries_id = (SELECT Id FROM DataSeries WHERE external_id = ?)
            AND sent_at IS NULL
            ORDER BY timestamp ASC
            LIMIT ?;
        "#,)
        .bind(dataseries_id.clone())
        // TODO: parameterize limit
        .bind(1000)
        .fetch_all(&mut *transaction)
        .await?;

        // TODO: check if should send again

        debug!("fetched {:?} rows", rows.len());

        let mut datapoints: Vec<DBDataPoint> = Vec::new();

        info!("transforming rows into datapoints");
        for row in rows.drain(..) {
            let timestamp: i64 = row
                .try_get("timestamp")?;
            let value: f64 = row.try_get("value")?;
            if let Some(timestamp) = Utc.timestamp_opt(timestamp, 0).single() {
                datapoints.push(DBDataPoint { timestamp, value });
            } else {
                error!("failed to parse timestamp");
            }
        }

        info!("datapoints count: {}", datapoints.len());
        self.publish_dataseries(dataseries_id.clone(), datapoints).await?;

        info!("marking datapoints as sent");
        sqlx::query(r#"
            UPDATE DataPoint
            SET sent_at = CURRENT_TIMESTAMP
            WHERE dataseries_id = (SELECT Id FROM DataSeries WHERE external_id = ?);
        "#,)
        .bind(dataseries_id)
        .execute(&mut *transaction)
        .await?;

        info!("committing transaction");
        transaction.commit().await?;
        info!("transaction committed");

        Ok(())
    }

    async fn publish_dataseries(
        &self,
        dataseries_id: String,
        datapoints: Vec<DBDataPoint>,
    ) -> Result<()> {
        info!("transforming dataseries into bytearray with capnp");
        info!("publishing dataseries to service bus");

        let buffer: Vec<u8> = {
            let mut message = intercom::CapnpBuilder::new_default();
            let mut dataseries =
                message.init_root::<intercom::schemas::persistor_capnp::persist_data_series::Builder>();
            dataseries.set_id(&dataseries_id);
            dataseries
                .set_type(intercom::schemas::persistor_capnp::persist_data_series::DataType::Numerical);
            let mut datapoints_builder = dataseries.init_values(datapoints.len() as u32);
            for (i, datapoint) in datapoints.iter().enumerate() {
                let mut datapoint_builder = datapoints_builder.reborrow().get(i as u32);
                datapoint_builder.set_timestamp(datapoint.timestamp.timestamp() as u64);
                datapoint_builder.init_data().set_numerical(datapoint.value);
            }

            let mut buffer = Vec::new();
            intercom::serialize_packed::write_message(&mut buffer, &message)?;

            buffer
        };

        info!("attempting to send dataseries to service bus");
        let publish_status = self.service_bus
            .intercom_publish("amq.direct", "persist-dataseries",
            intercom::MessageType::PersistDataSeries, &buffer, true,
            MessagePriority::Beta.into())
            .await;
        if let Err(e) = publish_status {
            error!("failed to send dataseries to service bus: {:?}", e);
            return Err(e);
        }
        info!("dataseries sent to service bus");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // use super::*;

    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
