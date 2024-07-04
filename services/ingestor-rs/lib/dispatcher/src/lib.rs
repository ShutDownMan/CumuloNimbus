use anyhow::Result;
use intercom::MessagePriority;
use sqlx::sqlite::SqlitePool;
use std::sync::Arc;
use tracing::{debug, error, info};

extern crate intercom;

pub use microkeeper::DataSeries;
pub use microkeeper::DataPoint;
pub use microkeeper::DataPointValue;

pub struct Dispatcher {
    sqlite_pool: Arc<SqlitePool>,
    service_bus: Arc<intercom::ServiceBus>,
    tokio_handle: tokio::runtime::Handle,
}

#[derive(Clone, Debug)]
pub enum DispatchTrigger {
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
        trigger: DispatchTrigger,
    },
}

#[derive(Clone, Debug)]
pub struct DispatcherConfig {
    // strategy of intercom message dispatch
    pub dispatch_strategy: DispatchStrategy,
    // duration if should persist in temporary storage
    pub temporary_storage: Option<chrono::Duration>,
}

impl Dispatcher {
    pub fn new(
        sqlite_pool: Arc<SqlitePool>,
        service_bus: Arc<intercom::ServiceBus>,
        tokio_handle: tokio::runtime::Handle,
    ) -> Result<Self> {
        // create the dispatcher
        Ok(Dispatcher {
            sqlite_pool,
            service_bus,
            tokio_handle,
        })
    }

    pub fn dispatch_to_persistor(&self, dataseries: &DataSeries, dispath_config: &DispatcherConfig) -> Result<()> {
        debug!(
            "dispatching dataseries of id {:?}",
            dataseries.dataseries_id
        );

        if dataseries.values.is_empty() {
            info!("empty dataseries, skipping");
            return Ok(());
        }

        let temporary_storage = dispath_config.temporary_storage.is_some();

        let handle = tokio::runtime::Handle::current();
        debug!("tokio handle: {:?}", handle);

        info!("sending dataseries of id {:?}", dataseries.dataseries_id);
        let priority = match dispath_config.dispatch_strategy {
            DispatchStrategy::Realtime => MessagePriority::Beta,
            DispatchStrategy::Batched { .. } => MessagePriority::Omega,
        };
        let realtime = match dispath_config.dispatch_strategy {
            DispatchStrategy::Realtime => true,
            DispatchStrategy::Batched { .. } => false,
        };
        // send the dataseries to the service_bus
        let send_dataseries_task = microkeeper::publish_dataseries(self.service_bus.clone(),
            dataseries, "persist-dataseries", priority, realtime);

        // block until the task is done
        let publish_result = self.tokio_handle.block_on(send_dataseries_task);
        let publish_result = match publish_result {
            Ok(_) => true,
            Err(e) => {
                error!("failed to publish dataseries: {:?}", e);
                false
            }
        };

        if temporary_storage {
            info!("persisting dataseries of id {:?}", dataseries.dataseries_id);
            let expiry = dispath_config.temporary_storage.unwrap();
            // save the dataseries to the database task
            let save_dataseries_task = microkeeper::save_dataseries(self.sqlite_pool.clone(), dataseries, expiry);

            // block until both tasks are done
            let save_result = self.tokio_handle.block_on(save_dataseries_task);
            let save_result = match save_result {
                Ok(datapoint_ids) => Ok(datapoint_ids),
                Err(e) => {
                    error!("failed to save dataseries: {:?}", e);
                    Err(e)
                }
            };

            // check if published and saved successfully to mark as sent
            if let (true, Ok(datapoint_ids)) = (publish_result, save_result) {
                info!("marking datapoints as sent");
                // mark datapoints as sent
                let mark_datapoints_as_sent_task = microkeeper::mark_datapoints_as_sent(
                    self.sqlite_pool.clone(), &datapoint_ids);

                // block until the task is done
                let mark_result = self.tokio_handle.block_on(mark_datapoints_as_sent_task);

                if let Err(e) = mark_result {
                    error!("failed to mark datapoints as sent: {:?}", e);
                    return Err(e);
                }
            } else {
                error!("failed to publish or save dataseries");
                anyhow::bail!("failed to publish or save dataseries");
            }
        }

        // TODO: housekeeping for temporary storage

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
