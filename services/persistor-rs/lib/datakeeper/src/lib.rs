use anyhow::Result;
use sqlx::postgres::PgPool;
use std::sync::Arc;
use tracing::{debug};

extern crate intercom;

use intercom::schemas;

#[derive(Debug)]
pub struct DataKeeper {
    db_pool: Arc<PgPool>,
    service_bus: Arc<intercom::ServiceBus>,
}

impl DataKeeper {
    pub fn new(
        db_pool: Arc<PgPool>,
        service_bus: Arc<intercom::ServiceBus>,
    ) -> Result<Self> {
        Ok(DataKeeper {
            db_pool,
            service_bus,
        })
    }

    pub async fn handle_messages(&self) -> Result<()> {
        // TODO: Subscribe to message types and run callbacks on them
        self.service_bus.subscribe("persistor.input", intercom::MessageType::PersistDataSeries, Self::handle_persist_data_series)?;
        self.service_bus.subscribe("persistor.input", intercom::MessageType::FetchDataSeries, Self::handle_fetch_data_series)?;

        self.service_bus.listen_forever().await?;

        Ok(())
    }

    fn handle_persist_data_series(
        reader: intercom::CapnpSegmentReader,
        metadata: intercom::LapinAMQPProperties
    ) -> Result<()> {
        let root = reader.get_root::<schemas::persistor_capnp::persist_data_series::Reader>()?;
        debug!("Id: {:?}", root.get_id()?);
        debug!("Type: {:?}", root.get_type()?);

        for value in root.get_values()? {
            debug!("Timestamp: {:?}", value.get_timestamp());
            debug!("Data: {:?}", value.get_data());
        }

        Ok(())
    }

    fn handle_fetch_data_series(
        reader: intercom::CapnpSegmentReader,
        metadata: intercom::LapinAMQPProperties
    ) -> Result<()> {
        let root = reader.get_root::<schemas::persistor_capnp::fetch_data_series::Reader>()?;
        debug!("Id: {:?}", root.get_id());

        Ok(())
    }
}
