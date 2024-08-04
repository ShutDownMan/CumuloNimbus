use anyhow::Result;
use sqlx::{postgres::PgPool, Acquire};
use std::sync::Arc;
use tracing::{info, debug};
use sqlx::Row;

extern crate intercom;

use intercom::schemas;
use crate::intercom::HasTypeId;

pub struct RecipeKeeper {
    db_pool: Arc<PgPool>,
    service_bus: Arc<intercom::ServiceBus>,
    tokio_handle: tokio::runtime::Handle,
}

impl RecipeKeeper {
    pub fn new(
        db_pool: Arc<PgPool>,
        service_bus: Arc<intercom::ServiceBus>,
        tokio_handle: tokio::runtime::Handle,
    ) -> Result<Self> {
        Ok(RecipeKeeper {
            db_pool,
            service_bus,
            tokio_handle,
        })
    }

    pub async fn handle_messages(&self) -> Result<()> {
        let db_pool_clone = self.db_pool.clone();
        let _service_bus_clone = self.service_bus.clone();
        let tokio_handle = self.tokio_handle.clone();

        let store_recipe_type_id = intercom::schemas::baker_capnp::store_recipe::Reader::TYPE_ID;
        self.service_bus.subscribe("baker.input", store_recipe_type_id, Box::new(move |reader, metadata| {
            debug!("Handling persist data series message");
            let db_pool = db_pool_clone.clone();
            let _service_bus = _service_bus_clone.clone();

            tokio_handle.block_on(async move {
                Self::handle_persist_recipe(db_pool, reader, metadata).await
            })
        }))?;

        let fetch_recipe_type_id = intercom::schemas::baker_capnp::fetch_recipe::Reader::TYPE_ID;
        self.service_bus.subscribe("baker.input", fetch_recipe_type_id, Box::new(move |reader, metadata| {
            debug!("Handling fetch data series message");

            Self::handle_fetch_recipe(reader, metadata)
        }))?;

        self.service_bus.listen_forever().await?;

        Ok(())
    }

    async fn handle_persist_recipe(
        db_pool: Arc<PgPool>,
        reader: intercom::CapnpMessageReader,
        metadata: intercom::LapinAMQPProperties
    ) -> Result<()> {
        let root = reader.get_root::<schemas::baker_capnp::store_recipe::Reader>()?;
        let recipe_id = root.get_id()?;

        info!("Persisting data series with id: {:?}", recipe_id);

        persist_recipe_from_message(db_pool, reader, metadata).await
    }

    fn handle_fetch_recipe(
        reader: intercom::CapnpMessageReader,
        metadata: intercom::LapinAMQPProperties
    ) -> Result<()> {
        let root = reader.get_root::<schemas::baker_capnp::fetch_recipe::Reader>()?;
        debug!("Id: {:?}", root.get_id());
        debug!("Metadata: {:?}", metadata);

        Ok(())
    }
}

pub async fn persist_recipe_from_message(
    db_pool: Arc<PgPool>,
    reader: intercom::CapnpMessageReader,
    _metadata: intercom::LapinAMQPProperties
) -> Result<()> {
    let root = reader.get_root::<schemas::baker_capnp::store_recipe::Reader>()?;
    let recipe_id = root.get_id()?;
    let recipe_name = root.get_name()?;
    let recipe_description = root.get_description()?;

    let simplified_expression = root.get_simplified_expression()?;
    let wasm_expression = root.get_wasm_expression()?;

    let mut executor = db_pool.acquire().await?;
    let mut transaction = executor.begin().await?;

    let res = sqlx::query(r#"
        INSERT INTO Recipe (external_id, name, description, simplified_expression, expression, created_at)
        VALUES ($1, $2, $3, $4, $5, NOW())
        ON CONFLICT (external_id) DO UPDATE SET updated_at = NOW()
        RETURNING id;
    "#,)
    .bind(uuid::Uuid::parse_str(&recipe_id)?)
    .bind(recipe_name)
    .bind(recipe_description)
    .bind(simplified_expression)
    .bind(wasm_expression)
    .fetch_one(&mut *transaction)
    .await?;

    let new_recipe_id: i32 = res.get(0);

    info!("Persisted recipe {:} with id: {:?}", recipe_name, new_recipe_id);

    transaction.commit().await?;

    debug!("Committed transaction");

    Ok(())
}
