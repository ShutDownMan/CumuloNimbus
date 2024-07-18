use anyhow::{Context, Result};
use sqlx::{postgres::PgPool, sqlite::SqlitePool, Acquire};
use std::{result, sync::Arc, vec};
use tracing::{info, debug};
use sqlx::Row;
use uuid::Uuid;
use sqlx::types::chrono::DateTime;
use wasmtime::*;

extern crate intercom;

use intercom::schemas;
use crate::intercom::HasTypeId;

#[derive(Clone, Debug)]
pub struct DataSeries {
    pub dataseries_id: Uuid,
    pub alias: String,
    pub values: Vec<DataPoint>,
}

#[derive(Clone, Debug)]
pub struct DataPoint {
    pub id: i64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub value: DataPointValue,
}

#[derive(Clone, Debug)]
pub enum DataPointValue {
    Numeric(f64),
    Text(String),
    Boolean(bool),
    Arbitrary(Vec<u8>),
    // Jsonb(serde_json::Value),
}

trait DataPointValueTrait {
    fn try_get_numerical(&self) -> Result<f64>;
    // fn try_get_text(&self) -> Result<&String>;
    // fn try_get_boolean(&self) -> Result<bool>;
    // fn try_get_arbitrary(&self) -> Result<&Vec<u8>>;
    // fn try_get_jsonb(&self) -> Result<serde_json::Value>;
}

impl DataPointValueTrait for DataPointValue {
    fn try_get_numerical(&self) -> Result<f64> {
        match self {
            DataPointValue::Numeric(value) => Ok(*value),
            _ => Err(anyhow::anyhow!("DataPointValue is not Numeric")),
        }
    }

    // fn try_get_text(&self) -> Result<&String> {
    //     match self {
    //         DataPointValue::Text(value) => Ok(value),
    //         _ => Err(anyhow::anyhow!("DataPointValue is not Text")),
    //     }
    // }

    // fn try_get_boolean(&self) -> Result<bool> {
    //     match self {
    //         DataPointValue::Boolean(value) => Ok(*value),
    //         _ => Err(anyhow::anyhow!("DataPointValue is not Boolean")),
    //     }
    // }

    // fn try_get_arbitrary(&self) -> Result<&Vec<u8>> {
    //     match self {
    //         DataPointValue::Arbitrary(value) => Ok(value),
    //         _ => Err(anyhow::anyhow!("DataPointValue is not Arbitrary")),
    //     }
    // }

    // fn try_get_jsonb(&self) -> Result<serde_json::Value> {
    //     match self {
    //         DataPointValue::Jsonb(value) => Ok(value.clone()),
    //         _ => Err(anyhow::anyhow!("DataPointValue is not Jsonb")),
    //     }
    // }
}

pub struct Processor {
    pg_pool: Arc<PgPool>,
    sqlite_pool: Arc<SqlitePool>,
    service_bus: Arc<intercom::ServiceBus>,
    tokio_handle: tokio::runtime::Handle,
}

impl Processor {
    pub fn new(
        pg_pool: Arc<PgPool>,
        sqlite_pool: Arc<SqlitePool>,
        service_bus: Arc<intercom::ServiceBus>,
        tokio_handle: tokio::runtime::Handle,
    ) -> Result<Self> {
        Ok(Processor {
            pg_pool,
            sqlite_pool,
            service_bus,
            tokio_handle,
        })
    }

    pub async fn handle_messages(&self) -> Result<()> {
        let pg_pool_clone = self.pg_pool.clone();
        let _service_bus_clone = self.service_bus.clone();
        let tokio_handle = self.tokio_handle.clone();

        let store_recipe_type_id = intercom::schemas::baker_capnp::compute_data_series::Reader::TYPE_ID;
        self.service_bus.subscribe("baker.input", store_recipe_type_id, Box::new(move |reader, metadata| {
            debug!("Handling compute data series message");
            let pg_pool = pg_pool_clone.clone();
            let _service_bus = _service_bus_clone.clone();

            tokio_handle.block_on(async move {
                handle_compute_data_series(pg_pool, reader, metadata).await
            })
        }))?;

        Ok(())
    }

}

async fn handle_persist_recipe(
    db_pool: Arc<PgPool>,
    reader: intercom::CapnpReader,
    metadata: intercom::LapinAMQPProperties
) -> Result<()> {
    let root = reader.get_root::<schemas::baker_capnp::store_recipe::Reader>()?;
    let recipe_id = root.get_id()?;

    info!("Persisting data series with id: {:?}", recipe_id);

    persist_recipe_from_message(db_pool, reader, metadata).await
}

pub async fn persist_recipe_from_message(
    db_pool: Arc<PgPool>,
    reader: intercom::CapnpReader,
    _metadata: intercom::LapinAMQPProperties
) -> Result<()> {
    let root = reader.get_root::<schemas::baker_capnp::store_recipe::Reader>()?;
    let recipe_id = root.get_id()?;
    let recipe_name = root.get_name()?;
    let recipe_description = root.get_description()?;

    let simplified_expression = root.get_simplified_expression()?;
    let expression = root.get_expression()?;

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
    .bind(expression)
    .fetch_one(&mut *transaction)
    .await?;

    let new_recipe_id: i32 = res.get(0);

    info!("Persisted recipe {:} with id: {:?}", recipe_name, new_recipe_id);

    transaction.commit().await?;

    debug!("Committed transaction");

    Ok(())
}

async fn handle_compute_data_series(
    db_pool: Arc<PgPool>,
    reader: intercom::CapnpReader,
    metadata: intercom::LapinAMQPProperties
) -> Result<()> {
    let root = reader.get_root::<schemas::baker_capnp::compute_data_series::Reader>()?;
    let recipe_id = root.get_id()?;

    info!("Computing data series with id: {:?}", recipe_id);

    compute_data_series_from_message(db_pool, reader, metadata).await
}

pub async fn compute_data_series_from_message(
    db_pool: Arc<PgPool>,
    reader: intercom::CapnpReader,
    _metadata: intercom::LapinAMQPProperties
) -> Result<()> {
    let root = reader.get_root::<schemas::baker_capnp::compute_data_series::Reader::<schemas::dataseries_capnp::numeric_data_series::Owned>>()?;
    let recipe_id = root.get_id()?;
    let dependencies = root.get_dependencies()?;

    info!("Computing data series with recipe id: {:?}", recipe_id);

    debug!("Getting recipe from database");
    let recipe = sqlx::query(r#"
        SELECT id, expression
        FROM Recipe
        WHERE external_id = $1
    "#)
    .bind(uuid::Uuid::parse_str(&recipe_id)?)
    .fetch_one(&*db_pool)
    .await?;

    let recipe_id: i32 = recipe.get(0);
    let expression: String = recipe.get(1);
    debug!("Recipe found: {:?}", recipe_id);
    debug!("Expression: {:?}", expression);

    let depencencies_info = sqlx::query(r#"
        SELECT data_series_external_id, alias
        FROM RecipeDataSeriesDependency
        WHERE recipe_id = $1
    "#)
    .bind(recipe_id)
    .fetch_all(&*db_pool)
    .await?;

    let alias_map = depencencies_info.iter().map(|row| {
        let dataseries_external_id: Uuid = row.get("data_series_external_id");
        let alias: String = row.get("alias");

        (dataseries_external_id, alias)
    }).collect::<std::collections::HashMap<Uuid, String>>();

    let mut dataseries = vec![];
    for dependency in dependencies.iter() {
        let dataseries_id = dependency.get_id()?;
        let data_series_values = dependency.get_values()?;

        let dataseries_id = Uuid::parse_str(&dataseries_id)?;

        let alias = alias_map.get(&dataseries_id)
            .context("Alias not found for data series")?;

        let mut data_points = vec![];
        for data_point in data_series_values.iter() {
            let timestamp = data_point.get_timestamp();
            let value = data_point.get_data();

            let timestamp = DateTime::from_timestamp(timestamp / 1_000_000_000, (timestamp % 1_000_000_000) as u32).unwrap();

            let numerical_value = match value.which()? {
                schemas::dataseries_capnp::data_point::data::Numerical(value) => value,
                _ => todo!("Handle other data types"),
            };

            let data_point = DataPoint {
                id: 0,
                timestamp,
                value: DataPointValue::Numeric(numerical_value),
            };

            data_points.push(data_point);
        }

        let data_series = DataSeries {
            dataseries_id,
            alias: alias.to_string(),
            values: data_points,
        };

        dataseries.push(data_series);
    }

    Ok(())
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }

    struct MyState {
        name: String,
        count: usize,
    }

    #[test]
    fn learning_wasm() -> Result<()> {
        // First the wasm module needs to be compiled. This is done with a global
        // "compilation environment" within an `Engine`. Note that engines can be
        // further configured through `Config` if desired instead of using the
        // default like this is here.
        println!("Compiling module...");
        let engine = Engine::default();

        let wat = r#"
            (module
                (func $hello (import "" "hello"))
                (func (export "run") (call $hello))
            )
        "#;

        let module = Module::new(&engine, wat)?;

        // After a module is compiled we create a `Store` which will contain
        // instantiated modules and other items like host functions. A Store
        // contains an arbitrary piece of host information, and we use `MyState`
        // here.
        println!("Initializing...");
        let mut store = Store::new(
            &engine,
            MyState {
                name: "hello, world!".to_string(),
                count: 0,
            },
        );

        // Our wasm module we'll be instantiating requires one imported function.
        // the function takes no parameters and returns no results. We create a host
        // implementation of that function here, and the `caller` parameter here is
        // used to get access to our original `MyState` value.
        println!("Creating callback...");
        let hello_func = Func::wrap(&mut store, |mut caller: Caller<'_, MyState>| {
            println!("Calling back...");
            println!("> {}", caller.data().name);
            caller.data_mut().count += 1;
        });

        // Once we've got that all set up we can then move to the instantiation
        // phase, pairing together a compiled module as well as a set of imports.
        // Note that this is where the wasm `start` function, if any, would run.
        println!("Instantiating module...");
        let imports = [hello_func.into()];
        let instance = Instance::new(&mut store, &module, &imports)?;

        // Next we poke around a bit to extract the `run` function from the module.
        println!("Extracting export...");
        let run = instance.get_typed_func::<(), ()>(&mut store, "run")?;

        // And last but not least we can call it!
        println!("Calling export...");
        run.call(&mut store, ())?;

        println!("Done.");
        Ok(())
    }
}