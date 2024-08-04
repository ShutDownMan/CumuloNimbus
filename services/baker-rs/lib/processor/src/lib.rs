use anyhow::{Context, Result};
use sqlx::{postgres::PgPool, sqlite::SqlitePool, Acquire};
use std::{result, sync::Arc, vec};
use tracing::{info, debug};
use sqlx::Row;
use uuid::Uuid;
use sqlx::types::chrono::DateTime;
use wasmtime::*;
use std::collections::HashMap;

extern crate intercom;

use intercom::schemas;
use crate::intercom::HasTypeId;

type DataSeriesReader<'a, T> = intercom::schemas::dataseries_capnp::data_series::Reader<'a, T>;
type NumericDataSeriesReader<'a> = DataSeriesReader<'a, intercom::schemas::dataseries_capnp::numeric_data_point::Owned>;
type ComputeDataSeriesReader<'a, T> = intercom::schemas::baker_capnp::compute_data_series::Reader<'a, T>;
type ComputeNumericDataSeriesReader<'a> = ComputeDataSeriesReader<'a, intercom::schemas::dataseries_capnp::numeric_data_point::Owned>;

pub trait DataPointType: Sized + Clone + std::fmt::Debug {
    // fn encode(&self) -> String;
    // fn decode<'r, DB: sqlx::Database>(value: <DB as sqlx::database::HasValueRef<'r>>::ValueRef) -> Result<Self>
    // where &'r str: sqlx::Decode<'r, DB>;

    // fn try_get_numerical(&self) -> Result<f64>;
    // fn try_get_text(&self) -> Result<String>;
}

#[derive(Clone, Debug)]
pub struct DataSeries<T>
where T: DataPointType {
    pub metadata: DataSeriesMetadata,
    pub values: Vec<DataPoint<T>>,
}

#[derive(Clone, Debug)]
pub struct DataSeriesMetadata {
    pub external_id: Uuid,
    pub name: String,
    pub description: String,
    pub data_type: intercom::schemas::dataseries_capnp::DataType,
    pub interpolation_strategy: intercom::schemas::baker_capnp::InterpolationStrategy,
    pub time_window: u64,
}

#[derive(Clone, Debug)]
pub struct DataPoint<T>
where T: DataPointType {
    pub id: i64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub value: T,
}

#[derive(Clone, Debug)]
pub struct NumericDataPoint {
    pub numeric: f64,
}

impl DataPointType for NumericDataPoint {}

#[derive(Clone, Debug)]
pub struct TextDataPoint {
    pub text: String,
}

impl DataPointType for TextDataPoint {}

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

        let store_recipe_type_id = ComputeNumericDataSeriesReader::TYPE_ID;
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
    reader: intercom::CapnpMessageReader,
    metadata: intercom::LapinAMQPProperties
) -> Result<()> {
    let root = reader.get_root::<schemas::baker_capnp::store_recipe::Reader>()?;
    let recipe_id = root.get_id()?;

    info!("Persisting data series with id: {:?}", recipe_id);

    persist_recipe_from_message(db_pool, reader, metadata).await
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

async fn handle_compute_data_series(
    db_pool: Arc<PgPool>,
    reader: intercom::CapnpMessageReader,
    metadata: intercom::LapinAMQPProperties
) -> Result<()> {
    let root = reader.get_root::<ComputeNumericDataSeriesReader>()?;
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

    Ok(())
}


#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use tracing::field::debug;

    use super::*;

    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }

    #[test]
    fn learning_wasm() -> Result<()> {
        struct MyState {
            name: String,
            count: usize,
        }

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

    struct MyState {
        dataseries: HashMap<i64, DataSeries<NumericDataPoint>>,
    }

    /*
        extern int64_t _get_dataseries_id_by_alias(const char* id);
        extern int64_t _copy_dataseries(int64_t dataseries_id);
        extern int64_t _set_interpolation_strategy(int64_t dataseries_id, int64_t interpolation_strategy);
        extern int64_t _set_time_window(int64_t dataseries_id, int64_t time_window);
        extern int64_t _add(int64_t dataseries_a_id, int64_t dataseries_b_id);
        extern int64_t _mirroring(int64_t dataseries_id, int64_t reference_dataseries_ids[], int32_t reference_dataseries_ids_count);
        extern void _set_result_dataseries(int64_t dataseries_id);
    */
    impl MyState {
        fn get_dataseries_id_by_alias(&self, alias: &str) -> Option<i64> {
            self.dataseries.iter().find(|(_, ds)| ds.metadata.name == alias).map(|(id, _)| id.clone())
        }

        fn copy_dataseries(&mut self, dataseries_id: i64) -> i64 {
            let dataseries = self.dataseries.get(&dataseries_id).unwrap();
            let new_dataseries = DataSeries {
                metadata: dataseries.metadata.clone(),
                values: dataseries.values.clone(),
            };
            let new_id = self.dataseries.len() as i64 + 1;
            self.dataseries.insert(new_id, new_dataseries);
            new_id
        }

        fn set_interpolation_strategy(&mut self, dataseries_id: i64, interpolation_strategy: i64) -> i64 {
            let dataseries = self.dataseries.get_mut(&dataseries_id).unwrap();
            dataseries.metadata.interpolation_strategy = match interpolation_strategy {
                1 => intercom::schemas::baker_capnp::InterpolationStrategy::Locf,
                2 => intercom::schemas::baker_capnp::InterpolationStrategy::Lerp,
                _ => intercom::schemas::baker_capnp::InterpolationStrategy::NoInterpolation,
            };
            dataseries_id
        }

        fn set_time_window(&mut self, dataseries_id: i64, time_window: u64) -> i64 {
            let dataseries = self.dataseries.get_mut(&dataseries_id).unwrap();
            dataseries.metadata.time_window = time_window;
            dataseries_id
        }

        fn get_value(&self, dataseries_id: i64, timestamp: DateTime<chrono::Utc>) -> Option<f64> {
            let dataseries = self.dataseries.get(&dataseries_id).unwrap();
            let value = match dataseries.metadata.interpolation_strategy {
                intercom::schemas::baker_capnp::InterpolationStrategy::Locf => {
                    let mut last = None;
                    for dp in dataseries.values.iter() {
                        if dp.timestamp <= timestamp {
                            last = Some(dp);
                        } else {
                            break;
                        }
                    }

                    match last {
                        Some(last) => {
                            let time_distance = timestamp.timestamp_nanos() - last.timestamp.timestamp_nanos();
                            let time_window = dataseries.metadata.time_window;

                            println!("Time distance: {:?}", time_distance);
                            println!("Time window: {:?}", time_window);

                            if time_distance <= time_window.try_into().unwrap() {
                                Some(last.value.numeric)
                            } else {
                                None
                            }
                        }
                        None => None,
                    }
                },
                intercom::schemas::baker_capnp::InterpolationStrategy::Lerp => {
                    let mut lower = None;
                    let mut upper = None;
                    for dp in dataseries.values.iter() {
                        if dp.timestamp <= timestamp {
                            lower = Some(dp);
                        } else {
                            upper = Some(dp);
                            break;
                        }
                    }

                    match (lower, upper) {
                        (Some(lower), Some(upper)) => {
                            let lower_value = lower.value.numeric;
                            let upper_value = upper.value.numeric;
                            let lower_timestamp = lower.timestamp;
                            let upper_timestamp = upper.timestamp;
                            let time_diff = upper_timestamp - lower_timestamp;
                            let value_diff = upper_value - lower_value;
                            let time_diff_seconds = time_diff.num_seconds() as f64;
                            let time_diff_total_seconds = time_diff_seconds + time_diff.num_microseconds().unwrap() as f64 / 1_000_000.0;
                            let time_diff_ratio = (timestamp - lower_timestamp).num_seconds() as f64 + (timestamp - lower_timestamp).num_microseconds().unwrap() as f64 / 1_000_000.0;
                            let value_diff_ratio = value_diff * time_diff_ratio / time_diff_total_seconds;
                            Some(lower_value + value_diff_ratio)
                        },
                        (Some(lower), None) => Some(lower.value.numeric),
                        (None, Some(upper)) => Some(upper.value.numeric),
                        (None, None) => None,
                    }
                },
                _ => {
                    println!("No interpolation strategy");

                    None
                },
            };

            value
        }

        fn add(&mut self, dataseries_a_id: i64, dataseries_b_id: i64) -> i64 {
            let dataseries_a = self.dataseries.get(&dataseries_a_id).unwrap();
            let dataseries_b = self.dataseries.get(&dataseries_b_id).unwrap();

            let mut new_timestamps = vec![];
            for dp in dataseries_a.values.iter() {
                new_timestamps.push(dp.timestamp);
            }
            for dp in dataseries_b.values.iter() {
                new_timestamps.push(dp.timestamp);
            }
            new_timestamps.sort();
            new_timestamps.dedup();

            let mut new_values = vec![];
            for timestamp in new_timestamps.iter() {
                let value_a = self.get_value(dataseries_a_id, *timestamp);
                let value_b = self.get_value(dataseries_b_id, *timestamp);
                match (value_a, value_b) {
                    (Some(value_a), Some(value_b)) => {
                        new_values.push(DataPoint {
                            id: 0,
                            timestamp: *timestamp,
                            value: NumericDataPoint {
                                numeric: value_a + value_b,
                            }
                        });
                    },
                    _ => {
                        println!("No value found for timestamp: {:?}", timestamp);
                    }
                }
            }

            let new_dataseries = DataSeries {
                metadata: DataSeriesMetadata {
                    external_id: Uuid::nil(),
                    name: "result".to_string(),
                    description: "result dataseries".to_string(),
                    data_type: intercom::schemas::dataseries_capnp::DataType::Numeric,
                    interpolation_strategy: intercom::schemas::baker_capnp::InterpolationStrategy::NoInterpolation,
                    time_window: 0,
                },
                values: new_values,
            };

            let new_id = self.dataseries.len() as i64 + 1;
            self.dataseries.insert(new_id, new_dataseries);

            println!("Result dataseries id: {:?}", new_id);
            new_id
        }

        // update the dataseries to mirror the timestamps of the reference dataseries using the interpolation strategy
        fn mirroring(&mut self, dataseries_id: i64, reference_dataseries_ids: Vec<i64>) -> i64 {
            println!("Mirroring dataseries in state...");
            let reference_dataseries = reference_dataseries_ids.iter()
                .map(|id| self.dataseries.get(id))
                .collect::<Vec<Option<&DataSeries<NumericDataPoint>>>>();

            // check if all reference dataseries are present
            if reference_dataseries.iter().any(|ds| ds.is_none()) {
                return -1;
            }

            let reference_dataseries = reference_dataseries.iter()
                .map(|ds| ds.unwrap())
                .collect::<Vec<&DataSeries<NumericDataPoint>>>();

            let mut new_timestamps = vec![];
            for ds in reference_dataseries.iter() {
                for dp in ds.values.iter() {
                    new_timestamps.push(dp.timestamp);
                }
            }
            new_timestamps.sort();
            new_timestamps.dedup();

            let mut new_values = vec![];
            for timestamp in new_timestamps.iter() {
                let value = self.get_value(dataseries_id, *timestamp);
                match value {
                    Some(value) => {
                        new_values.push(DataPoint {
                            id: 0,
                            timestamp: *timestamp,
                            value: NumericDataPoint {
                                numeric: value,
                            }
                        });
                    },
                    None => {
                        println!("No value found for timestamp: {:?}", timestamp);
                    }
                }
            }

            let dataseries = self.dataseries.get_mut(&dataseries_id).unwrap();
            dataseries.values = new_values;

            dataseries_id
        }

    }

    #[test]
    fn simple_compute() -> Result<()> {
        // Initialize the state with dataseries
        let mut dataseries_a = DataSeries {
            metadata: DataSeriesMetadata {
                external_id: Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")?,
                name: "dataseries_a".to_string(),
                description: "test dataseries A".to_string(),
                data_type: intercom::schemas::dataseries_capnp::DataType::Numeric,
                interpolation_strategy: intercom::schemas::baker_capnp::InterpolationStrategy::NoInterpolation,
                time_window: 0,
            },
            values: vec![]
        };
        let mut dataseries_b = DataSeries {
            metadata: DataSeriesMetadata {
                external_id: Uuid::parse_str("7d5ce80b-8c2f-4861-b1de-55aa6d564f36")?,
                name: "dataseries_b".to_string(),
                description: "test dataseries B".to_string(),
                data_type: intercom::schemas::dataseries_capnp::DataType::Numeric,
                interpolation_strategy: intercom::schemas::baker_capnp::InterpolationStrategy::NoInterpolation,
                time_window: 0,
            },
            values: vec![]
        };

        // now - 24 hours
        let reference_time = chrono::Utc::now() - chrono::Duration::hours(24);
        for i in 0..10 {
            // reference time + i hours
            let timestamp: DateTime<chrono::Utc> = reference_time + chrono::Duration::minutes(i);
            dataseries_a.values.push(DataPoint {
                id: i,
                timestamp,
                value: NumericDataPoint {
                    numeric: 1.0 + (i as f64) * 1.0,
                }
            });
        }
        for i in 0..10 {
            // reference time + i hours
            let timestamp: DateTime<chrono::Utc> = reference_time + chrono::Duration::minutes(i + 5);
            dataseries_b.values.push(DataPoint {
                id: i,
                timestamp,
                value: NumericDataPoint {
                    numeric: 1.0 + (i as f64) * 1.0,
                }
            });
        }

        let mut state = MyState {
            dataseries: HashMap::new(),
        };

        state.dataseries.insert(1, dataseries_a);
        state.dataseries.insert(2, dataseries_b);

        let engine = Engine::default();
        let mut store = Store::new(&engine, state);

        // Create the linker and add the functions
        let mut linker = Linker::new(&engine);

        // void *_get_dataseries_id_by_alias(const char* id);
        linker.func_wrap("env", "_get_dataseries_id_by_alias", |mut caller: Caller<'_, MyState>, alias_ptr: i32| -> i64 {
            println!("Getting dataseries id by alias...");
            let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
            let (data, store) = memory.data_and_store_mut(&mut caller);
            // Read the alias from the memory
            let c_alias = {
                data.get(alias_ptr as usize..)
                    .unwrap()
                    .split(|&b| b == 0)
                    .next()
                    .unwrap()
            };

            let alias = std::str::from_utf8(c_alias).unwrap();
            println!("Alias: {:?}", alias);

            // Find the dataseries by alias
            let dataseries = store.get_dataseries_id_by_alias(alias);
            match dataseries {
                Some(id) => id,
                None => -1,
            }
        })?;

        // int64_t _copy_dataseries(int64_t dataseries_id);
        linker.func_wrap("env", "_copy_dataseries", |mut caller: Caller<'_, MyState>, dataseries_id: i64| -> i64 {
            println!("Copying dataseries...");
            let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
            let (_data, store) = memory.data_and_store_mut(&mut caller);

            let new_id = store.copy_dataseries(dataseries_id);

            println!("New dataseries id: {:?}", new_id);
            new_id
        })?;

        // int64_t _set_interpolation_strategy(int64_t dataseries_id, int64_t interpolation_strategy);
        linker.func_wrap("env", "_set_interpolation_strategy", |mut caller: Caller<'_, MyState>, dataseries_id: i64, interpolation_strategy: i64| -> i64 {
            println!("Setting interpolation strategy {} for dataseries id: {}", interpolation_strategy, dataseries_id);
            let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
            let (_data, store) = memory.data_and_store_mut(&mut caller);

            let new_id = store.set_interpolation_strategy(dataseries_id, interpolation_strategy);

            println!("Dataseries id: {:?}", new_id);
            new_id
        })?;

        // int64_t _set_time_window(int64_t dataseries_id, uint64_t time_window);
        linker.func_wrap("env", "_set_time_window", |mut caller: Caller<'_, MyState>, dataseries_id: i64, time_window: u64| -> i64 {
            println!("Setting time window...");
            let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
            let (_data, store) = memory.data_and_store_mut(&mut caller);

            let new_id = store.set_time_window(dataseries_id, time_window);

            println!("Dataseries id: {:?}", new_id);
            new_id
        })?;

        // int64_t _add(int64_t dataseries_a_id, int64_t dataseries_b_id);
        linker.func_wrap("env", "_add", |mut caller: Caller<'_, MyState>, dataseries_a_id: i64, dataseries_b_id: i64| -> i64 {
            println!("Adding dataseries {} and {}...", dataseries_a_id, dataseries_b_id);
            let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
            let (_data, store) = memory.data_and_store_mut(&mut caller);

            let new_id = store.add(dataseries_a_id, dataseries_b_id);

            println!("Result dataseries id: {:?}", new_id);
            new_id
        })?;

        // int64_t _mirroring(int64_t dataseries_id, int64_t reference_dataseries_ids[], int32_t reference_dataseries_ids_count);
        linker.func_wrap("env", "_mirroring", |mut caller: Caller<'_, MyState>, dataseries_id: i64, reference_dataseries_ids_ptr: i32, reference_dataseries_ids_count: i32| -> i64 {
            println!("Mirroring dataseries...");
            let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
            let (data, store) = memory.data_and_store_mut(&mut caller);

            println!("Reference dataseries count: {:?}", reference_dataseries_ids_count);
            let reference_dataseries_ids = {
                let mut ids = vec![];
                for i in 0..reference_dataseries_ids_count {
                    let id = data
                        .get((reference_dataseries_ids_ptr + i as i32 * 8) as usize..(reference_dataseries_ids_ptr + i as i32 * 8 + 8) as usize)
                        .unwrap();
                    let id = i64::from_le_bytes(id.try_into().unwrap());

                    println!("Reference dataseries id: {:?}", id);
                    ids.push(id);
                }
                ids
            };

            let id = store.mirroring(dataseries_id, reference_dataseries_ids);
            println!("Mirroring result dataseries id: {:?}", id);

            id
        })?;

        // void _set_result_dataseries(int64_t dataseries_id);
        linker.func_wrap("env", "_set_result_dataseries", |mut caller: Caller<'_, MyState>, dataseries_id: i64| {
            let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
            let (data, store) = memory.data_and_store_mut(&mut caller);

            println!("Result dataseries id: {:?}", dataseries_id);
            println!("Result dataseries: {:?}", store.dataseries.get(&dataseries_id).unwrap());

            let dataseries = store.dataseries.get(&dataseries_id).unwrap();

            for dp in dataseries.values.iter() {
                let timestamp = dp.timestamp;
                let value = dp.value.numeric;
                println!("{:?}: {:?}", timestamp, value);
            }

        })?;

        let compute_module = Module::from_file(&engine, "test.wasm")?;

        linker.module(&mut store, "test.wasm", &compute_module)?;

        // Run the compute function
        println!("Running compute function...");
        let compute = linker.get(&mut store, "test.wasm", "custom_entry").unwrap().into_func().unwrap();
        let params = vec![];
        let mut resultes = vec![];
        println!("Calling compute function...");
        let result = compute.call(&mut store, &params, &mut resultes);

        match result {
            Ok(_) => println!("Compute function executed successfully"),
            Err(e) => println!("Error executing compute function: {:?}", e),
        }

        Ok(())
    }
}

// clear && emcc test.c -g -o test.wasm -s ERROR_ON_UNDEFINED_SYMBOLS=0
// "C:\Users\Jedson Gabriel\Documents\WABT\bin\wasm2wat.exe" test.wasm > test.wat
// clear && emcc test.c -g -o test.wasm -s ERROR_ON_UNDEFINED_SYMBOLS=0 --no-entry  && "C:\Users\Jedson Gabriel\Documents\WABT\bin\wasm2wat.exe" test.wasm > test.wat