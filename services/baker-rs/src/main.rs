use anyhow::Result;
use sqlx::postgres::{PgPoolOptions, PgPool};
use sqlx::sqlite::{SqlitePoolOptions, SqlitePool};
use sqlx::{migrate::MigrateDatabase, migrate::Migrator, Postgres, Sqlite};
use std::path::Path;
use std::sync::Arc;
use tracing::{error, debug, info};
use tracing_subscriber::EnvFilter;
use std::env;

extern crate intercom;
extern crate recipe_keeper;

const SQLITE_DB_URL: &str = "sqlite://./db/baker.db";

/**
* Main function to start the baker service.
* this service is responsible for computing dataseries from recipes and
* dispatching it to the service bus when queried.
*/
#[tokio::main]
async fn main() -> Result<()> {
    info!("Starting Baker Service");
    init_log();

    let postgres_pool = Arc::new(init_postgres_pool().await?);
    let sqlite_pool = Arc::new(init_sqlite_pool().await?);

    let service_bus = Arc::new(init_service_bus().await?);
    service_bus
        .simple_queue_declare("baker.input", "persist-dataseries")
        .await?;

    let recipe_keeper = init_recipe_keeper(postgres_pool, service_bus).await?;

    recipe_keeper.handle_messages().await?;

    Ok(())
}

fn init_log() {
    // install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_env("LOG_LEVEL"))
        .with_thread_ids(true)
        .with_thread_names(true)
        .init();
}

async fn init_postgres_pool() -> Result<PgPool> {
    debug!("Fetching database URL from env");
    let db_url = env::var("BAKER_DB_URL")?;
    let max_connections = env::var("BAKER_DB_MAX_CONNECTIONS")
        .unwrap_or("3".into())
        .parse::<u32>()?;

    debug!("{:#?}", db_url);

    info!("checking if database exists");
    if !Postgres::database_exists(db_url.as_str()).await.unwrap_or(false) {
        info!("creating database {}", db_url);
        match Postgres::create_database(db_url.as_str()).await {
            Ok(_) => info!("Create db success"),
            Err(error) => panic!("error: {}", error),
        }
    } else {
        info!("database already exists");
    }

    info!("initializing database");
    let postgres_pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(db_url.as_str())
        .await
        .expect("failed to connect to postgres");

    info!("running migrations");
    let migrator = Migrator::new(Path::new("./pg_migrations")).await?;
    migrator.run(&postgres_pool).await?;
    info!("migrations complete");

    Ok(postgres_pool)
}

async fn init_sqlite_pool() -> Result<SqlitePool> {
    // create the database if it doesn't exist
    info!("checking if database exists");
    if !Sqlite::database_exists(SQLITE_DB_URL).await.unwrap_or(false) {
        info!("creating database {}", SQLITE_DB_URL);
        match Sqlite::create_database(SQLITE_DB_URL).await {
            Ok(_) => info!("Create db success"),
            Err(error) => panic!("error: {}", error),
        }
    } else {
        info!("database already exists");
    }

    // initialize the database
    info!("initializing database");
    let sqlite_pool = SqlitePoolOptions::new()
        .max_connections(1)
        .min_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(30))
        .connect(SQLITE_DB_URL)
        .await?;

    // run migrations
    info!("running migrations");
    let migrator = Migrator::new(Path::new("./sqlite_migrations")).await?;
    migrator.run(&sqlite_pool).await?;
    info!("migrations complete");

    Ok(sqlite_pool)
}

async fn init_service_bus() -> Result<intercom::ServiceBus> {
    debug!("Instantiating service bus");
    let service_bus = intercom::ServiceBus::new().await;
    if let Err(e) = service_bus {
        error!("service bus initialize failed: {:?}", e);
        return Err(e);
    }
    // TODO: remove unwrap and treat connection errors with retries
    let service_bus = service_bus.unwrap();

    Ok(service_bus)
}

async fn init_recipe_keeper(
    postgres_pool: Arc<PgPool>,
    service_bus: Arc<intercom::ServiceBus>,
) -> Result<recipe_keeper::RecipeKeeper> {
    recipe_keeper::RecipeKeeper::new(postgres_pool, service_bus, tokio::runtime::Handle::current())
}
