use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use arrow::{
    array::RecordBatchIterator,
    datatypes::{DataType, Schema, SchemaRef},
    ffi_stream::FFI_ArrowArrayStream,
};
use async_trait::async_trait;
use datafusion::{catalog::TableProvider, sql::TableReference};
use datafusion_differential_testing::{
    DiscoveredTable, Error, LoadReport, ManagedProvider, ProviderFactory, ProviderIdentity,
    ProviderInstance, ProviderLifecycle, RemoteTableRef, ResolvedParams, Result, TableSelection,
    UnresolvedParams,
};
use datafusion_table_providers::{
    duckdb::DuckDBTableFactory, sql::db_connection_pool::duckdbpool::DuckDbConnectionPool,
};
use duckdb::{AccessMode, Config, Connection};
use futures::TryStreamExt;
use tokio::sync::Mutex;

#[derive(Debug, Default)]
pub struct DuckDbProviderFactory;

#[async_trait]
impl ProviderFactory for DuckDbProviderFactory {
    fn kind(&self) -> &'static str {
        "duckdb"
    }

    fn validate(&self, params: &UnresolvedParams) -> Result<()> {
        validate_params(params, "duckdb_open")
    }

    async fn connect(
        &self,
        name: &str,
        params: &ResolvedParams,
        lifecycle: &ProviderLifecycle,
    ) -> Result<Arc<dyn ProviderInstance>> {
        let path = PathBuf::from(required(params, "duckdb_open")?);
        let read_only = matches!(lifecycle, ProviderLifecycle::Existing);
        if read_only && !path.exists() {
            return Err(provider_message(
                name,
                format!("existing DuckDB file `{}` does not exist", path.display()),
            ));
        }
        if !read_only {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(|error| provider_error(name, error))?;
            }
        }
        open_connection(&path, read_only).map_err(|error| provider_error(name, error))?;
        Ok(Arc::new(DuckDbInstance {
            name: name.to_owned(),
            path,
            read_only,
            factory: Mutex::new(None),
            providers: Mutex::new(BTreeMap::new()),
        }))
    }
}

struct DuckDbInstance {
    name: String,
    path: PathBuf,
    read_only: bool,
    factory: Mutex<Option<Arc<DuckDBTableFactory>>>,
    providers: Mutex<BTreeMap<RemoteTableRef, Arc<dyn TableProvider>>>,
}

#[async_trait]
impl ProviderInstance for DuckDbInstance {
    fn identity(&self) -> ProviderIdentity {
        ProviderIdentity {
            name: self.name.clone(),
            kind: "duckdb".to_owned(),
        }
    }

    fn managed(&self) -> Option<&dyn ManagedProvider> {
        (!self.read_only).then_some(self)
    }

    async fn table_exists(&self, table: &RemoteTableRef) -> Result<bool> {
        self.table_exists_impl(table)
    }

    async fn discover(&self, _selection: &TableSelection) -> Result<Vec<DiscoveredTable>> {
        let connection = open_connection(&self.path, self.read_only)
            .map_err(|error| provider_error(&self.name, error))?;
        let mut statement = connection
            .prepare("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY table_name")
            .map_err(|error| provider_error(&self.name, error))?;
        let names = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|error| provider_error(&self.name, error))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| provider_error(&self.name, error))?;
        Ok(names
            .into_iter()
            .map(|name| DiscoveredTable {
                remote: RemoteTableRef(name.clone()),
                logical_name: name,
            })
            .collect())
    }

    async fn table_provider(&self, table: &RemoteTableRef) -> Result<Arc<dyn TableProvider>> {
        let mut providers = self.providers.lock().await;
        if let Some(provider) = providers.get(table) {
            return Ok(Arc::clone(provider));
        }
        let factory = {
            let mut factory = self.factory.lock().await;
            if let Some(factory) = factory.as_ref() {
                Arc::clone(factory)
            } else {
                let path = self.path.to_str().ok_or_else(|| {
                    provider_message(&self.name, "duckdb_open is not valid UTF-8")
                })?;
                let access = if self.read_only {
                    AccessMode::ReadOnly
                } else {
                    AccessMode::ReadWrite
                };
                let pool = Arc::new(
                    DuckDbConnectionPool::new_file(path, &access)
                        .map_err(|error| provider_error(&self.name, error))?,
                );
                let created = Arc::new(DuckDBTableFactory::new(pool));
                *factory = Some(Arc::clone(&created));
                created
            }
        };
        let provider = factory
            .table_provider(TableReference::bare(table.0.as_str()))
            .await
            .map_err(|error| provider_error(&self.name, error))?;
        providers.insert(table.clone(), Arc::clone(&provider));
        Ok(provider)
    }
}

#[async_trait]
impl ManagedProvider for DuckDbInstance {
    fn validate_schema(&self, schema: &Schema) -> Result<()> {
        for field in schema.fields() {
            duckdb_type(field.data_type())
                .map_err(|message| provider_message(&self.name, message))?;
        }
        Ok(())
    }

    async fn table_exists(&self, table: &RemoteTableRef) -> Result<bool> {
        self.table_exists_impl(table)
    }

    async fn create_table(&self, table: &RemoteTableRef, schema: SchemaRef) -> Result<()> {
        self.validate_schema(schema.as_ref())?;
        let columns = schema
            .fields()
            .iter()
            .map(|field| {
                format!(
                    "{} {}{}",
                    quote(field.name()),
                    duckdb_type(field.data_type()).expect("schema was validated"),
                    if field.is_nullable() { "" } else { " NOT NULL" }
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        Connection::open(&self.path)
            .and_then(|connection| {
                connection.execute_batch(&format!("CREATE TABLE {} ({columns})", quote(&table.0)))
            })
            .map_err(|error| provider_error(&self.name, error))
    }

    async fn load(
        &self,
        table: &RemoteTableRef,
        batches: datafusion::physical_plan::SendableRecordBatchStream,
    ) -> Result<LoadReport> {
        let schema = batches.schema();
        let batches = batches
            .try_collect::<Vec<_>>()
            .await
            .map_err(|error| provider_error(&self.name, error))?;
        let rows_loaded = batches.iter().map(|batch| batch.num_rows() as u64).sum();
        if batches.is_empty() {
            return Ok(LoadReport { rows_loaded });
        }
        let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
        let stream = FFI_ArrowArrayStream::new(Box::new(reader));
        let view = format!("__datafusion_diff_{}", safe_identifier(&table.0));
        let connection =
            Connection::open(&self.path).map_err(|error| provider_error(&self.name, error))?;
        connection
            .register_arrow_scan_view(&view, &stream)
            .map_err(|error| provider_error(&self.name, error))?;
        connection
            .execute_batch(&format!(
                "INSERT INTO {} SELECT * FROM {}",
                quote(&table.0),
                quote(&view)
            ))
            .map_err(|error| provider_error(&self.name, error))?;
        connection
            .execute_batch(&format!("DROP VIEW {}", quote(&view)))
            .map_err(|error| provider_error(&self.name, error))?;
        Ok(LoadReport { rows_loaded })
    }

    async fn drop_table(&self, table: &RemoteTableRef) -> Result<()> {
        Connection::open(&self.path)
            .and_then(|connection| {
                connection.execute_batch(&format!("DROP TABLE {}", quote(&table.0)))
            })
            .map_err(|error| provider_error(&self.name, error))?;
        self.providers.lock().await.remove(table);
        Ok(())
    }
}

impl DuckDbInstance {
    fn table_exists_impl(&self, table: &RemoteTableRef) -> Result<bool> {
        let connection = open_connection(&self.path, self.read_only)
            .map_err(|error| provider_error(&self.name, error))?;
        let count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main' AND table_name = ?",
                [&table.0],
                |row| row.get(0),
            )
            .map_err(|error| provider_error(&self.name, error))?;
        Ok(count != 0)
    }
}

fn open_connection(path: &std::path::Path, read_only: bool) -> duckdb::Result<Connection> {
    if read_only {
        Connection::open_with_flags(path, Config::default().access_mode(AccessMode::ReadOnly)?)
    } else {
        Connection::open(path)
    }
}

fn duckdb_type(data_type: &DataType) -> std::result::Result<&'static str, &'static str> {
    match data_type {
        DataType::Boolean => Ok("BOOLEAN"),
        DataType::Int8 => Ok("TINYINT"),
        DataType::Int16 => Ok("SMALLINT"),
        DataType::Int32 => Ok("INTEGER"),
        DataType::Int64 => Ok("BIGINT"),
        DataType::UInt8 => Ok("UTINYINT"),
        DataType::UInt16 => Ok("USMALLINT"),
        DataType::UInt32 => Ok("UINTEGER"),
        DataType::UInt64 => Ok("UBIGINT"),
        DataType::Float32 => Ok("FLOAT"),
        DataType::Float64 => Ok("DOUBLE"),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok("VARCHAR"),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => Ok("BLOB"),
        DataType::Date32 | DataType::Date64 => Ok("DATE"),
        DataType::Timestamp(_, _) => Ok("TIMESTAMP"),
        _ => Err("DuckDB adapter does not support this Arrow type"),
    }
}

fn validate_params(params: &UnresolvedParams, required_key: &str) -> Result<()> {
    if !params.contains_key(required_key) {
        return Err(provider_message(
            "duckdb",
            format!("missing `{required_key}` parameter"),
        ));
    }
    for key in params.keys() {
        if key != required_key {
            return Err(provider_message(
                "duckdb",
                format!("unknown parameter `{key}`"),
            ));
        }
    }
    Ok(())
}

fn required<'a>(params: &'a ResolvedParams, key: &str) -> Result<&'a str> {
    params
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| provider_message("duckdb", format!("missing `{key}` parameter")))
}

fn quote(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn safe_identifier(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '_'
            }
        })
        .collect()
}

fn provider_error(name: &str, error: impl std::fmt::Display) -> Error {
    provider_message(name, error.to_string())
}

fn provider_message(name: &str, message: impl Into<String>) -> Error {
    Error::Provider {
        provider: name.to_owned(),
        message: message.into(),
    }
}
