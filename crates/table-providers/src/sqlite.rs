use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    sync::Arc,
};

use arrow::{
    array::Array,
    datatypes::{DataType, Schema, SchemaRef},
    util::display::array_value_to_string,
};
use async_trait::async_trait;
use datafusion::{
    catalog::{TableProvider, TableProviderFactory},
    common::DFSchema,
    execution::session_state::SessionStateBuilder,
    logical_expr::CreateExternalTable,
    sql::TableReference,
};
use datafusion_differential_testing::{
    DiscoveredTable, Error, LoadReport, ManagedProvider, ProviderFactory, ProviderIdentity,
    ProviderInstance, ProviderLifecycle, RemoteTableRef, ResolvedParams, Result, TableSelection,
    UnresolvedParams,
};
use datafusion_table_providers::sqlite::{write::SqliteTableWriter, SqliteTableProviderFactory};
use futures::TryStreamExt;
use rusqlite::{Connection, OpenFlags};
use tokio::sync::Mutex;

#[derive(Debug, Default)]
pub struct SqliteProviderFactory;

#[async_trait]
impl ProviderFactory for SqliteProviderFactory {
    fn kind(&self) -> &'static str {
        "sqlite"
    }

    fn validate(&self, params: &UnresolvedParams) -> Result<()> {
        if !params.contains_key("sqlite_open") {
            return Err(provider_message(
                "sqlite",
                "missing `sqlite_open` parameter",
            ));
        }
        for key in params.keys() {
            if key != "sqlite_open" {
                return Err(provider_message(
                    "sqlite",
                    format!("unknown parameter `{key}`"),
                ));
            }
        }
        Ok(())
    }

    async fn connect(
        &self,
        name: &str,
        params: &ResolvedParams,
        lifecycle: &ProviderLifecycle,
    ) -> Result<Arc<dyn ProviderInstance>> {
        let path = PathBuf::from(
            params
                .get("sqlite_open")
                .ok_or_else(|| provider_message(name, "missing `sqlite_open` parameter"))?,
        );
        let read_only = matches!(lifecycle, ProviderLifecycle::Existing);
        if read_only && !path.exists() {
            return Err(provider_message(
                name,
                format!("existing SQLite file `{}` does not exist", path.display()),
            ));
        }
        if !read_only {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(|error| provider_error(name, error))?;
            }
        }
        open_connection(&path, read_only).map_err(|error| provider_error(name, error))?;
        Ok(Arc::new(SqliteInstance {
            name: name.to_owned(),
            path,
            read_only,
            schemas: Mutex::new(BTreeMap::new()),
            providers: Mutex::new(BTreeMap::new()),
        }))
    }
}

struct SqliteInstance {
    name: String,
    path: PathBuf,
    read_only: bool,
    schemas: Mutex<BTreeMap<RemoteTableRef, SchemaRef>>,
    providers: Mutex<BTreeMap<RemoteTableRef, Arc<dyn TableProvider>>>,
}

#[async_trait]
impl ProviderInstance for SqliteInstance {
    fn identity(&self) -> ProviderIdentity {
        ProviderIdentity {
            name: self.name.clone(),
            kind: "sqlite".to_owned(),
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
            .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
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
        let cached_schema = {
            let schemas = self.schemas.lock().await;
            schemas.get(table).cloned()
        };
        let schema = if let Some(schema) = cached_schema {
            schema
        } else {
            let schema = self.read_schema(table)?;
            self.schemas
                .lock()
                .await
                .insert(table.clone(), Arc::clone(&schema));
            schema
        };
        let path = self
            .path
            .to_str()
            .ok_or_else(|| provider_message(&self.name, "sqlite_open is not valid UTF-8"))?;
        let df_schema = Arc::new(DFSchema::try_from(schema.as_ref().clone())?);
        let command = CreateExternalTable::builder(
            TableReference::bare(table.0.as_str()),
            path,
            "SQLITE",
            df_schema,
        )
        .with_options(HashMap::from([
            ("file".to_owned(), path.to_owned()),
            ("mode".to_owned(), "file".to_owned()),
        ]))
        .build();
        let state = SessionStateBuilder::new().with_default_features().build();
        let provider = SqliteTableProviderFactory::new()
            .create(&state, &command)
            .await
            .map_err(|error| provider_error(&self.name, error))?;
        let writer = provider
            .downcast_ref::<SqliteTableWriter>()
            .ok_or_else(|| {
                provider_message(&self.name, "SQLite factory returned an unexpected provider")
            })?;
        let provider = writer.read_provider.clone();
        providers.insert(table.clone(), Arc::clone(&provider));
        Ok(provider)
    }
}

#[async_trait]
impl ManagedProvider for SqliteInstance {
    fn validate_schema(&self, schema: &Schema) -> Result<()> {
        for field in schema.fields() {
            sqlite_type(field.data_type())
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
                    sqlite_type(field.data_type()).expect("schema was validated"),
                    if field.is_nullable() { "" } else { " NOT NULL" }
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        Connection::open(&self.path)
            .and_then(|connection| {
                connection.execute_batch(&format!("CREATE TABLE {} ({columns})", quote(&table.0)))
            })
            .map_err(|error| provider_error(&self.name, error))?;
        self.schemas.lock().await.insert(table.clone(), schema);
        Ok(())
    }

    async fn load(
        &self,
        table: &RemoteTableRef,
        batches: datafusion::physical_plan::SendableRecordBatchStream,
    ) -> Result<LoadReport> {
        let batches = batches
            .try_collect::<Vec<_>>()
            .await
            .map_err(|error| provider_error(&self.name, error))?;
        let mut connection =
            Connection::open(&self.path).map_err(|error| provider_error(&self.name, error))?;
        let transaction = connection
            .transaction()
            .map_err(|error| provider_error(&self.name, error))?;
        let mut rows_loaded = 0_u64;
        for batch in batches {
            for row in 0..batch.num_rows() {
                let values = batch
                    .columns()
                    .iter()
                    .map(|array| sql_literal(array.as_ref(), row))
                    .collect::<Result<Vec<_>>>()?
                    .join(", ");
                transaction
                    .execute_batch(&format!(
                        "INSERT INTO {} VALUES ({values})",
                        quote(&table.0)
                    ))
                    .map_err(|error| provider_error(&self.name, error))?;
                rows_loaded += 1;
            }
        }
        transaction
            .commit()
            .map_err(|error| provider_error(&self.name, error))?;
        Ok(LoadReport { rows_loaded })
    }

    async fn drop_table(&self, table: &RemoteTableRef) -> Result<()> {
        Connection::open(&self.path)
            .and_then(|connection| {
                connection.execute_batch(&format!("DROP TABLE {}", quote(&table.0)))
            })
            .map_err(|error| provider_error(&self.name, error))?;
        self.schemas.lock().await.remove(table);
        self.providers.lock().await.remove(table);
        Ok(())
    }
}

impl SqliteInstance {
    fn table_exists_impl(&self, table: &RemoteTableRef) -> Result<bool> {
        let connection = open_connection(&self.path, self.read_only)
            .map_err(|error| provider_error(&self.name, error))?;
        let count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
                [&table.0],
                |row| row.get(0),
            )
            .map_err(|error| provider_error(&self.name, error))?;
        Ok(count != 0)
    }

    fn read_schema(&self, table: &RemoteTableRef) -> Result<SchemaRef> {
        let connection =
            open_connection(&self.path, true).map_err(|error| provider_error(&self.name, error))?;
        let mut statement = connection
            .prepare(&format!("PRAGMA table_info({})", quote(&table.0)))
            .map_err(|error| provider_error(&self.name, error))?;
        let fields = statement
            .query_map([], |row| {
                let name: String = row.get(1)?;
                let native_type: String = row.get(2)?;
                let not_null: bool = row.get(3)?;
                Ok((name, native_type, !not_null))
            })
            .map_err(|error| provider_error(&self.name, error))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| provider_error(&self.name, error))?
            .into_iter()
            .map(|(name, native_type, nullable)| {
                sqlite_arrow_type(&native_type)
                    .map(|data_type| arrow::datatypes::Field::new(name, data_type, nullable))
                    .map_err(|message| provider_message(&self.name, message))
            })
            .collect::<Result<Vec<_>>>()?;
        if fields.is_empty() {
            return Err(provider_message(
                &self.name,
                format!("SQLite table `{}` does not exist", table.0),
            ));
        }
        Ok(Arc::new(Schema::new(fields)))
    }
}

fn open_connection(path: &std::path::Path, read_only: bool) -> rusqlite::Result<Connection> {
    if read_only {
        Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
    } else {
        Connection::open(path)
    }
}

fn sqlite_arrow_type(native_type: &str) -> std::result::Result<DataType, String> {
    let normalized = native_type.trim().to_ascii_uppercase();
    if normalized.contains("INT") {
        Ok(DataType::Int64)
    } else if normalized.contains("CHAR")
        || normalized.contains("CLOB")
        || normalized.contains("TEXT")
    {
        Ok(DataType::Utf8)
    } else if normalized.contains("BLOB") || normalized.is_empty() {
        Ok(DataType::Binary)
    } else if normalized.contains("REAL")
        || normalized.contains("FLOA")
        || normalized.contains("DOUB")
    {
        Ok(DataType::Float64)
    } else {
        Err(format!(
            "SQLite declared type `{native_type}` cannot map exactly to Arrow"
        ))
    }
}

fn sqlite_type(data_type: &DataType) -> std::result::Result<&'static str, &'static str> {
    match data_type {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Date32
        | DataType::Date64
        | DataType::Timestamp(_, _) => Ok("INTEGER"),
        DataType::Float16 | DataType::Float32 | DataType::Float64 | DataType::Decimal128(_, _) => {
            Ok("REAL")
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok("TEXT"),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => Ok("BLOB"),
        _ => Err("SQLite adapter does not support this Arrow type"),
    }
}

fn sql_literal(array: &dyn Array, row: usize) -> Result<String> {
    if array.is_null(row) {
        return Ok("NULL".to_owned());
    }
    let value = array_value_to_string(array, row)?;
    match array.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            Ok(format!("'{}'", value.replace('\'', "''")))
        }
        DataType::Boolean => Ok(if value == "true" { "1" } else { "0" }.to_owned()),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => Err(provider_message(
            "sqlite",
            "binary loading is not available",
        )),
        _ => Ok(value),
    }
}

fn quote(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
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
