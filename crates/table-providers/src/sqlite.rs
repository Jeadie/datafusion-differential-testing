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
        // A missing `sqlite_open` key means the provider runs against an in-memory database.
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
        let read_only = matches!(lifecycle, ProviderLifecycle::Existing);
        let (target, guard) = match params.get("sqlite_open") {
            Some(raw) => {
                let path = PathBuf::from(raw);
                if read_only && !path.exists() {
                    return Err(provider_message(
                        name,
                        format!("existing SQLite file `{}` does not exist", path.display()),
                    ));
                }
                if !read_only {
                    if let Some(parent) = path.parent() {
                        std::fs::create_dir_all(parent)
                            .map_err(|error| provider_error(name, error))?;
                    }
                }
                open_connection(&path, read_only).map_err(|error| provider_error(name, error))?;
                (SqliteTarget::File(path), None)
            }
            None => {
                if read_only {
                    return Err(provider_message(
                        name,
                        "existing SQLite provider requires a `sqlite_open` path; an in-memory database has no backing file",
                    ));
                }
                // A shared-cache in-memory database is dropped when its last
                // connection closes. Keep a guard connection open for the whole
                // life of the instance and derive a stable name from the
                // provider so every connection targets the same database.
                let uri = memory_uri(name);
                let guard =
                    open_memory(&uri).map_err(|error| provider_error(name, error))?;
                (SqliteTarget::Memory(uri), Some(guard))
            }
        };
        Ok(Arc::new(SqliteInstance {
            name: name.to_owned(),
            target,
            read_only,
            _guard: Mutex::new(guard),
            schemas: Mutex::new(BTreeMap::new()),
            providers: Mutex::new(BTreeMap::new()),
        }))
    }
}

enum SqliteTarget {
    File(PathBuf),
    Memory(String),
}

struct SqliteInstance {
    name: String,
    target: SqliteTarget,
    read_only: bool,
    /// Held open so a shared-cache in-memory database survives for the whole
    /// life of the instance. Unused for file-backed instances.
    _guard: Mutex<Option<Connection>>,
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
        let connection = self.open(self.read_only)?;
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
        let path = self.connection_string()?;
        let df_schema = Arc::new(DFSchema::try_from(schema.as_ref().clone())?);
        // Point the read factory at the same file/URI (in file mode) so a
        // shared-cache in-memory database resolves to the one database this
        // instance uses for DDL and loads. The default rusqlite open flags
        // enable URI handling, so the `file:...?mode=memory&cache=shared` URI
        // is honoured here too.
        let command = CreateExternalTable::builder(
            TableReference::bare(table.0.as_str()),
            &path,
            "SQLITE",
            df_schema,
        )
        .with_options(HashMap::from([
            ("file".to_owned(), path.clone()),
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
        self.open(false)?
            .execute_batch(&format!("CREATE TABLE {} ({columns})", quote(&table.0)))
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
        let mut connection = self.open(false)?;
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
        self.open(false)?
            .execute_batch(&format!("DROP TABLE {}", quote(&table.0)))
            .map_err(|error| provider_error(&self.name, error))?;
        self.schemas.lock().await.remove(table);
        self.providers.lock().await.remove(table);
        Ok(())
    }
}

impl SqliteInstance {
    /// Open a connection to this instance's backing database.
    ///
    /// For an in-memory instance every connection targets the same shared-cache
    /// URI, so all operations observe the same database. `read_only` is honoured
    /// only for file-backed instances; an in-memory instance is always managed.
    fn open(&self, read_only: bool) -> Result<Connection> {
        match &self.target {
            SqliteTarget::File(path) => {
                open_connection(path, read_only).map_err(|error| provider_error(&self.name, error))
            }
            SqliteTarget::Memory(uri) => {
                open_memory(uri).map_err(|error| provider_error(&self.name, error))
            }
        }
    }

    /// The connection string handed to the read factory: a file path or the
    /// shared-cache in-memory URI.
    fn connection_string(&self) -> Result<String> {
        match &self.target {
            SqliteTarget::File(path) => path
                .to_str()
                .map(str::to_owned)
                .ok_or_else(|| provider_message(&self.name, "sqlite_open is not valid UTF-8")),
            SqliteTarget::Memory(uri) => Ok(uri.clone()),
        }
    }

    fn table_exists_impl(&self, table: &RemoteTableRef) -> Result<bool> {
        let connection = self.open(self.read_only)?;
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
        let connection = self.open(true)?;
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

/// Build a stable shared-cache in-memory URI for a provider. Every connection
/// opened with this URI targets the same in-memory database while at least one
/// connection stays open.
fn memory_uri(name: &str) -> String {
    format!("file:ddt_memdb_{}?mode=memory&cache=shared", safe_name(name))
}

fn open_memory(uri: &str) -> rusqlite::Result<Connection> {
    Connection::open_with_flags(
        uri,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_URI,
    )
}

fn safe_name(value: &str) -> String {
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
        | DataType::UInt64 => Ok("INTEGER"),
        DataType::Float16 | DataType::Float32 | DataType::Float64 | DataType::Decimal128(_, _) => {
            Ok("REAL")
        }
        // SQLite has no native date/time type; store them as ISO-8601 TEXT so the
        // provider's read decoder (which keys off TEXT storage) round-trips them.
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Date32
        | DataType::Date64
        | DataType::Timestamp(_, _) => Ok("TEXT"),
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
        // Strings and date/time render as their textual form and must be quoted;
        // an unquoted `2024-01-01` would be evaluated by SQLite as arithmetic.
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Date32
        | DataType::Date64
        | DataType::Timestamp(_, _) => Ok(format!("'{}'", value.replace('\'', "''"))),
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{Int64Array, StringArray},
        datatypes::Field,
        record_batch::RecordBatch,
    };
    use datafusion::{physical_plan::stream::RecordBatchStreamAdapter, prelude::SessionContext};

    fn sample_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    #[tokio::test]
    async fn in_memory_roundtrip_shares_one_database() {
        let factory = SqliteProviderFactory;
        let params = ResolvedParams::new();
        let lifecycle = ProviderLifecycle::Managed { teardown: true };
        let instance = factory
            .connect("in_memory_sqlite", &params, &lifecycle)
            .await
            .expect("connect in-memory instance");
        let managed = instance.managed().expect("managed provider");
        let schema = sample_schema();
        let table = RemoteTableRef("people".to_owned());

        managed
            .create_table(&table, Arc::clone(&schema))
            .await
            .expect("create table");
        assert!(managed.table_exists(&table).await.expect("table exists"));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])),
            ],
        )
        .expect("record batch");
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter(vec![Ok(batch)]),
        ));
        let report = managed.load(&table, stream).await.expect("load rows");
        assert_eq!(report.rows_loaded, 3);

        let provider = instance
            .table_provider(&table)
            .await
            .expect("table provider");
        let ctx = SessionContext::new();
        ctx.register_table("people", provider)
            .expect("register table");
        let results = ctx
            .sql("SELECT id, name FROM people ORDER BY id")
            .await
            .expect("plan query")
            .collect()
            .await
            .expect("run query");
        let rows: usize = results.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 3);
    }

    #[tokio::test]
    async fn in_memory_rejects_existing_lifecycle() {
        let factory = SqliteProviderFactory;
        let params = ResolvedParams::new();
        let result = factory
            .connect("in_memory_sqlite", &params, &ProviderLifecycle::Existing)
            .await;
        assert!(matches!(result, Err(Error::Provider { .. })));
    }

    #[test]
    fn validate_accepts_empty_params_and_rejects_unknown() {
        let factory = SqliteProviderFactory;
        assert!(factory.validate(&UnresolvedParams::new()).is_ok());
        let mut params = UnresolvedParams::new();
        params.insert("sqlite_open".to_owned(), "db.sqlite".to_owned());
        assert!(factory.validate(&params).is_ok());
        let mut unknown = UnresolvedParams::new();
        unknown.insert("nope".to_owned(), "x".to_owned());
        assert!(factory.validate(&unknown).is_err());
    }
}
