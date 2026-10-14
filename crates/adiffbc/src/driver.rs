//! Loads ADBC drivers and runs SQL on them, producing core [`QueryOutcome`]s.

use std::{
    panic::{catch_unwind, AssertUnwindSafe},
    sync::Arc,
    time::Instant,
};

use adbc_core::{
    error::Error as AdbcError,
    options::{InfoCode, OptionConnection, OptionDatabase, OptionValue},
    Connection, Database, Driver, Statement, LOAD_FLAG_DEFAULT,
};
use adbc_driver_manager::{ManagedConnection, ManagedDriver};
use arrow::{
    array::{Array, AsArray, RecordBatch},
    datatypes::{Schema, SchemaRef, UInt32Type},
    error::ArrowError,
};
use datafusion_differential_testing::{
    dataset::logical_hash, ExecutionError, QueryOutcome, QueryResult,
};

use crate::{
    config::{DriverLocation, DriverSpec, SchemaComparison, VariantSpec},
    sql::split_statements,
};

/// An open connection for one variant.
pub struct Session {
    connection: ManagedConnection,
}

impl Session {
    pub fn connect(variant: &VariantSpec) -> Result<Self, String> {
        let context = |error: AdbcError| format!("variant `{}`: {}", variant.name, error.message);
        let mut driver = load_driver(&variant.driver).map_err(|error| {
            format!(
                "variant `{}`: cannot load driver `{}`: {}",
                variant.name, variant.driver.name, error.message
            )
        })?;
        let database_options = variant
            .uri
            .iter()
            .map(|uri| (OptionDatabase::Uri, OptionValue::from(uri.as_str())))
            .chain(variant.database_options.iter().map(|(key, value)| {
                (
                    OptionDatabase::from(key.as_str()),
                    OptionValue::from(value.as_str()),
                )
            }));
        let database = driver
            .new_database_with_opts(database_options)
            .map_err(context)?;
        let connection_options = variant.connection_options.iter().map(|(key, value)| {
            (
                OptionConnection::from(key.as_str()),
                OptionValue::from(value.as_str()),
            )
        });
        let connection = database
            .new_connection_with_opts(connection_options)
            .map_err(context)?;
        Ok(Self { connection })
    }

    /// Run every statement of a setup script, discarding any results.
    pub fn execute_script(&mut self, sql: &str) -> Result<(), String> {
        for statement in split_statements(sql) {
            self.execute_update(statement)
                .map_err(|error| format!("{}\n  statement: {statement}", error.message))?;
        }
        Ok(())
    }

    fn execute_update(&mut self, sql: &str) -> Result<(), AdbcError> {
        let mut statement = self.connection.new_statement()?;
        statement.set_sql_query(sql)?;
        statement.execute_update()?;
        Ok(())
    }

    pub fn query(&mut self, sql: &str, schema: SchemaComparison) -> QueryOutcome {
        let started = Instant::now();
        let executed = catch_unwind(AssertUnwindSafe(|| self.collect(sql)));
        let elapsed = started.elapsed();
        let (result_schema, batches) = match executed {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => return QueryOutcome::Rejected(error),
            Err(payload) => {
                let message = payload
                    .downcast_ref::<&str>()
                    .map(|message| (*message).to_owned())
                    .or_else(|| payload.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "non-string panic payload".to_owned());
                return QueryOutcome::Panicked { message };
            }
        };
        let (result_schema, batches) = match schema {
            SchemaComparison::Strict => (result_schema, batches),
            SchemaComparison::Relaxed => match relax(&result_schema, batches) {
                Ok(relaxed) => relaxed,
                Err(error) => return QueryOutcome::Rejected(arrow_error(&error)),
            },
        };
        let content_hash = match logical_hash(&result_schema, &batches) {
            Ok(hash) => hash,
            Err(error) => {
                return QueryOutcome::Rejected(ExecutionError {
                    class: "encode".to_owned(),
                    message: error.to_string(),
                })
            }
        };
        QueryOutcome::Success(QueryResult {
            row_count: batches.iter().map(RecordBatch::num_rows).sum(),
            schema: result_schema,
            batches,
            content_hash,
            elapsed,
        })
    }

    fn collect(&mut self, sql: &str) -> Result<(SchemaRef, Vec<RecordBatch>), ExecutionError> {
        // The statement is declared first so it outlives the stream it produced.
        let mut statement = self.connection.new_statement().map_err(adbc_error)?;
        statement.set_sql_query(sql).map_err(adbc_error)?;
        let reader = statement.execute().map_err(adbc_error)?;
        let schema = reader.schema();
        let batches = reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| arrow_error(&error))?;
        Ok((schema, batches))
    }

    /// Driver and vendor name/version, as reported by the driver.
    pub fn info(&self) -> Result<Vec<(String, String)>, String> {
        let codes = [
            InfoCode::VendorName,
            InfoCode::VendorVersion,
            InfoCode::DriverName,
            InfoCode::DriverVersion,
        ];
        let reader = self
            .connection
            .get_info(Some(codes.into_iter().collect()))
            .map_err(|error| error.message)?;
        let mut info = Vec::new();
        for batch in reader {
            let batch = batch.map_err(|error| error.to_string())?;
            let names = batch.column(0).as_primitive::<UInt32Type>();
            let values = batch.column(1).as_union();
            for row in 0..batch.num_rows() {
                let code = format!("{:?}", InfoCode::from(names.value(row)));
                let value = values.value(row);
                // Every requested code is a `string_value`, union child 0.
                let value = match value.as_string_opt::<i32>() {
                    Some(text) if !text.is_null(0) => text.value(0).to_owned(),
                    _ => format!("{value:?}"),
                };
                info.push((code, value));
            }
        }
        Ok(info)
    }
}

fn load_driver(spec: &DriverSpec) -> Result<ManagedDriver, AdbcError> {
    let entrypoint = spec.entrypoint.as_deref().map(str::as_bytes);
    let name = match &spec.location {
        DriverLocation::Search(name) => name.as_ref(),
        DriverLocation::Path(path) => path.as_os_str(),
    };
    ManagedDriver::load_from_name(
        name,
        entrypoint,
        spec.version,
        LOAD_FLAG_DEFAULT,
        Some(spec.search_paths.clone()),
    )
}

/// Drop top-level nullability and all metadata, so drivers that agree on
/// values and types compare equal. Nested field nullability is kept.
fn relax(
    schema: &Schema,
    batches: Vec<RecordBatch>,
) -> Result<(SchemaRef, Vec<RecordBatch>), ArrowError> {
    let relaxed = Arc::new(Schema::new(
        schema
            .fields()
            .iter()
            .map(|field| {
                field
                    .as_ref()
                    .clone()
                    .with_nullable(true)
                    .with_metadata(Default::default())
            })
            .collect::<Vec<_>>(),
    ));
    let batches = batches
        .into_iter()
        .map(|batch| RecordBatch::try_new(relaxed.clone(), batch.columns().to_vec()))
        .collect::<Result<_, _>>()?;
    Ok((relaxed, batches))
}

fn adbc_error(error: AdbcError) -> ExecutionError {
    ExecutionError {
        class: format!("adbc:{:?}", error.status),
        message: error.message,
    }
}

fn arrow_error(error: &ArrowError) -> ExecutionError {
    ExecutionError {
        class: "arrow".to_owned(),
        message: error.to_string(),
    }
}
