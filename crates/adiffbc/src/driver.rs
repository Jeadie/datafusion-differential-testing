//! Loads ADBC drivers and runs SQL on them, producing core [`QueryOutcome`]s.

use std::{
    collections::BTreeMap,
    ffi::OsStr,
    fmt,
    panic::{catch_unwind, AssertUnwindSafe},
    path::PathBuf,
    sync::Arc,
    time::Instant,
};

use adbc_core::{
    error::Error as AdbcError,
    error::Status,
    options::{AdbcVersion, InfoCode, OptionConnection, OptionDatabase, OptionValue},
    Connection, Database, Driver, Statement, LOAD_FLAG_DEFAULT,
};
use adbc_driver_manager::{
    profile::{
        process_profile_value, ConnectionProfile, ConnectionProfileProvider,
        FilesystemProfileProvider,
    },
    ManagedConnection, ManagedDriver,
};
use arrow::{
    array::{Array, AsArray, RecordBatch},
    datatypes::{Schema, SchemaRef, UInt32Type},
    error::ArrowError,
};
use datafusion_differential_testing::{
    dataset::logical_hash, ExecutionError, QueryOutcome, QueryResult,
};

use crate::{
    spec::{DriverResolution, DriverSource, DriverSpec, Locator, SchemaComparison, VariantSpec},
    sql::split_statements,
};

/// An open connection for one variant.
pub struct Session {
    connection: ManagedConnection,
}

/// How a driver was located.
#[derive(Debug, Clone)]
pub enum DriverOrigin {
    Path(PathBuf),
    /// Found by name in the ADBC search paths.
    Found(String),
    /// Installed (or confirmed up to date) by `dbc`.
    Installed {
        name: String,
        version: String,
    },
}

impl fmt::Display for DriverOrigin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Path(path) => write!(formatter, "{}", path.display()),
            Self::Found(name) => write!(formatter, "`{name}` from the ADBC search paths"),
            Self::Installed { name, version } => write!(formatter, "`{name}` {version} via dbc"),
        }
    }
}

/// Loads each driver once, and installs missing registry drivers with `dbc`.
pub struct DriverLoader<'a> {
    resolution: &'a DriverResolution,
    loaded: BTreeMap<String, (ManagedDriver, DriverOrigin)>,
}

impl<'a> DriverLoader<'a> {
    pub fn new(resolution: &'a DriverResolution) -> Self {
        Self {
            resolution,
            loaded: BTreeMap::new(),
        }
    }

    pub fn load(&mut self, spec: &DriverSpec) -> Result<(ManagedDriver, DriverOrigin), String> {
        if let Some(loaded) = self.loaded.get(&spec.key) {
            return Ok(loaded.clone());
        }
        let loaded = self
            .resolve(spec)
            .map_err(|error| format!("cannot load driver `{}`: {error}", spec.key))?;
        self.loaded.insert(spec.key.clone(), loaded.clone());
        Ok(loaded)
    }

    fn resolve(&self, spec: &DriverSpec) -> Result<(ManagedDriver, DriverOrigin), String> {
        let load = |location: &OsStr| {
            ManagedDriver::load_from_name(
                location,
                None,
                AdbcVersion::default(),
                LOAD_FLAG_DEFAULT,
                Some(self.resolution.search_paths.clone()),
            )
        };
        let installer = self.resolution.installer.as_ref();
        match (&spec.source, installer) {
            (DriverSource::Path(path), _) => load(path.as_os_str())
                .map(|driver| (driver, DriverOrigin::Path(path.clone())))
                .map_err(describe),
            // A pinned version always goes through dbc, so that an
            // older driver elsewhere on the search path is never used.
            (
                DriverSource::Registry {
                    name,
                    version: Some(version),
                },
                Some(installer),
            ) => {
                let (manifest, status) = installer.install(name, Some(version))?;
                let driver = load(manifest.as_os_str()).map_err(describe)?;
                let origin = DriverOrigin::Installed {
                    name: status.driver,
                    version: status.version,
                };
                Ok((driver, origin))
            }
            (DriverSource::Registry { name, version }, installer) => {
                if version.is_some() {
                    eprintln!(
                        "warning: driver `{}`: installation is disabled, so the version is not enforced",
                        spec.key
                    );
                }
                match load(name.as_ref()) {
                    Ok(driver) => Ok((driver, DriverOrigin::Found(name.clone()))),
                    Err(error) if error.status == Status::NotFound => {
                        let Some(installer) = installer else {
                            let searched: Vec<_> = self
                                .resolution
                                .search_paths
                                .iter()
                                .map(|path| path.display().to_string())
                                .collect();
                            return Err(format!(
                                "{}, searched {} and the ADBC user and system directories. \
                                 Installation is disabled; install it with `dbc install {name}`",
                                error.message,
                                searched.join(", ")
                            ));
                        };
                        let (manifest, status) = installer.install(name, None)?;
                        let driver = load(manifest.as_os_str()).map_err(describe)?;
                        let origin = DriverOrigin::Installed {
                            name: status.driver,
                            version: status.version,
                        };
                        Ok((driver, origin))
                    }
                    Err(error) => Err(describe(error)),
                }
            }
        }
    }
}

/// The error message, plus the driver manager's search trace when present.
fn describe(error: AdbcError) -> String {
    let trace = error.details.iter().flatten().find_map(|(key, value)| {
        (key == "adbc.drivermanager.driver_load_trace").then(|| String::from_utf8_lossy(value))
    });
    match trace {
        Some(trace) => format!(
            "{}\n  search trace:\n    {}",
            error.message,
            trace.replace('\n', "\n    ")
        ),
        None => error.message,
    }
}

impl Session {
    pub fn connect(
        variant: &VariantSpec,
        drivers: &mut DriverLoader<'_>,
    ) -> Result<(Self, DriverOrigin), String> {
        let context = |error: String| format!("variant `{}`: {error}", variant.name);
        // Later options win: the URI or profile first, then `-o` options.
        let (driver, mut database_options) = match &variant.locator {
            Locator::Uri { driver, uri } => {
                let uri = uri
                    .iter()
                    .map(|uri| (OptionDatabase::Uri, OptionValue::from(uri.as_str())));
                (driver.clone(), uri.collect())
            }
            Locator::Profile(profile) => read_profile(profile).map_err(context)?,
        };
        database_options.extend(variant.database_options.iter().map(|(key, value)| {
            (
                OptionDatabase::from(key.as_str()),
                OptionValue::from(value.as_str()),
            )
        }));
        let (mut driver, origin) = drivers.load(&driver).map_err(context)?;
        let context = |error: AdbcError| context(error.message);
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
        Ok((Self { connection }, origin))
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

/// The driver and database options of an ADBC connection profile.
fn read_profile(profile: &str) -> Result<(DriverSpec, Vec<(OptionDatabase, OptionValue)>), String> {
    let context = |error: AdbcError| format!("profile `{profile}`: {}", error.message);
    let loaded = FilesystemProfileProvider::new_with_search_paths(None)
        .get_profile(profile)
        .map_err(context)?;
    let (driver, _) = loaded.get_driver_name().map_err(context)?;
    let driver = DriverSpec::parse(driver)?;
    let options = loaded
        .get_options()
        .map_err(context)?
        .into_iter()
        .map(|(key, value)| match value {
            OptionValue::String(text) => {
                require_profile_variables(&text).map_err(|error| {
                    format!("profile `{profile}`: option `{}`: {error}", key.as_ref())
                })?;
                process_profile_value(&text)
                    .map(|value| (key, value))
                    .map_err(context)
            }
            value => Ok((key, value)),
        })
        .collect::<Result<_, String>>()?;
    Ok((driver, options))
}

/// The driver manager replaces an unset `{{ env_var(NAME) }}` with an empty
/// string, which surfaces later as a vague driver error. Fail early instead.
fn require_profile_variables(text: &str) -> Result<(), String> {
    let mut rest = text;
    while let Some(start) = rest.find("env_var(") {
        let after = &rest[start + "env_var(".len()..];
        let Some(end) = after.find(')') else { break };
        let name = after[..end].trim();
        if !name.is_empty() && std::env::var_os(name).is_none() {
            return Err(format!("environment variable `{name}` is not set"));
        }
        rest = &after[end..];
    }
    Ok(())
}
