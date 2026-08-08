//! Managed DuckDB and SQLite provider adapters.

#[cfg(feature = "duckdb")]
mod duckdb;
#[cfg(feature = "sqlite")]
mod sqlite;

#[cfg(feature = "duckdb")]
pub use duckdb::DuckDbProviderFactory;
#[cfg(feature = "sqlite")]
pub use sqlite::SqliteProviderFactory;

#[must_use]
pub fn compiled_provider_kinds() -> Vec<&'static str> {
    vec![
        #[cfg(feature = "duckdb")]
        "duckdb",
        #[cfg(feature = "sqlite")]
        "sqlite",
    ]
}
