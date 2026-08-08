use std::{io, path::PathBuf};

use thiserror::Error;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid {kind} identifier `{value}`")]
    InvalidId { kind: &'static str, value: String },

    #[error("invalid campaign configuration at `{path}`: {message}")]
    Configuration { path: String, message: String },

    #[error("dataset operation failed for `{table}`: {message}")]
    Dataset { table: String, message: String },

    #[error("provider `{provider}` failed: {message}")]
    Provider { provider: String, message: String },

    #[error("variant `{variant}` failed: {message}")]
    Variant { variant: String, message: String },

    #[error("query `{query}` failed: {message}")]
    Execution { query: String, message: String },

    #[error("generator `{generator}` failed: {message}")]
    Generator { generator: String, message: String },

    #[error("artifact operation failed at `{path}`: {source}")]
    Artifact {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),

    #[error(transparent)]
    Json(#[from] serde_json::Error),
}
