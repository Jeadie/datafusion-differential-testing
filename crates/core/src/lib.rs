//! Differential correctness testing for [`datafusion::catalog::TableProvider`] `SessionContext` and `TableProviders`.
//!

pub mod artifact;
pub mod campaign;
pub mod compare;
pub mod dataset;
pub mod error;
pub mod generator;
pub mod id;
pub mod model;
pub mod mutate;
pub mod provider;
pub mod random_generator;
mod rng;
pub mod session;

pub use artifact::{ArtifactStore, FileArtifactStore, NullArtifactStore};
pub use campaign::{ArtifactStoreFactory, CampaignRunner, GeneratorFactory, RunReport};
pub use compare::{Comparator, StrictComparator, Ulps};
pub use dataset::{DatasetSnapshot, SnapshotBuilder, TableSnapshot};
pub use error::{Error, Result};
pub use generator::{
    CorpusGenerator, GenerationContext, GenerationStep, QueryGenerator, TableSchema,
};
pub use id::*;
pub use model::*;
pub use mutate::MutationGenerator;
pub use provider::*;
pub use random_generator::{RandomQueryGenerator, SchemaAwareGenerator};
pub use session::{SessionExtension, SessionExtensionRegistry};
