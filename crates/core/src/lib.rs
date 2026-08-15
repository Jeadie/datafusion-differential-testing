//! Extensible differential testing for DataFusion.

pub mod artifact;
pub mod campaign;
pub mod compare;
pub mod dataset;
pub mod error;
pub mod generator;
pub mod id;
pub mod model;
mod mutate;
pub mod provider;
pub mod random_generator;
mod rng;
pub mod session;

pub use artifact::{ArtifactStore, FileArtifactStore};
pub use campaign::{CampaignRunner, RunReport};
pub use compare::{Comparator, StrictComparator};
pub use dataset::{DatasetSnapshot, SnapshotBuilder, TableSnapshot};
pub use error::{Error, Result};
pub use generator::{CorpusGenerator, GenerationStep, QueryGenerator};
pub use random_generator::SchemaAwareGenerator;
pub use id::*;
pub use model::*;
pub use provider::*;
pub use session::{SessionExtension, SessionExtensionRegistry};
