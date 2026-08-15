//! Query generation as a pluggable extension point.
//!
//! The [`CampaignRunner`](crate::CampaignRunner) drives one or more
//! [`QueryGenerator`]s through a simple streaming protocol: each round it calls
//! [`QueryGenerator::generate`] with a [`GenerationContext`] and executes the
//! returned queries, then feeds that round's findings back into the next call.
//! Generators that ignore feedback (a fixed corpus, a schema fuzzer) simply
//! return their queries and then [`GenerationStep::Finished`]; feedback
//! generators (mutation) read [`GenerationContext::new_findings`].
//!
//! Implement this trait to plug a custom query source into a campaign via
//! [`CampaignRunner::with_generators`](crate::CampaignRunner::with_generators).

use std::collections::VecDeque;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;

use crate::{error::Result, Finding, GeneratedQuery};

/// A canonical table name and its Arrow schema, supplied to generators so they
/// can build queries against the dataset.
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub schema: SchemaRef,
}

/// State handed to a [`QueryGenerator`] on each generation round.
pub struct GenerationContext<'a> {
    /// Remaining query budget (`campaign.max_queries` minus attempts so far).
    pub remaining_queries: usize,
    /// Number of oracle-accepted queries executed so far.
    pub completed_queries: usize,
    /// The canonical dataset schema.
    pub schema: &'a [TableSchema],
    /// Findings produced by the previous round. Feedback generators mutate
    /// these; other generators ignore them. Empty on the first round.
    pub new_findings: &'a [Finding],
}

/// One step of generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GenerationStep {
    /// Queries to execute this round. May be empty (a feedback generator with
    /// nothing to react to yet).
    Queries(Vec<GeneratedQuery>),
    /// This generator will produce no further queries.
    Finished,
}

/// A pluggable source of queries for a campaign.
#[async_trait]
pub trait QueryGenerator: Send {
    /// A short, stable name used in logs and query provenance.
    fn name(&self) -> &str;

    /// Produce the next step of queries given the current campaign state.
    async fn generate(&mut self, context: &GenerationContext<'_>) -> Result<GenerationStep>;
}

/// A generator that replays a fixed list of queries (e.g. a SQL corpus).
pub struct CorpusGenerator {
    name: String,
    queries: VecDeque<GeneratedQuery>,
    batch_size: usize,
}

impl CorpusGenerator {
    #[must_use]
    pub fn new(queries: Vec<GeneratedQuery>) -> Self {
        Self {
            name: "corpus".to_owned(),
            queries: queries.into(),
            batch_size: usize::MAX,
        }
    }

    #[must_use]
    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size.max(1);
        self
    }
}

#[async_trait]
impl QueryGenerator for CorpusGenerator {
    fn name(&self) -> &str {
        &self.name
    }

    async fn generate(&mut self, context: &GenerationContext<'_>) -> Result<GenerationStep> {
        let count = self.batch_size.min(context.remaining_queries.max(1));
        let queries: Vec<_> = (0..count)
            .filter_map(|_| self.queries.pop_front())
            .collect();
        if queries.is_empty() {
            Ok(GenerationStep::Finished)
        } else {
            Ok(GenerationStep::Queries(queries))
        }
    }
}
