use std::collections::VecDeque;

use async_trait::async_trait;

use crate::{error::Result, GeneratedQuery};

#[derive(Debug, Clone, Default)]
pub struct GenerationContext {
    pub remaining_queries: usize,
    pub completed_queries: usize,
    pub findings: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GenerationStep {
    Queries(Vec<GeneratedQuery>),
    Finished,
}

#[async_trait]
pub trait QueryGenerator: Send {
    fn name(&self) -> &str;

    async fn generate(&mut self, context: GenerationContext) -> Result<GenerationStep>;
}

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
            batch_size: 1,
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

    async fn generate(&mut self, context: GenerationContext) -> Result<GenerationStep> {
        let count = self.batch_size.min(context.remaining_queries);
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
