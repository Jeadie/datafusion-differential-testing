//! Runs the corpus and generated queries on every variant, and compares each
//! candidate with the oracle.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    num::NonZeroUsize,
};

use datafusion_differential_testing::{
    Comparator, Comparison, CorpusGenerator, FindingId, FindingKind, GeneratedQuery,
    GenerationContext, GenerationStep, MutationGenerator, QueryGenerator, QueryId, QueryOutcome,
    RandomGeneratorSpec, RandomQueryGenerator, TableSchema, VariantId,
};
use futures::executor::block_on;
use serde::Serialize;

use crate::{
    driver::{DriverLoader, Session},
    spec::{Spec, VariantSpec},
    sql::LeadingComments,
};

/// Identifies a query in the report.
#[derive(Debug, Clone, Serialize)]
pub struct QueryLabel {
    /// Unique in a run: the `-- name:`, `FILE:N`, `inline:N`, `random-NNNNNN`,
    /// or `PARENT+mN` for the Nth mutant of query `PARENT`.
    pub name: String,
    /// `corpus`, `random` or `mutation`.
    pub generator: String,
    /// The query file, the random seed, or a mutant's parent query name.
    pub source: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct Finding {
    pub query: QueryLabel,
    pub candidate: String,
    pub kind: FindingKind,
    pub message: String,
    pub sql: String,
}

/// What a run with `--random` or `--mutate` needs to be repeated.
#[derive(Debug, Serialize)]
pub struct GenerationReport {
    pub seed: u64,
    pub exploratory: bool,
    /// The tables that random queries use.
    pub tables: Vec<String>,
}

#[derive(Debug, Default, Serialize)]
pub struct Report {
    pub generation: Option<GenerationReport>,
    pub queries_executed: usize,
    /// `queries_executed`, by generator.
    pub queries_by_generator: BTreeMap<String, usize>,
    /// Generated queries not run because the same SQL text was already queued.
    pub duplicate_queries: usize,
    /// Queries skipped because the oracle did not return a result.
    pub oracle_rejections: usize,
    pub comparisons: usize,
    pub findings: Vec<Finding>,
}

struct Candidate {
    name: String,
    /// The candidate name as a core id, for the mutation generator.
    id: VariantId,
    session: Session,
}

/// Runs rounds the same way as core's `CampaignRunner`: each round asks every
/// unfinished generator for queries, drops SQL that already ran, and runs the
/// rest. The round's findings go to the next round, where the mutation
/// generator reads them. The run ends when a round has no new queries, or at
/// a limit.
pub fn run(spec: &Spec, comparator: &dyn Comparator) -> Result<Report, String> {
    let mut drivers = DriverLoader::new(&spec.drivers);
    let mut oracle = open(&spec.oracle, &mut drivers)?;
    let mut candidates = spec
        .candidates
        .iter()
        .map(|variant| {
            Ok(Candidate {
                name: variant.name.clone(),
                id: variant_id(&variant.name)?,
                session: open(variant, &mut drivers)?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    let random = spec
        .generation
        .as_ref()
        .and_then(|generation| generation.random.as_ref());
    let tables: Vec<TableSchema> = match random {
        Some(random) => oracle
            .tables(&random.tables)
            .map_err(|error| format!("--random: variant `{}`: {error}", spec.oracle.name))?,
        None => Vec::new(),
    };
    let mut report = Report {
        generation: spec.generation.as_ref().map(|generation| GenerationReport {
            seed: generation.seed.value(),
            exploratory: random.is_some_and(|random| random.exploratory),
            tables: tables.iter().map(|table| table.name.clone()).collect(),
        }),
        ..Report::default()
    };

    let mut generators = generators(spec);
    let mut finished = vec![false; generators.len()];
    let mut enqueued: HashSet<String> = HashSet::new();
    let mut labels = Labels::default();
    let mut last_findings: Vec<datafusion_differential_testing::Finding> = Vec::new();
    let max_queries = spec.limits.max_queries;
    'run: while report.queries_executed < max_queries {
        let context = GenerationContext {
            remaining_queries: max_queries - report.queries_executed,
            completed_queries: report.queries_executed - report.oracle_rejections,
            schema: &tables,
            new_findings: &last_findings,
        };
        let mut batch: Vec<GeneratedQuery> = Vec::new();
        for (generator, finished) in generators.iter_mut().zip(&mut finished) {
            if *finished {
                continue;
            }
            let step = block_on(generator.generate(&context))
                .map_err(|error| format!("generator `{}`: {error}", generator.name()))?;
            match step {
                GenerationStep::Finished => *finished = true,
                GenerationStep::Queries(queries) => batch.extend(queries),
            }
        }
        let generated = batch.len();
        batch.retain(|query| enqueued.insert(query.sql.clone()));
        report.duplicate_queries += generated - batch.len();
        if batch.is_empty() {
            break;
        }

        let mut round_findings = Vec::new();
        for query in batch {
            if report.queries_executed >= max_queries {
                break;
            }
            let index = report.queries_executed;
            let label = labels.label(&query, index);
            report.queries_executed += 1;
            *report
                .queries_by_generator
                .entry(label.generator.clone())
                .or_default() += 1;
            let expected = oracle.query(&query.sql, spec.schema);
            if !matches!(expected, QueryOutcome::Success(_)) {
                report.oracle_rejections += 1;
                continue;
            }
            for candidate in &mut candidates {
                let actual = candidate.session.query(&query.sql, spec.schema);
                report.comparisons += 1;
                let Comparison::Different(difference) =
                    comparator.compare(&expected, &actual, query.is_ordered())
                else {
                    continue;
                };
                // Mutate the SQL without its leading comments, so that a
                // mutant does not show its parent's `-- name:`.
                let sql = LeadingComments::new(&query.sql).remainder();
                labels.parents.insert(sql.to_owned(), label.name.clone());
                round_findings.push(datafusion_differential_testing::Finding {
                    id: core_id(FindingId::new(format!(
                        "finding-{:06}",
                        report.findings.len()
                    )))?,
                    query_id: core_id(QueryId::new(format!("query-{index:06}")))?,
                    candidate: candidate.id.clone(),
                    kind: difference.kind.clone(),
                    message: difference.message.clone(),
                    sql: sql.to_owned(),
                });
                report.findings.push(Finding {
                    query: label.clone(),
                    candidate: candidate.name.clone(),
                    kind: difference.kind,
                    message: difference.message,
                    sql: query.sql.clone(),
                });
                if spec.limits.stop_on_finding || report.findings.len() >= spec.limits.max_findings
                {
                    break 'run;
                }
            }
        }
        last_findings = round_findings;
    }
    Ok(report)
}

/// The corpus, then random queries, then mutants, as in core's default
/// generator pipeline.
fn generators(spec: &Spec) -> Vec<Box<dyn QueryGenerator>> {
    let mut generators: Vec<Box<dyn QueryGenerator>> = Vec::new();
    if !spec.corpus.is_empty() {
        generators.push(Box::new(CorpusGenerator::new(spec.corpus.clone())));
    }
    let Some(generation) = &spec.generation else {
        return generators;
    };
    let seed = generation.seed.value();
    if let Some(random) = &generation.random {
        generators.push(Box::new(RandomQueryGenerator::new(RandomGeneratorSpec {
            seed,
            count: random.count.get(),
            nondeterministic: random.exploratory,
            mutate_on_finding: generation.mutations_per_finding.is_some(),
            mutations_per_finding: generation
                .mutations_per_finding
                .map_or(0, NonZeroUsize::get),
        })));
    }
    if let Some(per_finding) = generation.mutations_per_finding {
        // The same seed derivation as core, so one seed gives the same mutants.
        generators.push(Box::new(MutationGenerator::new(
            per_finding.get(),
            seed ^ 0x5F4D_C0DE,
        )));
    }
    generators
}

/// Gives each query a name that is unique in the run.
#[derive(Default)]
struct Labels {
    used: HashSet<String>,
    /// The name of each query that gave a finding, by the SQL that the
    /// mutation generator gets. A mutant's provenance source is this SQL.
    parents: HashMap<String, String>,
    /// The number of mutants named so far, by parent name.
    mutants: HashMap<String, usize>,
}

impl Labels {
    fn label(&mut self, query: &GeneratedQuery, index: usize) -> QueryLabel {
        let provenance = &query.provenance;
        let parent = provenance
            .source
            .as_ref()
            .and_then(|source| self.parents.get(source));
        let (name, source) = match (&provenance.provided_id, parent) {
            (Some(id), _) => (id.clone(), provenance.source.clone()),
            (None, Some(parent)) => {
                let count = self.mutants.entry(parent.clone()).or_default();
                *count += 1;
                (format!("{parent}+m{count}"), Some(parent.clone()))
            }
            (None, None) => (format!("query-{index:06}"), provenance.source.clone()),
        };
        QueryLabel {
            name: self.unique(name),
            generator: provenance.generator.clone(),
            source,
        }
    }

    /// `name`, or `name#2`, `name#3`, ... when a query file already used it.
    fn unique(&mut self, name: String) -> String {
        let mut unique = name.clone();
        let mut suffix = 1;
        while !self.used.insert(unique.clone()) {
            suffix += 1;
            unique = format!("{name}#{suffix}");
        }
        unique
    }
}

/// A core id holds 1 to 128 of `[A-Za-z0-9_-]`; other characters become `_`.
fn variant_id(name: &str) -> Result<VariantId, String> {
    let id: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .take(128)
        .collect();
    core_id(VariantId::new(if id.is_empty() {
        "_".to_owned()
    } else {
        id
    }))
}

fn core_id<T>(id: datafusion_differential_testing::Result<T>) -> Result<T, String> {
    id.map_err(|error| error.to_string())
}

/// Connect a variant and run its setup scripts.
fn open(variant: &VariantSpec, drivers: &mut DriverLoader<'_>) -> Result<Session, String> {
    let (mut session, _) = Session::connect(variant, drivers)?;
    for script in &variant.setup {
        session.execute_script(&script.sql).map_err(|error| {
            format!(
                "variant `{}`: setup `{}` failed: {error}",
                variant.name, script.source
            )
        })?;
    }
    Ok(session)
}

#[cfg(test)]
mod tests {
    use datafusion_differential_testing::QueryProvenance;

    use super::*;

    fn query(generator: &str, source: Option<&str>, id: Option<&str>) -> GeneratedQuery {
        GeneratedQuery {
            sql: "SELECT 1".to_owned(),
            ordering: Default::default(),
            provenance: QueryProvenance {
                generator: generator.to_owned(),
                source: source.map(str::to_owned),
                provided_id: id.map(str::to_owned),
            },
        }
    }

    #[test]
    fn mutants_are_named_after_their_parent() {
        let mut labels = Labels::default();
        let parent = labels.label(&query("corpus", Some("q.sql"), Some("totals")), 0);
        assert_eq!(parent.name, "totals");
        labels
            .parents
            .insert("SELECT 2".to_owned(), parent.name.clone());
        let first = labels.label(&query("mutation", Some("SELECT 2"), None), 1);
        let second = labels.label(&query("mutation", Some("SELECT 2"), None), 2);
        assert_eq!(first.name, "totals+m1");
        assert_eq!(first.source.as_deref(), Some("totals"));
        assert_eq!(second.name, "totals+m2");
        let unknown = labels.label(&query("other", None, None), 3);
        assert_eq!(unknown.name, "query-000003");
    }

    #[test]
    fn generated_names_never_repeat_a_used_name() {
        let mut labels = Labels::default();
        labels.label(&query("corpus", None, Some("random-000000")), 0);
        let random = labels.label(&query("random", Some("seed=1"), Some("random-000000")), 1);
        assert_eq!(random.name, "random-000000#2");
    }

    #[test]
    fn variant_ids_replace_invalid_characters() {
        assert_eq!(variant_id("duck.db v2").unwrap().as_str(), "duck_db_v2");
        assert_eq!(variant_id("ok-name_1").unwrap().as_str(), "ok-name_1");
    }
}
