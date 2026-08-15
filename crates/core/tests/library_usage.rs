//! Uses `datafusion-differential-testing` as a library, with no CLI or YAML:
//! build a campaign programmatically, plug in a custom `QueryGenerator`, and run
//! it with an injected no-op artifact store so findings come back purely in the
//! returned `RunReport`.

use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use datafusion_differential_testing::{
    CampaignRunner, CampaignSpec, DatasetSpec, FixtureSource, GeneratedQuery, GenerationContext,
    GenerationStep, NullArtifactStore, Ordering, QueryGenerator, QueryProvenance, Result, RunLimits,
    SessionSpec, TableBinding, TableSource, VariantSpec,
};

/// A custom query source. Any implementor of `QueryGenerator` plugs into a run
/// via `CampaignRunner::with_generators`.
struct FixedQueries {
    queries: Vec<GeneratedQuery>,
    done: bool,
}

#[async_trait]
impl QueryGenerator for FixedQueries {
    fn name(&self) -> &str {
        "fixed"
    }

    async fn generate(&mut self, _context: &GenerationContext<'_>) -> Result<GenerationStep> {
        if self.done {
            return Ok(GenerationStep::Finished);
        }
        self.done = true;
        Ok(GenerationStep::Queries(self.queries.clone()))
    }
}

fn memtable_variant(name: &str, oracle: bool) -> VariantSpec {
    VariantSpec {
        name: name.to_owned(),
        oracle,
        session: SessionSpec::default(),
        tables: BTreeMap::from([("*".to_owned(), TableBinding::MemTable)]),
    }
}

fn query(sql: &str) -> GeneratedQuery {
    GeneratedQuery {
        sql: sql.to_owned(),
        ordering: Ordering::Detect,
        provenance: QueryProvenance {
            generator: "fixed".to_owned(),
            source: None,
            provided_id: None,
        },
    }
}

#[tokio::test]
async fn drive_core_with_custom_generator_and_injected_store() {
    let temp = tempfile::tempdir().expect("temp dir");

    let spec = CampaignSpec {
        name: "library".to_owned(),
        dataset: DatasetSpec::Tables {
            sources: vec![TableSource::Fixture(FixtureSource::SqlInline {
                sql: "CREATE TABLE t(i INT); INSERT INTO t VALUES (1),(2),(3);".to_owned(),
            })],
            verify_unchanged: true,
        },
        providers: BTreeMap::new(),
        variants: BTreeMap::from([
            ("oracle".to_owned(), memtable_variant("oracle", true)),
            ("candidate".to_owned(), memtable_variant("candidate", false)),
        ]),
        corpus: Vec::new(),
        random_generators: Vec::new(),
        limits: RunLimits::default(),
        artifacts: temp.path().to_path_buf(),
    };

    let report = CampaignRunner::new()
        // Plug in a custom query source instead of the spec's corpus/random.
        .with_generators(Arc::new(|_spec| {
            vec![Box::new(FixedQueries {
                queries: vec![query("SELECT i FROM t"), query("SELECT count(i) FROM t")],
                done: false,
            })]
        }))
        // Discard artifacts; consume findings from the RunReport instead.
        .with_artifact_store(Arc::new(|root| Box::new(NullArtifactStore::new(root))))
        .run(spec)
        .await
        .expect("campaign runs");

    // Both custom queries executed; the two identical memtable variants agree,
    // so there are no findings.
    assert_eq!(report.executed_queries, 2);
    assert!(report.is_success());

    // The injected NullArtifactStore wrote none of its files to disk.
    assert!(!report.artifact_directory.join("run.json").exists());
    assert!(!report.artifact_directory.join("findings").exists());
    assert!(!report.artifact_directory.join("summary.json").exists());
}
