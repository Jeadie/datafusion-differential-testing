use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use arrow::record_batch::RecordBatch;
use datafusion::{
    datasource::MemTable,
    execution::{context::SQLOptions, session_state::SessionStateBuilder},
    physical_plan::stream::RecordBatchStreamAdapter,
    prelude::{SessionConfig, SessionContext},
};
use serde::Serialize;

use crate::{
    artifact::{ArtifactStore, FileArtifactStore},
    compare::{Comparator, StrictComparator},
    dataset::{logical_hash, SnapshotBuilder},
    error::{Error, Result},
    CampaignSpec, Comparison, DatasetSnapshot, ExecutionError, Finding, FindingId,
    ProviderInstance, ProviderLifecycle, ProviderRegistry, QueryId, QueryOutcome, QueryResult,
    RemoteTableRef, RunId, RunManifest, SessionExtensionRegistry, TableBinding, VariantId,
};
use futures::stream;

#[derive(Debug, Clone, Serialize)]
pub struct RunReport {
    pub run_id: RunId,
    pub artifact_directory: std::path::PathBuf,
    pub generated_queries: usize,
    pub executed_queries: usize,
    pub oracle_rejections: usize,
    pub findings: Vec<Finding>,
}

impl RunReport {
    #[must_use]
    pub fn is_success(&self) -> bool {
        self.findings.is_empty()
    }

    pub fn ensure_no_findings(&self) -> Result<()> {
        if self.findings.is_empty() {
            Ok(())
        } else {
            Err(Error::Execution {
                query: "campaign".to_owned(),
                message: format!("campaign produced {} finding(s)", self.findings.len()),
            })
        }
    }
}

pub struct CampaignRunner {
    providers: ProviderRegistry,
    extensions: SessionExtensionRegistry,
    comparator: Arc<dyn Comparator>,
}

impl Default for CampaignRunner {
    fn default() -> Self {
        Self::new()
    }
}

impl CampaignRunner {
    #[must_use]
    pub fn new() -> Self {
        Self {
            providers: ProviderRegistry::new(),
            extensions: SessionExtensionRegistry::new(),
            comparator: Arc::new(StrictComparator),
        }
    }

    #[must_use]
    pub fn with_provider_registry(mut self, providers: ProviderRegistry) -> Self {
        self.providers = providers;
        self
    }

    #[must_use]
    pub fn with_extension_registry(mut self, extensions: SessionExtensionRegistry) -> Self {
        self.extensions = extensions;
        self
    }

    #[must_use]
    pub fn with_comparator(mut self, comparator: Arc<dyn Comparator>) -> Self {
        self.comparator = comparator;
        self
    }

    pub fn validate(&self, spec: &CampaignSpec) -> Result<()> {
        if spec.variants.len() < 2 {
            return configuration("variants", "define at least two variants");
        }
        if !spec.variants.contains_key(&spec.oracle) {
            return configuration("oracle", "reference a defined variant");
        }
        if spec.corpus.is_empty() {
            return configuration("generators", "define at least one corpus query");
        }
        if spec.fixtures.is_empty() {
            return configuration("dataset.fixtures", "define at least one fixture");
        }
        for (name, provider) in &spec.providers {
            if &provider.name != name {
                return configuration(
                    &format!("providers.{name}"),
                    "provider map key and provider name differ",
                );
            }
            let factory =
                self.providers
                    .get(&provider.kind)
                    .ok_or_else(|| Error::Configuration {
                        path: format!("providers.{name}.type"),
                        message: format!("provider kind `{}` is not registered", provider.kind),
                    })?;
            factory.validate(&provider.params)?;
            if matches!(provider.lifecycle, ProviderLifecycle::Existing)
                && provider.params.contains_key("teardown")
            {
                return configuration(
                    &format!("providers.{name}.teardown"),
                    "existing providers do not accept teardown",
                );
            }
        }
        for (name, variant) in &spec.variants {
            if &variant.name != name {
                return configuration(
                    &format!("variants.{name}"),
                    "variant map key and variant name differ",
                );
            }
            let settings: HashMap<_, _> = variant.session.config.clone().into_iter().collect();
            SessionConfig::from_string_hash_map(&settings).map_err(|error| {
                Error::Configuration {
                    path: format!("variants.{name}.session.config"),
                    message: error.to_string(),
                }
            })?;
            for extension in &variant.session.extensions {
                let implementation =
                    self.extensions
                        .get(&extension.kind)
                        .ok_or_else(|| Error::Configuration {
                            path: format!("variants.{name}.session.extensions"),
                            message: format!("extension `{}` is not registered", extension.kind),
                        })?;
                implementation.validate(&extension.params)?;
            }
            for binding in variant.tables.values() {
                if let TableBinding::Provider { instance, .. } = binding {
                    if !spec.providers.contains_key(instance) {
                        return configuration(
                            &format!("variants.{name}.tables"),
                            &format!("provider `{instance}` is not defined"),
                        );
                    }
                }
            }
        }
        for (index, query) in spec.corpus.iter().enumerate() {
            if query.sql.len() > spec.limits.max_sql_bytes {
                return configuration(
                    &format!("generators.corpus[{index}]"),
                    "query exceeds campaign.max_sql_bytes",
                );
            }
        }
        Ok(())
    }

    pub async fn run(&self, spec: CampaignSpec) -> Result<RunReport> {
        self.validate(&spec)?;
        let run_id = create_run_id()?;
        let run_directory = spec.artifacts.join(slug(&spec.name)).join(run_id.as_str());
        let store = FileArtifactStore::new(&run_directory);
        let mut variant_names: Vec<_> = spec.variants.keys().cloned().collect();
        variant_names.sort();
        store.initialize(&RunManifest {
            format_version: 1,
            run_id: run_id.clone(),
            campaign: spec.name.clone(),
            oracle: spec.oracle.clone(),
            variants: variant_names,
        })?;
        let snapshot = SnapshotBuilder::new(run_directory.join("dataset"))
            .build(&spec.fixtures)
            .await?;
        store.write_dataset_manifest(&snapshot)?;
        let connected = self.connect_providers(&spec).await?;
        let owned = prepare_managed_providers(&spec, &snapshot, &connected).await?;
        let sessions = match self.build_sessions(&spec, &snapshot, &connected).await {
            Ok(sessions) => sessions,
            Err(primary) => {
                if let Err(cleanup) = rollback_managed_providers(&connected, &owned).await {
                    return Err(Error::Provider {
                        provider: "cleanup".to_owned(),
                        message: format!("{primary}; rollback also failed: {cleanup}"),
                    });
                }
                return Err(primary);
            }
        };
        let oracle = sessions.get(&spec.oracle).ok_or_else(|| Error::Variant {
            variant: spec.oracle.clone(),
            message: "oracle session is unavailable".to_owned(),
        })?;
        let candidates: Vec<_> = sessions
            .iter()
            .filter(|(name, _)| *name != &spec.oracle)
            .collect();

        let mut report = RunReport {
            run_id,
            artifact_directory: run_directory,
            generated_queries: 0,
            executed_queries: 0,
            oracle_rejections: 0,
            findings: Vec::new(),
        };
        for (query_index, query) in spec.corpus.iter().take(spec.limits.max_queries).enumerate() {
            report.generated_queries += 1;
            let query_id = QueryId::new(format!("query-{query_index:06}"))?;
            let oracle_outcome = execute_query(oracle, query, spec.limits.query_timeout).await;
            if !matches!(oracle_outcome, QueryOutcome::Success(_)) {
                report.oracle_rejections += 1;
                continue;
            }
            report.executed_queries += 1;
            for (candidate_name, candidate) in &candidates {
                let outcome = execute_query(candidate, query, spec.limits.query_timeout).await;
                if let Comparison::Different(difference) =
                    self.comparator
                        .compare(&oracle_outcome, &outcome, query.is_ordered())
                {
                    let finding_index = report.findings.len();
                    let finding = Finding {
                        id: FindingId::new(format!("finding-{finding_index:06}"))?,
                        query_id: query_id.clone(),
                        candidate: VariantId::new((*candidate_name).clone())?,
                        kind: difference.kind,
                        message: difference.message,
                        sql: query.sql.clone(),
                    };
                    store.write_finding(&finding)?;
                    report.findings.push(finding);
                    if report.findings.len() >= spec.limits.max_findings
                        || spec.limits.stop_on_finding
                    {
                        break;
                    }
                }
            }
            if report.findings.len() >= spec.limits.max_findings
                || (spec.limits.stop_on_finding && !report.findings.is_empty())
            {
                break;
            }
        }
        cleanup_managed_providers(&spec, &connected, &owned).await?;
        store.write_summary(&serde_json::to_value(&report)?)?;
        Ok(report)
    }

    async fn connect_providers(
        &self,
        spec: &CampaignSpec,
    ) -> Result<BTreeMap<String, Arc<dyn ProviderInstance>>> {
        let mut connected = BTreeMap::new();
        for (name, provider) in &spec.providers {
            let factory =
                self.providers
                    .get(&provider.kind)
                    .ok_or_else(|| Error::Configuration {
                        path: format!("providers.{name}.type"),
                        message: format!("provider kind `{}` is not registered", provider.kind),
                    })?;
            connected.insert(name.clone(), factory.connect(name, &provider.params).await?);
        }
        Ok(connected)
    }

    async fn build_sessions(
        &self,
        spec: &CampaignSpec,
        snapshot: &DatasetSnapshot,
        providers: &BTreeMap<String, Arc<dyn ProviderInstance>>,
    ) -> Result<BTreeMap<String, SessionContext>> {
        let mut sessions = BTreeMap::new();
        for (name, variant) in &spec.variants {
            let settings: HashMap<_, _> = variant.session.config.clone().into_iter().collect();
            let config =
                SessionConfig::from_string_hash_map(&settings).map_err(|error| Error::Variant {
                    variant: name.clone(),
                    message: error.to_string(),
                })?;
            let mut builder = SessionStateBuilder::new()
                .with_config(config)
                .with_default_features();
            for extension in &variant.session.extensions {
                let implementation =
                    self.extensions
                        .get(&extension.kind)
                        .ok_or_else(|| Error::Variant {
                            variant: name.clone(),
                            message: format!("extension `{}` is unavailable", extension.kind),
                        })?;
                builder = implementation.apply(builder, &extension.params)?;
            }
            let context = SessionContext::new_with_state(builder.build());
            let expanded = expand_bindings(&variant.tables, snapshot)?;
            for (table_name, binding) in expanded {
                match binding {
                    TableBinding::MemTable => {
                        let table =
                            snapshot
                                .tables
                                .get(&table_name)
                                .ok_or_else(|| Error::Dataset {
                                    table: table_name.clone(),
                                    message: "variant references an unknown table".to_owned(),
                                })?;
                        let batches = table.read_batches()?;
                        let provider = MemTable::try_new(table.schema.clone(), vec![batches])?;
                        context.register_table(&table_name, Arc::new(provider))?;
                    }
                    TableBinding::Provider { instance, remote } => {
                        let provider = providers.get(&instance).ok_or_else(|| Error::Provider {
                            provider: instance.clone(),
                            message: "provider is not connected".to_owned(),
                        })?;
                        let remote = remote.unwrap_or_else(|| RemoteTableRef(table_name.clone()));
                        let table_provider = provider.table_provider(&remote).await?;
                        context.register_table(&table_name, table_provider)?;
                    }
                }
            }
            sessions.insert(name.clone(), context);
        }
        Ok(sessions)
    }
}

type ManagedTarget = (String, RemoteTableRef, String);

fn managed_targets(spec: &CampaignSpec, snapshot: &DatasetSnapshot) -> Result<Vec<ManagedTarget>> {
    let mut targets: BTreeMap<(String, RemoteTableRef), String> = BTreeMap::new();
    for variant in spec.variants.values() {
        for (logical, binding) in expand_bindings(&variant.tables, snapshot)? {
            let TableBinding::Provider { instance, remote } = binding else {
                continue;
            };
            let remote = remote.unwrap_or_else(|| RemoteTableRef(logical.clone()));
            if let Some(previous) =
                targets.insert((instance.clone(), remote.clone()), logical.clone())
            {
                if previous != logical {
                    return configuration(
                        "variants.*.tables",
                        &format!(
                            "logical tables `{previous}` and `{logical}` map to provider target `{instance}:{}`",
                            remote.0
                        ),
                    );
                }
            }
        }
    }
    Ok(targets
        .into_iter()
        .map(|((provider, remote), logical)| (provider, remote, logical))
        .collect())
}

async fn prepare_managed_providers(
    spec: &CampaignSpec,
    snapshot: &DatasetSnapshot,
    providers: &BTreeMap<String, Arc<dyn ProviderInstance>>,
) -> Result<Vec<ManagedTarget>> {
    let targets = managed_targets(spec, snapshot)?;
    for (provider_name, remote, logical) in &targets {
        let provider_spec = &spec.providers[provider_name];
        let provider = &providers[provider_name];
        match provider_spec.lifecycle {
            ProviderLifecycle::Managed { .. } => {
                let managed = provider.managed().ok_or_else(|| Error::Provider {
                    provider: provider_name.clone(),
                    message: "provider does not support managed lifecycle".to_owned(),
                })?;
                let table = &snapshot.tables[logical];
                managed.validate_schema(table.schema.as_ref())?;
                if managed.table_exists(remote).await? {
                    return Err(Error::Provider {
                        provider: provider_name.clone(),
                        message: format!(
                            "managed target `{}` already exists; no table was changed",
                            remote.0
                        ),
                    });
                }
            }
            ProviderLifecycle::Existing => {
                return Err(Error::Provider {
                    provider: provider_name.clone(),
                    message: format!(
                        "existing target `{}` is not available in this developer preview",
                        remote.0
                    ),
                });
            }
        }
    }

    let mut owned = Vec::new();
    for (provider_name, remote, logical) in &targets {
        if !matches!(
            spec.providers[provider_name].lifecycle,
            ProviderLifecycle::Managed { .. }
        ) {
            continue;
        }
        let managed = providers[provider_name]
            .managed()
            .expect("managed support was checked during preflight");
        let table = &snapshot.tables[logical];
        if let Err(primary) = managed.create_table(remote, table.schema.clone()).await {
            rollback_managed_providers(providers, &owned).await?;
            return Err(primary);
        }
        owned.push((provider_name.clone(), remote.clone(), logical.clone()));
        let batches = table.read_batches()?;
        let input = RecordBatchStreamAdapter::new(
            table.schema.clone(),
            stream::iter(batches.into_iter().map(Ok)),
        );
        if let Err(primary) = managed.load(remote, Box::pin(input)).await {
            if let Err(cleanup) = rollback_managed_providers(providers, &owned).await {
                return Err(Error::Provider {
                    provider: provider_name.clone(),
                    message: format!("{primary}; rollback also failed: {cleanup}"),
                });
            }
            return Err(primary);
        }
    }
    Ok(owned)
}

async fn rollback_managed_providers(
    providers: &BTreeMap<String, Arc<dyn ProviderInstance>>,
    owned: &[ManagedTarget],
) -> Result<()> {
    for (provider_name, remote, _) in owned.iter().rev() {
        providers[provider_name]
            .managed()
            .expect("owned target has managed provider")
            .drop_table(remote)
            .await?;
    }
    Ok(())
}

async fn cleanup_managed_providers(
    spec: &CampaignSpec,
    providers: &BTreeMap<String, Arc<dyn ProviderInstance>>,
    owned: &[ManagedTarget],
) -> Result<()> {
    for (provider_name, remote, _) in owned.iter().rev() {
        if !matches!(
            spec.providers[provider_name].lifecycle,
            ProviderLifecycle::Managed { teardown: true }
        ) {
            continue;
        }
        providers[provider_name]
            .managed()
            .expect("owned target has managed provider")
            .drop_table(remote)
            .await?;
    }
    Ok(())
}

fn expand_bindings(
    configured: &BTreeMap<String, TableBinding>,
    snapshot: &DatasetSnapshot,
) -> Result<BTreeMap<String, TableBinding>> {
    let wildcard = configured.get("*");
    let mut expanded = BTreeMap::new();
    for table in snapshot.tables.keys() {
        let binding = configured
            .get(table)
            .or(wildcard)
            .ok_or_else(|| Error::Configuration {
                path: format!("variants.*.tables.{table}"),
                message: "table has no binding".to_owned(),
            })?;
        expanded.insert(table.clone(), binding.clone());
    }
    for table in configured.keys().filter(|name| name.as_str() != "*") {
        if !snapshot.tables.contains_key(table) {
            return configuration(
                &format!("variants.*.tables.{table}"),
                "binding references an unknown canonical table",
            );
        }
    }
    Ok(expanded)
}

async fn execute_query(
    context: &SessionContext,
    query: &crate::GeneratedQuery,
    timeout: Duration,
) -> QueryOutcome {
    let started = Instant::now();
    let execution = async {
        let options = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false);
        let frame = context.sql_with_options(&query.sql, options).await?;
        frame.collect().await
    };
    match tokio::time::timeout(timeout, execution).await {
        Err(_) => QueryOutcome::TimedOut {
            elapsed: started.elapsed(),
        },
        Ok(Err(error)) => QueryOutcome::Rejected(ExecutionError {
            class: "datafusion".to_owned(),
            message: error.to_string(),
        }),
        Ok(Ok(batches)) => match query_result(batches, started.elapsed()) {
            Ok(result) => QueryOutcome::Success(result),
            Err(error) => QueryOutcome::Rejected(ExecutionError {
                class: "result".to_owned(),
                message: error.to_string(),
            }),
        },
    }
}

fn query_result(batches: Vec<RecordBatch>, elapsed: Duration) -> Result<QueryResult> {
    let schema = batches
        .first()
        .map(RecordBatch::schema)
        .unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty()));
    let row_count = batches.iter().map(RecordBatch::num_rows).sum();
    let content_hash = logical_hash(schema.as_ref(), &batches)?;
    Ok(QueryResult {
        schema,
        batches,
        row_count,
        content_hash,
        elapsed,
    })
}

fn create_run_id() -> Result<RunId> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| Error::Configuration {
            path: "clock".to_owned(),
            message: error.to_string(),
        })?
        .as_nanos();
    RunId::new(format!("run-{nanos}-{}", std::process::id()))
}

fn slug(value: &str) -> String {
    let slug: String = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '-'
            }
        })
        .collect();
    if slug.is_empty() {
        "campaign".to_owned()
    } else {
        slug
    }
}

fn configuration<T>(path: &str, message: &str) -> Result<T> {
    Err(Error::Configuration {
        path: path.to_owned(),
        message: message.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::{
        FixtureSource, GeneratedQuery, Ordering, QueryProvenance, RunLimits, SessionSpec,
        VariantSpec,
    };

    use super::*;

    #[tokio::test]
    async fn memtable_campaign_runs_identical_variants() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let variants = ["oracle", "candidate"]
            .into_iter()
            .map(|name| {
                (
                    name.to_owned(),
                    VariantSpec {
                        name: name.to_owned(),
                        session: SessionSpec::default(),
                        tables: [("*".to_owned(), TableBinding::MemTable)].into(),
                    },
                )
            })
            .collect();
        let report = CampaignRunner::new()
            .run(CampaignSpec {
                name: "smoke".to_owned(),
                oracle: "oracle".to_owned(),
                fixtures: vec![FixtureSource::SqlInline {
                    sql: "CREATE TABLE t(i INT); INSERT INTO t VALUES (1), (2), (2), (NULL);"
                        .to_owned(),
                }],
                providers: BTreeMap::new(),
                variants,
                corpus: vec![GeneratedQuery {
                    sql: "SELECT * FROM t".to_owned(),
                    ordering: Ordering::Unordered,
                    provenance: QueryProvenance {
                        generator: "test".to_owned(),
                        source: None,
                        provided_id: None,
                    },
                }],
                limits: RunLimits::default(),
                artifacts: PathBuf::from(temporary.path()),
            })
            .await
            .expect("campaign succeeds");
        assert!(report.is_success());
        assert_eq!(report.executed_queries, 1);
        assert!(report.artifact_directory.join("summary.json").exists());
    }
}
