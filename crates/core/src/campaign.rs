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
    CampaignSpec, Comparison, DatasetSnapshot, DatasetSpec, ExecutionError, Finding, FindingId,
    LogicalTableName, ProviderInstance, ProviderLifecycle, ProviderRegistry, QueryId, QueryOutcome,
    QueryResult, RemoteTableRef, RunId, RunManifest, SessionExtensionRegistry, TableBinding,
    TableSelection, TableSource, VariantId, VariantSpec,
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
        let oracle_name = oracle_name(spec)?;
        if spec.corpus.is_empty() && spec.random_generators.is_empty() {
            return configuration(
                "generators",
                "define at least one corpus query or random generator",
            );
        }
        if let DatasetSpec::Tables { sources, .. } = &spec.dataset {
            if sources.is_empty() {
                return configuration("dataset", "define at least one table source");
            }
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
        match &spec.dataset {
            DatasetSpec::Tables { sources, .. } => {
                for source in sources {
                    let TableSource::FromVariant { table, variant } = source else {
                        continue;
                    };
                    let Some(variant_spec) = spec.variants.get(variant) else {
                        return configuration(
                            "dataset",
                            &format!("table `{table}` references unknown variant `{variant}`"),
                        );
                    };
                    let binding = variant_spec
                        .tables
                        .get(table)
                        .or_else(|| variant_spec.tables.get("*"));
                    let Some(TableBinding::Provider { instance, .. }) = binding else {
                        return configuration(
                            &format!("variants.{variant}.tables.{table}"),
                            "a variant-sourced table must bind a provider",
                        );
                    };
                    if !spec.providers.contains_key(instance) {
                        return configuration(
                            &format!("variants.{variant}.tables.{table}"),
                            &format!("provider `{instance}` is not defined"),
                        );
                    }
                }
            }
            DatasetSpec::Variant { variant, .. } => {
                if variant.as_str() != oracle_name {
                    return configuration(
                        "dataset.variant",
                        "a whole-variant dataset must reference the oracle variant",
                    );
                }
                let oracle = &spec.variants[oracle_name];
                if oracle.tables.is_empty() {
                    return configuration(
                        &format!("variants.{oracle_name}.tables"),
                        "the oracle variant defines no table bindings",
                    );
                }
                for (table, binding) in &oracle.tables {
                    let TableBinding::Provider { instance, .. } = binding else {
                        return configuration(
                            &format!("variants.{oracle_name}.tables.{table}"),
                            "an existing oracle table must bind an existing provider",
                        );
                    };
                    if !matches!(
                        spec.providers[instance].lifecycle,
                        ProviderLifecycle::Existing
                    ) {
                        return configuration(
                            &format!("providers.{instance}.lifecycle"),
                            "an existing oracle requires lifecycle `existing`",
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
        let oracle_name = oracle_name(&spec)?.to_owned();
        let run_id = create_run_id()?;
        let run_directory = spec.artifacts.join(slug(&spec.name)).join(run_id.as_str());
        let store = FileArtifactStore::new(&run_directory);
        let mut variant_names: Vec<_> = spec.variants.keys().cloned().collect();
        variant_names.sort();
        store.initialize(&RunManifest {
            format_version: 1,
            run_id: run_id.clone(),
            campaign: spec.name.clone(),
            oracle: oracle_name.clone(),
            variants: variant_names,
        })?;
        let connected = self.connect_providers(&spec).await?;
        let snapshot_builder = SnapshotBuilder::new(run_directory.join("dataset"));
        let snapshot = self
            .build_snapshot(&spec, &connected, &snapshot_builder)
            .await?;
        store.write_dataset_manifest(&snapshot)?;
        verify_existing_targets(&spec, &snapshot, &connected).await?;
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
        let oracle = sessions.get(&oracle_name).ok_or_else(|| Error::Variant {
            variant: oracle_name.clone(),
            message: "oracle session is unavailable".to_owned(),
        })?;
        let candidates: Vec<_> = sessions
            .iter()
            .filter(|(name, _)| *name != &oracle_name)
            .collect();

        // Schema-aware generators run here, once the canonical snapshot (and thus
        // every table schema) exists. Generated queries follow the fixed corpus.
        let generated = generate_random_queries(&spec, &snapshot);
        let all_queries: Vec<&crate::GeneratedQuery> =
            spec.corpus.iter().chain(generated.iter()).collect();

        let mut report = RunReport {
            run_id,
            artifact_directory: run_directory,
            generated_queries: 0,
            executed_queries: 0,
            oracle_rejections: 0,
            findings: Vec::new(),
        };
        for (query_index, query) in all_queries
            .iter()
            .copied()
            .take(spec.limits.max_queries)
            .enumerate()
        {
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
        let verify_unchanged = match &spec.dataset {
            DatasetSpec::Tables {
                verify_unchanged, ..
            }
            | DatasetSpec::Variant {
                verify_unchanged, ..
            } => *verify_unchanged,
        };
        let drift = if verify_unchanged {
            verify_existing_targets(&spec, &snapshot, &connected)
                .await
                .err()
        } else {
            None
        };
        cleanup_managed_providers(&spec, &connected, &owned).await?;
        store.write_summary(&serde_json::to_value(&report)?)?;
        if let Some(error) = drift {
            return Err(error);
        }
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
            connected.insert(
                name.clone(),
                factory
                    .connect(name, &provider.params, &provider.lifecycle)
                    .await?,
            );
        }
        Ok(connected)
    }

    async fn build_snapshot(
        &self,
        spec: &CampaignSpec,
        providers: &BTreeMap<String, Arc<dyn ProviderInstance>>,
        builder: &SnapshotBuilder,
    ) -> Result<DatasetSnapshot> {
        match &spec.dataset {
            DatasetSpec::Tables { sources, .. } => {
                let mut fixtures = Vec::new();
                let mut from_variant: Vec<(LogicalTableName, String)> = Vec::new();
                for source in sources {
                    match source {
                        TableSource::Fixture(fixture) => fixtures.push(fixture.clone()),
                        TableSource::FromVariant { table, variant } => {
                            from_variant.push((table.clone(), variant.clone()));
                        }
                    }
                }
                let (mut schemas, mut batches) = builder.collect_fixtures(&fixtures).await?;
                for (table, variant_name) in &from_variant {
                    let variant = &spec.variants[variant_name];
                    let (remote, instance) = resolve_variant_binding(variant, table)?;
                    let targets = vec![(table.clone(), remote, instance)];
                    let (table_schemas, table_batches) =
                        self.snapshot_variant_tables(variant, &targets, providers).await?;
                    for (name, schema) in table_schemas {
                        if schemas.contains_key(&name) {
                            return Err(Error::Dataset {
                                table: name,
                                message: "table is defined by more than one dataset source"
                                    .to_owned(),
                            });
                        }
                        schemas.insert(name, schema);
                    }
                    for (name, value) in table_batches {
                        batches.insert(name, value);
                    }
                }
                if schemas.is_empty() {
                    return Err(Error::Dataset {
                        table: "*".to_owned(),
                        message: "dataset produced no tables".to_owned(),
                    });
                }
                builder.build_from_batches(schemas, batches)
            }
            DatasetSpec::Variant { variant, .. } => {
                let _ = oracle_name;
                let variant_spec = &spec.variants[variant];
                let mut targets = Vec::new();
                let mut seen = std::collections::BTreeSet::new();
                for binding in variant_spec.tables.values() {
                    let TableBinding::Provider { instance, .. } = binding else {
                        continue;
                    };
                    if !seen.insert(instance.clone()) {
                        continue;
                    }
                    let discovered = providers[instance]
                        .discover(&TableSelection::default())
                        .await?;
                    for table in discovered {
                        targets.push((table.logical_name, table.remote, instance.clone()));
                    }
                }
                if targets.is_empty() {
                    return Err(Error::Dataset {
                        table: "*".to_owned(),
                        message: format!("variant `{variant}` discovered no tables"),
                    });
                }
                let (schemas, batches) = self
                    .snapshot_variant_tables(variant_spec, &targets, providers)
                    .await?;
                builder.build_from_batches(schemas, batches)
            }
        }
    }

    /// Snapshot a set of logical tables from a variant's provider(s). Each target is
    /// `(logical_name, remote_ref, provider_instance)`. The tables are registered into
    /// a session built from the variant and materialized with `SELECT *`.
    async fn snapshot_variant_tables(
        &self,
        variant: &VariantSpec,
        targets: &[(LogicalTableName, RemoteTableRef, String)],
        providers: &BTreeMap<String, Arc<dyn ProviderInstance>>,
    ) -> Result<(
        BTreeMap<String, arrow::datatypes::SchemaRef>,
        BTreeMap<String, Vec<RecordBatch>>,
    )> {
        let context = self.create_session(variant)?;
        let mut provider_schemas = BTreeMap::new();
        for (logical, remote, instance) in targets {
            let table_provider = providers[instance].table_provider(remote).await?;
            provider_schemas.insert(logical.clone(), table_provider.schema());
            context.register_table(logical, table_provider)?;
        }
        let mut schemas = BTreeMap::new();
        let mut batches = BTreeMap::new();
        for (logical, _, _) in targets {
            let escaped = logical.replace('"', "\"\"");
            let result = context
                .sql(&format!("SELECT * FROM \"{escaped}\""))
                .await?
                .collect()
                .await?;
            let schema = result
                .first()
                .map(RecordBatch::schema)
                .unwrap_or_else(|| Arc::clone(&provider_schemas[logical]));
            schemas.insert(logical.clone(), schema);
            batches.insert(logical.clone(), result);
        }
        Ok((schemas, batches))
    }

    fn create_session(&self, variant: &VariantSpec) -> Result<SessionContext> {
        let settings: HashMap<_, _> = variant.session.config.clone().into_iter().collect();
        let config =
            SessionConfig::from_string_hash_map(&settings).map_err(|error| Error::Variant {
                variant: variant.name.clone(),
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
                        variant: variant.name.clone(),
                        message: format!("extension `{}` is unavailable", extension.kind),
                    })?;
            builder = implementation.apply(builder, &extension.params)?;
        }
        Ok(SessionContext::new_with_state(builder.build()))
    }

    async fn build_sessions(
        &self,
        spec: &CampaignSpec,
        snapshot: &DatasetSnapshot,
        providers: &BTreeMap<String, Arc<dyn ProviderInstance>>,
    ) -> Result<BTreeMap<String, SessionContext>> {
        let mut sessions = BTreeMap::new();
        for (name, variant) in &spec.variants {
            let context = self.create_session(variant)?;
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

fn generate_random_queries(
    spec: &CampaignSpec,
    snapshot: &DatasetSnapshot,
) -> Vec<crate::GeneratedQuery> {
    if spec.random_generators.is_empty() {
        return Vec::new();
    }
    let generator = crate::SchemaAwareGenerator::new(
        snapshot
            .tables
            .iter()
            .map(|(name, table)| (name.as_str(), &table.schema)),
    );
    spec.random_generators
        .iter()
        .flat_map(|generator_spec| {
            generator.generate(
                generator_spec.seed,
                generator_spec.count,
                generator_spec.nondeterministic,
            )
        })
        .collect()
}

fn oracle_name(spec: &CampaignSpec) -> Result<&str> {
    let names: Vec<_> = spec
        .variants
        .iter()
        .filter_map(|(name, variant)| variant.oracle.then_some(name.as_str()))
        .collect();
    match names.as_slice() {
        [name] => Ok(name),
        [] => configuration("variants", "mark exactly one variant with `oracle: true`"),
        _ => configuration("variants", "only one variant can set `oracle: true`"),
    }
}

fn resolve_variant_binding(
    variant: &VariantSpec,
    logical: &str,
) -> Result<(RemoteTableRef, String)> {
    let binding = variant
        .tables
        .get(logical)
        .or_else(|| variant.tables.get("*"));
    let Some(TableBinding::Provider { instance, remote }) = binding else {
        return configuration(
            &format!("variants.{}.tables.{logical}", variant.name),
            "a variant-sourced table must bind a provider",
        );
    };
    let remote = remote
        .clone()
        .unwrap_or_else(|| RemoteTableRef(logical.to_owned()));
    Ok((remote, instance.clone()))
}

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

async fn verify_existing_targets(
    spec: &CampaignSpec,
    snapshot: &DatasetSnapshot,
    providers: &BTreeMap<String, Arc<dyn ProviderInstance>>,
) -> Result<()> {
    for (provider_name, remote, logical) in managed_targets(spec, snapshot)? {
        if !matches!(
            spec.providers[&provider_name].lifecycle,
            ProviderLifecycle::Existing
        ) {
            continue;
        }
        let provider = &providers[&provider_name];
        if !provider.table_exists(&remote).await? {
            return Err(Error::Provider {
                provider: provider_name,
                message: format!("existing target `{}` does not exist", remote.0),
            });
        }
        let table_provider = provider.table_provider(&remote).await?;
        let provider_schema = table_provider.schema();
        let context = SessionContext::new();
        context.register_table("__existing", table_provider)?;
        let batches = context
            .sql("SELECT * FROM __existing")
            .await?
            .collect()
            .await?;
        let schema = batches
            .first()
            .map(RecordBatch::schema)
            .unwrap_or(provider_schema);
        let expected = &snapshot.tables[&logical];
        let content_hash = logical_hash(schema.as_ref(), &batches)?;
        if schema != expected.schema || content_hash != expected.content_hash {
            return Err(Error::Provider {
                provider: provider_name,
                message: format!(
                    "existing target `{}` does not match canonical table `{logical}`",
                    remote.0
                ),
            });
        }
    }
    Ok(())
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
                if !provider.table_exists(remote).await? {
                    return Err(Error::Provider {
                        provider: provider_name.clone(),
                        message: format!("existing target `{}` does not exist", remote.0),
                    });
                }
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
        DatasetSpec, FixtureSource, GeneratedQuery, Ordering, QueryProvenance, RunLimits,
        SessionSpec, TableSource, VariantSpec,
    };

    fn sql_fixtures(sql: &str) -> DatasetSpec {
        DatasetSpec::Tables {
            sources: vec![TableSource::Fixture(FixtureSource::SqlInline {
                sql: sql.to_owned(),
            })],
            verify_unchanged: true,
        }
    }

    use super::*;

    fn memtable_spec(oracles: &[&str]) -> CampaignSpec {
        let variants = ["first", "second"]
            .into_iter()
            .map(|name| {
                (
                    name.to_owned(),
                    VariantSpec {
                        name: name.to_owned(),
                        oracle: oracles.contains(&name),
                        session: SessionSpec::default(),
                        tables: [("*".to_owned(), TableBinding::MemTable)].into(),
                    },
                )
            })
            .collect();
        CampaignSpec {
            name: "validation".to_owned(),
            dataset: sql_fixtures("CREATE TABLE t(i INT);"),
            providers: BTreeMap::new(),
            variants,
            corpus: vec![GeneratedQuery::corpus("SELECT * FROM t", None)],
            random_generators: Vec::new(),
            limits: RunLimits::default(),
            artifacts: PathBuf::from("artifacts"),
        }
    }

    #[test]
    fn requires_exactly_one_variant_oracle() {
        let runner = CampaignRunner::new();
        assert!(runner.validate(&memtable_spec(&[])).is_err());
        assert!(runner
            .validate(&memtable_spec(&["first", "second"]))
            .is_err());
        assert!(runner.validate(&memtable_spec(&["second"])).is_ok());
    }

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
                        oracle: name == "oracle",
                        session: SessionSpec::default(),
                        tables: [("*".to_owned(), TableBinding::MemTable)].into(),
                    },
                )
            })
            .collect();
        let report = CampaignRunner::new()
            .run(CampaignSpec {
                name: "smoke".to_owned(),
                dataset: sql_fixtures(
                    "CREATE TABLE t(i INT); INSERT INTO t VALUES (1), (2), (2), (NULL);",
                ),
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
                random_generators: Vec::new(),
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
