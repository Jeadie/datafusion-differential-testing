use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use datafusion_differential_testing::{
    CampaignSpec, DatasetSpec, FixtureSource, GeneratedQuery, Ordering, ProviderLifecycle,
    ProviderSpec, QueryProvenance, RemoteTableRef, RunLimits, SessionExtensionSpec, SessionSpec,
    TableBinding, TableSource, VariantSpec,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "default_version")]
    pub version: u32,
    #[serde(default = "default_name")]
    pub name: String,
    pub dataset: DatasetConfig,
    #[serde(default)]
    pub campaign: CampaignConfig,
    #[serde(default)]
    pub artifacts: ArtifactConfig,
    #[serde(default)]
    pub providers: BTreeMap<String, ProviderConfig>,
    pub variants: BTreeMap<String, VariantConfig>,
    pub generators: Vec<GeneratorConfig>,
}

/// The `dataset:` value accepts three shapes:
/// - a sequence of per-table entries (`Entries`), fixture type inferred from the
///   file extension or a single table sourced from another variant;
/// - a map with `paths:` (multi-table SQL fixtures) or `variant:` (whole-variant
///   discovery), both handled by `Map`.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum DatasetConfig {
    Entries(Vec<DatasetEntry>),
    Map(DatasetMap),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetMap {
    #[serde(default)]
    pub paths: Vec<PathBuf>,
    #[serde(default)]
    pub variant: Option<String>,
    #[serde(default = "default_true")]
    pub verify_existing_unchanged: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetEntry {
    pub name: String,
    #[serde(default)]
    pub path: Option<PathBuf>,
    #[serde(default)]
    pub variant: Option<String>,
    #[serde(default)]
    pub options: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct CampaignConfig {
    pub max_queries: Option<usize>,
    pub max_findings: Option<usize>,
    pub query_timeout: Option<String>,
    pub max_sql_bytes: Option<usize>,
    pub max_result_rows: Option<usize>,
    pub max_result_bytes: Option<usize>,
    #[serde(default)]
    pub stop_on_finding: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactConfig {
    pub directory: PathBuf,
}

impl Default for ArtifactConfig {
    fn default() -> Self {
        Self {
            directory: PathBuf::from("artifacts"),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderConfig {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub lifecycle: Option<String>,
    pub teardown: Option<bool>,
    #[serde(default)]
    pub params: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VariantConfig {
    #[serde(default)]
    pub oracle: bool,
    pub tables: BTreeMap<String, BindingConfig>,
    #[serde(default)]
    pub session: SessionConfig,
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct SessionConfig {
    #[serde(default)]
    pub config: BTreeMap<String, String>,
    #[serde(default)]
    pub extensions: Vec<ExtensionConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExtensionConfig {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub params: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum BindingConfig {
    Name(String),
    Provider {
        provider: String,
        #[serde(rename = "from")]
        remote: Option<String>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum GeneratorConfig {
    Corpus {
        #[serde(default)]
        paths: Vec<String>,
        #[serde(default)]
        sql: Vec<String>,
    },
}

pub fn load(path: &Path) -> Result<CampaignSpec, String> {
    let text = fs::read_to_string(path)
        .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    let value: serde_yaml::Value = serde_yaml::from_str(&text)
        .map_err(|error| format!("invalid YAML in {}: {error}", path.display()))?;
    validate_yaml_shape(&value, "$", path)?;
    let config: Config = serde_yaml::from_value(value)
        .map_err(|error| format!("invalid YAML in {}: {error}", path.display()))?;
    config.into_spec(path.parent().unwrap_or_else(|| Path::new(".")))
}

fn validate_yaml_shape(
    value: &serde_yaml::Value,
    location: &str,
    path: &Path,
) -> Result<(), String> {
    match value {
        serde_yaml::Value::Mapping(mapping) => {
            for (key, value) in mapping {
                let serde_yaml::Value::String(key) = key else {
                    return Err(format!(
                        "invalid YAML in {}: mapping key at {location} must be a string",
                        path.display()
                    ));
                };
                if key == "<<" {
                    return Err(format!(
                        "invalid YAML in {}: merge keys are not supported at {location}",
                        path.display()
                    ));
                }
                validate_yaml_shape(value, &format!("{location}.{key}"), path)?;
            }
        }
        serde_yaml::Value::Sequence(sequence) => {
            for (index, value) in sequence.iter().enumerate() {
                validate_yaml_shape(value, &format!("{location}[{index}]"), path)?;
            }
        }
        serde_yaml::Value::Tagged(_) => {
            return Err(format!(
                "invalid YAML in {}: custom tags are not supported at {location}",
                path.display()
            ));
        }
        _ => {}
    }
    Ok(())
}

impl Config {
    fn into_spec(self, base: &Path) -> Result<CampaignSpec, String> {
        if self.version != 1 {
            return Err(format!(
                "unsupported config version {}; expected 1",
                self.version
            ));
        }
        let dataset = self.dataset.into_spec(base)?;

        let providers = self
            .providers
            .into_iter()
            .map(|(name, provider)| {
                let lifecycle = match provider.lifecycle.as_deref().unwrap_or("managed") {
                    "managed" => ProviderLifecycle::Managed {
                        teardown: provider.teardown.unwrap_or(true),
                    },
                    "existing" if provider.teardown.is_none() => ProviderLifecycle::Existing,
                    "existing" => {
                        return Err(format!(
                            "providers.{name}.teardown is invalid for an existing provider"
                        ));
                    }
                    value => {
                        return Err(format!(
                            "providers.{name}.lifecycle has unsupported value `{value}`"
                        ));
                    }
                };
                let params = provider
                    .params
                    .into_iter()
                    .map(|(key, value)| {
                        let value = resolve_environment(&value)?;
                        let value = if matches!(key.as_str(), "duckdb_open" | "sqlite_open") {
                            resolve(base, Path::new(&value))?
                                .to_string_lossy()
                                .into_owned()
                        } else {
                            value
                        };
                        Ok::<_, String>((key, value))
                    })
                    .collect::<Result<_, _>>()?;
                Ok((
                    name.clone(),
                    ProviderSpec {
                        name,
                        kind: provider.kind,
                        lifecycle,
                        params,
                    },
                ))
            })
            .collect::<Result<_, String>>()?;

        let variants = self
            .variants
            .into_iter()
            .map(|(name, variant)| {
                let tables = variant
                    .tables
                    .into_iter()
                    .map(|(table, binding)| {
                        let binding = match binding {
                            BindingConfig::Name(value) if value == "memtable" => {
                                TableBinding::MemTable
                            }
                            BindingConfig::Name(value) => {
                                return Err(format!(
                                    "variants.{name}.tables.{table} has unsupported binding `{value}`"
                                ));
                            }
                            BindingConfig::Provider { provider, remote } => {
                                TableBinding::Provider {
                                    instance: provider,
                                    remote: remote.map(RemoteTableRef),
                                }
                            }
                        };
                        Ok((table, binding))
                    })
                    .collect::<Result<_, String>>()?;
                Ok((
                    name.clone(),
                    VariantSpec {
                        name,
                        oracle: variant.oracle,
                        session: SessionSpec {
                            config: variant.session.config,
                            extensions: variant
                                .session
                                .extensions
                                .into_iter()
                                .map(|extension| SessionExtensionSpec {
                                    kind: extension.kind,
                                    params: extension.params,
                                })
                                .collect(),
                        },
                        tables,
                    },
                ))
            })
            .collect::<Result<_, String>>()?;

        let mut corpus = Vec::new();
        for generator in self.generators {
            match generator {
                GeneratorConfig::Corpus { paths, sql } => {
                    corpus.extend(sql.into_iter().map(|sql| GeneratedQuery::corpus(sql, None)));
                    for pattern in paths {
                        reject_expansion(&pattern)?;
                        let absolute = base.join(&pattern);
                        let pattern_text = absolute.to_string_lossy();
                        let mut matches: Vec<_> = glob::glob(&pattern_text)
                            .map_err(|error| format!("invalid corpus glob `{pattern}`: {error}"))?
                            .collect::<Result<Vec<_>, _>>()
                            .map_err(|error| {
                                format!("cannot read corpus glob `{pattern}`: {error}")
                            })?;
                        matches.sort();
                        if matches.is_empty() {
                            return Err(format!("corpus path `{pattern}` matched no files"));
                        }
                        for query_path in matches {
                            let query = fs::read_to_string(&query_path).map_err(|error| {
                                format!("cannot read {}: {error}", query_path.display())
                            })?;
                            corpus.push(GeneratedQuery {
                                sql: query,
                                ordering: Ordering::Detect,
                                provenance: QueryProvenance {
                                    generator: "corpus".to_owned(),
                                    source: Some(query_path.display().to_string()),
                                    provided_id: None,
                                },
                            });
                        }
                    }
                }
            }
        }

        let defaults = RunLimits::default();
        let query_timeout = self
            .campaign
            .query_timeout
            .as_deref()
            .map(humantime::parse_duration)
            .transpose()
            .map_err(|error| format!("campaign.query_timeout is invalid: {error}"))?
            .unwrap_or(defaults.query_timeout);
        if query_timeout == Duration::ZERO {
            return Err("campaign.query_timeout must be positive".to_owned());
        }
        Ok(CampaignSpec {
            name: self.name,
            dataset,
            providers,
            variants,
            corpus,
            limits: RunLimits {
                max_queries: self.campaign.max_queries.unwrap_or(defaults.max_queries),
                max_findings: self.campaign.max_findings.unwrap_or(defaults.max_findings),
                query_timeout,
                max_sql_bytes: self
                    .campaign
                    .max_sql_bytes
                    .unwrap_or(defaults.max_sql_bytes),
                max_result_rows: self
                    .campaign
                    .max_result_rows
                    .unwrap_or(defaults.max_result_rows),
                max_result_bytes: self
                    .campaign
                    .max_result_bytes
                    .unwrap_or(defaults.max_result_bytes),
                stop_on_finding: self.campaign.stop_on_finding,
            },
            artifacts: resolve(base, &self.artifacts.directory)?,
        })
    }
}

impl DatasetConfig {
    fn into_spec(self, base: &Path) -> Result<DatasetSpec, String> {
        match self {
            DatasetConfig::Map(map) => {
                let has_paths = !map.paths.is_empty();
                match (has_paths, map.variant) {
                    (true, Some(_)) => {
                        Err("dataset accepts `paths` or `variant`, not both".to_owned())
                    }
                    (false, None) => {
                        Err("dataset requires `paths`, `variant`, or a table sequence".to_owned())
                    }
                    (true, None) => {
                        let mut sources = Vec::new();
                        for path in map.paths {
                            sources.push(TableSource::Fixture(FixtureSource::Sql {
                                path: resolve(base, &path)?,
                            }));
                        }
                        Ok(DatasetSpec::Tables {
                            sources,
                            verify_unchanged: map.verify_existing_unchanged,
                        })
                    }
                    (false, Some(variant)) => Ok(DatasetSpec::Variant {
                        variant,
                        verify_unchanged: map.verify_existing_unchanged,
                    }),
                }
            }
            DatasetConfig::Entries(entries) => {
                let mut sources = Vec::new();
                for (index, entry) in entries.into_iter().enumerate() {
                    sources.push(entry.into_source(base, index)?);
                }
                Ok(DatasetSpec::Tables {
                    sources,
                    verify_unchanged: true,
                })
            }
        }
    }
}

impl DatasetEntry {
    fn into_source(self, base: &Path, index: usize) -> Result<TableSource, String> {
        match (self.path, self.variant) {
            (Some(_), Some(_)) => Err(format!("dataset[{index}] sets both `path` and `variant`")),
            (None, None) => Err(format!("dataset[{index}] requires `path` or `variant`")),
            (None, Some(variant)) => {
                if !self.options.is_empty() {
                    return Err(format!(
                        "dataset[{index}].options is not supported for a variant source"
                    ));
                }
                Ok(TableSource::FromVariant {
                    table: self.name,
                    variant,
                })
            }
            (Some(path), None) => {
                let extension = path
                    .extension()
                    .and_then(|value| value.to_str())
                    .map(str::to_ascii_lowercase);
                let resolved = resolve(base, &path)?;
                let fixture = match extension.as_deref() {
                    Some("arrow") => {
                        reject_options(&self.options, index, "Arrow IPC")?;
                        FixtureSource::ArrowIpc {
                            name: self.name,
                            path: resolved,
                        }
                    }
                    Some("parquet") => {
                        reject_options(&self.options, index, "Parquet")?;
                        FixtureSource::Parquet {
                            name: self.name,
                            path: resolved,
                        }
                    }
                    Some("csv") => FixtureSource::Csv {
                        name: self.name,
                        path: resolved,
                        options: self.options,
                    },
                    other => {
                        return Err(format!(
                            "dataset[{index}].path has unsupported file extension `{}`; \
                             expected .arrow, .csv, or .parquet",
                            other.unwrap_or("")
                        ));
                    }
                };
                Ok(TableSource::Fixture(fixture))
            }
        }
    }
}

fn reject_options(
    options: &BTreeMap<String, String>,
    index: usize,
    kind: &str,
) -> Result<(), String> {
    if options.is_empty() {
        Ok(())
    } else {
        Err(format!("dataset[{index}].options is not supported for {kind}"))
    }
}

fn resolve(base: &Path, value: &Path) -> Result<PathBuf, String> {
    reject_expansion(&value.to_string_lossy())?;
    Ok(if value.is_absolute() {
        value.to_owned()
    } else {
        base.join(value)
    })
}

fn reject_expansion(value: &str) -> Result<(), String> {
    if value.starts_with('~') || value.contains("$(") || value.contains('`') {
        Err(format!("path `{value}` uses unsupported shell expansion"))
    } else {
        Ok(())
    }
}

fn resolve_environment(value: &str) -> Result<String, String> {
    let Some(name) = value
        .strip_prefix("${env:")
        .and_then(|rest| rest.strip_suffix('}'))
    else {
        return Ok(value.to_owned());
    };
    std::env::var(name).map_err(|_| format!("environment variable `{name}` is not set"))
}

const fn default_true() -> bool {
    true
}

const fn default_version() -> u32 {
    1
}

fn default_name() -> String {
    "campaign".to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec_from(yaml: &str) -> CampaignSpec {
        let value: serde_yaml::Value = serde_yaml::from_str(yaml).expect("valid YAML");
        let config: Config = serde_yaml::from_value(value).expect("valid config");
        config.into_spec(Path::new("/base")).expect("into_spec")
    }

    const VARIANTS_AND_GENERATORS: &str = r#"
variants:
  oracle:
    oracle: true
    tables:
      "*": memtable
  candidate:
    tables:
      "*": memtable
generators:
  - type: corpus
    sql:
      - "SELECT 1"
"#;

    #[test]
    fn dataset_paths_map_parses_to_sql_tables() {
        let yaml = format!(
            "name: t\ndataset:\n  paths:\n    - fixtures/data.sql\n{VARIANTS_AND_GENERATORS}"
        );
        let spec = spec_from(&yaml);
        let DatasetSpec::Tables {
            sources,
            verify_unchanged,
        } = spec.dataset
        else {
            panic!("expected Tables dataset");
        };
        assert!(verify_unchanged);
        assert_eq!(sources.len(), 1);
        assert_eq!(
            sources[0],
            TableSource::Fixture(FixtureSource::Sql {
                path: PathBuf::from("/base/fixtures/data.sql"),
            })
        );
    }

    #[test]
    fn dataset_entry_sequence_infers_extensions_and_variant() {
        let yaml = format!(
            r#"name: t
dataset:
  - name: orders
    path: fixtures/orders.arrow
  - name: customers
    path: fixtures/customers.csv
    options:
      has_header: "true"
  - name: events
    path: fixtures/events.parquet
  - name: lineitem
    variant: oracle
{VARIANTS_AND_GENERATORS}"#
        );
        let spec = spec_from(&yaml);
        let DatasetSpec::Tables { sources, .. } = spec.dataset else {
            panic!("expected Tables dataset");
        };
        assert_eq!(sources.len(), 4);
        assert!(matches!(
            sources[0],
            TableSource::Fixture(FixtureSource::ArrowIpc { .. })
        ));
        assert!(matches!(
            sources[1],
            TableSource::Fixture(FixtureSource::Csv { .. })
        ));
        assert!(matches!(
            sources[2],
            TableSource::Fixture(FixtureSource::Parquet { .. })
        ));
        assert_eq!(
            sources[3],
            TableSource::FromVariant {
                table: "lineitem".to_owned(),
                variant: "oracle".to_owned(),
            }
        );
    }

    #[test]
    fn dataset_variant_map_parses_to_variant() {
        let yaml =
            format!("name: t\ndataset:\n  variant: oracle\n{VARIANTS_AND_GENERATORS}");
        let spec = spec_from(&yaml);
        assert_eq!(
            spec.dataset,
            DatasetSpec::Variant {
                variant: "oracle".to_owned(),
                verify_unchanged: true,
            }
        );
    }

    #[test]
    fn dataset_entry_rejects_unknown_extension() {
        let yaml = format!(
            "name: t\ndataset:\n  - name: bad\n    path: fixtures/data.txt\n{VARIANTS_AND_GENERATORS}"
        );
        let value: serde_yaml::Value = serde_yaml::from_str(&yaml).expect("valid YAML");
        let config: Config = serde_yaml::from_value(value).expect("valid config");
        let error = config
            .into_spec(Path::new("/base"))
            .expect_err("unknown extension must fail");
        assert!(error.contains("unsupported file extension"));
    }

    #[test]
    fn dataset_entry_rejects_path_and_variant() {
        let yaml = format!(
            "name: t\ndataset:\n  - name: bad\n    path: a.csv\n    variant: oracle\n{VARIANTS_AND_GENERATORS}"
        );
        let value: serde_yaml::Value = serde_yaml::from_str(&yaml).expect("valid YAML");
        let config: Config = serde_yaml::from_value(value).expect("valid config");
        let error = config
            .into_spec(Path::new("/base"))
            .expect_err("path and variant must fail");
        assert!(error.contains("both `path` and `variant`"));
    }

    #[test]
    fn provider_lifecycle_defaults_to_managed_and_params_optional() {
        let yaml = format!(
            r#"name: t
dataset:
  paths:
    - fixtures/data.sql
providers:
  in_memory:
    type: duckdb
{VARIANTS_AND_GENERATORS}"#
        );
        let spec = spec_from(&yaml);
        let provider = &spec.providers["in_memory"];
        assert_eq!(
            provider.lifecycle,
            ProviderLifecycle::Managed { teardown: true }
        );
        assert!(provider.params.is_empty());
    }

    #[test]
    fn version_and_name_are_optional() {
        let yaml = format!(
            "dataset:\n  paths:\n    - fixtures/data.sql\n{VARIANTS_AND_GENERATORS}"
        );
        let spec = spec_from(&yaml);
        assert_eq!(spec.name, "campaign");
    }

    #[test]
    fn yaml_rejects_duplicate_keys() {
        let error = serde_yaml::from_str::<serde_yaml::Value>("version: 1\nversion: 1\n")
            .expect_err("duplicate keys must fail");
        assert!(error.to_string().contains("duplicate"));
    }

    #[test]
    fn yaml_rejects_merge_keys() {
        let value: serde_yaml::Value = serde_yaml::from_str(
            "defaults: &defaults\n  lifecycle: managed\nprovider:\n  <<: *defaults\n",
        )
        .expect("valid YAML syntax");
        let error = validate_yaml_shape(&value, "$", Path::new("campaign.yaml"))
            .expect_err("merge keys must fail");
        assert!(error.contains("merge keys are not supported"));
    }

    #[test]
    fn yaml_rejects_custom_tags_and_non_string_keys() {
        let tagged: serde_yaml::Value =
            serde_yaml::from_str("value: !custom content\n").expect("valid YAML syntax");
        assert!(validate_yaml_shape(&tagged, "$", Path::new("campaign.yaml")).is_err());

        let numeric_key: serde_yaml::Value =
            serde_yaml::from_str("1: value\n").expect("valid YAML syntax");
        assert!(validate_yaml_shape(&numeric_key, "$", Path::new("campaign.yaml")).is_err());
    }
}
