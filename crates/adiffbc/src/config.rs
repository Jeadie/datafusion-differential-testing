//! YAML campaign configuration, parsed into a validated [`Spec`].

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use adbc_core::options::AdbcVersion;
use datafusion_differential_testing::GeneratedQuery;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    #[serde(default = "default_name")]
    name: String,
    #[serde(default)]
    setup: SetupConfig,
    #[serde(default)]
    campaign: CampaignConfig,
    drivers: BTreeMap<String, DriverConfig>,
    variants: BTreeMap<String, VariantConfig>,
    generators: Vec<GeneratorConfig>,
}

/// SQL that is run on every variant before any query, e.g. to create and fill tables.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct SetupConfig {
    #[serde(default)]
    paths: Vec<PathBuf>,
    #[serde(default)]
    sql: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct CampaignConfig {
    max_queries: Option<usize>,
    max_findings: Option<usize>,
    #[serde(default)]
    stop_on_finding: bool,
    #[serde(default)]
    schema: SchemaComparison,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DriverConfig {
    /// A driver name or manifest (searched for by the ADBC driver manager), or a
    /// path to a shared library or `.toml` manifest (relative to the config file).
    /// May be a `${env:NAME}` reference.
    driver: String,
    entrypoint: Option<String>,
    adbc_version: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct VariantConfig {
    #[serde(default)]
    oracle: bool,
    driver: String,
    uri: Option<String>,
    #[serde(default)]
    options: BTreeMap<String, String>,
    #[serde(default)]
    connection_options: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum GeneratorConfig {
    Corpus {
        #[serde(default)]
        paths: Vec<String>,
        #[serde(default)]
        sql: Vec<String>,
    },
}

/// How strictly result schemas are compared between the oracle and a candidate.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaComparison {
    /// Ignore top-level field nullability and all schema/field metadata. Drivers
    /// rarely agree on these, so a strict comparison mostly reports noise.
    #[default]
    Relaxed,
    /// Compare schemas exactly.
    Strict,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DriverLocation {
    /// Resolved by the driver manager: a library name or a manifest name.
    Search(String),
    /// An absolute path to a shared library or `.toml` manifest.
    Path(PathBuf),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DriverSpec {
    pub name: String,
    pub location: DriverLocation,
    pub entrypoint: Option<String>,
    pub version: AdbcVersion,
    /// Extra directories searched for driver manifests.
    pub search_paths: Vec<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VariantSpec {
    pub name: String,
    pub driver: DriverSpec,
    pub uri: Option<String>,
    pub database_options: BTreeMap<String, String>,
    pub connection_options: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetupScript {
    pub source: String,
    pub sql: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub max_queries: usize,
    pub max_findings: usize,
    pub stop_on_finding: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Spec {
    pub name: String,
    pub setup: Vec<SetupScript>,
    pub limits: Limits,
    pub schema: SchemaComparison,
    pub oracle: VariantSpec,
    /// Never empty.
    pub candidates: Vec<VariantSpec>,
    pub corpus: Vec<GeneratedQuery>,
}

impl Spec {
    pub fn variants(&self) -> impl Iterator<Item = &VariantSpec> {
        std::iter::once(&self.oracle).chain(&self.candidates)
    }
}

pub fn load(path: &Path) -> Result<Spec, String> {
    let text = fs::read_to_string(path)
        .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    parse(&text, path.parent().unwrap_or_else(|| Path::new(".")))
        .map_err(|error| format!("{}: {error}", path.display()))
}

fn parse(text: &str, base: &Path) -> Result<Spec, String> {
    let config: Config =
        serde_yaml::from_str(text).map_err(|error| format!("invalid YAML: {error}"))?;
    config.into_spec(base)
}

impl Config {
    fn into_spec(self, base: &Path) -> Result<Spec, String> {
        let drivers = self
            .drivers
            .into_iter()
            .map(|(name, driver)| {
                let spec = driver.into_spec(&name, base)?;
                Ok((name, spec))
            })
            .collect::<Result<BTreeMap<_, _>, String>>()?;

        let mut oracle = None;
        let mut candidates = Vec::new();
        for (name, variant) in self.variants {
            let driver = drivers.get(&variant.driver).cloned().ok_or_else(|| {
                format!(
                    "variants.{name}.driver: driver `{}` is not defined in `drivers`",
                    variant.driver
                )
            })?;
            let spec = VariantSpec {
                driver,
                uri: variant
                    .uri
                    .as_deref()
                    .map(resolve_environment)
                    .transpose()?,
                database_options: resolve_options(variant.options)?,
                connection_options: resolve_options(variant.connection_options)?,
                name,
            };
            if !variant.oracle {
                candidates.push(spec);
            } else if let Some(previous) = oracle.replace(spec) {
                return Err(format!(
                    "variants: only one variant can set `oracle: true` (found `{}` and another)",
                    previous.name
                ));
            }
        }
        let oracle = oracle.ok_or("variants: mark exactly one variant with `oracle: true`")?;
        if candidates.is_empty() {
            return Err("variants: define at least one non-oracle variant".to_owned());
        }

        let mut setup: Vec<SetupScript> = self
            .setup
            .sql
            .into_iter()
            .enumerate()
            .map(|(index, sql)| SetupScript {
                source: format!("setup.sql[{index}]"),
                sql,
            })
            .collect();
        for path in self.setup.paths {
            let path = base.join(path);
            let sql = fs::read_to_string(&path)
                .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
            setup.push(SetupScript {
                source: path.display().to_string(),
                sql,
            });
        }

        let mut corpus = Vec::new();
        for generator in self.generators {
            match generator {
                GeneratorConfig::Corpus { paths, sql } => {
                    corpus.extend(sql.into_iter().map(|sql| GeneratedQuery::corpus(sql, None)));
                    for pattern in paths {
                        corpus.extend(load_corpus_glob(base, &pattern)?);
                    }
                }
            }
        }
        if corpus.is_empty() {
            return Err("generators: no queries were configured".to_owned());
        }

        Ok(Spec {
            name: self.name,
            setup,
            limits: Limits {
                max_queries: self.campaign.max_queries.unwrap_or(1_000),
                max_findings: self.campaign.max_findings.unwrap_or(100),
                stop_on_finding: self.campaign.stop_on_finding,
            },
            schema: self.campaign.schema,
            oracle,
            candidates,
            corpus,
        })
    }
}

impl DriverConfig {
    fn into_spec(self, name: &str, base: &Path) -> Result<DriverSpec, String> {
        let driver = resolve_environment(&self.driver)?;
        let looks_like_path = driver.contains(['/', '\\']) || driver.starts_with('.');
        let location = if looks_like_path {
            DriverLocation::Path(base.join(&driver))
        } else {
            DriverLocation::Search(driver)
        };
        let version = match self.adbc_version {
            Some(version) => version
                .parse()
                .map_err(|_| format!("drivers.{name}.adbc_version: expected `1.0.0` or `1.1.0`"))?,
            None => AdbcVersion::default(),
        };
        Ok(DriverSpec {
            name: name.to_owned(),
            location,
            entrypoint: self.entrypoint,
            version,
            search_paths: vec![base.to_path_buf()],
        })
    }
}

fn load_corpus_glob(base: &Path, pattern: &str) -> Result<Vec<GeneratedQuery>, String> {
    let absolute = base.join(pattern);
    let mut matches = glob::glob(&absolute.to_string_lossy())
        .map_err(|error| format!("invalid corpus glob `{pattern}`: {error}"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("cannot read corpus glob `{pattern}`: {error}"))?;
    if matches.is_empty() {
        return Err(format!("corpus path `{pattern}` matched no files"));
    }
    matches.sort();
    matches
        .into_iter()
        .map(|path| {
            let sql = fs::read_to_string(&path)
                .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
            Ok(GeneratedQuery::corpus(
                sql,
                Some(path.display().to_string()),
            ))
        })
        .collect()
}

fn resolve_options(options: BTreeMap<String, String>) -> Result<BTreeMap<String, String>, String> {
    options
        .into_iter()
        .map(|(key, value)| Ok((key, resolve_environment(&value)?)))
        .collect()
}

/// Expand a whole-value `${env:NAME}` reference, so secrets stay out of the file.
fn resolve_environment(value: &str) -> Result<String, String> {
    let Some(name) = value
        .strip_prefix("${env:")
        .and_then(|rest| rest.strip_suffix('}'))
    else {
        return Ok(value.to_owned());
    };
    std::env::var(name).map_err(|_| format!("environment variable `{name}` is not set"))
}

fn default_name() -> String {
    "campaign".to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = r#"
drivers:
  sqlite:
    driver: adbc_driver_sqlite
  duckdb:
    driver: ./lib/libduckdb.dylib
    entrypoint: duckdb_adbc_init
    adbc_version: 1.0.0
variants:
  a:
    oracle: true
    driver: sqlite
    uri: ":memory:"
  b:
    driver: duckdb
generators:
  - type: corpus
    sql: ["SELECT 1"]
"#;

    #[test]
    fn parses_oracle_candidates_and_driver_locations() {
        let spec = parse(MINIMAL, Path::new("/base")).expect("valid config");
        assert_eq!(spec.oracle.name, "a");
        assert_eq!(spec.oracle.uri.as_deref(), Some(":memory:"));
        assert_eq!(
            spec.oracle.driver.location,
            DriverLocation::Search("adbc_driver_sqlite".to_owned())
        );
        let [candidate] = spec.candidates.as_slice() else {
            panic!("expected one candidate");
        };
        assert_eq!(
            candidate.driver.location,
            DriverLocation::Path(PathBuf::from("/base/./lib/libduckdb.dylib"))
        );
        assert_eq!(candidate.driver.version, AdbcVersion::V100);
        assert_eq!(spec.schema, SchemaComparison::Relaxed);
    }

    #[test]
    fn rejects_missing_or_duplicate_oracle() {
        let none = MINIMAL.replace("oracle: true", "oracle: false");
        assert!(parse(&none, Path::new("/"))
            .unwrap_err()
            .contains("exactly one"));
        let two = MINIMAL.replace("driver: duckdb\n", "driver: duckdb\n    oracle: true\n");
        assert!(parse(&two, Path::new("/"))
            .unwrap_err()
            .contains("only one"));
    }

    #[test]
    fn rejects_unknown_driver_reference() {
        let config = MINIMAL.replace("driver: duckdb\n", "driver: nope\n");
        assert!(parse(&config, Path::new("/"))
            .unwrap_err()
            .contains("driver `nope` is not defined"));
    }
}
