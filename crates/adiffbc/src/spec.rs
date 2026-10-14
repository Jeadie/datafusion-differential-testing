//! A validated campaign, and the parsers that build it from command-line text.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use datafusion_differential_testing::GeneratedQuery;

/// How strictly result schemas are compared between the oracle and a candidate.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum SchemaComparison {
    /// Ignore top-level field nullability and all schema/field metadata. Drivers
    /// rarely agree on these, so a strict comparison mostly reports noise.
    #[default]
    Relaxed,
    /// Compare schemas exactly.
    Strict,
}

/// A `dbc` version constraint, e.g. `1.5.6` (exact) or `>=1.4,<2`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionConstraint(String);

impl VersionConstraint {
    /// The `dbc install` package argument, e.g. `duckdb=1.5.6`.
    pub fn package(&self, name: &str) -> String {
        if self.0.starts_with(|c: char| c.is_ascii_digit()) {
            format!("{name}={}", self.0)
        } else {
            format!("{name}{}", self.0)
        }
    }

    /// A directory name for this constraint, so pinned versions do not
    /// overwrite each other's manifests.
    pub fn directory_name(&self, name: &str) -> String {
        let constraint: String = self
            .0
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '.' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        format!("{name}@{constraint}")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DriverSource {
    /// A shared library or `.toml` manifest. Never installed.
    Path(PathBuf),
    /// Found by name in the ADBC search paths, and installable with `dbc`.
    Registry {
        name: String,
        version: Option<VersionConstraint>,
    },
}

/// A driver reference as written by the user: `duckdb`, `duckdb@1.5.6`, or a path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DriverSpec {
    /// The text the user wrote. Loaded drivers are cached under it.
    pub key: String,
    pub source: DriverSource,
}

impl DriverSpec {
    pub fn parse(text: &str) -> Result<Self, String> {
        let is_path = text.contains(['/', '\\'])
            || Path::new(text)
                .extension()
                .and_then(|extension| extension.to_str())
                .is_some_and(|extension| matches!(extension, "toml" | "so" | "dylib" | "dll"));
        let source = if is_path {
            let path = std::path::absolute(text)
                .map_err(|error| format!("cannot resolve driver path `{text}`: {error}"))?;
            DriverSource::Path(path)
        } else {
            let (name, version) = match text.split_once('@') {
                Some((name, version)) => (name, Some(version)),
                None => (text, None),
            };
            if name.is_empty() || version.is_some_and(str::is_empty) {
                return Err(format!(
                    "invalid driver `{text}`: expected NAME or NAME@VERSION"
                ));
            }
            DriverSource::Registry {
                name: name.to_owned(),
                version: version.map(|version| VersionConstraint(version.to_owned())),
            }
        };
        Ok(Self {
            key: text.to_owned(),
            source,
        })
    }

    /// A short name for the driver, used as the default variant name.
    fn default_variant_name(&self) -> String {
        match &self.source {
            DriverSource::Registry { name, .. } => name.clone(),
            DriverSource::Path(path) => {
                let stem = path
                    .file_stem()
                    .map(|stem| stem.to_string_lossy().into_owned())
                    .unwrap_or_default();
                stem.strip_prefix("lib").map(str::to_owned).unwrap_or(stem)
            }
        }
    }
}

/// Where a variant's database comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Locator {
    /// An ADBC driver URI: `DRIVER:URI`, `DRIVER://...`, or a bare `DRIVER`.
    Uri {
        driver: DriverSpec,
        /// Sent as the ADBC `uri` database option.
        uri: Option<String>,
    },
    /// An ADBC connection profile: `profile://NAME` or `profile://PATH.toml`.
    Profile(String),
}

impl Locator {
    /// Parse a driver URI the way the ADBC driver manager does: the driver is
    /// the text before the first `:`. A `DRIVER://...` URI is sent whole, any
    /// other `DRIVER:REST` sends only `REST`. A bare `DRIVER` sends no URI.
    pub fn parse(text: &str) -> Result<Self, String> {
        if let Some(profile) = text
            .strip_prefix("profile://")
            .or_else(|| text.strip_prefix("profile:"))
        {
            if profile.is_empty() {
                return Err(format!(
                    "invalid profile URI `{text}`: missing profile name"
                ));
            }
            return Ok(Self::Profile(profile.to_owned()));
        }
        let (driver, uri) = match text.split_once(':') {
            None => (text, None),
            Some((driver, "")) => (driver, None),
            Some((driver, rest)) if rest.starts_with('/') => (driver, Some(text.to_owned())),
            Some((driver, rest)) => (driver, Some(rest.to_owned())),
        };
        Ok(Self::Uri {
            driver: DriverSpec::parse(driver)?,
            uri,
        })
    }

    fn default_variant_name(&self) -> String {
        match self {
            Self::Uri { driver, .. } => driver.default_variant_name(),
            Self::Profile(profile) => Path::new(profile)
                .file_stem()
                .map(|stem| stem.to_string_lossy().into_owned())
                .unwrap_or_else(|| profile.clone()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetupScript {
    pub source: String,
    pub sql: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VariantSpec {
    pub name: String,
    pub locator: Locator,
    pub database_options: BTreeMap<String, String>,
    pub connection_options: BTreeMap<String, String>,
    /// Run in order after connecting, before any query.
    pub setup: Vec<SetupScript>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Installer {
    /// Where `dbc` installs drivers and their manifests.
    pub directory: PathBuf,
    /// `None` means find `dbc` (or `uvx dbc`, `pipx run dbc`) on `PATH`.
    pub command: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DriverResolution {
    /// Searched before the driver manager's user and system directories.
    pub search_paths: Vec<PathBuf>,
    /// `None` when installation is disabled.
    pub installer: Option<Installer>,
}

impl DriverResolution {
    pub fn new(
        install_directory: PathBuf,
        installer_command: Option<Vec<String>>,
        install: bool,
    ) -> Self {
        // `dbc` installs into these environments by default, but the driver
        // manager does not search them.
        let environments = ["VIRTUAL_ENV", "CONDA_PREFIX"]
            .into_iter()
            .filter_map(std::env::var_os)
            .map(|prefix| PathBuf::from(prefix).join("etc/adbc/drivers"));
        Self {
            search_paths: std::iter::once(install_directory.clone())
                .chain(environments)
                .collect(),
            installer: install.then_some(Installer {
                directory: install_directory,
                command: installer_command,
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub max_queries: usize,
    pub max_findings: usize,
    pub stop_on_finding: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Spec {
    pub limits: Limits,
    pub schema: SchemaComparison,
    pub drivers: DriverResolution,
    pub oracle: VariantSpec,
    /// Never empty.
    pub candidates: Vec<VariantSpec>,
    /// Never empty.
    pub corpus: Vec<GeneratedQuery>,
}

/// Which variants an `-o`/`-c` option applies to.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Target {
    All,
    Variant(String),
}

/// A parsed `NAME.KEY=VALUE` option.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TargetedOption {
    target: Target,
    key: String,
    value: String,
}

impl TargetedOption {
    fn applies_to(&self, variant: &str) -> bool {
        match &self.target {
            Target::All => true,
            Target::Variant(name) => name == variant,
        }
    }

    fn parse(text: &str) -> Result<Self, String> {
        let invalid =
            || format!("invalid option `{text}`: expected NAME.KEY=VALUE (NAME may be `*`)");
        let (target, rest) = text.split_once('.').ok_or_else(invalid)?;
        let (key, value) = rest.split_once('=').ok_or_else(invalid)?;
        if target.is_empty() || key.is_empty() {
            return Err(invalid());
        }
        Ok(Self {
            target: if target == "*" {
                Target::All
            } else {
                Target::Variant(target.to_owned())
            },
            key: key.to_owned(),
            value: value.to_owned(),
        })
    }
}

/// Split an optional `NAME=` prefix. A name holds only `[A-Za-z0-9_-]`, so the
/// `=` inside a URI query string (after a `:`) is never taken as a name.
pub fn split_name(text: &str) -> (Option<&str>, &str) {
    match text.split_once('=') {
        Some((name, rest))
            if !name.is_empty()
                && name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-')) =>
        {
            (Some(name), rest)
        }
        _ => (None, text),
    }
}

/// The variant and option arguments, before validation.
#[derive(Debug, Default)]
pub struct VariantArgs<'a> {
    /// `[NAME=]URI`; the first one is the oracle when running a campaign.
    pub variants: &'a [String],
    pub database_options: &'a [String],
    pub connection_options: &'a [String],
    /// `[NAME=]FILE`; without a name the script runs on every variant.
    pub setup: &'a [String],
}

impl VariantArgs<'_> {
    pub fn parse(&self) -> Result<Vec<VariantSpec>, String> {
        let mut variants: Vec<VariantSpec> = Vec::new();
        for text in self.variants {
            let (name, uri) = split_name(text);
            let locator = Locator::parse(uri)?;
            let name = name.map_or_else(|| locator.default_variant_name(), str::to_owned);
            if variants.iter().any(|variant| variant.name == name) {
                return Err(format!(
                    "two variants are named `{name}`; name them, e.g. `a={uri}` and `b=...`"
                ));
            }
            variants.push(VariantSpec {
                name,
                locator,
                database_options: BTreeMap::new(),
                connection_options: BTreeMap::new(),
                setup: Vec::new(),
            });
        }

        let options = self
            .database_options
            .iter()
            .map(|text| (text, true))
            .chain(self.connection_options.iter().map(|text| (text, false)));
        for (text, is_database) in options {
            let option = TargetedOption::parse(text)?;
            let mut matched = false;
            for variant in &mut variants {
                if option.applies_to(&variant.name) {
                    matched = true;
                    let options = if is_database {
                        &mut variant.database_options
                    } else {
                        &mut variant.connection_options
                    };
                    options.insert(option.key.clone(), option.value.clone());
                }
            }
            if !matched {
                return Err(format!("option `{text}`: no variant is named that"));
            }
        }

        for text in self.setup {
            let (target, path) = match split_name(text) {
                (Some(name), path) if variants.iter().any(|variant| variant.name == name) => {
                    (Some(name), path)
                }
                _ => (None, text.as_str()),
            };
            let sql = std::fs::read_to_string(path)
                .map_err(|error| format!("cannot read setup file {path}: {error}"))?;
            for variant in &mut variants {
                if target.is_none_or(|name| name == variant.name) {
                    variant.setup.push(SetupScript {
                        source: path.to_owned(),
                        sql: sql.clone(),
                    });
                }
            }
        }
        Ok(variants)
    }
}

/// Read query files in order, then inline queries. A file argument holding
/// `*`, `?` or `[` is a glob, expanded in sorted order.
pub fn load_corpus(files: &[String], inline: &[String]) -> Result<Vec<GeneratedQuery>, String> {
    let mut paths = Vec::new();
    for file in files {
        if !file.contains(['*', '?', '[']) {
            paths.push(PathBuf::from(file));
            continue;
        }
        let mut matches = glob::glob(file)
            .map_err(|error| format!("invalid query glob `{file}`: {error}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("cannot read query glob `{file}`: {error}"))?;
        if matches.is_empty() {
            return Err(format!("query glob `{file}` matched no files"));
        }
        matches.sort();
        paths.extend(matches);
    }
    let mut corpus = paths
        .iter()
        .map(|path| {
            let sql = std::fs::read_to_string(path)
                .map_err(|error| format!("cannot read query file {}: {error}", path.display()))?;
            Ok(GeneratedQuery::corpus(
                sql,
                Some(path.display().to_string()),
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    corpus.extend(
        inline
            .iter()
            .map(|sql| GeneratedQuery::corpus(sql.clone(), None)),
    );
    if corpus.is_empty() {
        return Err("no queries: pass `--queries FILE` or `-e SQL`".to_owned());
    }
    Ok(corpus)
}

/// The per-user cache directory for installed drivers.
pub fn default_install_directory() -> PathBuf {
    let home = std::env::var_os("HOME").map(PathBuf::from);
    let cache = if cfg!(target_os = "macos") {
        home.map(|home| home.join("Library/Caches"))
    } else if cfg!(windows) {
        std::env::var_os("LOCALAPPDATA").map(PathBuf::from)
    } else {
        std::env::var_os("XDG_CACHE_HOME")
            .map(PathBuf::from)
            .or_else(|| home.map(|home| home.join(".cache")))
    };
    cache.map_or_else(
        || PathBuf::from(".adiffbc/drivers"),
        |cache| cache.join("adiffbc/drivers"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry(name: &str, version: Option<&str>) -> DriverSource {
        DriverSource::Registry {
            name: name.to_owned(),
            version: version.map(|version| VersionConstraint(version.to_owned())),
        }
    }

    fn uri(text: &str) -> (DriverSource, Option<String>) {
        match Locator::parse(text).expect("valid connection") {
            Locator::Uri { driver, uri } => (driver.source, uri),
            Locator::Profile(_) => panic!("expected a driver URI"),
        }
    }

    #[test]
    fn parses_driver_uris_like_the_driver_manager() {
        assert_eq!(
            uri("sqlite::memory:"),
            (registry("sqlite", None), Some(":memory:".into()))
        );
        assert_eq!(
            uri("flightsql:grpc://localhost:50051"),
            (
                registry("flightsql", None),
                Some("grpc://localhost:50051".into())
            )
        );
        assert_eq!(
            uri("postgresql://app@db:5432/shop?sslmode=require"),
            (
                registry("postgresql", None),
                Some("postgresql://app@db:5432/shop?sslmode=require".into())
            )
        );
        assert_eq!(uri("datafusion"), (registry("datafusion", None), None));
        assert_eq!(uri("datafusion:"), (registry("datafusion", None), None));
        assert_eq!(
            uri("sqlite@>=1.10,<2:file.db"),
            (
                registry("sqlite", Some(">=1.10,<2")),
                Some("file.db".into())
            )
        );
        assert!(matches!(
            uri("/opt/lib/libduckdb.dylib::memory:").0,
            DriverSource::Path(path) if path == Path::new("/opt/lib/libduckdb.dylib")
        ));
        assert_eq!(
            Locator::parse("profile://sf_shop"),
            Ok(Locator::Profile("sf_shop".into()))
        );
    }

    #[test]
    fn version_constraints_map_to_dbc_packages_and_directories() {
        let exact = VersionConstraint("1.5.6".into());
        assert_eq!(exact.package("duckdb"), "duckdb=1.5.6");
        let range = VersionConstraint(">=1.4,<2".into());
        assert_eq!(range.package("duckdb"), "duckdb>=1.4,<2");
        assert_eq!(range.directory_name("duckdb"), "duckdb@__1.4__2");
    }

    #[test]
    fn names_come_from_prefix_driver_or_profile() {
        assert_eq!(
            split_name("pg=postgresql://h/db?a=b"),
            (Some("pg"), "postgresql://h/db?a=b")
        );
        assert_eq!(
            split_name("postgresql://h/db?a=b"),
            (None, "postgresql://h/db?a=b")
        );
        let args = [
            "sqlite::memory:".to_owned(),
            "fast=duckdb::memory:".to_owned(),
            "profile:///etc/p/sf_shop.toml".to_owned(),
            "./libmy_driver.so:file.db".to_owned(),
            "pg=postgresql://user@host/db".to_owned(),
        ];
        let variants = VariantArgs {
            variants: &args,
            ..Default::default()
        }
        .parse()
        .expect("valid variants");
        let names: Vec<_> = variants
            .iter()
            .map(|variant| variant.name.as_str())
            .collect();
        assert_eq!(names, ["sqlite", "fast", "sf_shop", "my_driver", "pg"]);
    }

    #[test]
    fn rejects_duplicate_names_and_unknown_option_targets() {
        let args = ["duckdb::memory:".to_owned(), "duckdb:x.db".to_owned()];
        let error = VariantArgs {
            variants: &args,
            ..Default::default()
        }
        .parse()
        .unwrap_err();
        assert!(error.contains("two variants are named `duckdb`"), "{error}");

        let args = ["duckdb::memory:".to_owned()];
        let options = ["duck.threads=1".to_owned()];
        let error = VariantArgs {
            variants: &args,
            database_options: &options,
            ..Default::default()
        }
        .parse()
        .unwrap_err();
        assert!(error.contains("no variant is named that"), "{error}");
    }

    #[test]
    fn options_apply_to_one_or_all_variants() {
        let args = [
            "a=duckdb::memory:".to_owned(),
            "b=duckdb::memory:".to_owned(),
        ];
        let options = ["*.threads=4".to_owned(), "a.threads=1".to_owned()];
        let connection = ["b.adbc.connection.readonly=true".to_owned()];
        let variants = VariantArgs {
            variants: &args,
            database_options: &options,
            connection_options: &connection,
            ..Default::default()
        }
        .parse()
        .expect("valid variants");
        assert_eq!(variants[0].database_options["threads"], "1");
        assert_eq!(variants[1].database_options["threads"], "4");
        assert_eq!(
            variants[1].connection_options["adbc.connection.readonly"],
            "true"
        );
        assert!(variants[0].connection_options.is_empty());
    }
}
