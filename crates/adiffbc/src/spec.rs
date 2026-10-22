//! A validated campaign, and the parsers that build it from command-line text.

use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    num::NonZeroUsize,
    path::{Path, PathBuf},
};

use datafusion_differential_testing::GeneratedQuery;

use crate::sql::{split_statements, Comment, LeadingComments};

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

/// The seed of the random and mutation generators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Seed {
    /// From `--seed`.
    Given(u64),
    /// Picked at start because `--seed` was not given.
    Entropy(u64),
}

impl Seed {
    /// A seed from the clock and process id, for runs without `--seed`.
    pub fn entropy() -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos() as u64)
            .unwrap_or(0);
        Self::Entropy(nanos ^ (u64::from(std::process::id()) << 32))
    }

    pub fn value(self) -> u64 {
        match self {
            Self::Given(seed) | Self::Entropy(seed) => seed,
        }
    }
}

/// The tables that random queries use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableSelection {
    /// Every table and view in the oracle's default schema.
    All,
    /// Only these tables (`--tables`). Each one must exist.
    Only(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RandomQueries {
    pub count: NonZeroUsize,
    /// Also emit `LIMIT` and float aggregates. These reach more query shapes,
    /// but correct engines can return different results for them.
    pub exploratory: bool,
    pub tables: TableSelection,
}

/// The generators that run after the corpus.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Generation {
    pub seed: Seed,
    pub random: Option<RandomQueries>,
    /// Mutants per finding, when `--mutate` is set.
    pub mutations_per_finding: Option<NonZeroUsize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Spec {
    pub limits: Limits,
    pub schema: SchemaComparison,
    pub drivers: DriverResolution,
    pub oracle: VariantSpec,
    /// Never empty.
    pub candidates: Vec<VariantSpec>,
    /// Runs first. Each query's name is in `provenance.provided_id`. Empty
    /// only when `generation` has random queries.
    pub corpus: Vec<GeneratedQuery>,
    /// `None` when neither `--random` nor `--mutate` is set.
    pub generation: Option<Generation>,
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

/// Where a corpus query comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueryLocation<'a> {
    /// The 1-based statement number in a query file.
    File { path: &'a str, statement: usize },
    /// The 1-based position among `-e` values.
    Inline { position: usize },
}

impl QueryLocation<'_> {
    /// The name of a query without a `-- name:` comment.
    fn default_name(self) -> String {
        match self {
            Self::File { path, statement } => format!("{path}:{statement}"),
            Self::Inline { position } => format!("inline:{position}"),
        }
    }

    fn source(self) -> Option<String> {
        match self {
            Self::File { path, .. } => Some(path.to_owned()),
            Self::Inline { .. } => None,
        }
    }
}

impl fmt::Display for QueryLocation<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::File { path, statement } => write!(formatter, "{path} statement {statement}"),
            Self::Inline { position } => write!(formatter, "-e value {position}"),
        }
    }
}

/// The name in a `-- name: NAME` comment before the statement's SQL.
fn query_name(statement: &str) -> Result<Option<&str>, String> {
    let mut names = LeadingComments::new(statement).filter_map(|comment| match comment {
        Comment::Line(text) => text.trim().strip_prefix("name:").map(str::trim),
        Comment::Block(_) => None,
    });
    match (names.next(), names.next()) {
        (Some(_), Some(_)) => Err("more than one `-- name:` comment".to_owned()),
        (Some(""), None) => Err("empty `-- name:` comment".to_owned()),
        (name, _) => Ok(name),
    }
}

/// Collects corpus queries, and rejects two queries with one name.
#[derive(Default)]
struct Corpus {
    queries: Vec<GeneratedQuery>,
    /// Where each name was first used, for the duplicate error.
    names: HashMap<String, String>,
}

impl Corpus {
    fn push(&mut self, sql: &str, location: QueryLocation<'_>) -> Result<(), String> {
        let name = query_name(sql)
            .map_err(|error| format!("{location}: {error}"))?
            .map_or_else(|| location.default_name(), str::to_owned);
        if let Some(first) = self.names.get(&name) {
            return Err(format!(
                "two queries are named `{name}`: {first} and {location}"
            ));
        }
        self.names.insert(name.clone(), location.to_string());
        let mut query = GeneratedQuery::corpus(sql, location.source());
        query.provenance.provided_id = Some(name);
        self.queries.push(query);
        Ok(())
    }
}

/// Read query files in order, then inline queries. A file argument holding
/// `*`, `?` or `[` is a glob, expanded in sorted order. Files are split at
/// top-level `;`; each `-e` value is one query.
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
    let mut corpus = Corpus::default();
    for path in &paths {
        let path = path.display().to_string();
        let script = std::fs::read_to_string(&path)
            .map_err(|error| format!("cannot read query file {path}: {error}"))?;
        let statements = split_statements(&script);
        if statements.is_empty() {
            return Err(format!("query file {path} holds no SQL statement"));
        }
        for (index, sql) in statements.into_iter().enumerate() {
            let location = QueryLocation::File {
                path: &path,
                statement: index + 1,
            };
            corpus.push(sql, location)?;
        }
    }
    for (index, sql) in inline.iter().enumerate() {
        corpus.push(
            sql,
            QueryLocation::Inline {
                position: index + 1,
            },
        )?;
    }
    Ok(corpus.queries)
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
    fn query_names_come_from_leading_name_comments() {
        assert_eq!(query_name("-- name: totals\nSELECT 1"), Ok(Some("totals")));
        assert_eq!(
            query_name("  -- a note\n  --   name:   by day  \r\n/* x */ SELECT 1"),
            Ok(Some("by day"))
        );
        assert_eq!(query_name("--name:x\nSELECT 1"), Ok(Some("x")));
        assert_eq!(query_name("SELECT 1 -- name: late"), Ok(None));
        assert_eq!(query_name("/* name: block */ SELECT 1"), Ok(None));
        assert_eq!(query_name("-- names: x\nSELECT 1"), Ok(None));
        assert!(query_name("-- name:  \nSELECT 1").is_err());
        assert!(query_name("-- name: a\n-- name: b\nSELECT 1").is_err());
    }

    fn write(directory: &tempfile::TempDir, file: &str, sql: &str) -> String {
        let path = directory.path().join(file);
        std::fs::write(&path, sql).expect("write query file");
        path.display().to_string()
    }

    fn names(corpus: &[GeneratedQuery]) -> Vec<&str> {
        corpus
            .iter()
            .map(|query| query.provenance.provided_id.as_deref().unwrap_or("?"))
            .collect()
    }

    #[test]
    fn query_files_are_split_and_named() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let file = write(
            &directory,
            "q.sql",
            "SELECT 1;\n-- name: totals\nSELECT count(*) FROM t;\n\nSELECT 'a;b';\n-- end\n",
        );
        let single = write(&directory, "one.sql", "SELECT 2\n");
        let inline = [
            "SELECT 3".to_owned(),
            " -- name: three\nSELECT 3;".to_owned(),
        ];
        let corpus = load_corpus(&[file.clone(), single.clone()], &inline).expect("valid corpus");
        assert_eq!(
            names(&corpus),
            [
                format!("{file}:1").as_str(),
                "totals",
                &format!("{file}:3"),
                &format!("{single}:1"),
                "inline:1",
                "three",
            ]
        );
        let sql: Vec<_> = corpus.iter().map(|query| query.sql.as_str()).collect();
        assert_eq!(
            sql,
            [
                "SELECT 1",
                "-- name: totals\nSELECT count(*) FROM t",
                "SELECT 'a;b'",
                "SELECT 2",
                "SELECT 3",
                " -- name: three\nSELECT 3;",
            ]
        );
        assert_eq!(corpus[1].provenance.source.as_deref(), Some(file.as_str()));
        assert_eq!(corpus[4].provenance.source, None);
    }

    #[test]
    fn rejects_duplicate_names_and_empty_files() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let file = write(&directory, "q.sql", "SELECT 1;\n-- name: a\nSELECT 2;");
        let error = load_corpus(
            std::slice::from_ref(&file),
            &["-- name: a\nSELECT 3".to_owned()],
        )
        .unwrap_err();
        assert_eq!(
            error,
            format!("two queries are named `a`: {file} statement 2 and -e value 1")
        );

        let error = load_corpus(&[file.clone(), file.clone()], &[]).unwrap_err();
        assert!(
            error.contains(&format!("two queries are named `{file}:1`")),
            "{error}"
        );

        let error = load_corpus(&[], &["-- name: \nSELECT 1".to_owned()]).unwrap_err();
        assert_eq!(error, "-e value 1: empty `-- name:` comment");

        let empty = write(&directory, "empty.sql", "-- nothing here\n;\n");
        let error = load_corpus(&[empty], &[]).unwrap_err();
        assert!(error.contains("holds no SQL statement"), "{error}");
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
