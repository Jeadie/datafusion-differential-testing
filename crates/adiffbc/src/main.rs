//! `adiffbc`: differential testing for arbitrary ADBC drivers.

mod driver;
mod install;
mod run;
mod spec;
mod sql;

use std::{num::NonZeroUsize, path::PathBuf, process::ExitCode};

use clap::{ArgGroup, Args, Parser, Subcommand, ValueEnum};
use datafusion_differential_testing::StrictComparator;

use crate::{
    driver::{DriverLoader, Session},
    spec::{
        DriverResolution, Generation, Limits, RandomQueries, SchemaComparison, Seed, Spec,
        TableSelection, VariantArgs,
    },
};

const VARIANT_HELP: &str = "\
A variant is `[NAME=]DRIVER:URI` or `[NAME=]profile://PROFILE`. Without `NAME=`, the
name is the driver or profile name. Names must be unique.

  VARIANT                            NAME         DRIVER          URI OPTION
  sqlite::memory:                    sqlite       sqlite          :memory:
  flightsql:grpc://localhost:50051   flightsql    flightsql       grpc://localhost:50051
  pg=postgresql://user@host/db       pg           postgresql      postgresql://user@host/db
  datafusion                         datafusion   datafusion      (none)
  old=sqlite@1.11.0::memory:         old          sqlite 1.11.0   :memory:
  ./libmy_driver.so:file.db          my_driver    that library    file.db
  profile://staging                  staging      from profile    from profile

`DRIVER://...` sends the whole URI, and `DRIVER:REST` sends only `REST`. Use
`flightsql:grpc://host:port`, not `flightsql://host:port`.

`profile://PROFILE` loads an ADBC connection profile: a TOML file with a driver and
its options, which can read environment variables. adiffbc stops if a variable that
a profile uses is not set.

A library path uses the driver's default entrypoint name. For a driver with another
entrypoint, such as DuckDB, give its `.toml` manifest, or let dbc install it.

Drivers are searched for in `$ADBC_DRIVER_PATH`, `--install-dir`,
`$VIRTUAL_ENV/etc/adbc/drivers`, `$CONDA_PREFIX/etc/adbc/drivers`, and then the ADBC
user and system directories. A driver that is not found is installed with dbc
(https://docs.columnar.tech/dbc/), unless `--no-install` is set. A pinned driver
(`DRIVER@1.2.3`, or a range such as `'DRIVER@>=1.4,<2'`) always goes through dbc,
into its own directory.";

const SHORT_HELP_POINTER: &str = "See `--help` for variant syntax and how drivers are found.";

const TOP_LEVEL_HELP: &str = "\
Example:
  adiffbc run --oracle sqlite::memory: --candidate duckdb::memory: -q 'queries/*.sql'
See `adiffbc run --help` for variant syntax and how drivers are found.";

#[derive(Debug, Parser)]
#[command(name = "adiffbc", version, about, after_help = TOP_LEVEL_HELP)]
struct Cli {
    #[arg(long, global = true, value_enum, default_value_t = OutputFormat::Human)]
    format: OutputFormat,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum OutputFormat {
    Human,
    Json,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run queries on an oracle and candidates, and report differences.
    /// Exits with 1 when there are findings, and 2 on an error.
    #[command(after_help = SHORT_HELP_POINTER, after_long_help = VARIANT_HELP)]
    Run(RunArgs),
    /// Load (and install) drivers, connect, and print driver info.
    #[command(after_help = SHORT_HELP_POINTER, after_long_help = VARIANT_HELP)]
    Probe {
        /// `[NAME=]URI` of each variant to probe.
        #[arg(required = true, value_name = "[NAME=]URI")]
        variants: Vec<String>,
        #[command(flatten)]
        drivers: DriverArgs,
    },
}

#[derive(Debug, Args)]
#[command(group(ArgGroup::new("generators").args(["random", "mutate"]).multiple(true)))]
struct RunArgs {
    /// The variant whose results are taken as correct.
    #[arg(long, value_name = "[NAME=]URI")]
    oracle: String,
    /// A variant compared against the oracle. Repeatable.
    #[arg(long = "candidate", required = true, value_name = "[NAME=]URI")]
    candidates: Vec<String>,
    /// SQL to run before the queries, on every variant or only on NAME.
    /// Repeatable; runs in the given order. Split at top-level `;`.
    #[arg(long, value_name = "[NAME=]FILE")]
    setup: Vec<String>,
    /// An inline query, not split. Repeatable; runs after query files. Its
    /// name is `inline:N`, or the name in a `-- name: NAME` comment.
    #[arg(
        short = 'e',
        long = "execute",
        value_name = "SQL",
        allow_hyphen_values = true
    )]
    execute: Vec<String>,
    /// Query files, split at top-level `;`. Takes several files, and expands
    /// quoted globs in sorted order: `--queries 'queries/*.sql'`. Repeatable.
    /// A query's name is `FILE:N` (its statement number), or the name in a
    /// `-- name: NAME` comment before it. Names must be unique.
    #[arg(short = 'q', long, value_name = "FILE", num_args = 1..)]
    queries: Vec<String>,
    /// Add COUNT random queries, made from the oracle's tables after setup.
    /// They run after the query files and inline queries.
    #[arg(long, value_name = "COUNT")]
    random: Option<NonZeroUsize>,
    /// Make random queries only from these tables. Each table must exist.
    /// Default: all tables and views in the oracle's current schema.
    #[arg(long, value_name = "TABLE", num_args = 1.., requires = "random")]
    tables: Vec<String>,
    /// The seed for `--random` and `--mutate`. The same seed gives the same
    /// queries. Without it, adiffbc picks a seed, prints it, and sets
    /// `--exploratory`.
    #[arg(long, value_name = "SEED", requires = "generators")]
    seed: Option<u64>,
    /// Let random queries use `LIMIT` and float aggregates. These find more
    /// bugs, but correct drivers can also return different results for them.
    #[arg(long, requires = "random")]
    exploratory: bool,
    /// Change each query that gives a finding into nearby queries, and run
    /// them. Applies to all queries, not only random ones.
    #[arg(long)]
    mutate: bool,
    /// The number of changed queries to make from each finding.
    #[arg(long, value_name = "COUNT", default_value = "4", requires = "mutate")]
    mutations_per_finding: NonZeroUsize,
    #[arg(long, value_enum, default_value_t = SchemaComparison::Relaxed)]
    schema: SchemaComparison,
    /// The maximum number of queries to run, from all sources together.
    #[arg(long, default_value_t = 1_000)]
    max_queries: usize,
    #[arg(long, default_value_t = 100)]
    max_findings: usize,
    #[arg(long)]
    stop_on_finding: bool,
    #[command(flatten)]
    drivers: DriverArgs,
}

#[derive(Debug, Args)]
struct DriverArgs {
    /// An ADBC database option for variant NAME, or `*` for all. Repeatable;
    /// later values win, and override the URI and profile. Keys are not
    /// checked by adiffbc; most drivers reject unknown keys.
    #[arg(short = 'o', long = "option", value_name = "NAME.KEY=VALUE")]
    database_options: Vec<String>,
    /// An ADBC connection option for variant NAME, or `*` for all. Repeatable.
    /// Keys are not checked by adiffbc; most drivers reject unknown keys.
    #[arg(short = 'c', long = "connection-option", value_name = "NAME.KEY=VALUE")]
    connection_options: Vec<String>,
    /// Never install a missing driver.
    #[arg(long)]
    no_install: bool,
    /// Where dbc installs drivers.
    #[arg(long, value_name = "DIR", default_value_os_t = spec::default_install_directory())]
    install_dir: PathBuf,
    /// The dbc command line, e.g. "uvx dbc" [default: dbc, uvx dbc, or pipx run dbc].
    #[arg(long, value_name = "COMMAND")]
    dbc: Option<String>,
}

impl DriverArgs {
    fn resolution(&self) -> Result<DriverResolution, String> {
        let directory = std::path::absolute(&self.install_dir)
            .map_err(|error| format!("cannot resolve {}: {error}", self.install_dir.display()))?;
        let command = self
            .dbc
            .as_ref()
            .map(|command| command.split_whitespace().map(str::to_owned).collect());
        Ok(DriverResolution::new(directory, command, !self.no_install))
    }

    fn variants(
        &self,
        variants: &[String],
        setup: &[String],
    ) -> Result<Vec<spec::VariantSpec>, String> {
        VariantArgs {
            variants,
            database_options: &self.database_options,
            connection_options: &self.connection_options,
            setup,
        }
        .parse()
    }
}

impl RunArgs {
    fn into_spec(self) -> Result<Spec, String> {
        let names: Vec<String> = std::iter::once(self.oracle)
            .chain(self.candidates)
            .collect();
        let mut variants = self.drivers.variants(&names, &self.setup)?.into_iter();
        let oracle = variants.next().ok_or("--oracle is required")?;
        let corpus = spec::load_corpus(&self.queries, &self.execute)?;
        if corpus.is_empty() && self.random.is_none() {
            return Err(
                "no queries: pass `--queries FILE`, `-e SQL` or `--random COUNT`".to_owned(),
            );
        }
        let generation = (self.random.is_some() || self.mutate).then(|| Generation {
            seed: self.seed.map_or_else(Seed::entropy, Seed::Given),
            random: self.random.map(|count| RandomQueries {
                count,
                exploratory: self.exploratory || self.seed.is_none(),
                tables: if self.tables.is_empty() {
                    TableSelection::All
                } else {
                    TableSelection::Only(self.tables)
                },
            }),
            mutations_per_finding: self.mutate.then_some(self.mutations_per_finding),
        });
        Ok(Spec {
            limits: Limits {
                max_queries: self.max_queries,
                max_findings: self.max_findings,
                stop_on_finding: self.stop_on_finding,
            },
            schema: self.schema,
            drivers: self.drivers.resolution()?,
            oracle,
            candidates: variants.collect(),
            corpus,
            generation,
        })
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let format = cli.format;
    match execute(cli.command, format) {
        Ok(code) => code,
        Err(message) => {
            eprintln!("error: {message}");
            ExitCode::from(2)
        }
    }
}

fn execute(command: Command, format: OutputFormat) -> Result<ExitCode, String> {
    match command {
        Command::Probe { variants, drivers } => {
            let resolution = drivers.resolution()?;
            let variants = drivers.variants(&variants, &[])?;
            let mut loader = DriverLoader::new(&resolution);
            let mut probed = Vec::new();
            for variant in &variants {
                let (session, origin) = Session::connect(variant, &mut loader)?;
                probed.push((variant, origin, session.info()?));
            }
            match format {
                OutputFormat::Human => {
                    for (variant, origin, info) in &probed {
                        println!("{} (driver: {origin})", variant.name);
                        for (code, value) in info {
                            println!("  {code}: {value}");
                        }
                    }
                }
                OutputFormat::Json => {
                    let variants: serde_json::Map<_, _> = probed
                        .iter()
                        .map(|(variant, origin, info)| {
                            let info: serde_json::Map<_, _> = info
                                .iter()
                                .map(|(code, value)| (code.clone(), value.clone().into()))
                                .collect();
                            let entry = serde_json::json!({
                                "origin": origin.to_string(),
                                "info": info,
                            });
                            (variant.name.clone(), entry)
                        })
                        .collect();
                    println!(
                        "{}",
                        serde_json::json!({"event": "probed", "variants": variants})
                    );
                }
            }
            Ok(ExitCode::SUCCESS)
        }
        Command::Run(args) => {
            let spec = args.into_spec()?;
            if let Some(Generation {
                seed: Seed::Entropy(seed),
                random,
                ..
            }) = &spec.generation
            {
                // Print before running, so the seed is known if the run fails.
                let flags = if random.is_some() {
                    format!("--seed {seed} --exploratory")
                } else {
                    format!("--seed {seed}")
                };
                eprintln!("seed: {seed} (no --seed given; to repeat this run, add `{flags}`)");
            }
            let report = run::run(&spec, &StrictComparator)?;
            match format {
                OutputFormat::Human => {
                    for finding in &report.findings {
                        println!(
                            "finding: query `{}` on `{}`: {:?}\n  sql: {}\n  {}",
                            finding.query.name,
                            finding.candidate,
                            finding.kind,
                            finding.sql.trim().replace('\n', "\n       "),
                            finding.message.replace('\n', "\n  ")
                        );
                    }
                    if let Some(generation) = &report.generation {
                        if !generation.tables.is_empty() {
                            eprintln!("random tables: {}", generation.tables.join(", "));
                        }
                    }
                    let by_generator: Vec<String> = report
                        .queries_by_generator
                        .iter()
                        .map(|(generator, count)| format!("{generator} {count}"))
                        .collect();
                    eprintln!(
                        "{} queries ({}), {} duplicates skipped, {} oracle rejections, \
                         {} comparisons, {} findings",
                        report.queries_executed,
                        by_generator.join(", "),
                        report.duplicate_queries,
                        report.oracle_rejections,
                        report.comparisons,
                        report.findings.len()
                    );
                }
                OutputFormat::Json => println!(
                    "{}",
                    serde_json::to_string(&report).map_err(|error| error.to_string())?
                ),
            }
            Ok(if report.findings.is_empty() {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            })
        }
    }
}
