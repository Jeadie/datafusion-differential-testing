//! `adiffbc`: differential testing for arbitrary ADBC drivers.

mod driver;
mod install;
mod run;
mod spec;
mod sql;

use std::{path::PathBuf, process::ExitCode};

use clap::{Args, Parser, Subcommand, ValueEnum};
use datafusion_differential_testing::StrictComparator;

use crate::{
    driver::{DriverLoader, Session},
    spec::{DriverResolution, Limits, SchemaComparison, Spec, VariantArgs},
};

const VARIANT_HELP: &str = "\
A variant is an ADBC driver URI or connection profile, optionally named NAME=:
  sqlite::memory:                     driver `sqlite`, uri `:memory:`
  flightsql:grpc://localhost:50051    driver `flightsql`, uri `grpc://localhost:50051`
  postgresql://user@host/db           driver `postgresql`, the whole URI is the uri
  datafusion                          driver `datafusion`, no uri
  sqlite@1.11.0::memory:              pin a driver version (installed separately)
  ./libmy_driver.so:file.db           a driver library or manifest path
  pg=profile://staging                an ADBC profile, named `pg`
Missing drivers are installed with dbc (https://docs.columnar.tech/dbc/).";

#[derive(Debug, Parser)]
#[command(name = "adiffbc", version, about)]
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
    /// Exits with 1 when there are findings.
    #[command(after_help = VARIANT_HELP)]
    Run(RunArgs),
    /// Load (and install) drivers, connect, and print driver info.
    #[command(after_help = VARIANT_HELP)]
    Probe {
        /// `[NAME=]URI` of each variant to probe.
        #[arg(required = true, value_name = "[NAME=]URI")]
        variants: Vec<String>,
        #[command(flatten)]
        drivers: DriverArgs,
    },
}

#[derive(Debug, Args)]
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
    /// An inline query. Repeatable; runs after query files.
    #[arg(short = 'e', long = "execute", value_name = "SQL")]
    execute: Vec<String>,
    /// Query files, one query per file. Takes several files, and expands
    /// quoted globs: `--queries 'queries/*.sql'`. Repeatable.
    #[arg(short = 'q', long, value_name = "FILE", num_args = 1..)]
    queries: Vec<String>,
    #[arg(long, value_enum, default_value_t = SchemaComparison::Relaxed)]
    schema: SchemaComparison,
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
    /// later values win, and override the URI and profile.
    #[arg(short = 'o', long = "option", value_name = "NAME.KEY=VALUE")]
    database_options: Vec<String>,
    /// An ADBC connection option for variant NAME, or `*` for all. Repeatable.
    #[arg(short = 'c', long = "connection-option", value_name = "NAME.KEY=VALUE")]
    connection_options: Vec<String>,
    /// Never install a missing driver.
    #[arg(long)]
    no_install: bool,
    /// Where dbc installs drivers [default: the user cache directory].
    #[arg(long, value_name = "DIR")]
    install_dir: Option<PathBuf>,
    /// The dbc command line, e.g. "uvx dbc" [default: dbc, uvx dbc, or pipx run dbc].
    #[arg(long, value_name = "COMMAND")]
    dbc: Option<String>,
}

impl DriverArgs {
    fn resolution(&self) -> Result<DriverResolution, String> {
        let directory = self
            .install_dir
            .clone()
            .unwrap_or_else(spec::default_install_directory);
        let directory = std::path::absolute(&directory)
            .map_err(|error| format!("cannot resolve {}: {error}", directory.display()))?;
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
            corpus: spec::load_corpus(&self.queries, &self.execute)?,
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
            let report = run::run(&spec, &StrictComparator)?;
            match format {
                OutputFormat::Human => {
                    for finding in &report.findings {
                        println!(
                            "finding: query #{} ({}) on `{}`: {:?}\n  sql: {}\n  {}",
                            finding.query,
                            finding.source.as_deref().unwrap_or("inline"),
                            finding.candidate,
                            finding.kind,
                            finding.sql.trim(),
                            finding.message.replace('\n', "\n  ")
                        );
                    }
                    eprintln!(
                        "{} queries, {} oracle rejections, {} comparisons, {} findings",
                        report.queries_executed,
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
