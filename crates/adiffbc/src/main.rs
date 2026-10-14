//! `adiffbc`: differential testing for arbitrary ADBC drivers.

mod config;
mod driver;
mod run;
mod sql;

use std::{path::PathBuf, process::ExitCode};

use clap::{Parser, Subcommand, ValueEnum};
use datafusion_differential_testing::StrictComparator;

use crate::{config::DriverLocation, driver::Session};

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
    /// Parse and validate a campaign without loading any driver.
    Validate { campaign: PathBuf },
    /// Load each variant's driver, connect, and print the driver's info.
    Probe { campaign: PathBuf },
    /// Run a campaign. Exits with 1 when there are findings.
    Run { campaign: PathBuf },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match execute(&cli) {
        Ok(code) => code,
        Err(message) => {
            eprintln!("error: {message}");
            ExitCode::from(2)
        }
    }
}

fn execute(cli: &Cli) -> Result<ExitCode, String> {
    match &cli.command {
        Command::Validate { campaign } => {
            let spec = config::load(campaign)?;
            match cli.format {
                OutputFormat::Human => {
                    println!(
                        "Campaign `{}` is valid: oracle `{}`, {} candidate(s), {} setup script(s), {} queries.",
                        spec.name,
                        spec.oracle.name,
                        spec.candidates.len(),
                        spec.setup.len(),
                        spec.corpus.len()
                    );
                }
                OutputFormat::Json => println!(
                    "{}",
                    serde_json::json!({
                        "event": "validated",
                        "campaign": spec.name,
                        "oracle": spec.oracle.name,
                        "candidates": spec.candidates.iter().map(|v| &v.name).collect::<Vec<_>>(),
                        "queries": spec.corpus.len(),
                    })
                ),
            }
            Ok(ExitCode::SUCCESS)
        }
        Command::Probe { campaign } => {
            let spec = config::load(campaign)?;
            let mut variants = Vec::new();
            for variant in spec.variants() {
                let info = Session::connect(variant)?.info()?;
                variants.push((variant, info));
            }
            match cli.format {
                OutputFormat::Human => {
                    for (variant, info) in &variants {
                        let location = match &variant.driver.location {
                            DriverLocation::Search(name) => name.clone(),
                            DriverLocation::Path(path) => path.display().to_string(),
                        };
                        println!(
                            "{} (driver `{}`: {location})",
                            variant.name, variant.driver.name
                        );
                        for (code, value) in info {
                            println!("  {code}: {value}");
                        }
                    }
                }
                OutputFormat::Json => {
                    let variants: serde_json::Map<_, _> = variants
                        .iter()
                        .map(|(variant, info)| {
                            let info: serde_json::Map<_, _> = info
                                .iter()
                                .map(|(code, value)| (code.clone(), value.clone().into()))
                                .collect();
                            (variant.name.clone(), info.into())
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
        Command::Run { campaign } => {
            let spec = config::load(campaign)?;
            let report = run::run(&spec, &StrictComparator)?;
            match cli.format {
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
