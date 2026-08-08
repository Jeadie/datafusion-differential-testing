mod config;

use std::{
    fs,
    path::{Path, PathBuf},
    process::ExitCode,
};

use clap::{Parser, Subcommand, ValueEnum};
use datafusion_differential_testing::{CampaignRunner, ProviderRegistry, SessionExtensionRegistry};
use std::sync::Arc;

#[derive(Debug, Parser)]
#[command(name = "datafusion-differential-test", version, about)]
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
    /// Create a runnable example campaign.
    Init {
        #[arg(default_value = ".")]
        directory: PathBuf,
    },
    /// Validate a campaign without provider mutation.
    Validate { campaign: PathBuf },
    /// List compiled provider adapters.
    Providers,
    /// List compiled session extensions.
    Extensions,
    /// Run a campaign.
    Run { campaign: PathBuf },
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match execute(cli).await {
        Ok(code) => code,
        Err(message) => {
            eprintln!("error: {message}");
            ExitCode::from(2)
        }
    }
}

async fn execute(cli: Cli) -> Result<ExitCode, String> {
    match cli.command {
        Command::Init { directory } => {
            initialize(&directory)?;
            match cli.format {
                OutputFormat::Human => eprintln!(
                    "Created {}. Run `datafusion-differential-test run {}`.",
                    directory.display(),
                    directory.join("campaign.yaml").display()
                ),
                OutputFormat::Json => println!(
                    "{}",
                    serde_json::json!({"version":1,"event":"initialized","directory":directory})
                ),
            }
            Ok(ExitCode::SUCCESS)
        }
        Command::Validate { campaign } => {
            let spec = config::load(&campaign)?;
            runner()?
                .validate(&spec)
                .map_err(|error| error.to_string())?;
            match cli.format {
                OutputFormat::Human => eprintln!(
                    "Valid campaign `{}`: {} variants, {} queries.",
                    spec.name,
                    spec.variants.len(),
                    spec.corpus.len()
                ),
                OutputFormat::Json => println!(
                    "{}",
                    serde_json::json!({
                        "version":1,"event":"validated","campaign":spec.name,
                        "variants":spec.variants.len(),"queries":spec.corpus.len()
                    })
                ),
            }
            Ok(ExitCode::SUCCESS)
        }
        Command::Providers => {
            #[cfg(any(feature = "duckdb", feature = "sqlite"))]
            let values = datafusion_differential_testing_table_providers::compiled_provider_kinds();
            #[cfg(not(any(feature = "duckdb", feature = "sqlite")))]
            let values = Vec::new();
            emit_list(cli.format, "providers", &values);
            Ok(ExitCode::SUCCESS)
        }
        Command::Extensions => {
            #[cfg(feature = "federation")]
            let values = datafusion_differential_testing_federation::compiled_extension_kinds();
            #[cfg(not(feature = "federation"))]
            let values = &[];
            emit_list(cli.format, "extensions", values);
            Ok(ExitCode::SUCCESS)
        }
        Command::Run { campaign } => {
            let spec = config::load(&campaign)?;
            let report = runner()?
                .run(spec)
                .await
                .map_err(|error| error.to_string())?;
            match cli.format {
                OutputFormat::Human => {
                    eprintln!(
                        "Completed {} queries with {} findings. Artifacts: {}",
                        report.executed_queries,
                        report.findings.len(),
                        report.artifact_directory.display()
                    );
                    for finding in &report.findings {
                        let sql: String = finding.sql.split_whitespace().collect::<Vec<_>>().join(" ");
                        let sql: String = sql.chars().take(80).collect();
                        eprintln!(
                            "  - [{}] {:?}: {sql}",
                            finding.candidate.as_str(),
                            finding.kind
                        );
                    }
                }
                OutputFormat::Json => println!(
                    "{}",
                    serde_json::to_string(&report).map_err(|error| error.to_string())?
                ),
            }
            Ok(if report.is_success() {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            })
        }
    }
}

fn emit_list(format: OutputFormat, kind: &str, values: &[&str]) {
    match format {
        OutputFormat::Human => {
            if values.is_empty() {
                println!("No {kind} are compiled into this build.");
            } else {
                for value in values {
                    println!("{value}");
                }
            }
        }
        OutputFormat::Json => println!(
            "{}",
            serde_json::json!({"version":1,"event":kind,"values":values})
        ),
    }
}

fn initialize(directory: &Path) -> Result<(), String> {
    if directory.exists() {
        return Err(format!(
            "refusing to overwrite existing path {}",
            directory.display()
        ));
    }
    fs::create_dir_all(directory.join("fixtures"))
        .map_err(|error| format!("cannot create {}: {error}", directory.display()))?;
    fs::create_dir_all(directory.join("queries"))
        .map_err(|error| format!("cannot create {}: {error}", directory.display()))?;
    fs::write(directory.join("campaign.yaml"), INIT_CONFIG)
        .map_err(|error| format!("cannot write campaign.yaml: {error}"))?;
    fs::write(directory.join("fixtures/data.sql"), INIT_FIXTURE)
        .map_err(|error| format!("cannot write fixture: {error}"))?;
    fs::write(directory.join("queries/count.sql"), INIT_QUERY)
        .map_err(|error| format!("cannot write query: {error}"))?;
    fs::write(directory.join("queries/join.sql"), INIT_JOIN_QUERY)
        .map_err(|error| format!("cannot write query: {error}"))?;
    Ok(())
}

fn runner() -> Result<CampaignRunner, String> {
    let mut providers = ProviderRegistry::new();
    #[cfg(feature = "duckdb")]
    providers
        .register(Arc::new(
            datafusion_differential_testing_table_providers::DuckDbProviderFactory,
        ))
        .map_err(|error| error.to_string())?;
    #[cfg(feature = "sqlite")]
    providers
        .register(Arc::new(
            datafusion_differential_testing_table_providers::SqliteProviderFactory,
        ))
        .map_err(|error| error.to_string())?;

    let mut extensions = SessionExtensionRegistry::new();
    #[cfg(feature = "federation")]
    extensions
        .register(Arc::new(
            datafusion_differential_testing_federation::FederationExtension,
        ))
        .map_err(|error| error.to_string())?;
    Ok(CampaignRunner::new()
        .with_provider_registry(providers)
        .with_extension_registry(extensions))
}

const INIT_CONFIG: &str = r#"name: four-variant-smoke

dataset:
  paths:
    - fixtures/data.sql

campaign:
  max_queries: 100
  query_timeout: 30s

artifacts:
  directory: artifacts

providers:
  duck_shared:
    type: duckdb
    params:
      duckdb_open: .datafusion-differential-test/duck-shared.db
  duck_orders:
    type: duckdb
    params:
      duckdb_open: .datafusion-differential-test/duck-orders.db
  duck_customers:
    type: duckdb
    params:
      duckdb_open: .datafusion-differential-test/duck-customers.db
  sqlite:
    type: sqlite
    params:
      sqlite_open: .datafusion-differential-test/sqlite.db

variants:
  oracle:
    oracle: true
    tables:
      "*": memtable
  duckdb_shared_federated:
    tables:
      "*":
        provider: duck_shared
    session:
      extensions:
        - type: federation
  duckdb_split_federated:
    tables:
      orders:
        provider: duck_orders
      customers:
        provider: duck_customers
    session:
      extensions:
        - type: federation
  sqlite_nonfederated:
    tables:
      "*":
        provider: sqlite

generators:
  - type: corpus
    paths:
      - queries/*.sql
"#;

const INIT_FIXTURE: &str = r#"CREATE TABLE orders (
    id BIGINT,
    customer_id BIGINT,
    amount BIGINT
);
INSERT INTO orders VALUES
    (1, 10, 10),
    (2, 20, 20),
    (3, 10, 20),
    (4, NULL, 5);

CREATE TABLE customers (
    id BIGINT,
    code BIGINT
);
INSERT INTO customers VALUES
    (10, 100),
    (20, 200);
"#;

const INIT_QUERY: &str =
    "SELECT amount, COUNT(*) AS orders FROM orders GROUP BY amount ORDER BY amount;\n";

const INIT_JOIN_QUERY: &str = "SELECT o.id, c.code, o.amount FROM orders o LEFT JOIN customers c ON o.customer_id = c.id ORDER BY o.id;\n";

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn retained_sqlite_can_supply_an_existing_oracle() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let database = temporary.path().join("retained.sqlite");
        let fixture_path = temporary.path().join("items.sql");
        fs::write(
            &fixture_path,
            "CREATE TABLE items(id BIGINT, value BIGINT); INSERT INTO items VALUES (1, 10), (2, 20);",
        )
        .expect("write seed fixture");
        let seed_path = temporary.path().join("seed.yaml");
        let seed = format!(
            r#"name: seed-existing-oracle
dataset:
  paths:
    - "{fixture}"
artifacts:
  directory: seed-artifacts
providers:
  retained:
    type: sqlite
    teardown: false
    params:
      sqlite_open: "{database}"
variants:
  memtable:
    oracle: true
    tables:
      "*": memtable
  retained:
    tables:
      "*":
        provider: retained
generators:
  - type: corpus
    sql:
      - "SELECT * FROM items ORDER BY id"
"#,
            fixture = fixture_path.display(),
            database = database.display(),
        );
        fs::write(&seed_path, seed).expect("write seed campaign");
        let seed_report = runner()
            .expect("create runner")
            .run(config::load(&seed_path).expect("load seed campaign"))
            .await
            .expect("run seed campaign");
        assert!(seed_report.is_success());

        let campaign_path = temporary.path().join("campaign.yaml");
        let campaign = format!(
            r#"name: existing-oracle
dataset:
  variant: retained
artifacts:
  directory: campaign-artifacts
providers:
  retained:
    type: sqlite
    lifecycle: existing
    params:
      sqlite_open: "{}"
variants:
  retained:
    oracle: true
    tables:
      "*":
        provider: retained
  memtable:
    tables:
      "*": memtable
generators:
  - type: corpus
    sql:
      - "SELECT SUM(value) AS total FROM items"
"#,
            database.display()
        );
        fs::write(&campaign_path, campaign).expect("write existing-oracle campaign");
        let report = runner()
            .expect("create runner")
            .run(config::load(&campaign_path).expect("load existing-oracle campaign"))
            .await
            .expect("run existing-oracle campaign");

        assert!(report.is_success());
        assert_eq!(report.executed_queries, 1);
    }
}
