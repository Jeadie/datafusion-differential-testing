use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion::catalog::TableProvider;
use serde::{Deserialize, Serialize};

use crate::{DatasetId, FindingId, QueryId, RunId, VariantId};

pub type LogicalTableName = String;
pub type ProviderInstanceName = String;
pub type ResolvedParams = BTreeMap<String, String>;
pub type UnresolvedParams = BTreeMap<String, String>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderLifecycle {
    Managed { teardown: bool },
    Existing,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderSpec {
    pub name: String,
    pub kind: String,
    pub lifecycle: ProviderLifecycle,
    pub params: UnresolvedParams,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RemoteTableRef(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TableBinding {
    MemTable,
    Provider {
        instance: ProviderInstanceName,
        remote: Option<RemoteTableRef>,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionSpec {
    pub config: BTreeMap<String, String>,
    pub extensions: Vec<SessionExtensionSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionExtensionSpec {
    pub kind: String,
    pub params: ResolvedParams,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VariantSpec {
    pub name: String,
    pub oracle: bool,
    pub session: SessionSpec,
    pub tables: BTreeMap<LogicalTableName, TableBinding>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FixtureSource {
    Sql {
        path: PathBuf,
    },
    SqlInline {
        sql: String,
    },
    ArrowIpc {
        name: String,
        path: PathBuf,
    },
    Parquet {
        name: String,
        path: PathBuf,
    },
    Csv {
        name: String,
        path: PathBuf,
        options: BTreeMap<String, String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatasetSpec {
    Fixtures(Vec<FixtureSource>),
    ExistingOracle {
        include: Vec<LogicalTableName>,
        verify_unchanged: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CampaignSpec {
    pub name: String,
    pub dataset: DatasetSpec,
    pub providers: BTreeMap<String, ProviderSpec>,
    pub variants: BTreeMap<String, VariantSpec>,
    pub corpus: Vec<GeneratedQuery>,
    pub limits: RunLimits,
    pub artifacts: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunLimits {
    pub max_queries: usize,
    pub max_findings: usize,
    pub query_timeout: Duration,
    pub max_sql_bytes: usize,
    pub max_result_rows: usize,
    pub max_result_bytes: usize,
    pub stop_on_finding: bool,
}

impl Default for RunLimits {
    fn default() -> Self {
        Self {
            max_queries: 1_000,
            max_findings: 100,
            query_timeout: Duration::from_secs(30),
            max_sql_bytes: 1_048_576,
            max_result_rows: 1_000_000,
            max_result_bytes: 268_435_456,
            stop_on_finding: false,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Ordering {
    Ordered,
    Unordered,
    #[default]
    Detect,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryProvenance {
    pub generator: String,
    pub source: Option<String>,
    pub provided_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GeneratedQuery {
    pub sql: String,
    pub ordering: Ordering,
    pub provenance: QueryProvenance,
}

impl GeneratedQuery {
    #[must_use]
    pub fn corpus(sql: impl Into<String>, source: Option<String>) -> Self {
        Self {
            sql: sql.into(),
            ordering: Ordering::Detect,
            provenance: QueryProvenance {
                generator: "corpus".to_owned(),
                source,
                provided_id: None,
            },
        }
    }

    #[must_use]
    pub fn is_ordered(&self) -> bool {
        match self.ordering {
            Ordering::Ordered => true,
            Ordering::Unordered => false,
            Ordering::Detect => has_top_level_order_by(&self.sql),
        }
    }
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
    pub row_count: usize,
    pub content_hash: String,
    pub elapsed: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionError {
    pub class: String,
    pub message: String,
}

#[derive(Debug, Clone)]
pub enum QueryOutcome {
    Success(QueryResult),
    Rejected(ExecutionError),
    TimedOut { elapsed: Duration },
    Panicked { message: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FindingKind {
    CandidateRejected,
    SchemaMismatch,
    RowMismatch,
    Timeout,
    PanicOrCrash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComparisonDifference {
    pub kind: FindingKind,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Comparison {
    Equal,
    Different(ComparisonDifference),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Finding {
    pub id: FindingId,
    pub query_id: QueryId,
    pub candidate: VariantId,
    pub kind: FindingKind,
    pub message: String,
    pub sql: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetManifest {
    pub id: DatasetId,
    pub tables: BTreeMap<String, TableManifest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableManifest {
    pub row_count: u64,
    pub content_hash: String,
    pub ipc_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunManifest {
    pub format_version: u32,
    pub run_id: RunId,
    pub campaign: String,
    pub oracle: String,
    pub variants: Vec<String>,
}

pub type TableProviderRef = Arc<dyn TableProvider>;

pub fn has_top_level_order_by(sql: &str) -> bool {
    let mut depth = 0_i32;
    let mut quote = None;
    let mut words = Vec::new();
    let mut word = String::new();
    let chars: Vec<char> = sql.chars().collect();
    let mut index = 0;
    while index < chars.len() {
        let character = chars[index];
        if let Some(expected) = quote {
            if character == expected {
                if index + 1 < chars.len() && chars[index + 1] == expected {
                    index += 1;
                } else {
                    quote = None;
                }
            }
            index += 1;
            continue;
        }
        if matches!(character, '\'' | '"') {
            quote = Some(character);
            index += 1;
            continue;
        }
        if character == '-' && chars.get(index + 1) == Some(&'-') {
            while index < chars.len() && chars[index] != '\n' {
                index += 1;
            }
            continue;
        }
        if character == '/' && chars.get(index + 1) == Some(&'*') {
            index += 2;
            while index + 1 < chars.len() && !(chars[index] == '*' && chars[index + 1] == '/') {
                index += 1;
            }
            index = (index + 2).min(chars.len());
            continue;
        }
        match character {
            '(' => depth += 1,
            ')' => depth -= 1,
            value if depth == 0 && (value.is_ascii_alphanumeric() || value == '_') => {
                word.push(value.to_ascii_lowercase());
            }
            _ if depth == 0 && !word.is_empty() => words.push(std::mem::take(&mut word)),
            _ => {}
        }
        index += 1;
    }
    if !word.is_empty() {
        words.push(word);
    }
    words.windows(2).any(|pair| pair == ["order", "by"])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_detection_ignores_nested_comments_and_strings() {
        assert!(has_top_level_order_by(
            "SELECT * FROM t /* order by x */ ORDER BY y"
        ));
        assert!(!has_top_level_order_by(
            "SELECT (SELECT x FROM t ORDER BY x), 'order by'"
        ));
    }
}
