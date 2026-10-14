//! Runs the corpus on every variant and compares each candidate with the oracle.

use datafusion_differential_testing::{Comparator, Comparison, FindingKind, QueryOutcome};
use serde::Serialize;

use crate::{config::Spec, driver::Session};

#[derive(Debug, Serialize)]
pub struct Finding {
    pub query: usize,
    pub source: Option<String>,
    pub candidate: String,
    pub kind: FindingKind,
    pub message: String,
    pub sql: String,
}

#[derive(Debug, Default, Serialize)]
pub struct Report {
    pub queries_executed: usize,
    /// Queries skipped because the oracle did not return a result.
    pub oracle_rejections: usize,
    pub comparisons: usize,
    pub findings: Vec<Finding>,
}

struct Candidate {
    name: String,
    session: Session,
}

pub fn run(spec: &Spec, comparator: &dyn Comparator) -> Result<Report, String> {
    let mut oracle = open(spec, &spec.oracle)?;
    let mut candidates = spec
        .candidates
        .iter()
        .map(|variant| {
            Ok(Candidate {
                name: variant.name.clone(),
                session: open(spec, variant)?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    let mut report = Report::default();
    'queries: for (index, query) in spec.corpus.iter().take(spec.limits.max_queries).enumerate() {
        report.queries_executed += 1;
        let expected = oracle.query(&query.sql, spec.schema);
        if !matches!(expected, QueryOutcome::Success(_)) {
            report.oracle_rejections += 1;
            continue;
        }
        for candidate in &mut candidates {
            let actual = candidate.session.query(&query.sql, spec.schema);
            report.comparisons += 1;
            let Comparison::Different(difference) =
                comparator.compare(&expected, &actual, query.is_ordered())
            else {
                continue;
            };
            report.findings.push(Finding {
                query: index,
                source: query.provenance.source.clone(),
                candidate: candidate.name.clone(),
                kind: difference.kind,
                message: difference.message,
                sql: query.sql.clone(),
            });
            if spec.limits.stop_on_finding || report.findings.len() >= spec.limits.max_findings {
                break 'queries;
            }
        }
    }
    Ok(report)
}

/// Connect a variant and run the setup scripts on it.
fn open(spec: &Spec, variant: &crate::config::VariantSpec) -> Result<Session, String> {
    let mut session = Session::connect(variant)?;
    for script in &spec.setup {
        session.execute_script(&script.sql).map_err(|error| {
            format!(
                "variant `{}`: setup `{}` failed: {error}",
                variant.name, script.source
            )
        })?;
    }
    Ok(session)
}
