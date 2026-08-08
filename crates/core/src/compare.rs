use std::collections::BTreeMap;

use arrow::record_batch::RecordBatch;
use arrow_row::{RowConverter, SortField};

use crate::{Comparison, ComparisonDifference, FindingKind, QueryOutcome, QueryResult};

pub trait Comparator: Send + Sync {
    fn compare(&self, oracle: &QueryOutcome, candidate: &QueryOutcome, ordered: bool)
        -> Comparison;
}

#[derive(Debug, Default)]
pub struct StrictComparator;

impl Comparator for StrictComparator {
    fn compare(
        &self,
        oracle: &QueryOutcome,
        candidate: &QueryOutcome,
        ordered: bool,
    ) -> Comparison {
        match (oracle, candidate) {
            (QueryOutcome::Success(oracle), QueryOutcome::Success(candidate)) => {
                compare_successes(oracle, candidate, ordered)
            }
            (_, QueryOutcome::Rejected(error)) => difference(
                FindingKind::CandidateRejected,
                format!("candidate rejected the query: {}", error.message),
            ),
            (_, QueryOutcome::TimedOut { elapsed }) => difference(
                FindingKind::Timeout,
                format!("candidate timed out after {} ms", elapsed.as_millis()),
            ),
            (_, QueryOutcome::Panicked { message }) => difference(
                FindingKind::PanicOrCrash,
                format!("candidate panicked: {message}"),
            ),
            _ => difference(
                FindingKind::CandidateRejected,
                "oracle did not produce a comparable result".to_owned(),
            ),
        }
    }
}

fn compare_successes(oracle: &QueryResult, candidate: &QueryResult, ordered: bool) -> Comparison {
    if oracle.schema != candidate.schema {
        return difference(
            FindingKind::SchemaMismatch,
            format!(
                "strict schema mismatch: oracle={:?}, candidate={:?}",
                oracle.schema, candidate.schema
            ),
        );
    }
    if oracle.row_count != candidate.row_count {
        return difference(
            FindingKind::RowMismatch,
            format!(
                "row count mismatch: oracle={}, candidate={}",
                oracle.row_count, candidate.row_count
            ),
        );
    }
    match (
        encode_rows(&oracle.batches),
        encode_rows(&candidate.batches),
    ) {
        (Ok(left), Ok(right)) if ordered && left == right => Comparison::Equal,
        (Ok(left), Ok(right)) if !ordered && multiset(&left) == multiset(&right) => {
            Comparison::Equal
        }
        (Ok(_), Ok(_)) => difference(
            FindingKind::RowMismatch,
            if ordered {
                "ordered rows differ".to_owned()
            } else {
                "unordered row multisets differ".to_owned()
            },
        ),
        (Err(message), _) | (_, Err(message)) => difference(FindingKind::RowMismatch, message),
    }
}

pub(crate) fn encode_rows(batches: &[RecordBatch]) -> Result<Vec<Vec<u8>>, String> {
    let Some(first) = batches.first() else {
        return Ok(Vec::new());
    };
    let fields = first
        .schema()
        .fields()
        .iter()
        .map(|field| SortField::new(field.data_type().clone()))
        .collect();
    let converter = RowConverter::new(fields).map_err(|error| error.to_string())?;
    let mut encoded = Vec::new();
    for batch in batches {
        let rows = converter
            .convert_columns(batch.columns())
            .map_err(|error| error.to_string())?;
        encoded.extend(rows.iter().map(|row| row.as_ref().to_vec()));
    }
    Ok(encoded)
}

fn multiset(rows: &[Vec<u8>]) -> BTreeMap<&[u8], usize> {
    let mut counts = BTreeMap::new();
    for row in rows {
        *counts.entry(row.as_slice()).or_default() += 1;
    }
    counts
}

fn difference(kind: FindingKind, message: String) -> Comparison {
    Comparison::Different(ComparisonDifference { kind, message })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema},
    };

    use super::*;

    fn result(values: Vec<Option<i32>>) -> QueryOutcome {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(values))],
        )
        .expect("valid batch");
        QueryOutcome::Success(QueryResult {
            schema,
            row_count: batch.num_rows(),
            batches: vec![batch],
            content_hash: String::new(),
            elapsed: std::time::Duration::ZERO,
        })
    }

    #[test]
    fn unordered_comparison_preserves_duplicates_and_nulls() {
        let comparator = StrictComparator;
        assert_eq!(
            comparator.compare(
                &result(vec![Some(1), None, Some(1)]),
                &result(vec![None, Some(1), Some(1)]),
                false
            ),
            Comparison::Equal
        );
        assert!(matches!(
            comparator.compare(
                &result(vec![Some(1), None, Some(1)]),
                &result(vec![None, None, Some(1)]),
                false
            ),
            Comparison::Different(_)
        ));
    }
}
