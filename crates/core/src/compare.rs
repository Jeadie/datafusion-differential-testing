use std::collections::BTreeMap;

use arrow::{
    array::{Array, ArrowPrimitiveType, AsArray},
    compute::concat_batches,
    datatypes::{DataType, Float32Type, Float64Type, Schema},
    error::ArrowError,
    record_batch::RecordBatch,
};
use arrow_row::{RowConverter, SortField};

use crate::{Comparison, ComparisonDifference, FindingKind, QueryOutcome, QueryResult};

pub trait Comparator: Send + Sync {
    fn compare(&self, oracle: &QueryOutcome, candidate: &QueryOutcome, ordered: bool)
        -> Comparison;
}

/// Largest distance, in units in the last place (ULPs), at which two floats compare equal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ulps(pub u32);

impl Default for Ulps {
    /// Absorbs the rounding noise of float aggregates computed in a different order, such
    /// as a parallel `SUM`.
    fn default() -> Self {
        Self(2)
    }
}

impl Ulps {
    /// NaN equals NaN, +0.0 equals -0.0, and an infinity equals only the same infinity.
    fn matches<F: UlpFloat>(self, left: F, right: F) -> bool {
        if left.is_nan() || right.is_nan() {
            return left.is_nan() && right.is_nan();
        }
        // The ordered bits put MAX and infinity 1 apart, so infinities must match exactly.
        if left.is_infinite() || right.is_infinite() {
            return left == right;
        }
        left.ordered_bits().abs_diff(right.ordered_bits()) <= u64::from(self.0)
    }
}

/// A float whose values can be compared by ULP distance.
trait UlpFloat: Copy + PartialEq {
    fn is_nan(self) -> bool;
    fn is_infinite(self) -> bool;
    /// The IEEE bits as an integer whose order matches the float order. Both zeros map to 0.
    fn ordered_bits(self) -> i64;
}

impl UlpFloat for f32 {
    fn is_nan(self) -> bool {
        f32::is_nan(self)
    }

    fn is_infinite(self) -> bool {
        f32::is_infinite(self)
    }

    fn ordered_bits(self) -> i64 {
        let bits = self.to_bits() as i32;
        i64::from(if bits < 0 { i32::MIN - bits } else { bits })
    }
}

impl UlpFloat for f64 {
    fn is_nan(self) -> bool {
        f64::is_nan(self)
    }

    fn is_infinite(self) -> bool {
        f64::is_infinite(self)
    }

    fn ordered_bits(self) -> i64 {
        let bits = self.to_bits() as i64;
        if bits < 0 {
            i64::MIN - bits
        } else {
            bits
        }
    }
}

/// Compares schemas and rows exactly, except that values in top-level `Float32` and
/// `Float64` columns match within `float_tolerance`.
#[derive(Debug, Default, Clone, Copy)]
pub struct StrictComparator {
    pub float_tolerance: Ulps,
}

impl Comparator for StrictComparator {
    fn compare(
        &self,
        oracle: &QueryOutcome,
        candidate: &QueryOutcome,
        ordered: bool,
    ) -> Comparison {
        match (oracle, candidate) {
            (QueryOutcome::Success(oracle), QueryOutcome::Success(candidate)) => {
                compare_successes(oracle, candidate, ordered, self.float_tolerance)
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

fn compare_successes(
    oracle: &QueryResult,
    candidate: &QueryResult,
    ordered: bool,
    float_tolerance: Ulps,
) -> Comparison {
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
    let exactly_equal = match (
        encode_rows(&oracle.batches),
        encode_rows(&candidate.batches),
    ) {
        (Ok(left), Ok(right)) if ordered => left == right,
        (Ok(left), Ok(right)) => multiset(&left) == multiset(&right),
        (Err(message), _) | (_, Err(message)) => {
            return difference(FindingKind::RowMismatch, message)
        }
    };
    if exactly_equal {
        return Comparison::Equal;
    }
    let note = match equal_within_float_tolerance(oracle, candidate, ordered, float_tolerance) {
        Ok(true) => return Comparison::Equal,
        Ok(false) => String::new(),
        Err(error) => format!(" (float-tolerant comparison failed: {error})"),
    };
    difference(
        FindingKind::RowMismatch,
        format!(
            "{}{note}\n{}",
            if ordered {
                "ordered rows differ"
            } else {
                "unordered row multisets differ"
            },
            full_results(oracle, candidate),
        ),
    )
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

/// Float columns compared within a ULP tolerance. Only top-level `Float32` and `Float64`
/// columns qualify; `Float16`, decimals, and floats nested in lists or structs stay exact.
#[derive(Debug, Clone, Copy)]
enum FloatWidth {
    F32,
    F64,
}

impl FloatWidth {
    fn of(data_type: &DataType) -> Option<Self> {
        match data_type {
            DataType::Float32 => Some(Self::F32),
            DataType::Float64 => Some(Self::F64),
            _ => None,
        }
    }
}

/// Compares rows again with top-level float values matched within `tolerance` and all
/// other columns still exact. Returns false when there is no top-level float column.
fn equal_within_float_tolerance(
    oracle: &QueryResult,
    candidate: &QueryResult,
    ordered: bool,
    tolerance: Ulps,
) -> Result<bool, ArrowError> {
    let schema = &oracle.schema;
    let mut exact_columns = Vec::new();
    let mut float_columns = Vec::new();
    for (index, field) in schema.fields().iter().enumerate() {
        match FloatWidth::of(field.data_type()) {
            Some(width) => float_columns.push((index, width)),
            None => exact_columns.push(index),
        }
    }
    if float_columns.is_empty() {
        return Ok(false);
    }
    let left = concat_batches(schema, &oracle.batches)?;
    let right = concat_batches(schema, &candidate.batches)?;
    if left.num_rows() != right.num_rows() {
        return Ok(false);
    }

    // Each pair is (oracle row, candidate row).
    let pairs: Vec<(usize, usize)> = if ordered {
        (0..left.num_rows()).map(|row| (row, row)).collect()
    } else {
        // Sort by the exact columns first so that rows with equal exact values stay together
        // and their near-equal floats line up. Known limit: rows that share all exact values
        // and have several float columns can pair up suboptimally and compare as different.
        let sort_columns: Vec<usize> = exact_columns
            .iter()
            .copied()
            .chain(float_columns.iter().map(|&(index, _)| index))
            .collect();
        let converter = row_converter(schema, &sort_columns)?;
        sorted_rows(&converter, &left, &sort_columns)?
            .into_iter()
            .zip(sorted_rows(&converter, &right, &sort_columns)?)
            .collect()
    };

    if !exact_columns.is_empty() {
        let converter = row_converter(schema, &exact_columns)?;
        let left_rows = converter.convert_columns(left.project(&exact_columns)?.columns())?;
        let right_rows = converter.convert_columns(right.project(&exact_columns)?.columns())?;
        if pairs
            .iter()
            .any(|&(l, r)| left_rows.row(l) != right_rows.row(r))
        {
            return Ok(false);
        }
    }
    Ok(float_columns.iter().all(|&(index, width)| {
        let columns = (left.column(index).as_ref(), right.column(index).as_ref());
        match width {
            FloatWidth::F32 => floats_match::<Float32Type>(columns, &pairs, tolerance),
            FloatWidth::F64 => floats_match::<Float64Type>(columns, &pairs, tolerance),
        }
    }))
}

fn row_converter(schema: &Schema, columns: &[usize]) -> Result<RowConverter, ArrowError> {
    RowConverter::new(
        columns
            .iter()
            .map(|&index| SortField::new(schema.field(index).data_type().clone()))
            .collect(),
    )
}

/// Row indices of `batch`, in ascending order of the encoded `columns`.
fn sorted_rows(
    converter: &RowConverter,
    batch: &RecordBatch,
    columns: &[usize],
) -> Result<Vec<usize>, ArrowError> {
    let rows = converter.convert_columns(batch.project(columns)?.columns())?;
    let mut order: Vec<usize> = (0..rows.num_rows()).collect();
    order.sort_unstable_by(|&a, &b| rows.row(a).cmp(&rows.row(b)));
    Ok(order)
}

/// Null matches only null; values match within `tolerance`.
fn floats_match<T>(
    (left, right): (&dyn Array, &dyn Array),
    pairs: &[(usize, usize)],
    tolerance: Ulps,
) -> bool
where
    T: ArrowPrimitiveType,
    T::Native: UlpFloat,
{
    let (Some(left), Some(right)) = (left.as_primitive_opt::<T>(), right.as_primitive_opt::<T>())
    else {
        return false;
    };
    pairs
        .iter()
        .all(|&(l, r)| match (left.is_valid(l), right.is_valid(r)) {
            (true, true) => tolerance.matches(left.value(l), right.value(r)),
            (left_valid, right_valid) => left_valid == right_valid,
        })
}

/// Render every row of the oracle and candidate results so row-mismatch findings carry
/// the actual differing values, not just a verdict.
fn full_results(oracle: &QueryResult, candidate: &QueryResult) -> String {
    fn render(batches: &[RecordBatch]) -> String {
        arrow::util::pretty::pretty_format_batches(batches)
            .map(|d| d.to_string())
            .unwrap_or_else(|error| format!("<unrenderable: {error}>"))
    }
    format!(
        "  oracle ({} rows):\n{}\n  candidate ({} rows):\n{}",
        oracle.row_count,
        render(&oracle.batches),
        candidate.row_count,
        render(&candidate.batches)
    )
}

fn difference(kind: FindingKind, message: String) -> Comparison {
    Comparison::Different(ComparisonDifference { kind, message })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Float32Array, Float64Array, Int32Array};

    use super::*;

    fn outcome(columns: Vec<(&str, ArrayRef)>) -> QueryOutcome {
        let batch = RecordBatch::try_from_iter_with_nullable(
            columns.into_iter().map(|(name, array)| (name, array, true)),
        )
        .expect("valid batch");
        QueryOutcome::Success(QueryResult {
            schema: batch.schema(),
            row_count: batch.num_rows(),
            batches: vec![batch],
            content_hash: String::new(),
            elapsed: std::time::Duration::ZERO,
        })
    }

    fn result(values: Vec<Option<i32>>) -> QueryOutcome {
        outcome(vec![("value", Arc::new(Int32Array::from(values)))])
    }

    fn f64s(values: &[Option<f64>]) -> QueryOutcome {
        outcome(vec![("f", Arc::new(Float64Array::from(values.to_vec())))])
    }

    fn f32s(values: &[Option<f32>]) -> QueryOutcome {
        outcome(vec![("f", Arc::new(Float32Array::from(values.to_vec())))])
    }

    /// The float `ulps` steps above the positive `value`.
    fn up(value: f64, ulps: u64) -> f64 {
        f64::from_bits(value.to_bits() + ulps)
    }

    fn up32(value: f32, ulps: u32) -> f32 {
        f32::from_bits(value.to_bits() + ulps)
    }

    fn is_row_mismatch(comparison: &Comparison) -> bool {
        matches!(
            comparison,
            Comparison::Different(ComparisonDifference {
                kind: FindingKind::RowMismatch,
                ..
            })
        )
    }

    #[test]
    fn unordered_comparison_preserves_duplicates_and_nulls() {
        let comparator = StrictComparator::default();
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

    #[test]
    fn default_float_tolerance_is_two_ulps() {
        assert_eq!(StrictComparator::default().float_tolerance, Ulps(2));
    }

    #[test]
    fn f64_values_match_within_two_ulps() {
        let comparator = StrictComparator::default();
        for ordered in [true, false] {
            for ulps in [1, 2] {
                assert_eq!(
                    comparator.compare(&f64s(&[Some(1.0)]), &f64s(&[Some(up(1.0, ulps))]), ordered),
                    Comparison::Equal,
                    "{ulps} ULPs, ordered={ordered}"
                );
            }
            assert!(is_row_mismatch(&comparator.compare(
                &f64s(&[Some(1.0)]),
                &f64s(&[Some(up(1.0, 3))]),
                ordered
            )));
        }
    }

    #[test]
    fn f32_values_match_within_two_ulps() {
        let comparator = StrictComparator::default();
        for ordered in [true, false] {
            for ulps in [1, 2] {
                assert_eq!(
                    comparator.compare(
                        &f32s(&[Some(1.0)]),
                        &f32s(&[Some(up32(1.0, ulps))]),
                        ordered
                    ),
                    Comparison::Equal,
                    "{ulps} ULPs, ordered={ordered}"
                );
            }
            assert!(is_row_mismatch(&comparator.compare(
                &f32s(&[Some(1.0)]),
                &f32s(&[Some(up32(1.0, 3))]),
                ordered
            )));
        }
    }

    #[test]
    fn special_float_values() {
        let comparator = StrictComparator::default();
        let compare_both = |left: &[Option<f64>], right: &[Option<f64>]| {
            [true, false].map(|ordered| comparator.compare(&f64s(left), &f64s(right), ordered))
        };
        let assert_equal = |left: &[Option<f64>], right: &[Option<f64>]| {
            for comparison in compare_both(left, right) {
                assert_eq!(comparison, Comparison::Equal, "{left:?} vs {right:?}");
            }
        };
        let assert_mismatch = |left: &[Option<f64>], right: &[Option<f64>]| {
            for comparison in compare_both(left, right) {
                assert!(is_row_mismatch(&comparison), "{left:?} vs {right:?}");
            }
        };
        // Different NaN bit patterns are not equal bytewise, so this uses the tolerant path.
        assert_equal(&[Some(f64::NAN)], &[Some(-f64::NAN)]);
        assert_equal(&[Some(0.0)], &[Some(-0.0)]);
        assert_mismatch(&[Some(f64::INFINITY)], &[Some(f64::MAX)]);
        assert_mismatch(&[Some(f64::NEG_INFINITY)], &[Some(f64::MIN)]);
        assert_mismatch(&[Some(f64::INFINITY)], &[Some(f64::NEG_INFINITY)]);
        assert_mismatch(&[Some(f64::NAN)], &[Some(1.0)]);
        // The second row forces the tolerant path for the null row.
        assert_equal(&[None, Some(1.0)], &[None, Some(up(1.0, 1))]);
        assert_mismatch(&[None], &[Some(1.0)]);
        assert_mismatch(&[Some(1.0)], &[None]);

        // Distance is counted across the sign boundary.
        let tolerance = Ulps::default();
        let tiny = f64::from_bits(1);
        assert!(tolerance.matches(tiny, -tiny));
        assert!(!tolerance.matches(f64::from_bits(2), -tiny));
        let tiny32 = f32::from_bits(1);
        assert!(tolerance.matches(tiny32, -tiny32));
        assert!(!tolerance.matches(f32::from_bits(2), -tiny32));
        assert!(!tolerance.matches(f32::INFINITY, f32::MAX));
        assert!(tolerance.matches(f32::NAN, -f32::NAN));
    }

    #[test]
    fn unordered_floats_match_across_row_order() {
        let comparator = StrictComparator::default();
        assert_eq!(
            comparator.compare(
                &f64s(&[Some(1.0), Some(2.0)]),
                &f64s(&[Some(up(2.0, 1)), Some(up(1.0, 1))]),
                false
            ),
            Comparison::Equal
        );
    }

    #[test]
    fn unordered_rows_pair_by_exact_columns_before_floats() {
        // Sorting by `f` first would pair k=1 with k=2.
        let oracle = outcome(vec![
            (
                "f",
                Arc::new(Float64Array::from(vec![1.0, up(1.0, 1)])) as ArrayRef,
            ),
            ("k", Arc::new(Int32Array::from(vec![1, 2]))),
        ]);
        let candidate = outcome(vec![
            (
                "f",
                Arc::new(Float64Array::from(vec![1.0, up(1.0, 2)])) as ArrayRef,
            ),
            ("k", Arc::new(Int32Array::from(vec![2, 1]))),
        ]);
        assert_eq!(
            StrictComparator::default().compare(&oracle, &candidate, false),
            Comparison::Equal
        );
    }

    #[test]
    fn ordered_comparison_keeps_row_order() {
        let comparator = StrictComparator::default();
        assert!(is_row_mismatch(&comparator.compare(
            &f64s(&[Some(1.0), Some(2.0)]),
            &f64s(&[Some(2.0), Some(1.0)]),
            true
        )));
        assert!(is_row_mismatch(&comparator.compare(
            &f64s(&[Some(1.0), Some(2.0)]),
            &f64s(&[Some(up(2.0, 1)), Some(up(1.0, 1))]),
            true
        )));
    }

    #[test]
    fn non_float_columns_stay_exact() {
        let oracle = outcome(vec![
            ("k", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
            ("f", Arc::new(Float64Array::from(vec![1.0]))),
        ]);
        let candidate = outcome(vec![
            ("k", Arc::new(Int32Array::from(vec![2])) as ArrayRef),
            ("f", Arc::new(Float64Array::from(vec![up(1.0, 1)]))),
        ]);
        for ordered in [true, false] {
            assert!(is_row_mismatch(
                &StrictComparator::default().compare(&oracle, &candidate, ordered)
            ));
        }
    }

    #[test]
    fn row_mismatch_shows_every_row_of_both_sides() {
        let Comparison::Different(found) = StrictComparator::default().compare(
            &result((0..25).map(Some).collect()),
            &result((100..125).map(Some).collect()),
            true,
        ) else {
            panic!("expected a difference");
        };
        for value in (0..25).chain(100..125) {
            assert!(
                found.message.contains(&format!("| {value} ")),
                "row {value} missing from:\n{}",
                found.message
            );
        }
    }
}
