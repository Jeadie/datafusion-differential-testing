//! Schema-aware random query generator.
//!
//! Given the canonical dataset schema, this generator emits deterministic,
//! syntactically valid `SELECT` statements: projections, filters, aggregates
//! (including empty-projection `count(*)`/`count(1)`), and joins.
//!
//! The generator has two modes, chosen by `nondeterministic`:
//!
//! Sound mode (`nondeterministic == false`, used when the campaign pins a seed)
//! keeps every query result a well-defined function of the data. The strict
//! comparator flags any oracle/candidate difference as a finding, so this mode
//! never emits `LIMIT` (which without a total order picks an arbitrary subset)
//! or float-valued aggregates (`avg`, `sum` over floats) whose last bits differ
//! between engines. A reported difference is therefore a real one. Row *order*
//! is always safe because the comparator hashes the sorted row multiset unless
//! the query has a top-level `ORDER BY`.
//!
//! Exploratory mode (`nondeterministic == true`, used when no seed is given)
//! also emits `LIMIT` and float aggregates. These reach query shapes the sound
//! mode cannot, at the cost of some findings that are legitimate engine
//! non-determinism rather than bugs — triage accordingly. The resolved seed is
//! still recorded in each query's provenance, so a run stays reproducible after
//! the fact.

use arrow::datatypes::{DataType, SchemaRef};

use crate::{rng::Rng, GeneratedQuery, Ordering, QueryProvenance};

#[derive(Clone, Copy, PartialEq, Eq)]
enum ColumnKind {
    Int,
    Float,
    Utf8,
    Bool,
    Date,
    Timestamp,
    Other,
}

impl ColumnKind {
    fn from_arrow(data_type: &DataType) -> Self {
        match data_type {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => Self::Int,
            DataType::Float16 | DataType::Float32 | DataType::Float64 | DataType::Decimal128(_, _) => {
                Self::Float
            }
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Self::Utf8,
            DataType::Boolean => Self::Bool,
            DataType::Date32 | DataType::Date64 => Self::Date,
            DataType::Timestamp(_, _) => Self::Timestamp,
            _ => Self::Other,
        }
    }

    /// A literal of this kind, or `None` for kinds we cannot safely write.
    fn literal(self, rng: &mut Rng) -> Option<String> {
        match self {
            Self::Int => Some(format!("{}", rng.below(21) as i64 - 5)),
            Self::Float => Some(format!("{}.5", rng.below(20))),
            Self::Utf8 => Some(format!("'{}'", rng.pick(&["a", "x", "z", ""]))),
            Self::Bool => Some((*rng.pick(&["TRUE", "FALSE"])).to_owned()),
            Self::Date => Some("DATE '2020-01-01'".to_owned()),
            Self::Timestamp => Some("TIMESTAMP '2020-01-01 00:00:00'".to_owned()),
            Self::Other => None,
        }
    }
}

struct Column {
    name: String,
    kind: ColumnKind,
}

struct Table {
    name: String,
    columns: Vec<Column>,
}

/// Produces valid, deterministic `SELECT` statements from a dataset schema.
pub struct SchemaAwareGenerator {
    tables: Vec<Table>,
}

impl SchemaAwareGenerator {
    /// Build a generator from `(table name, schema)` pairs. Tables with no
    /// columns are ignored.
    #[must_use]
    pub fn new<'a>(schemas: impl IntoIterator<Item = (&'a str, &'a SchemaRef)>) -> Self {
        let tables = schemas
            .into_iter()
            .map(|(name, schema)| Table {
                name: name.to_owned(),
                columns: schema
                    .fields()
                    .iter()
                    .map(|field| Column {
                        name: field.name().clone(),
                        kind: ColumnKind::from_arrow(field.data_type()),
                    })
                    .collect(),
            })
            .filter(|table| !table.columns.is_empty())
            .collect();
        Self { tables }
    }

    /// Emit `count` queries seeded by `seed`. The query stream is a
    /// deterministic function of `(seed, nondeterministic)`; `nondeterministic`
    /// additionally enables `LIMIT` and float aggregates (see the module docs).
    #[must_use]
    pub fn generate(&self, seed: u64, count: usize, nondeterministic: bool) -> Vec<GeneratedQuery> {
        if self.tables.is_empty() {
            return Vec::new();
        }
        let mut rng = Rng::new(seed);
        let mode = if nondeterministic {
            " (nondeterministic)"
        } else {
            ""
        };
        (0..count)
            .map(|index| {
                let sql = self.query(&mut rng, nondeterministic);
                GeneratedQuery {
                    // `Detect` lets the comparator honor a top-level ORDER BY and
                    // otherwise compare the sorted row multiset.
                    ordering: Ordering::Detect,
                    sql,
                    provenance: QueryProvenance {
                        generator: "random".to_owned(),
                        source: Some(format!("seed={seed}{mode}")),
                        provided_id: Some(format!("random-{index:06}")),
                    },
                }
            })
            .collect()
    }

    fn query(&self, rng: &mut Rng, nd: bool) -> String {
        // Weighted shape choice: aggregates and joins are the interesting cases,
        // but a join needs a compatible column pair on two tables.
        match rng.below(10) {
            0..=3 => self.aggregate_query(rng, nd),
            4..=5 if self.tables.len() >= 2 => self.join_query(rng, nd),
            _ => self.projection_query(rng, nd),
        }
    }

    fn projection_query(&self, rng: &mut Rng, nd: bool) -> String {
        let table = rng.pick(&self.tables);
        let projection = if rng.chance(1, 5) {
            "*".to_owned()
        } else {
            rng.subset(&table.columns)
                .iter()
                .map(|column| quote(&column.name))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let mut sql = format!("SELECT {projection} FROM {}", quote(&table.name));
        if let Some(predicate) = self.predicate(rng, table) {
            sql.push_str(&format!(" WHERE {predicate}"));
        }
        if rng.chance(1, 2) {
            let order = rng
                .subset(&table.columns)
                .iter()
                .map(|column| quote(&column.name))
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(&format!(" ORDER BY {order}"));
        }
        maybe_limit(&mut sql, rng, nd);
        sql
    }

    fn aggregate_query(&self, rng: &mut Rng, nd: bool) -> String {
        let table = rng.pick(&self.tables);
        // Optional grouping column.
        let group = if rng.chance(1, 2) {
            Some(rng.pick(&table.columns))
        } else {
            None
        };
        let mut selections = Vec::new();
        if let Some(group) = group {
            selections.push(quote(&group.name));
        }
        // At least one aggregate. Empty-projection counts are included on
        // purpose: they exercise the zero-column scan path. Exact aggregates
        // (count/min/max, sum over integers) are always allowed; float-valued
        // aggregates are exploratory-only because their last bits can differ
        // between engines.
        let variants = if nd { 8 } else { 6 };
        let aggregate = match rng.below(variants) {
            0 => "count(*)".to_owned(),
            1 => "count(1)".to_owned(),
            2 => format!("count({})", quote(&rng.pick(&table.columns).name)),
            3 => format!("min({})", quote(&rng.pick(&table.columns).name)),
            4 => format!("max({})", quote(&rng.pick(&table.columns).name)),
            5 => match integer_column(rng, table) {
                Some(name) => format!("sum({})", quote(name)),
                None => "count(*)".to_owned(),
            },
            6 => match float_column(rng, table) {
                Some(name) => format!("avg({})", quote(name)),
                None => "count(*)".to_owned(),
            },
            _ => match float_column(rng, table) {
                Some(name) => format!("sum({})", quote(name)),
                None => "count(*)".to_owned(),
            },
        };
        selections.push(aggregate);
        let mut sql = format!("SELECT {} FROM {}", selections.join(", "), quote(&table.name));
        if let Some(predicate) = self.predicate(rng, table) {
            sql.push_str(&format!(" WHERE {predicate}"));
        }
        if let Some(group) = group {
            sql.push_str(&format!(" GROUP BY {}", quote(&group.name)));
            if rng.chance(1, 2) {
                sql.push_str(&format!(" ORDER BY {}", quote(&group.name)));
            }
        }
        maybe_limit(&mut sql, rng, nd);
        sql
    }

    fn join_query(&self, rng: &mut Rng, nd: bool) -> String {
        let left = rng.below(self.tables.len());
        let mut right = rng.below(self.tables.len());
        if right == left {
            right = (right + 1) % self.tables.len();
        }
        let left = &self.tables[left];
        let right = &self.tables[right];
        // Find a column pair of the same kind to join on; fall back to a
        // projection query if the two tables share no comparable kind.
        let Some((left_key, right_key)) = join_key(rng, left, right) else {
            return self.projection_query(rng, nd);
        };
        let join = rng.pick(&["JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"]);
        let left_projection = rng
            .subset(&left.columns)
            .iter()
            .map(|column| format!("a.{}", quote(&column.name)))
            .collect::<Vec<_>>();
        let right_projection = rng
            .subset(&right.columns)
            .iter()
            .map(|column| format!("b.{}", quote(&column.name)))
            .collect::<Vec<_>>();
        let projection = left_projection
            .into_iter()
            .chain(right_projection)
            .collect::<Vec<_>>()
            .join(", ");
        let mut sql = format!(
            "SELECT {projection} FROM {} AS a {join} {} AS b ON a.{} = b.{}",
            quote(&left.name),
            quote(&right.name),
            quote(&left_key),
            quote(&right_key),
        );
        maybe_limit(&mut sql, rng, nd);
        sql
    }

    /// Zero to two ANDed predicates over `table`.
    fn predicate(&self, rng: &mut Rng, table: &Table) -> Option<String> {
        let count = rng.below(3);
        let mut parts = Vec::new();
        for _ in 0..count {
            let column = rng.pick(&table.columns);
            let name = quote(&column.name);
            let clause = match rng.below(8) {
                0 => format!("{name} IS NULL"),
                1 => format!("{name} IS NOT NULL"),
                _ => {
                    let Some(literal) = column.kind.literal(rng) else {
                        continue;
                    };
                    let op = rng.pick(&["=", "<>", "<", ">", "<=", ">="]);
                    format!("{name} {op} {literal}")
                }
            };
            parts.push(clause);
        }
        if parts.is_empty() {
            None
        } else {
            Some(parts.join(" AND "))
        }
    }
}

/// Append a `LIMIT` in exploratory mode only. A `LIMIT` without a total order is
/// intentionally non-deterministic, so the sound mode never emits one.
fn maybe_limit(sql: &mut String, rng: &mut Rng, nd: bool) {
    if nd && rng.chance(1, 3) {
        sql.push_str(&format!(" LIMIT {}", rng.below(10) + 1));
    }
}

fn integer_column<'a>(rng: &mut Rng, table: &'a Table) -> Option<&'a str> {
    column_of_kind(rng, table, ColumnKind::Int)
}

fn float_column<'a>(rng: &mut Rng, table: &'a Table) -> Option<&'a str> {
    column_of_kind(rng, table, ColumnKind::Float)
}

fn column_of_kind<'a>(rng: &mut Rng, table: &'a Table, kind: ColumnKind) -> Option<&'a str> {
    let matching: Vec<&Column> = table
        .columns
        .iter()
        .filter(|column| column.kind == kind)
        .collect();
    if matching.is_empty() {
        None
    } else {
        Some(rng.pick(&matching).name.as_str())
    }
}

fn join_key(rng: &mut Rng, left: &Table, right: &Table) -> Option<(String, String)> {
    let mut pairs = Vec::new();
    for left_column in &left.columns {
        for right_column in &right.columns {
            if left_column.kind == right_column.kind && left_column.kind != ColumnKind::Other {
                pairs.push((left_column.name.clone(), right_column.name.clone()));
            }
        }
    }
    if pairs.is_empty() {
        None
    } else {
        Some(rng.pick(&pairs).clone())
    }
}

fn quote(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{Field, Schema};

    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ]))
    }

    #[test]
    fn generation_is_deterministic_for_a_seed() {
        let orders = schema();
        let generator = SchemaAwareGenerator::new([("orders", &orders)]);
        let first = generator.generate(7, 50, false);
        let second = generator.generate(7, 50, false);
        assert_eq!(first, second);
        assert_eq!(first.len(), 50);
    }

    #[test]
    fn different_seeds_diverge() {
        let orders = schema();
        let generator = SchemaAwareGenerator::new([("orders", &orders)]);
        let a = generator.generate(1, 30, false);
        let b = generator.generate(2, 30, false);
        assert_ne!(a, b);
    }

    #[test]
    fn every_query_parses_as_a_single_statement() {
        use datafusion::sql::parser::DFParser;
        let orders = schema();
        let customers = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("code", DataType::Int64, true),
        ]));
        let generator =
            SchemaAwareGenerator::new([("orders", &orders), ("customers", &customers)]);
        // Both modes must produce parseable single statements.
        for nondeterministic in [false, true] {
            for query in generator.generate(99, 500, nondeterministic) {
                let statements = DFParser::parse_sql(&query.sql).unwrap_or_else(|error| {
                    panic!("query failed to parse: {}\n{error}", query.sql)
                });
                assert_eq!(
                    statements.len(),
                    1,
                    "expected exactly one statement: {}",
                    query.sql
                );
            }
        }
    }

    #[test]
    fn emits_empty_projection_counts() {
        let orders = schema();
        let generator = SchemaAwareGenerator::new([("orders", &orders)]);
        let queries = generator.generate(3, 500, false);
        assert!(
            queries
                .iter()
                .any(|query| query.sql.contains("count(1)") || query.sql.contains("count(*)")),
            "generator should exercise empty-projection counts"
        );
    }

    #[test]
    fn sound_mode_avoids_limit_and_float_aggregates() {
        let orders = schema();
        let generator = SchemaAwareGenerator::new([("orders", &orders)]);
        for query in generator.generate(11, 1000, false) {
            assert!(!query.sql.contains(" LIMIT "), "sound mode emitted LIMIT: {}", query.sql);
            assert!(!query.sql.contains("avg("), "sound mode emitted avg: {}", query.sql);
        }
    }

    #[test]
    fn exploratory_mode_emits_limit() {
        let orders = schema();
        let generator = SchemaAwareGenerator::new([("orders", &orders)]);
        assert!(
            generator
                .generate(11, 1000, true)
                .iter()
                .any(|query| query.sql.contains(" LIMIT ")),
            "exploratory mode should emit LIMIT"
        );
    }
}
