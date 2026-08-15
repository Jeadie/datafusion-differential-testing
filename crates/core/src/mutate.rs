//! Feedback mutator: given a SQL query that produced a finding, emit nearby
//! variants so the campaign explores around the interesting query.
//!
//! Mutations are textual and local (swap a comparison operator, perturb a
//! literal, swap an aggregate, drop a `WHERE`/`ORDER BY`, tweak a `LIMIT`). Every
//! candidate is re-parsed and only syntactically valid, changed, non-duplicate
//! statements are returned. Semantically invalid mutants are harmless: the
//! oracle rejects them and the run skips them, so they never become findings. A
//! mutant never *adds* a `LIMIT` (which would introduce non-determinism); it only
//! edits one already present.

use datafusion::sql::parser::DFParser;

use crate::rng::Rng;

const COMPARISONS: [&str; 6] = [" <> ", " <= ", " >= ", " = ", " < ", " > "];
const AGGREGATES: [&str; 5] = ["count(", "sum(", "min(", "max(", "avg("];

/// Produce up to `count` distinct, parseable mutations of `sql`.
#[must_use]
pub fn mutate(sql: &str, rng: &mut Rng, count: usize) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut attempts = 0;
    let budget = count.saturating_mul(8).max(8);
    while out.len() < count && attempts < budget {
        attempts += 1;
        if let Some(candidate) = apply_one(sql, rng) {
            if candidate != sql && !out.contains(&candidate) && parses(&candidate) {
                out.push(candidate);
            }
        }
    }
    out
}

fn parses(sql: &str) -> bool {
    DFParser::parse_sql(sql).map(|s| s.len() == 1).unwrap_or(false)
}

fn apply_one(sql: &str, rng: &mut Rng) -> Option<String> {
    match rng.below(6) {
        0 => swap_comparison(sql, rng),
        1 => perturb_number(sql, rng),
        2 => swap_aggregate(sql, rng),
        3 => drop_clause(sql, " WHERE ", &[" GROUP BY ", " ORDER BY ", " LIMIT "]),
        4 => drop_clause(sql, " ORDER BY ", &[" LIMIT "]),
        _ => edit_limit(sql, rng),
    }
}

/// Replace one comparison operator token with a different one.
fn swap_comparison(sql: &str, rng: &mut Rng) -> Option<String> {
    let occurrences = find_tokens(sql, &COMPARISONS);
    let &(start, token) = pick(&occurrences, rng)?;
    let mut replacement = *rng.pick(&COMPARISONS);
    if replacement == token {
        replacement = COMPARISONS[(index_of(&COMPARISONS, token) + 1) % COMPARISONS.len()];
    }
    Some(splice(sql, start, token.len(), replacement))
}

/// Replace one aggregate function name with a different one.
fn swap_aggregate(sql: &str, rng: &mut Rng) -> Option<String> {
    let occurrences = find_tokens(sql, &AGGREGATES);
    let &(start, token) = pick(&occurrences, rng)?;
    let mut replacement = *rng.pick(&AGGREGATES);
    if replacement == token {
        replacement = AGGREGATES[(index_of(&AGGREGATES, token) + 1) % AGGREGATES.len()];
    }
    Some(splice(sql, start, token.len(), replacement))
}

/// Change one integer literal by a small delta.
fn perturb_number(sql: &str, rng: &mut Rng) -> Option<String> {
    let runs = integer_runs(sql);
    let &(start, end) = pick(&runs, rng)?;
    let value: i64 = sql[start..end].parse().ok()?;
    let delta = rng.below(11) as i64 - 5;
    let next = if delta == 0 { value + 1 } else { value + delta };
    Some(splice(sql, start, end - start, &next.to_string()))
}

/// Drop the clause starting at `keyword` up to the next of `stops` (or the end).
fn drop_clause(sql: &str, keyword: &str, stops: &[&str]) -> Option<String> {
    let start = sql.find(keyword)?;
    let after = start + keyword.len();
    let end = stops
        .iter()
        .filter_map(|stop| sql[after..].find(stop).map(|offset| after + offset))
        .min()
        .unwrap_or(sql.len());
    let mutated = format!("{}{}", &sql[..start], &sql[end..]);
    Some(mutated)
}

/// Change or drop an existing `LIMIT n` (never adds one).
fn edit_limit(sql: &str, rng: &mut Rng) -> Option<String> {
    let keyword = " LIMIT ";
    let start = sql.find(keyword)?;
    let after = start + keyword.len();
    let digits_end = after + sql[after..].find(|c: char| !c.is_ascii_digit()).unwrap_or(sql.len() - after);
    if rng.chance(1, 2) {
        // Drop the whole `LIMIT n`.
        Some(format!("{}{}", &sql[..start], &sql[digits_end..]))
    } else {
        let next = rng.below(20) + 1;
        Some(format!("{}{}{}", &sql[..after], next, &sql[digits_end..]))
    }
}

/// Byte offsets of maximal ASCII-digit runs that are standalone integers
/// (not inside quotes, not part of an identifier, not a fractional part).
fn integer_runs(sql: &str) -> Vec<(usize, usize)> {
    let bytes = sql.as_bytes();
    let mut runs = Vec::new();
    let mut quote: Option<u8> = None;
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if let Some(q) = quote {
            if b == q {
                quote = None;
            }
            i += 1;
            continue;
        }
        if b == b'\'' || b == b'"' {
            quote = Some(b);
            i += 1;
            continue;
        }
        if b.is_ascii_digit() {
            let prev = if i == 0 { 0 } else { bytes[i - 1] };
            let boundary = !(prev.is_ascii_alphanumeric() || prev == b'_' || prev == b'.');
            let start = i;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
            let next = bytes.get(i).copied().unwrap_or(b' ');
            // Reject identifiers (2x) and floats (1.5) so we only touch integers.
            let clean = next != b'.' && !next.is_ascii_alphabetic() && next != b'_';
            if boundary && clean {
                runs.push((start, i));
            }
            continue;
        }
        i += 1;
    }
    runs
}

/// Byte offsets (and matched token) of every occurrence of any token in `tokens`.
fn find_tokens<'a>(sql: &str, tokens: &[&'a str]) -> Vec<(usize, &'a str)> {
    let mut found = Vec::new();
    for &token in tokens {
        let mut from = 0;
        while let Some(offset) = sql[from..].find(token) {
            let at = from + offset;
            found.push((at, token));
            from = at + token.len();
        }
    }
    found
}

fn splice(sql: &str, start: usize, len: usize, replacement: &str) -> String {
    format!("{}{}{}", &sql[..start], replacement, &sql[start + len..])
}

fn index_of(tokens: &[&str], token: &str) -> usize {
    tokens.iter().position(|&t| t == token).unwrap_or(0)
}

fn pick<'a, T>(items: &'a [T], rng: &mut Rng) -> Option<&'a T> {
    if items.is_empty() {
        None
    } else {
        Some(&items[rng.below(items.len())])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mutations_are_valid_changed_and_deterministic() {
        let sql = r#"SELECT "id", count(1) FROM "orders" WHERE "amount" > 10 GROUP BY "id" ORDER BY "id" LIMIT 5"#;
        let mut rng = Rng::new(42);
        let a = mutate(sql, &mut rng, 12);
        let mut rng = Rng::new(42);
        let b = mutate(sql, &mut rng, 12);
        assert_eq!(a, b, "mutation is deterministic for a seed");
        assert!(!a.is_empty(), "should produce mutants");
        for mutant in &a {
            assert_ne!(mutant, sql);
            assert!(parses(mutant), "mutant must parse: {mutant}");
        }
    }

    #[test]
    fn perturb_only_touches_integer_literals_not_identifiers() {
        // "id2" is a quoted identifier; the only bare integer is 7.
        let sql = r#"SELECT "id2" FROM "t" WHERE "id2" = 7"#;
        let runs = integer_runs(sql);
        assert_eq!(runs.len(), 1);
        let (start, end) = runs[0];
        assert_eq!(&sql[start..end], "7");
    }

    #[test]
    fn drop_where_removes_only_the_where_clause() {
        let sql = r#"SELECT "id" FROM "t" WHERE "a" = 1 ORDER BY "id""#;
        let dropped = drop_clause(sql, " WHERE ", &[" GROUP BY ", " ORDER BY ", " LIMIT "]).unwrap();
        assert_eq!(dropped, r#"SELECT "id" FROM "t" ORDER BY "id""#);
    }

    #[test]
    fn no_mutation_available_returns_empty() {
        // A trivial query with no operators, numbers, aggregates, or clauses.
        let sql = r#"SELECT "id" FROM "t""#;
        let mut rng = Rng::new(1);
        assert!(mutate(sql, &mut rng, 5).is_empty());
    }
}
