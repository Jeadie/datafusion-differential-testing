//! Dialect-agnostic SQL script splitting.

/// Split a script into statements at top-level `;`, keeping each statement's
/// original text. Semicolons inside quotes (`'`, `"`, `` ` ``) and comments
/// (`--`, `/* */`) are ignored. Dialect-specific quoting such as PostgreSQL
/// dollar quotes is not understood.
pub fn split_statements(script: &str) -> Vec<&str> {
    let bytes = script.as_bytes();
    let mut statements = Vec::new();
    let mut start = 0;
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            quote @ (b'\'' | b'"' | b'`') => {
                index += 1;
                while index < bytes.len() {
                    if bytes[index] == quote {
                        // A doubled quote is an escaped quote.
                        if bytes.get(index + 1) == Some(&quote) {
                            index += 1;
                        } else {
                            break;
                        }
                    }
                    index += 1;
                }
            }
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index += 2;
                while index < bytes.len() && !bytes[index..].starts_with(b"*/") {
                    index += 1;
                }
                index += 1;
            }
            b';' => {
                statements.push(&script[start..index]);
                start = index + 1;
            }
            _ => {}
        }
        index += 1;
    }
    statements.push(&script[start..]);
    statements
        .into_iter()
        .map(str::trim)
        .filter(|statement| !is_blank(statement))
        .collect()
}

/// True when the statement holds only whitespace and comments.
fn is_blank(statement: &str) -> bool {
    let mut rest = statement.trim_start();
    loop {
        if let Some(after) = rest.strip_prefix("--") {
            rest = after.split_once('\n').map_or("", |(_, tail)| tail);
        } else if let Some(after) = rest.strip_prefix("/*") {
            rest = after.split_once("*/").map_or("", |(_, tail)| tail);
        } else {
            return rest.is_empty();
        }
        rest = rest.trim_start();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_on_top_level_semicolons_only() {
        let script = "CREATE TABLE t (s TEXT); -- a; comment\n\
                      INSERT INTO t VALUES ('a;b'), ('it''s;'); /* x; y */\n\
                      SELECT \"c;d\" FROM t;\n  -- trailing\n";
        assert_eq!(
            split_statements(script),
            vec![
                "CREATE TABLE t (s TEXT)",
                "-- a; comment\nINSERT INTO t VALUES ('a;b'), ('it''s;')",
                "/* x; y */\nSELECT \"c;d\" FROM t",
            ]
        );
    }

    #[test]
    fn keeps_a_final_statement_without_semicolon() {
        assert_eq!(
            split_statements("SELECT 1; SELECT 2"),
            vec!["SELECT 1", "SELECT 2"]
        );
        assert!(split_statements(" ;\n-- only a comment\n").is_empty());
    }
}
