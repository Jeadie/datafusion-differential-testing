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
    LeadingComments::new(statement).remainder().is_empty()
}

/// A comment before the first SQL token of a statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Comment<'a> {
    /// The text after `--`, up to the end of the line.
    Line(&'a str),
    /// The text between `/*` and `*/`.
    Block(&'a str),
}

/// The comments at the start of a statement, in order.
#[derive(Debug, Clone)]
pub struct LeadingComments<'a> {
    rest: &'a str,
}

impl<'a> LeadingComments<'a> {
    pub fn new(statement: &'a str) -> Self {
        Self {
            rest: statement.trim_start(),
        }
    }

    /// The statement after its leading comments and whitespace.
    pub fn remainder(mut self) -> &'a str {
        while self.next().is_some() {}
        self.rest
    }
}

impl<'a> Iterator for LeadingComments<'a> {
    type Item = Comment<'a>;

    fn next(&mut self) -> Option<Comment<'a>> {
        let (comment, rest) = if let Some(after) = self.rest.strip_prefix("--") {
            let (text, rest) = after.split_once('\n').unwrap_or((after, ""));
            (Comment::Line(text), rest)
        } else {
            let after = self.rest.strip_prefix("/*")?;
            let (text, rest) = after.split_once("*/").unwrap_or((after, ""));
            (Comment::Block(text), rest)
        };
        self.rest = rest.trim_start();
        Some(comment)
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

    #[test]
    fn reads_leading_comments_and_the_rest() {
        let statement = "  -- one\r\n/* two\n */ --three\nSELECT 1 -- not leading";
        let comments: Vec<_> = LeadingComments::new(statement).collect();
        assert_eq!(
            comments,
            [
                Comment::Line(" one\r"),
                Comment::Block(" two\n "),
                Comment::Line("three"),
            ]
        );
        assert_eq!(
            LeadingComments::new(statement).remainder(),
            "SELECT 1 -- not leading"
        );
        assert_eq!(LeadingComments::new("-- only").remainder(), "");
    }
}
