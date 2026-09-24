//! DataFusion's error text, restated in DuckDB's words (roadmap 6.4).
//!
//! nuthatch attaches its hints by matching DuckDB's phrasing (`sql_errors.rs`). Each message this
//! recognises gets DuckDB's first line, with DataFusion's own text kept below it, so a hint keys the
//! same and nothing the engine said is lost. Checked class by class against DuckDB by
//! `burrmill-bench error-parity`.

/// DuckDB's name for an Arrow type as DataFusion prints it.
fn duck_type(t: &str) -> String {
    let t = t.trim();
    let named = match t {
        "Utf8" | "Utf8View" | "LargeUtf8" => "VARCHAR",
        "Boolean" => "BOOLEAN",
        "Int8" => "TINYINT",
        "Int16" => "SMALLINT",
        "Int32" => "INTEGER",
        "Int64" => "BIGINT",
        "UInt8" => "UTINYINT",
        "UInt16" => "USMALLINT",
        "UInt32" => "UINTEGER",
        "UInt64" => "UBIGINT",
        "Float32" => "FLOAT",
        "Float64" => "DOUBLE",
        "Date32" => "DATE",
        "Binary" | "BinaryView" | "LargeBinary" => "BLOB",
        _ if t.starts_with("Timestamp(") => "TIMESTAMP",
        _ => {
            if let Some(ps) = t
                .strip_prefix("Decimal128(")
                .and_then(|r| r.strip_suffix(')'))
            {
                return format!("DECIMAL({})", ps.replace(' ', ""));
            }
            return t.to_string();
        }
    };
    named.to_string()
}

fn after<'a>(s: &'a str, marker: &str) -> Option<&'a str> {
    s.find(marker).map(|i| &s[i + marker.len()..])
}

fn unquote(s: &str) -> &str {
    s.strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .unwrap_or(s)
}

/// DuckDB's first line for a DataFusion error, if it is one nuthatch keys a hint on.
pub fn duckdb_phrase(msg: &str) -> Option<String> {
    if let Some(rest) = after(msg, "no table ") {
        let name = rest.split_whitespace().next()?;
        return Some(format!(
            "Catalog Error: Table with name {name} does not exist!"
        ));
    }
    if let Some(rest) = after(msg, "No field named ") {
        let field = rest.split(". ").next()?.trim_end_matches('.');
        return Some(match field.split_once('.') {
            Some((table, col)) if !field.starts_with('"') => format!(
                "Binder Error: Table \"{}\" does not have a column named \"{}\"",
                unquote(table),
                unquote(col)
            ),
            _ => format!(
                "Binder Error: Referenced column \"{}\" not found in FROM clause!",
                unquote(field)
            ),
        });
    }
    if msg.contains("ParserError(") {
        let found = after(msg, "found: ").map(|r| {
            r.split(" at Line")
                .next()
                .unwrap_or(r)
                .trim_end_matches(['"', ')'])
        });
        return Some(match found {
            Some("EOF") | None => "Parser Error: syntax error at end of input".into(),
            Some(tok) => format!("Parser Error: syntax error at or near \"{tok}\""),
        });
    }
    if let Some(rest) = after(msg, "Failed to coerce then (") {
        let (then, rest) = rest.split_once(") and else (")?;
        let (els, _) = rest.split_once(')')?;
        return Some(format!(
            "Binder Error: Cannot mix values of type {} and {} in CASE expression - an explicit \
             cast is required",
            duck_type(els),
            duck_type(then)
        ));
    }
    if let Some(rest) = after(
        msg,
        "No function matches the given name and argument types '",
    ) {
        let (call, _) = rest.split_once("'")?;
        let (func, args) = call.split_once('(')?;
        let args: Vec<String> = args
            .trim_end_matches(')')
            .split(',')
            .map(duck_type)
            .collect();
        if func.eq_ignore_ascii_case("coalesce")
            && args.iter().any(|a| a == "VARCHAR")
            && args.iter().any(|a| a == "BOOLEAN")
        {
            return Some(
                "Binder Error: Cannot mix values of type VARCHAR and BOOLEAN in COALESCE \
                 operator - an explicit cast is required"
                    .into(),
            );
        }
        return Some(no_function(func, &args));
    }
    if let Some(rest) = after(msg, "Function '") {
        let (func, rest) = rest.split_once('\'')?;
        if rest.starts_with(" failed to match any signature") {
            let ty = after(rest, "(DataType: ")?.split(')').next()?;
            return Some(no_function(func, &[duck_type(ty)]));
        }
    }
    if let Some(rest) = after(msg, "Resources exhausted: ") {
        return Some(format!(
            "Out of Memory Error: {}",
            rest.lines().next().unwrap_or(rest)
        ));
    }
    None
}

fn no_function(func: &str, args: &[String]) -> String {
    format!(
        "Binder Error: No function matches the given name and argument types '{func}({})'. You \
         might need to add explicit type casts.",
        args.join(", ")
    )
}

/// DuckDB's line first when there is one, then DataFusion's own text. The kept text must not
/// trip a class of its own ahead of the restated one, so its "No function matches" is lower-cased.
pub fn restate(msg: String) -> String {
    match duckdb_phrase(&msg) {
        Some(p) => format!(
            "{p}\n{}",
            msg.replace("No function matches", "no function matches")
        ),
        None => msg,
    }
}

#[cfg(test)]
mod tests {
    use super::duckdb_phrase as p;

    #[test]
    fn each_class_reads_as_duckdb_does() {
        assert_eq!(
            p("Error during planning: no table nosuch").unwrap(),
            "Catalog Error: Table with name nosuch does not exist!"
        );
        assert_eq!(
            p("Schema error: No field named valu. Did you mean 'token__transfer.value'?").unwrap(),
            "Binder Error: Referenced column \"valu\" not found in FROM clause!"
        );
        assert_eq!(
            p("Schema error: No field named t.valu. Did you mean 't.value'?").unwrap(),
            "Binder Error: Table \"t\" does not have a column named \"valu\""
        );
        assert_eq!(
            p("SQL error: ParserError(\"Expected: identifier, found: EOF\")").unwrap(),
            "Parser Error: syntax error at end of input"
        );
        assert_eq!(
            p("SQL error: ParserError(\"Expected: end of statement, found: foo at Line: 1, Column: 8\")")
                .unwrap(),
            "Parser Error: syntax error at or near \"foo\""
        );
        assert!(
            p(
                "Function 'sum' failed to match any signature, errors: Function 'sum' requires \
                   Decimal, but received String (DataType: Utf8View)."
            )
            .unwrap()
            .contains("'sum(VARCHAR)'")
        );
        assert!(
            p("No function matches the given name and argument types 'bool_and(Utf8View)'.")
                .unwrap()
                .contains("'bool_and(VARCHAR)'")
        );
        assert!(p("No function matches the given name and argument types 'coalesce(Utf8View, Boolean)'.")
            .unwrap()
            .contains("Cannot mix values of type VARCHAR and BOOLEAN"));
        assert!(p("Failed to coerce then (Utf8View) and else (Boolean) to common types in CASE WHEN expression")
            .unwrap()
            .contains("Cannot mix values of type BOOLEAN and VARCHAR in CASE"));
        assert!(
            p("Resources exhausted: Failed to allocate 10.0 MB")
                .unwrap()
                .starts_with("Out of Memory Error")
        );
        assert_eq!(p("Arrow error: Divide by zero error"), None);
        let r = super::restate(
            "No function matches the given name and argument types 'coalesce(Utf8View, Boolean)'."
                .into(),
        );
        assert_eq!(r.matches("No function matches").count(), 0, "{r}");
    }
}
