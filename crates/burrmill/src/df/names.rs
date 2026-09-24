//! DuckDB's default names for unaliased result columns (roadmap 6.5).
//!
//! They are JSON keys in nuthatch's output, so `SELECT count(*)` must answer `count_star()`. DuckDB
//! prints the parsed expression; this prints the forms measured against it (`burrmill-bench
//! duck-names`) and answers `None` for anything else, which keeps DataFusion's name. Rules, as
//! measured on DuckDB 1.5:
//!
//! - a bare top-level column is its name, unquoted; inside an expression an identifier is quoted
//!   when it is a keyword of any category or is not `[A-Za-z0-9_]`;
//! - binary operators are parenthesised, `LIKE` prints as `~~`, `true` as `CAST('t' AS BOOLEAN)`;
//! - types in a cast are bare when SQL grammar keywords, quoted when catalogue names.

use sqlparser::ast::{
    BinaryOperator, CastKind, DataType, DuplicateTreatment, ExactNumberInfo, Expr, FunctionArg,
    FunctionArgExpr, FunctionArguments, Query, SelectItem, SetExpr, UnaryOperator, Value,
};

#[path = "generated/duckdb_keywords.rs"]
mod keywords;

fn ident(s: &str) -> String {
    let plain = !s.is_empty()
        && !s.starts_with(|c: char| c.is_ascii_digit())
        && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && keywords::KEYWORDS
            .binary_search(&s.to_ascii_lowercase().as_str())
            .is_err();
    if plain {
        s.to_string()
    } else {
        format!("\"{}\"", s.replace('"', "\"\""))
    }
}

fn ty(t: &DataType) -> Option<String> {
    Some(match t {
        DataType::Decimal(ExactNumberInfo::PrecisionAndScale(p, s)) => format!("DECIMAL({p}, {s})"),
        DataType::HugeInt => "\"HUGEINT\"".into(),
        DataType::UBigInt => "\"UBIGINT\"".into(),
        DataType::Double(_) | DataType::DoublePrecision => "\"DOUBLE\"".into(),
        DataType::Varchar(_) | DataType::Text | DataType::String(_) => "VARCHAR".into(),
        DataType::Int(_) | DataType::Integer(_) => "INTEGER".into(),
        DataType::BigInt(_) => "BIGINT".into(),
        DataType::Boolean | DataType::Bool => "BOOLEAN".into(),
        _ => return None,
    })
}

fn op(o: &BinaryOperator) -> Option<&'static str> {
    use BinaryOperator::*;
    Some(match o {
        Plus => "+",
        Minus => "-",
        Multiply => "*",
        Divide => "/",
        DuckIntegerDivide => "//",
        Modulo => "%",
        StringConcat => "||",
        Eq => "=",
        NotEq => "!=",
        Lt => "<",
        Gt => ">",
        LtEq => "<=",
        GtEq => ">=",
        And => "AND",
        Or => "OR",
        _ => return None,
    })
}

fn print(e: &Expr) -> Option<String> {
    Some(match e {
        Expr::Identifier(i) => ident(&i.value),
        Expr::CompoundIdentifier(v) => v
            .iter()
            .map(|i| ident(&i.value))
            .collect::<Vec<_>>()
            .join("."),
        Expr::Nested(x) => print(x)?,
        Expr::Value(v) => match &v.value {
            Value::Number(n, _) if !n.contains(['e', 'E']) => n.clone(),
            Value::SingleQuotedString(s) => format!("'{}'", s.replace('\'', "''")),
            Value::Boolean(b) => format!("CAST('{}' AS BOOLEAN)", if *b { "t" } else { "f" }),
            Value::Null => "NULL".into(),
            _ => return None,
        },
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match expr.as_ref() {
            Expr::Value(v) if matches!(v.value, Value::Number(..)) => format!("-{}", print(expr)?),
            _ => format!("-({})", print(expr)?),
        },
        // DuckDB folds a negated comparison before naming it: `NOT (a > b)` is `(a <= b)`.
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => {
            let mut inner = expr.as_ref();
            while let Expr::Nested(x) = inner {
                inner = x;
            }
            use BinaryOperator::*;
            match inner {
                Expr::BinaryOp {
                    left,
                    op: o @ (Gt | Lt | GtEq | LtEq | Eq | NotEq),
                    right,
                } => {
                    let flipped = match o {
                        Gt => LtEq,
                        Lt => GtEq,
                        GtEq => Lt,
                        LtEq => Gt,
                        Eq => NotEq,
                        _ => Eq,
                    };
                    format!("({} {} {})", print(left)?, op(&flipped)?, print(right)?)
                }
                _ => format!("(NOT {})", print(expr)?),
            }
        }
        Expr::BinaryOp { left, op: o, right } => {
            format!("({} {} {})", print(left)?, op(o)?, print(right)?)
        }
        Expr::IsNull(x) => format!("({} IS NULL)", print(x)?),
        Expr::IsNotNull(x) => format!("({} IS NOT NULL)", print(x)?),
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            let l: Option<Vec<String>> = list.iter().map(print).collect();
            format!("({} IN ({}))", print(expr)?, l?.join(", "))
        }
        Expr::Like {
            negated: false,
            any: false,
            expr,
            pattern,
            escape_char: None,
        } => {
            format!("({} ~~ {})", print(expr)?, print(pattern)?)
        }
        Expr::Cast {
            kind,
            expr,
            data_type,
            format: None,
            ..
        } => {
            let f = match kind {
                CastKind::Cast | CastKind::DoubleColon => "CAST",
                CastKind::TryCast | CastKind::SafeCast => "TRY_CAST",
            };
            format!("{f}({} AS {})", print(expr)?, ty(data_type)?)
        }
        Expr::Function(f) => {
            if f.over.is_some() || !f.within_group.is_empty() {
                return None;
            }
            let name = f.name.0.last()?.as_ident()?.value.to_ascii_lowercase();
            let FunctionArguments::List(list) = &f.args else {
                return None;
            };
            if !list.clauses.is_empty() {
                return None;
            }
            let star = matches!(
                list.args.as_slice(),
                [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)]
            );
            let mut out = if name == "count" && star && list.duplicate_treatment.is_none() {
                "count_star()".to_string()
            } else {
                let args: Option<Vec<String>> = list
                    .args
                    .iter()
                    .map(|a| match a {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(x)) => print(x),
                        _ => None,
                    })
                    .collect();
                let distinct = match list.duplicate_treatment {
                    Some(DuplicateTreatment::Distinct) => "DISTINCT ",
                    _ => "",
                };
                let name = if name == "coalesce" {
                    "COALESCE".to_string()
                } else {
                    name
                };
                format!("{name}({distinct}{})", args?.join(", "))
            };
            if let Some(filter) = &f.filter {
                out.push_str(&format!(" FILTER (WHERE {})", print(filter)?));
            }
            out
        }
        _ => return None,
    })
}

/// The name DuckDB gives each result column of `q`, where this knows it; `None` for an aliased
/// column or a form not covered. Empty when the columns cannot be matched by position.
pub fn default_names(q: &Query) -> Vec<Option<String>> {
    let mut body = q.body.as_ref();
    loop {
        match body {
            SetExpr::Select(s) => {
                let mut out = Vec::with_capacity(s.projection.len());
                for item in &s.projection {
                    match item {
                        SelectItem::UnnamedExpr(e) => out.push(match e {
                            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => None,
                            e => print(e),
                        }),
                        SelectItem::ExprWithAlias { .. } => out.push(None),
                        _ => return vec![],
                    }
                }
                return out;
            }
            SetExpr::SetOperation { left, .. } => body = left,
            SetExpr::Query(inner) => body = inner.body.as_ref(),
            _ => return vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::DuckDbDialect;
    use sqlparser::parser::Parser;

    fn names(sql: &str) -> Vec<Option<String>> {
        let stmt = Parser::parse_sql(&DuckDbDialect {}, sql).unwrap().remove(0);
        let sqlparser::ast::Statement::Query(q) = stmt else {
            panic!()
        };
        default_names(&q)
    }

    // Each expected string is what DuckDB 1.5 printed for the same SQL (`duck-names`).
    #[test]
    fn matches_duckdb() {
        let got = names(
            r#"SELECT count(*), count("from"), sum(block_number), sum(CAST("value" AS HUGEINT)),
                      min(t."from"), max("tokensRewards"), count(DISTINCT "from"),
                      sum(block_number) FILTER (WHERE log_index > 0), block_number + 1,
                      "value"::HUGEINT, lower("from"), "from" || 'x', 7 / 2, COALESCE("from", 'z'),
                      -block_number, block_number // 2, ("from" = 'a'), NOT true,
                      block_number IS NULL, "from" LIKE 'a%', 'it''s', -1, 2.50,
                      CAST(x.plain AS DECIMAL(38,0)), CAST(x.plain AS DOUBLE), abs(x."has space"),
                      abs(x.Upper), TRY_CAST("value" AS DECIMAL(38,0)), block_number, t."to", 1 AS one
               FROM t"#,
        );
        let want = [
            "count_star()",
            "count(\"from\")",
            "sum(block_number)",
            "sum(CAST(\"value\" AS \"HUGEINT\"))",
            "min(t.\"from\")",
            "max(tokensRewards)",
            "count(DISTINCT \"from\")",
            "sum(block_number) FILTER (WHERE (log_index > 0))",
            "(block_number + 1)",
            "CAST(\"value\" AS \"HUGEINT\")",
            "lower(\"from\")",
            "(\"from\" || 'x')",
            "(7 / 2)",
            "COALESCE(\"from\", 'z')",
            "-(block_number)",
            "(block_number // 2)",
            "(\"from\" = 'a')",
            "(NOT CAST('t' AS BOOLEAN))",
            "(block_number IS NULL)",
            "(\"from\" ~~ 'a%')",
            "'it''s'",
            "-1",
            "2.50",
            "CAST(x.plain AS DECIMAL(38, 0))",
            "CAST(x.plain AS \"DOUBLE\")",
            "abs(x.\"has space\")",
            "abs(x.Upper)",
            "TRY_CAST(\"value\" AS DECIMAL(38, 0))",
        ];
        let mut want: Vec<Option<String>> = want.iter().map(|s| Some(s.to_string())).collect();
        want.extend([None, None, None]);
        assert_eq!(got, want);
    }
}
