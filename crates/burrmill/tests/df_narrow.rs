//! NarrowJoins: a page over left joins derives only the rows it keeps, and answers as before.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;

fn write(segs: &Path, table: &str, cols: Vec<(&str, ArrayRef)>) {
    let schema = Arc::new(Schema::new(
        cols.iter()
            .map(|(n, a)| Field::new(*n, a.data_type().clone(), true))
            .collect::<Vec<_>>(),
    ));
    let batch =
        RecordBatch::try_new(schema.clone(), cols.into_iter().map(|(_, a)| a).collect()).unwrap();
    let f = std::fs::File::create(segs.join(format!("{table}-{:064x}.parquet", 1))).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

fn text(v: Vec<String>) -> ArrayRef {
    Arc::new(StringArray::from(v))
}

fn num(v: Vec<u64>) -> ArrayRef {
    Arc::new(UInt64Array::from(v))
}

const ENTITIES: u64 = 1_000;
const EVENTS: u64 = 5_000;

/// `ent` holds one row per id; `ev` several events per id, more for low ids, none for some; `one`
/// exactly one event per id. The entity view is a chain of left joins from `ent`, one of them keyed
/// by a column an earlier joined side computes.
fn engine() -> (tempfile::TempDir, Engine) {
    let tmp = tempfile::tempdir().unwrap();
    let segs = tmp.path().join("segments");
    std::fs::create_dir(&segs).unwrap();
    let id = |i: u64| format!("k{i:04}");
    write(
        &segs,
        "ent",
        vec![
            ("id", text((0..ENTITIES).map(id).collect())),
            ("v", num((0..ENTITIES).map(|i| i % 7).collect())),
            // Larger on disk than every other table together, as a nest's entity source is.
            (
                "pad",
                text((0..ENTITIES).map(|i| format!("{i:0>2000}")).collect()),
            ),
        ],
    );
    // Ids 0..900 get five or six events each; 900.. none.
    let ev_id: Vec<u64> = (0..EVENTS).map(|i| (i * 7) % 900).collect();
    write(
        &segs,
        "ev",
        vec![
            ("id", text(ev_id.iter().map(|i| id(*i)).collect())),
            (
                "amount",
                num((0..EVENTS).map(|i| (i * 31) % 1000).collect()),
            ),
            ("pos", num((0..EVENTS).map(|i| i * 10 + 1).collect())),
        ],
    );
    write(
        &segs,
        "one",
        vec![
            ("id", text((0..ENTITIES).rev().map(id).collect())),
            (
                "pos",
                num((0..ENTITIES).map(|i| (i * 7919) % 100_000).collect()),
            ),
        ],
    );
    let mut e = Engine::open_segments(&segs).unwrap();
    for (name, body) in [
        (
            "lastev",
            "SELECT id, max(pos) AS last_pos, count(*) AS n, sum(amount) AS total FROM ev GROUP BY id",
        ),
        ("evrow", "SELECT pos, amount, id FROM ev"),
        (
            "ranked",
            "SELECT id, pos, row_number() OVER (ORDER BY pos) AS gr, \
             row_number() OVER (PARTITION BY id ORDER BY pos) AS pr FROM one",
        ),
        (
            "tagged",
            "SELECT id || '-x' AS k, count(*) AS c FROM ev GROUP BY id",
        ),
        (
            "nullish",
            "SELECT CASE WHEN id < 'k0003' THEN NULL ELSE id END AS id, count(*) AS c \
             FROM ev GROUP BY 1",
        ),
        (
            "entity",
            "SELECT e.id, e.v, l.n, l.total, l.last_pos, r.amount AS last_amount, t.c, \
                    k.gr, k.pr \
             FROM ent e \
             LEFT JOIN lastev l ON l.id = e.id \
             LEFT JOIN evrow r ON r.pos = l.last_pos \
             LEFT JOIN tagged t ON t.k = e.id || '-x' \
             LEFT JOIN ranked k ON k.id = e.id",
        ),
    ] {
        e.register_view(name, body).unwrap();
    }
    (tmp, e)
}

fn rows(e: &Engine, sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    for b in e.sql(sql).unwrap_or_else(|err| panic!("{sql}\n{err}")) {
        let cols: Vec<ArrayRef> = b
            .columns()
            .iter()
            .map(|c| arrow::compute::cast(c, &DataType::Utf8).unwrap())
            .collect();
        for i in 0..b.num_rows() {
            let row: Vec<String> = cols
                .iter()
                .map(|c| {
                    let s = c.as_any().downcast_ref::<StringArray>().unwrap();
                    if s.is_null(i) {
                        "NULL".into()
                    } else {
                        s.value(i).into()
                    }
                })
                .collect();
            out.push(row.join("|"));
        }
    }
    out
}

/// The page as the whole ordered answer has it: the oracle no limit can narrow.
fn page(
    e: &Engine,
    select: &str,
    order: &str,
    skip: usize,
    take: usize,
) -> (Vec<String>, Vec<String>) {
    let all = rows(e, &format!("{select} ORDER BY {order}"));
    let want = all.into_iter().skip(skip).take(take).collect();
    let got = rows(
        e,
        &format!("{select} ORDER BY {order} LIMIT {take} OFFSET {skip}"),
    );
    (got, want)
}

#[test]
fn a_page_over_left_joins_answers_as_the_whole_answer_does() {
    let (_t, e) = engine();
    let select = "SELECT * FROM entity";
    for (order, skip, take) in [
        ("id", 0, 5),
        ("id", 3, 20),
        ("id DESC", 0, 7),
        ("id DESC", 95, 10),
        ("v, id", 0, 9),
        ("v DESC, id DESC", 40, 13),
        ("id", 990, 20),
        ("id", 0, 1),
    ] {
        let (got, want) = page(&e, select, order, skip, take);
        assert_eq!(got, want, "ORDER BY {order} LIMIT {take} OFFSET {skip}");
    }
    // Through the projections a caller puts on top, with expressions the order reads.
    let select = "SELECT upper(id) AS u, n, last_amount, gr FROM entity";
    let (got, want) = page(&e, select, "u DESC", 2, 6);
    assert_eq!(got, want);
}

/// A side matched more than once per kept row: the page is whole groups, compared as sets.
#[test]
fn a_page_whose_rows_repeat_through_a_join_keeps_every_repeat() {
    let (_t, e) = engine();
    for select in [
        "SELECT e.id, x.pos, x.amount FROM ent e LEFT JOIN ev x ON x.id = e.id",
        // Keyed on an aggregate's result, which no key filter can pass below.
        "SELECT e.id, l.id, l.n FROM ent e LEFT JOIN lastev l ON l.n = e.v + 5",
        "SELECT e.id, l.id FROM ent e LEFT JOIN lastev l ON l.n = e.v + 5 AND l.id < 'k0100'",
    ] {
        let all = rows(&e, &format!("{select} ORDER BY e.id"));
        let ids: Vec<&str> = all.iter().map(|r| r.split('|').next().unwrap()).collect();
        let second = ids.iter().position(|i| *i != ids[0]).unwrap();
        let third = second
            + ids[second..]
                .iter()
                .position(|i| *i != ids[second])
                .unwrap();
        let mut want: Vec<String> = all[..third].to_vec();
        assert!(want.len() > 2, "the first ids repeat: {select}");
        let mut got = rows(&e, &format!("{select} ORDER BY e.id LIMIT {}", want.len()));
        got.sort();
        want.sort();
        assert_eq!(got, want, "{select}");
    }
}

/// An inner join drops rows, so the kept rows cannot be chosen before it.
#[test]
fn a_page_over_an_inner_join_is_not_chosen_before_it() {
    let (_t, e) = engine();
    let select =
        "SELECT e.id, l.n FROM ent e JOIN lastev l ON l.id = e.id LEFT JOIN one o ON o.id = e.id";
    let (got, want) = page(&e, select, "e.id DESC", 0, 5);
    assert_eq!(got.len(), 5);
    assert_eq!(got, want);
}

/// A `LIMIT` with no order, after a filter that already chose the row, as `bet(id)` asks.
#[test]
fn a_limit_after_a_filter_answers_the_filtered_row() {
    let (_t, e) = engine();
    for id in ["k0003", "k0950", "k0899"] {
        let all = rows(&e, &format!("SELECT * FROM entity WHERE id = '{id}'"));
        assert_eq!(all.len(), 1);
        let got = rows(
            &e,
            &format!("SELECT * FROM entity WHERE id = '{id}' LIMIT 1"),
        );
        assert_eq!(got, all, "{id}");
    }
}

/// A join that matches NULL to NULL keeps its NULL matches: a semi join on the keys would not.
#[test]
fn a_join_matching_null_to_null_keeps_its_null_matches() {
    let (_t, e) = engine();
    let select = "SELECT e.id, n.c FROM ent e LEFT JOIN nullish n \
                  ON n.id IS NOT DISTINCT FROM CASE WHEN e.v = 0 THEN NULL ELSE e.id END";
    let (got, want) = page(&e, select, "e.id", 0, 8);
    assert_eq!(got, want);
    assert!(
        !got[0].ends_with("|NULL"),
        "k0000 matches the NULL group: {got:?}"
    );
}

/// A row number over every row is not one over the kept rows' partitions: the key filter stays
/// above that window, and goes below one partitioned by the key.
#[test]
fn a_window_over_every_row_numbers_every_row() {
    let (_t, e) = engine();
    let (got, want) = page(&e, "SELECT id, gr, pr FROM entity", "id", 10, 5);
    assert_eq!(got, want);
    assert!(got.iter().all(|r| !r.ends_with("|NULL|NULL")), "{got:?}");
}

/// The largest row count an operator of `kind` reported. `EXPLAIN ANALYZE` reports rows only for an
/// operator that ran.
fn most_rows(e: &Engine, sql: &str, kind: &str) -> u64 {
    let plan = rows(e, &format!("EXPLAIN ANALYZE {sql}")).join("\n");
    plan.lines()
        .filter(|l| l.trim_start().starts_with(kind))
        .filter_map(|l| {
            let at = l.find("output_rows=")? + "output_rows=".len();
            let n: String = l[at..]
                .chars()
                .take_while(|c| c.is_ascii_digit() || *c == '.')
                .collect();
            let unit = l[at + n.len()..].chars().nth(1);
            let n: f64 = n.parse().ok()?;
            Some(match unit {
                Some('K') => n * 1e3,
                Some('M') => n * 1e6,
                _ => n,
            } as u64)
        })
        .max()
        .unwrap_or(0)
}

/// The keys a semi join filters by are what it builds its table on. By bytes read, the keys of
/// the kept rows looked as large as everything read to choose them, so the joined side was built
/// instead: on BetSwirl 160,802 rows and 100 MB to find twenty.
#[test]
fn a_semi_join_builds_on_the_kept_keys() {
    let (_t, e) = engine();
    let plan = rows(
        &e,
        "EXPLAIN ANALYZE SELECT * FROM entity ORDER BY id LIMIT 10",
    )
    .join("\n");
    let semis: Vec<&str> = plan
        .lines()
        .filter(|l| l.contains("HashJoinExec") && l.contains("Semi"))
        .collect();
    assert!(!semis.is_empty(), "{plan}");
    for l in semis {
        assert!(built_on(l) <= 10, "{l}");
    }
}

fn built_on(l: &str) -> u64 {
    let at = l.find("build_input_rows=").expect(l) + "build_input_rows=".len();
    l[at..]
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect::<String>()
        .parse()
        .unwrap()
}

/// With a `LIMIT` and no order, the optimizer copies the limit into each left side of the chain.
/// Each of those copies is a few rows too: by bytes read the joins over them built on the joined
/// side instead, and `bet(id)` on BetSwirl ran out of its 512 MB building five of them.
#[test]
fn every_join_over_the_kept_rows_builds_on_them() {
    let (_t, e) = engine();
    let plan = rows(
        &e,
        "EXPLAIN ANALYZE SELECT * FROM entity WHERE id = 'k0003' LIMIT 1",
    )
    .join("\n");
    let joins: Vec<&str> = plan
        .lines()
        .filter(|l| l.contains("HashJoinExec"))
        .collect();
    assert!(joins.len() >= 4, "{plan}");
    for l in joins {
        assert!(built_on(l) <= 1, "{l}");
    }
}

/// The definitions a statement's plan holds, as `EXPLAIN` lists them.
fn definitions(e: &Engine, sql: &str) -> usize {
    let plan = rows(e, &format!("EXPLAIN {sql}")).join("\n");
    plan.lines()
        .find_map(|l| {
            let at = l.find("SharedDefs: ids=[")? + "SharedDefs: ids=[".len();
            Some(l[at..].split(']').next()?.split(',').count())
        })
        .unwrap_or(0)
}

/// The kept rows are held once; a joined side is held only when a later join takes its keys from
/// it, and otherwise streams into its join as before.
#[test]
fn a_joined_side_is_held_only_when_a_later_join_reads_its_keys() {
    let (_t, e) = engine();
    let one = "SELECT e.id, x.pos FROM ent e LEFT JOIN ev x ON x.id = e.id ORDER BY e.id LIMIT 10";
    assert_eq!(definitions(&e, one), 1);
    let read = "SELECT e.id, r.amount FROM ent e LEFT JOIN lastev l ON l.id = e.id \
                LEFT JOIN evrow r ON r.pos = l.last_pos ORDER BY e.id LIMIT 10";
    assert_eq!(definitions(&e, read), 2);
    let (got, want) = page(
        &e,
        "SELECT e.id, r.amount FROM ent e LEFT JOIN lastev l ON l.id = e.id \
         LEFT JOIN evrow r ON r.pos = l.last_pos",
        "e.id",
        0,
        10,
    );
    assert_eq!(got, want);
}

/// A side holding a subquery is left whole. A `NOT IN` beside an `OR` plans as a mark join with
/// scalar subqueries over it: with the keys filter on that, this statement never finished, and on
/// Lodestar's indexers page DataFusion's sort pushdown panicked in the release gate.
#[test]
fn a_side_holding_a_subquery_is_not_narrowed() {
    let (_t, mut e) = engine();
    e.register_view(
        "marked",
        "SELECT id, pos FROM ev WHERE pos < 20000 OR id NOT IN (SELECT id FROM one WHERE pos < 50000)",
    )
    .unwrap();
    let chain = "SELECT e.id, m.pos FROM ent e LEFT JOIN marked m ON m.id = e.id";
    let all = rows(&e, &format!("SELECT * FROM ({chain}) x ORDER BY id DESC"));
    let got = rows(
        &e,
        &format!("SELECT * FROM ({chain}) x ORDER BY id DESC LIMIT 3"),
    );
    assert_eq!(got, all[..3].to_vec());
    // Nor are the rows kept from one held once.
    let kept = "SELECT m.id, m.pos, l.n FROM marked m LEFT JOIN lastev l ON l.id = m.id";
    let all = rows(
        &e,
        &format!("SELECT * FROM ({kept}) x ORDER BY id DESC, pos"),
    );
    let got = rows(
        &e,
        &format!("SELECT * FROM ({kept}) x ORDER BY id DESC, pos LIMIT 3"),
    );
    assert_eq!(got, all[..3].to_vec());
    assert_eq!(
        definitions(
            &e,
            &format!("SELECT * FROM ({kept}) x ORDER BY id DESC, pos LIMIT 3")
        ),
        0
    );
}

/// Nothing is narrowed without a limit.
#[test]
fn a_statement_without_a_limit_is_not_narrowed() {
    let (_t, e) = engine();
    let plan = rows(&e, "EXPLAIN SELECT * FROM entity ORDER BY id").join("\n");
    assert!(!plan.contains("__burrmill_keys"), "{plan}");
    assert_eq!(definitions(&e, "SELECT * FROM entity ORDER BY id"), 0);
}

/// A recursive term runs once a round over that round's rows, so a limit in it keeps its rows
/// there. Taken out to be computed once, its work table was read before anything filled it.
#[test]
fn a_limit_in_a_recursive_term_stays_in_it() {
    let (_t, e) = engine();
    let sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL \
               SELECT n + 1 FROM (SELECT r.n FROM r LEFT JOIN one o ON o.pos = r.n \
               ORDER BY r.n LIMIT 5) s WHERE n < 6) SELECT n FROM r ORDER BY n";
    assert_eq!(rows(&e, sql), vec!["1", "2", "3", "4", "5", "6"]);
}

/// nuthatch#1951: the first page of an entity derived every entity before the limit applied. The
/// aggregates on the joined sides now see only the ten kept ids.
#[test]
fn a_page_derives_only_the_rows_it_keeps() {
    let (_t, e) = engine();
    let sql = "SELECT * FROM entity ORDER BY id LIMIT 10";
    let grouped = most_rows(&e, sql, "AggregateExec");
    assert!(
        grouped <= 10,
        "an aggregate grouped {grouped} rows for a page of 10"
    );
    let whole = most_rows(&e, "SELECT * FROM entity ORDER BY id", "AggregateExec");
    assert!(whole >= 900, "without a limit every id is grouped, {whole}");
}
