//! nuthatch #1792: engines that share a pool share its bound. A second engine's statement competes
//! with what the first holds, rather than reserving a bound of its own beside it.
#![cfg(feature = "datafusion")]

use burrmill::{Budget, Engine, SharedPool};
use serde_json::{Value, json};
use std::sync::mpsc;

const BOUND: usize = 64 << 20;

fn engine(pool: Option<&SharedPool>) -> Engine {
    let budget = Budget {
        memory_bytes: BOUND,
        threads: 1,
        spill: None,
    };
    let mut e = match pool {
        Some(p) => Engine::open_empty_sharing(budget, p),
        None => Engine::open_empty_budgeted(budget),
    }
    .unwrap();
    let rows: Vec<Value> = (0..ROWS)
        .map(|i| json!({ "k": format!("{:0180}", (i * 7919) % ROWS), "n": i.to_string() }))
        .collect();
    e.register_rows("t", &rows).unwrap();
    e
}

const ROWS: u64 = 150_000;
const SORT: &str = "SELECT k, n FROM t ORDER BY k";

/// Runs `SORT` on `first`, and while its first batch is out, `SORT` on `second`. Returns what
/// `first` held at that moment and how the second statement ended.
fn overlapped(first: &Engine, second: &Engine) -> (usize, burrmill::Result<()>) {
    let (held_tx, held_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel::<()>();
    std::thread::scope(|s| {
        let a = s.spawn(move || {
            let mut sent = false;
            first
                .sql_for_each(SORT, |_| {
                    if !sent {
                        sent = true;
                        held_tx.send(first.memory_reserved()).unwrap();
                        done_rx.recv().unwrap();
                    }
                    Ok(())
                })
                .unwrap();
        });
        let held = held_rx.recv().unwrap();
        let r = second.sql_for_each(SORT, |_| Ok(()));
        done_tx.send(()).unwrap();
        a.join().unwrap();
        (held, r)
    })
}

#[test]
fn a_second_engine_on_the_pool_competes_for_what_the_first_holds() {
    let pool = SharedPool::new(BOUND);
    let (a, b) = (engine(Some(&pool)), engine(Some(&pool)));
    b.sql_for_each(SORT, |_| Ok(()))
        .expect("the sort fits the bound alone");
    assert_eq!(pool.reserved(), 0);

    let (held, second) = overlapped(&a, &b);
    assert!(
        held > BOUND / 4,
        "the first sort must hold a large share of the bound for this to mean anything: {held}"
    );
    assert!(
        second.is_err(),
        "the second sort reserved beside {held} bytes the first held, past one bound of {BOUND}"
    );
    assert_eq!(pool.reserved(), 0, "both returned what they held");
}

#[test]
fn engines_with_pools_of_their_own_do_not_compete() {
    let (a, b) = (engine(None), engine(None));
    let (held, second) = overlapped(&a, &b);
    assert!(held > BOUND / 4, "{held}");
    second.expect("separate pools, separate bounds");
}
