//! Admission control: a FIFO gate in front of the fold's thread pool (roadmap 5.3).
//!
//! # The bug this exists for
//!
//! Roadmap 5.2 measured thirty-two clients against one handle for **twenty seconds**. One client
//! completed 220 queries. Another completed **none** - it spent the entire window inside its first
//! query and never came back. That is not a slow queue, it is a liveness failure: a query that never
//! runs is worse than a query that is refused, because the caller has nothing to act on.
//!
//! There is no lock in the fold, which was supposed to make concurrency the easy part. The mechanism
//! turned out not to need one. Rayon's workers prefer their own local deques and only look at the
//! injector when those run dry; a query in flight keeps eight workers generating subtasks for each
//! other, so under continuous load the injector - where every other query is waiting - can go
//! unvisited indefinitely. **A bounded pool with no fairness starves the queue exactly as a mutex
//! does**, and the absence of a mutex says nothing about it.
//!
//! # What this is
//!
//! A ticket lock with a width. Every query takes a ticket on the way in and may proceed once
//! `ticket < released + capacity`, so admission is strictly first-come-first-served and at most
//! `capacity` queries are inside the pool at once. Nothing here is clever; the point is that the
//! ordering is **ours** and therefore knowable, rather than an emergent property of a work-stealing
//! scheduler that was never asked to be fair.
//!
//! Capacity is not one. Serialising queries would fix fairness and throw away the throughput: 5.2
//! measured that many narrow queries beat one wide one under load, because parallelism inside a
//! query is not free and a query that is already sharing the machine should stop paying for it.

use std::sync::{Condvar, Mutex};

#[derive(Debug, Default)]
struct State {
    /// Next ticket to hand out.
    next: u64,
    /// How many tickets have finished. A holder of ticket `t` may run once `t < released + width`.
    released: u64,
    /// High-water mark of concurrent holders, for `EXPLAIN`-style reporting.
    peak: usize,
    in_flight: usize,
}

/// A first-come-first-served admission gate of fixed width.
#[derive(Debug)]
pub struct Gate {
    state: Mutex<State>,
    cv: Condvar,
    width: usize,
}

impl Gate {
    pub fn new(width: usize) -> Self {
        Self { state: Mutex::new(State::default()), cv: Condvar::new(), width: width.max(1) }
    }

    pub fn width(&self) -> usize {
        self.width
    }

    /// Wait for a turn. The returned guard releases it on drop, **including on panic**, which is why
    /// it is a guard and not a pair of calls.
    pub fn enter(&self) -> Pass<'_> {
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let ticket = st.next;
        st.next += 1;
        while ticket >= st.released + self.width as u64 {
            st = self.cv.wait(st).unwrap_or_else(|e| e.into_inner());
        }
        st.in_flight += 1;
        st.peak = st.peak.max(st.in_flight);
        Pass { gate: self, in_flight: st.in_flight }
    }

    pub fn peak_in_flight(&self) -> usize {
        self.state.lock().map(|s| s.peak).unwrap_or(0)
    }
}

/// A turn through the gate. Releasing it wakes whoever is next in line.
pub struct Pass<'a> {
    gate: &'a Gate,
    /// How many queries were inside the pool when this one was admitted, itself included.
    ///
    /// A query that is sharing the machine should stop paying for parallelism it cannot use: 5.2
    /// measured that splitting every query across the whole pool regardless of the queue behind it
    /// costs about half the throughput of running more queries narrower. This is the number that
    /// decides how wide to be.
    in_flight: usize,
}

impl Pass<'_> {
    pub fn in_flight(&self) -> usize {
        self.in_flight
    }
}

impl Drop for Pass<'_> {
    fn drop(&mut self) {
        let mut st = self.gate.state.lock().unwrap_or_else(|e| e.into_inner());
        st.released += 1;
        st.in_flight -= 1;
        drop(st);
        // Every waiter, not one: a ticket lock's next holder is a specific thread and `notify_one`
        // may wake the wrong one, which on a busy gate is a stall nobody can reproduce.
        self.gate.cv.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// **Nobody waits forever, and that is the whole point.** With thirty-two threads contending on
    /// a width-four gate, every one of them must get through - repeatedly - rather than a lucky
    /// subset doing all the work while one thread never runs at all, which is what rayon's injector
    /// did under the same load.
    #[test]
    fn every_waiter_is_served_and_none_is_starved() {
        let gate = Arc::new(Gate::new(4));
        let counts: Vec<AtomicUsize> = (0..32).map(|_| AtomicUsize::new(0)).collect();
        let counts = Arc::new(counts);
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(400);
        std::thread::scope(|s| {
            for i in 0..32 {
                let (gate, counts) = (gate.clone(), counts.clone());
                s.spawn(move || {
                    while std::time::Instant::now() < deadline {
                        let _pass = gate.enter();
                        // Long enough that the gate, not the scheduler, decides the order.
                        std::thread::sleep(std::time::Duration::from_micros(200));
                        counts[i].fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
        });
        let got: Vec<usize> = counts.iter().map(|c| c.load(Ordering::Relaxed)).collect();
        let (lo, hi) = (*got.iter().min().unwrap(), *got.iter().max().unwrap());
        // **The property that matters, asserted hard.** Rayon's injector let a client wait twenty
        // seconds for one query; the bar here is that nobody waits at all.
        assert!(lo > 0, "a waiter was never served at all: {got:?}");
        // The spread is asserted loosely on purpose. FIFO hands turns out in order, but a thread's
        // wake-up latency decides when it re-queues, so a tight bound here measures the operating
        // system rather than the gate and fails once in a while for no reason anybody can act on.
        // Three-to-one still catches a gate that has quietly stopped being fair.
        assert!(lo * 3 >= hi, "unfair: min {lo}, max {hi}, {got:?}");
        assert!(gate.peak_in_flight() <= 4, "the width was exceeded");
    }

    #[test]
    fn the_width_is_never_exceeded() {
        let gate = Arc::new(Gate::new(3));
        let live = Arc::new(AtomicUsize::new(0));
        let worst = Arc::new(AtomicUsize::new(0));
        std::thread::scope(|s| {
            for _ in 0..16 {
                let (gate, live, worst) = (gate.clone(), live.clone(), worst.clone());
                s.spawn(move || {
                    for _ in 0..50 {
                        let _pass = gate.enter();
                        let n = live.fetch_add(1, Ordering::SeqCst) + 1;
                        worst.fetch_max(n, Ordering::SeqCst);
                        std::thread::sleep(std::time::Duration::from_micros(50));
                        live.fetch_sub(1, Ordering::SeqCst);
                    }
                });
            }
        });
        assert!(worst.load(Ordering::SeqCst) <= 3, "saw {} in flight", worst.load(Ordering::SeqCst));
    }

    /// A panicking holder must not wedge the gate. The guard releases on unwind, so the next ticket
    /// still runs; without that, one bad query would take the whole serving path down with it.
    #[test]
    fn a_panicking_holder_still_releases_its_turn() {
        let gate = Arc::new(Gate::new(1));
        let g = gate.clone();
        let _ = std::thread::spawn(move || {
            let _pass = g.enter();
            panic!("a query died holding its turn");
        })
        .join();
        // If the turn leaked, this blocks forever and the test times out rather than failing - so it
        // is done on a thread with a deadline.
        let g = gate.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _pass = g.enter();
            let _ = tx.send(());
        });
        assert!(
            rx.recv_timeout(std::time::Duration::from_secs(5)).is_ok(),
            "the gate was wedged by a panicking holder"
        );
    }
}
