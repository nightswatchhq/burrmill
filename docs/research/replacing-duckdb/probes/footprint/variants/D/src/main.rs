fn main() {
    let dir = std::env::args().nth(1).unwrap_or_else(|| consumer::default_fixture_dir());
    let rows = consumer::net_balances(std::path::Path::new(&dir));
    println!("{} parties; first = {:?}", rows.len(), rows.first());
}
