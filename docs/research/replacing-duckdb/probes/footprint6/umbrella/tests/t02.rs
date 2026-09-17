#[test]
fn net_balances_t02() {
    let rows = consumer::net_balances(std::path::Path::new(&consumer::default_fixture_dir()));
    assert!(rows.len() > 1, "expected more than 1 parties, got {}", rows.len());
    assert!(rows.windows(2).all(|w| w[0].0 <= w[1].0), "not ordered");
}
