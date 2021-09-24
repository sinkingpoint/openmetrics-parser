use std::{fs, path::Path};

use super::parsers::parse_prometheus;

#[test]
fn test_prometheus_parser() {
    let child_str = fs::read_to_string(Path::new("./src/prometheus/testdata/upstream_example.txt")).unwrap();

    parse_prometheus(&child_str).unwrap();
}