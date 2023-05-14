use std::fs;

use super::parsers::parse_prometheus;

#[test]
fn test_prometheus_parser() {
    for file in fs::read_dir("./src/prometheus/testdata").unwrap() {
        let file = file.unwrap();
        let path = file.path();
        if path.extension().unwrap() == "txt" {
            let child_str = fs::read_to_string(&path).unwrap();
            let result = parse_prometheus(&child_str);
            assert!(result.is_ok(), "failed to parse {}: {}", path.display(), result.err().unwrap());
        }
    }
}
