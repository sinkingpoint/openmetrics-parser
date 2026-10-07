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

#[test]
fn counter_exemplars_are_kept() {
    use crate::PrometheusValue;

    let exposition =
        parse_prometheus("# TYPE foo_total counter\nfoo_total 1 # {trace_id=\"abc\"} 0.5\n").unwrap();
    let sample = exposition.families["foo_total"].iter_samples().next().unwrap();
    let exemplar = match &sample.value {
        PrometheusValue::Counter(c) => c.exemplar.as_ref().expect("exemplar was dropped"),
        v => panic!("expected a counter, got {:?}", v),
    };

    assert_eq!(exemplar.labels["trace_id"], "abc");
    assert_eq!(exemplar.id, 0.5);
}
