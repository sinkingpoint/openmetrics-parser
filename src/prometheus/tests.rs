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

#[test]
fn escapes_are_resolved() {
    let test_str = "# HELP foo A \"quoted\" \\\\ help\\nstring\n# TYPE foo gauge\nfoo{a=\"x\\\"y\\\\z\\nw\"} 1\n";
    let exposition = parse_prometheus(test_str).unwrap();
    let family = &exposition.families["foo"];
    assert_eq!(family.help, "A \"quoted\" \\ help\nstring");

    let sample = family.iter_samples().next().unwrap();
    assert_eq!(sample.get_labelset().unwrap().get_label_value("a"), Some("x\"y\\z\nw"));

    // Rendering escapes them again
    assert_eq!(exposition.to_string(), test_str);
}

#[test]
fn quoted_utf8_names_are_parsed() {
    // What the OTel Go exporter serves with the NoTranslation strategy and `escaping=allow-utf-8`
    let exposition = parse_prometheus(
        "# HELP \"http.server.requests\" Requests.\n# TYPE \"http.server.requests\" counter\n{\"http.server.requests\",\"http.method\"=\"GET\"} 1\n",
    )
    .unwrap();
    let family = &exposition.families["http.server.requests"];
    assert_eq!(family.get_label_names(), ["http.method"]);

    let sample = family.iter_samples().next().unwrap();
    assert_eq!(sample.get_labelset().unwrap().get_label_value("http.method"), Some("GET"));
}

#[test]
fn quoted_names_are_unescaped() {
    let exposition = parse_prometheus("{\"a\\\"b\", \"c\\\\d\"=\"1\"} 1\n").unwrap();
    let family = &exposition.families["a\"b"];
    assert_eq!(family.get_label_names(), ["c\\d"]);
}

#[test]
fn empty_quoted_names_are_rejected() {
    assert!(parse_prometheus("{\"\"} 1\n").is_err());
    assert!(parse_prometheus("foo{\"\"=\"1\"} 1\n").is_err());
}

#[test]
fn quoted_names_round_trip() {
    // Rendering has to quote the names again, or the output can't be parsed back
    for test_str in [
        "# TYPE \"http.server.requests\" counter\n{\"http.server.requests\",\"http.method\"=\"GET\"} 1\n",
        "{\"a\\nb\"} 1\n",
    ] {
        let rendered = parse_prometheus(test_str).unwrap().to_string();
        assert_eq!(rendered, test_str);
    }
}
