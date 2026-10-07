use serde::Deserialize;
use std::{
    fs,
    path::{Path, PathBuf},
};

#[derive(Deserialize, Debug)]
struct TestMeta {
    #[serde(alias = "shouldParse")]
    should_parse: bool,
}

fn read_child_file(parent: &Path, filename: &str) -> String {
    let mut child_path = PathBuf::new();
    child_path.push(parent);
    child_path.push(filename);

    assert!(child_path.exists());
    assert!(child_path.is_file());

    let child_str = fs::read_to_string(child_path);
    assert!(child_str.is_ok());

    child_str.unwrap()
}

#[test]
fn run_openmetrics_validation() {
    let tests = fs::read_dir("./OpenMetrics/tests/testdata/parsers");
    assert!(tests.is_ok());

    for test in tests.unwrap() {
        assert!(test.is_ok());
        let test = test.unwrap();
        let path = test.path();
        let test_name = path.file_name().unwrap();

        assert!(path.is_dir());

        let metrics_str = read_child_file(&path, "metrics");
        let test_meta_str = read_child_file(&path, "test.json");

        let meta = serde_json::from_str::<TestMeta>(&test_meta_str);
        assert!(meta.is_ok());
        let meta = meta.unwrap();

        println!("\n[TEST{:?}]", test_name);
        let parsed = crate::openmetrics::parse_openmetrics(&metrics_str);
        let metrics_str = metrics_str.replace(" ", ".").replace("\t", "->");

        if meta.should_parse {
            assert!(
                parsed.is_ok(),
                "\n{}\n Test should parse, but didn't ({:?})",
                metrics_str,
                parsed
            );
        } else {
            assert!(
                parsed.is_err(),
                "\n{}\n Test shouldn't parse, but did ({:?})",
                metrics_str,
                parsed
            );
        }
    }
}

#[test]
fn counter_exemplars_are_kept() {
    use crate::OpenMetricsValue;

    let exposition = crate::openmetrics::parse_openmetrics(
        "# TYPE foo counter\nfoo_total 1 # {trace_id=\"abc\"} 0.5 123\n# EOF\n",
    )
    .unwrap();
    let sample = exposition.families["foo"].iter_samples().next().unwrap();
    let exemplar = match &sample.value {
        OpenMetricsValue::Counter(c) => c.exemplar.as_ref().expect("exemplar was dropped"),
        v => panic!("expected a counter, got {:?}", v),
    };

    assert_eq!(exemplar.labels["trace_id"], "abc");
    assert_eq!(exemplar.id, 0.5);
    assert_eq!(exemplar.timestamp, Some(123.));
}
