use std::{fs, path::PathBuf};
use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct TestMeta {
    #[serde(alias = "type")]
    exposition_format: String,
    file: String,
    #[serde(alias = "shouldParse")]
    should_parse: bool
}

fn read_child_file(parent: &PathBuf, filename: &str) -> String {
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
            assert!(parsed.is_ok(), "\n{}\n Test should parse, but didn't ({:?})", metrics_str, parsed);
        }
        else {
            assert!(parsed.is_err(), "\n{}\n Test shouldn't parse, but did ({:?})", metrics_str, parsed);
        }
    }
}