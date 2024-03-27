use std::fs;

use crate::ParseError;
use super::parsers::parse_openmetrics;

/// Test the parser on cases that parse successfully.
#[test]
fn test_openmetrics_parser() {
    for file in fs::read_dir("./src/openmetrics/testdata").unwrap() {
        let file = file.unwrap();
        let path = file.path();
        if path.extension().unwrap() == "txt" {
            let child_str = fs::read_to_string(&path).unwrap();
            let result = parse_openmetrics(&child_str);
            assert!(result.is_ok(), "failed to parse {}: {}", path.display(), result.err().unwrap());
        }
    }
}

#[test]
fn test_openmetrics_parser_enforce_no_leading_digit_metric_name() {
    let result = parse_openmetrics(r#"
# HELP 1_leading_integer_not_allowed A summary of the RPC duration in seconds.
# TYPE 1_leading_integer_not_allowed summary
1_leading_integer_not_allowed{quantile="0.01"} 3102
    "#);
    dbg!(&result);
    match result {
        Err(ParseError::ParseError(x)) => {
            assert!(x.contains("expected metricfamily"));
        }
        _ => {
            panic!("Expected ParseError::ParseError");
        }
    };
}
