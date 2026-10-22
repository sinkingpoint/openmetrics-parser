//! Parses the test data from prometheus/prometheus (see tests/upstream/UPSTREAM.md).

use std::fs;

use openmetrics_parser::{openmetrics::parse_openmetrics, prometheus::parse_prometheus};

/// Files we can't parse yet, and why. Remove a file from here once it parses.
const KNOWN_FAILURES: &[&str] = &[
    // No TYPE lines, so the families are grouped by sample name
    "alltypes.237mfs.nometa.prom.txt",
    // Histograms cannot have a sum with a negative bucket
    "alltypes.5mfs.om.txt",
    // Counter `jobs_completed` without the `_total` suffix
    "alltypes.bench.om.txt",
    // OpenMetrics 2.0: composite values and start timestamps (`st@`)
    "alltypes.bench.om2.txt",
    "alltypes.om2.txt",
    "ct.bench.om2.txt",
    "histograms.bench.om2.txt",
    "prometheus.om2.txt",
    // Some metrics in a family have a timestamp and some don't
    "ct.bench.om.txt",
    // Histograms with a unit
    "histograms.bench.om.txt",
    // Summary without quantiles
    "prometheus.om1.txt",
];

#[test]
fn upstream_testdata() {
    let mut files: Vec<_> = fs::read_dir("tests/upstream/prometheus")
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    files.sort();
    assert!(!files.is_empty(), "no upstream test data found");

    let mut unexpected = Vec::new();
    for path in files {
        let name = path.file_name().unwrap().to_str().unwrap();
        let input = fs::read_to_string(&path).unwrap();
        let error = if name.ends_with(".prom.txt") {
            parse_prometheus(&input).err().map(|e| e.to_string())
        } else {
            parse_openmetrics(&input).err().map(|e| e.to_string())
        };

        match (KNOWN_FAILURES.contains(&name), error) {
            (false, Some(e)) => unexpected.push(format!("{} failed to parse: {}", name, e)),
            (true, None) => unexpected.push(format!("{} parses now, remove it from KNOWN_FAILURES", name)),
            _ => {}
        }
    }

    assert!(unexpected.is_empty(), "\n{}", unexpected.join("\n"));
}
