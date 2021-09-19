use pest::Parser;

#[derive(Parser)]
#[grammar = "openmetrics.pest"]
pub struct OpenMetricsParser;