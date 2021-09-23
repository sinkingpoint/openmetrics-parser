use std::collections::HashMap;

pub type Timestamp = f64;

#[derive(Debug, Clone)]
pub struct Exemplar {
    labels: HashMap<String, String>,
    timestamp: Option<f64>,
    id: f64
}

impl Exemplar {
    pub fn new(labels: HashMap<String, String>, id: f64, timestamp: Option<f64>) -> Exemplar {
        return Exemplar {
            labels,
            id,
            timestamp
        }
    }
}

#[derive(Debug)]
pub struct CounterValue {
    pub value: MetricNumber,
    pub created: Option<Timestamp>,
    pub exemplar: Option<Exemplar>
}

#[derive(Debug, Clone)]
pub struct HistogramBucket {
    pub count: MetricNumber,
    pub upper_bound: f64,
    pub exemplar: Option<Exemplar>
}

#[derive(Debug, Default)]
pub struct HistogramValue {
    pub sum: Option<MetricNumber>,
    pub count: Option<u64>,
    pub timestamp: Option<Timestamp>,
    pub buckets: Vec<HistogramBucket>
}

#[derive(Debug)]
pub struct State {
    pub name: String,
    pub enabled: bool
}

#[derive(Debug)]
pub struct Quantile {
    pub quantile: f64,
    pub value: MetricNumber
}

#[derive(Debug, Default)]
pub struct SummaryValue {
    pub sum: Option<MetricNumber>,
    pub count: Option<u64>,
    pub timestamp: Option<Timestamp>,
    pub quantiles: Vec<Quantile>
}

#[derive(Debug, PartialEq, Clone)]
pub enum OpenMetricsType {
    Counter,
    Gauge,
    Histogram,
    GaugeHistogram,
    StateSet,
    Summary,
    Info,
    Unknown
}

enum MetricValue {
    Unknown(MetricNumber),
    Gauge(MetricNumber),
    Counter(CounterValue),
    Histogram(HistogramValue),
    StateSet(MetricNumber),
    GaugeHistogram(HistogramValue),
    Info,
    Summary(SummaryValue),
}

struct Metric {

}

#[derive(Debug, Clone)]
pub enum MetricNumber {
    Float(f64),
    Int(i64)
}

impl MetricNumber {
    pub fn as_f64(&self) -> f64 {
        match self {
            MetricNumber::Int(i) => *i as f64,
            MetricNumber::Float(f) => *f
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            MetricNumber::Int(i) => Some(*i),
            MetricNumber::Float(f) if f.round() == *f => Some(*f as i64),
            _ => None
        }
    }
}