use std::collections::HashMap;

pub type Timestamp = f64;

#[derive(Debug, Clone)]
pub struct Exemplar {
    pub labels: HashMap<String, String>,
    pub timestamp: Option<f64>,
    pub id: f64,
}

#[derive(Debug)]
pub struct MetricFamily<TypeSet, ValueType> {
    pub name: String,
    pub label_names: Vec<String>,
    pub family_type: TypeSet,
    pub help: Option<String>,
    pub unit: Option<String>,
    pub metrics: Vec<Metric<ValueType>>,
}

#[derive(Debug)]
pub struct MetricsExposition<TypeSet, ValueType> {
    pub families: HashMap<String, MetricFamily<TypeSet, ValueType>>,
}

impl<TypeSet, ValueType> MetricsExposition<TypeSet, ValueType> {
    pub fn new() -> MetricsExposition<TypeSet, ValueType> {
        return MetricsExposition {
            families: HashMap::new(),
        };
    }
}

impl Exemplar {
    pub fn new(labels: HashMap<String, String>, id: f64, timestamp: Option<f64>) -> Exemplar {
        return Exemplar {
            labels,
            id,
            timestamp,
        };
    }
}

#[derive(Debug)]
pub struct CounterValue {
    pub value: MetricNumber,
    pub created: Option<Timestamp>,
    pub exemplar: Option<Exemplar>,
}

#[derive(Debug, Clone)]
pub struct HistogramBucket {
    pub count: MetricNumber,
    pub upper_bound: f64,
    pub exemplar: Option<Exemplar>,
}

#[derive(Debug, Default)]
pub struct HistogramValue {
    pub sum: Option<MetricNumber>,
    pub count: Option<u64>,
    pub timestamp: Option<Timestamp>,
    pub buckets: Vec<HistogramBucket>,
}

#[derive(Debug)]
pub struct State {
    pub name: String,
    pub enabled: bool,
}

#[derive(Debug)]
pub struct Quantile {
    pub quantile: f64,
    pub value: MetricNumber,
}

#[derive(Debug, Default)]
pub struct SummaryValue {
    pub sum: Option<MetricNumber>,
    pub count: Option<u64>,
    pub timestamp: Option<Timestamp>,
    pub quantiles: Vec<Quantile>,
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
    Unknown,
}

#[derive(Debug)]
pub enum OpenMetricsValue {
    Unknown(MetricNumber),
    Gauge(MetricNumber),
    Counter(CounterValue),
    Histogram(HistogramValue),
    StateSet(MetricNumber),
    GaugeHistogram(HistogramValue),
    Info,
    Summary(SummaryValue),
}

#[derive(Debug, PartialEq, Clone)]
pub enum PrometheusType {
    Counter,
    Gauge,
    Histogram,
    Summary,
    Unknown,
}

#[derive(Debug)]
pub enum PrometheusValue {
    Unknown(MetricNumber),
    Gauge(MetricNumber),
    Counter(CounterValue),
    Histogram(HistogramValue),
    Summary(SummaryValue),
}

#[derive(Debug)]
pub struct Metric<ValueType> {
    pub label_values: Vec<String>,
    pub timestamp: Option<Timestamp>,
    pub value: ValueType,
}

#[derive(Debug, Clone)]
pub enum MetricNumber {
    Float(f64),
    Int(i64),
}

impl MetricNumber {
    pub fn as_f64(&self) -> f64 {
        match self {
            MetricNumber::Int(i) => *i as f64,
            MetricNumber::Float(f) => *f,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            MetricNumber::Int(i) => Some(*i),
            MetricNumber::Float(f) if f.round() == *f => Some(*f as i64),
            _ => None,
        }
    }
}
