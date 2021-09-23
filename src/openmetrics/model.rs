trait MarshalledMetricFamily {
    type Error;
    fn process_new_metric(&mut self, metric_name: &str, value: MetricNumber, label_names: Vec<String>, label_values: Vec<String>, timestamp: Option<Timestamp>, exemplar: Option<Exemplar>) -> Result<(), Self::Error>;
}

#[derive(Debug, Default)]
pub struct CounterValue {
    value: Option<MetricNumber>,
    created: Option<Timestamp>,
    exemplar: Option<Exemplar>
}

#[derive(Debug, Clone)]
pub struct HistogramBucket {
    count: MetricNumber,
    upper_bound: f64,
    exemplar: Option<Exemplar>
}

#[derive(Debug, Default)]
pub struct HistogramValue {
    sum: Option<MetricNumber>,
    count: Option<u64>,
    timestamp: Option<Timestamp>,
    buckets: Vec<HistogramBucket>
}

#[derive(Debug)]
pub struct State {
    name: String,
    enabled: bool
}

#[derive(Debug)]
pub struct Quantile {
    quantile: f64,
    value: MetricNumber
}

#[derive(Debug, Default)]
pub struct SummaryValue {
    sum: Option<MetricNumber>,
    count: Option<u64>,
    timestamp: Option<Timestamp>,
    quantiles: Vec<Quantile>
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
    Unknown(Option<MetricNumber>),
    Gauge(Option<MetricNumber>),
    Counter(CounterValue),
    Histogram(HistogramValue),
    StateSet(Option<MetricNumber>),
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
    fn as_f64(&self) -> f64 {
        match self {
            MetricNumber::Int(i) => *i as f64,
            MetricNumber::Float(f) => *f
        }
    }

    fn as_i64(&self) -> Option<i64> {
        match self {
            MetricNumber::Int(i) => Some(*i),
            MetricNumber::Float(f) if f.round() == *f => Some(*f as i64),
            _ => None
        }
    }
}