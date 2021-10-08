use std::{collections::HashMap, fmt, ops};



pub type Timestamp = f64;

/// An OpenMetrics Exemplar (that is also valid in Prometheus)
/// https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md#exemplars
/// Exemplars are references to data outside of the MetricSet. A common use case are IDs of program traces.
/// Exemplars MUST consist of a LabelSet and a value, and MAY have a timestamp. They MAY each be different from the MetricPoints' LabelSet and timestamp.
/// The combined length of the label names and values of an Exemplar's LabelSet MUST NOT exceed 128 UTF-8 characters.
/// Other characters in the text rendering of an exemplar such as ",= are not included in this limit for implementation
/// simplicity and for consistency between the text and proto formats.
#[derive(Debug, Clone)]
pub struct Exemplar {
    pub labels: HashMap<String, String>,
    pub timestamp: Option<f64>,
    pub id: f64,
}

impl Exemplar {
    pub fn new(labels: HashMap<String, String>, id: f64, timestamp: Option<f64>) -> Exemplar {
        Exemplar {
            labels,
            id,
            timestamp,
        }
    }
}

/// A MetricFamily is a collection of metrics with the same type, name, and label names
/// https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md#metricfamily
/// A MetricFamily MAY have zero or more Metrics. A MetricFamily MUST have a name, HELP, TYPE, and UNIT metadata.
/// Every Metric within a MetricFamily MUST have a unique LabelSet.
#[derive(Debug, Default)]
pub struct MetricFamily<TypeSet, ValueType> {
    pub name: String,
    pub label_names: Vec<String>,
    pub family_type: TypeSet,
    pub help: String,
    pub unit: String,
    pub metrics: Vec<Sample<ValueType>>,
}

/// Exposition is the top level object of the parser. It's a collection of metric families, indexed by name
#[derive(Debug)]
pub struct MetricsExposition<TypeSet, ValueType> {
    pub families: HashMap<String, MetricFamily<TypeSet, ValueType>>,
}

impl<TypeSet, ValueType> Default for MetricsExposition<TypeSet, ValueType> {
    fn default() -> Self {
        Self::new()
    }
}

impl<TypeSet, ValueType> MetricsExposition<TypeSet, ValueType> {
    pub fn new() -> MetricsExposition<TypeSet, ValueType> {
        MetricsExposition {
            families: HashMap::new(),
        }
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

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum OpenMetricsType {
    /// A Counter that only goes up
    /// Counters measure discrete events. Common examples are the number of HTTP requests received,
    /// CPU seconds spent, or bytes sent. For counters how quickly they are increasing over time is what is of interest to a user.
    /// A MetricPoint in a Metric with the type Counter MUST have one value called Total. A Total is a non-NaN and MUST be
    /// monotonically non-decreasing over time, starting from 0.
    /// A MetricPoint in a Metric with the type Counter SHOULD have a Timestamp value called Created. This can help ingestors discern between new metrics and long-running ones it did not see before.
    /// A MetricPoint in a Metric's Counter's Total MAY reset to 0. If present, the corresponding Created time MUST also be set to the timestamp of the reset.
    /// A MetricPoint in a Metric's Counter's Total MAY have an exemplar.
    Counter,

    /// A Gauge that can go up or down
    /// https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md#gauge
    /// Gauges are current measurements, such as bytes of memory currently used or the number of items in a queue.
    /// For gauges the absolute value is what is of interest to a user.
    /// A MetricPoint in a Metric with the type gauge MUST have a single value.
    /// Gauges MAY increase, decrease, or stay constant over time. Even if they only ever go in one direction,
    /// they might still be gauges and not counters. The size of a log file would usually only increase,
    /// a resource might decrease, and the limit of a queue size may be constant.
    /// A gauge MAY be used to encode an enum where the enum has many states and changes over time, it is the most efficient but least user friendly.
    Gauge,

    /// A Histogram that has a number of buckets that count events, and a _sum and _count
    /// https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md#histogram
    /// Histograms measure distributions of discrete events. Common examples are the latency of HTTP requests, function runtimes, or I/O request sizes.
    /// A Histogram MetricPoint MUST contain at least one bucket, and SHOULD contain Sum, and Created values. Every bucket MUST have a threshold and a value.
    /// Histogram MetricPoints MUST have at least a bucket with an +Inf threshold. Buckets MUST be cumulative. As an example for a metric representing
    /// request latency in seconds its values for buckets with thresholds 1, 2, 3, and +Inf MUST follow value_1 <= value_2 <= value_3 <= value_+Inf.
    /// If ten requests took 1 second each, the values of the 1, 2, 3, and +Inf buckets MUST equal 10.
    /// The +Inf bucket counts all requests. If present, the Sum value MUST equal the Sum of all the measured event values.
    /// Bucket thresholds within a MetricPoint MUST be unique.
    /// Semantically, Sum, and buckets values are counters so MUST NOT be NaN or negative.
    /// Negative threshold buckets MAY be used, but then the Histogram MetricPoint MUST NOT contain a sum value as it would
    /// no longer be a counter semantically. Bucket thresholds MUST NOT equal NaN. Count and bucket values MUST be integers.
    /// A Histogram MetricPoint SHOULD have a Timestamp value called Created. This can help ingestors discern between new
    /// metrics and long-running ones it did not see before.
    /// A Histogram's Metric's LabelSet MUST NOT have a "le" label name.
    /// Bucket values MAY have exemplars. Buckets are cumulative to allow monitoring systems to drop any non-+Inf bucket for performance/anti-denial-of-service reasons in a way that loses granularity but is still a valid Histogram.
    Histogram,

    /// GaugeHistograms measure current distributions. Common examples are how long items have been waiting in a queue, or size of the requests in a queue.
    /// A GaugeHistogram MetricPoint MUST have at least one bucket with an +Inf threshold, and SHOULD contain a Gsum value.
    /// Every bucket MUST have a threshold and a value.
    /// The buckets for a GaugeHistogram follow all the same rules as for a Histogram.
    /// The bucket and Gsum of a GaugeHistogram are conceptually gauges, however bucket values MUST NOT be negative or NaN.
    /// If negative threshold buckets are present, then sum MAY be negative. Gsum MUST NOT be NaN. Bucket values MUST be integers.
    /// A GaugeHistogram's Metric's LabelSet MUST NOT have a "le" label name.
    /// Bucket values can have exemplars.
    /// Each bucket covers the values less and or equal to it, and the value of the exemplar MUST be within this range. E
    /// Exemplars SHOULD be put into the bucket with the highest value. A bucket MUST NOT have more than one exemplar.
    GaugeHistogram,

    /// StateSets represent a series of related boolean values, also called a bitset. If ENUMs need to be encoded this MAY be done via StateSet.
    /// A point of a StateSet metric MAY contain multiple states and MUST contain one boolean per State. States have a name which are Strings.
    /// A StateSet Metric's LabelSet MUST NOT have a label name which is the same as the name of its MetricFamily.
    /// If encoded as a StateSet, ENUMs MUST have exactly one Boolean which is true within a MetricPoint.
    /// This is suitable where the enum value changes over time, and the number of States isn't much more than a handful.
    StateSet,

    /// Summaries also measure distributions of discrete events and MAY be used when Histograms are too expensive and/or an average event size is sufficient.
    /// They MAY also be used for backwards compatibility, because some existing instrumentation libraries
    /// expose precomputed quantiles and do not support Histograms. Precomputed quantiles SHOULD NOT be used,
    /// because quantiles are not aggregatable and the user often can not deduce what timeframe they cover.
    /// A Summary MetricPoint MAY consist of a Count, Sum, Created, and a set of quantiles.
    /// Semantically, Count and Sum values are counters so MUST NOT be NaN or negative. Count MUST be an integer.
    /// A MetricPoint in a Metric with the type Summary which contains Count or Sum values SHOULD have a
    /// Timestamp value called Created. This can help ingestors discern between new metrics and long-running ones it did not see before.
    /// Created MUST NOT relate to the collection period of quantile values.
    /// Quantiles are a map from a quantile to a value. An example is a quantile 0.95 with value 0.2 in a metric called
    /// myapp_http_request_duration_seconds which means that the 95th percentile latency is 200ms over an unknown timeframe.
    /// If there are no events in the relevant timeframe, the value for a quantile MUST be NaN.
    /// A Quantile's Metric's LabelSet MUST NOT have "quantile" label name. Quantiles MUST be between 0 and 1 inclusive.
    /// Quantile values MUST NOT be negative. Quantile values SHOULD represent the recent values. Commonly this would be over the last 5-10 minutes.
    Summary,

    /// Info metrics are used to expose textual information which SHOULD NOT change during process lifetime.
    /// Common examples are an application's version, revision control commit, and the version of a compiler.
    /// A MetricPoint of an Info Metric contains a LabelSet. An Info MetricPoint's LabelSet MUST NOT have a label name which
    /// is the same as the name of a label of the LabelSet of its Metric.
    /// Info MAY be used to encode ENUMs whose values do not change over time, such as the type of a network interface.
    /// MetricFamilies of type Info MUST have an empty Unit string.
    Info,

    /// Unknown SHOULD NOT be used. Unknown MAY be used when it is impossible to determine the types of individual metrics from 3rd party systems.
    /// A point in a metric with the unknown type MUST have a single value.
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

#[derive(Debug, Clone)]
pub struct PrometheusCounterValue {
    pub value: MetricNumber,
    pub exemplar: Option<Exemplar>
}

#[derive(Debug)]
pub enum PrometheusValue {
    Unknown(MetricNumber),
    Gauge(MetricNumber),
    Counter(PrometheusCounterValue),
    Histogram(HistogramValue),
    Summary(SummaryValue),
}

#[derive(Debug)]
pub struct Sample<ValueType> {
    pub label_values: Vec<String>,
    pub timestamp: Option<Timestamp>,
    pub value: ValueType,
}

#[derive(Debug, Copy, Clone, PartialEq)]
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
            MetricNumber::Float(f) if (f.round() - *f).abs() < f64::EPSILON => Some(*f as i64),
            _ => None,
        }
    }
}

impl ops::Add for MetricNumber {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        &self + &rhs
    }
}

impl ops::Sub for MetricNumber {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        &self - &rhs
    }
}

impl ops::Mul for MetricNumber {
    type Output = Self;

    fn mul(self, rhs: Self) -> Self::Output {
        &self * &rhs
    }
}

impl ops::Div for MetricNumber {
    type Output = Self;

    fn div(self, rhs: Self) -> Self::Output {
        &self / &rhs
    }
}

impl ops::Add for &MetricNumber {
    type Output = MetricNumber;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (MetricNumber::Float(f), MetricNumber::Float(f2)) => MetricNumber::Float(f + f2),
            (MetricNumber::Float(f), MetricNumber::Int(i)) => MetricNumber::Float(f + *i as f64),
            (MetricNumber::Int(i), MetricNumber::Float(f)) => MetricNumber::Float(f + *i as f64),
            (MetricNumber::Int(i), MetricNumber::Int(i2)) => MetricNumber::Int(i + i2),
        }
    }
}

impl ops::Sub for &MetricNumber {
    type Output = MetricNumber;

    fn sub(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (MetricNumber::Float(f), MetricNumber::Float(f2)) => MetricNumber::Float(f - f2),
            (MetricNumber::Float(f), MetricNumber::Int(i)) => MetricNumber::Float(f - *i as f64),
            (MetricNumber::Int(i), MetricNumber::Float(f)) => MetricNumber::Float(f - *i as f64),
            (MetricNumber::Int(i), MetricNumber::Int(i2)) => MetricNumber::Int(i - i2),
        }
    }
}

impl ops::Mul for &MetricNumber {
    type Output = MetricNumber;

    fn mul(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (MetricNumber::Float(f), MetricNumber::Float(f2)) => MetricNumber::Float(f * f2),
            (MetricNumber::Float(f), MetricNumber::Int(i)) => MetricNumber::Float(f * *i as f64),
            (MetricNumber::Int(i), MetricNumber::Float(f)) => MetricNumber::Float(f * *i as f64),
            (MetricNumber::Int(i), MetricNumber::Int(i2)) => MetricNumber::Int(i * i2),
        }
    }
}

impl ops::Div for &MetricNumber {
    type Output = MetricNumber;

    fn div(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (MetricNumber::Float(f), MetricNumber::Float(f2)) => MetricNumber::Float(f / f2),
            (MetricNumber::Float(f), MetricNumber::Int(i)) => MetricNumber::Float(f / *i as f64),
            (MetricNumber::Int(i), MetricNumber::Float(f)) => MetricNumber::Float(f / *i as f64),
            (MetricNumber::Int(i), MetricNumber::Int(i2)) => MetricNumber::Int(i / i2),
        }
    }
}

#[derive(Debug)]
pub enum ParseError {
    ParseError(String),
    DuplicateMetric,
    InvalidMetric(String),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::ParseError(e) => e.fmt(f),
            ParseError::DuplicateMetric => f.write_str("Found two metrics with the same labelset"),
            ParseError::InvalidMetric(s) => f.write_str(s),
        }
    }
}
