use std::{
    collections::HashMap,
    fmt::{self, Write},
    sync::Arc,
};

use auto_ops::impl_op_ex;

use crate::internal::{render_label_values, RenderableMetricValue};

pub type Timestamp = f64;

/// An OpenMetrics Exemplar (that is also valid in Prometheus)
/// https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md#exemplars
/// Exemplars are references to data outside of the MetricSet. A common use case are IDs of program traces.
/// Exemplars MUST consist of a LabelSet and a value, and MAY have a timestamp. They MAY each be different from the MetricPoints' LabelSet and timestamp.
/// The combined length of the label names and values of an Exemplar's LabelSet MUST NOT exceed 128 UTF-8 characters.
/// Other characters in the text rendering of an exemplar such as ",= are not included in this limit for implementation
/// simplicity and for consistency between the text and proto formats.
#[derive(Debug, Clone, PartialEq)]
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

impl fmt::Display for Exemplar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names: Vec<&str> = self.labels.keys().map(|s| s.as_str()).collect();
        let values: Vec<&str> = self.labels.keys().map(|s| s.as_str()).collect();
        write!(f, " # {} {}", render_label_values(&names, &values), self.id)?;
        if let Some(timestamp) = self.timestamp {
            write!(f, " {}", timestamp)?;
        }

        Ok(())
    }
}

/// A MetricFamily is a collection of metrics with the same type, name, and label names
/// https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md#metricfamily
/// A MetricFamily MAY have zero or more Metrics. A MetricFamily MUST have a name, HELP, TYPE, and UNIT metadata.
/// Every Metric within a MetricFamily MUST have a unique LabelSet.
#[derive(Debug)]
pub struct MetricFamily<TypeSet, ValueType> {
    pub family_name: String,
    label_names: Arc<Vec<String>>,
    pub family_type: TypeSet,
    pub help: String,
    pub unit: String,
    metrics: Vec<Sample<ValueType>>,
}

impl<TypeSet, ValueType> MetricFamily<TypeSet, ValueType>
where
    TypeSet: Clone,
    ValueType: RenderableMetricValue + Clone,
{
    pub fn new(
        family_name: String,
        label_names: Vec<String>,
        family_type: TypeSet,
        help: String,
        unit: String,
    ) -> Self {
        Self {
            family_name,
            label_names: Arc::new(label_names),
            family_type,
            help,
            unit,
            metrics: Vec::new(),
        }
    }

    pub fn with_labels<'a, T>(&self, labels: T) -> Self
    where
        T: IntoIterator<Item = (&'a str, &'a str)>,
    {
        let mut label_names = self.label_names.as_ref().clone();
        let mut samples = self.metrics.clone();
        for (k, v) in labels {
            match label_names.iter().position(|n| k == *n) {
                Some(idx) => {
                    for sample in samples.iter_mut() {
                        sample.label_values[idx] = v.to_owned();
                    }
                }
                None => {
                    label_names.push(k.to_owned());
                    for sample in samples.iter_mut() {
                        sample.label_values.push(v.to_owned());
                    }
                }
            }
        }

        Self::new(
            self.family_name.clone(),
            label_names,
            self.family_type.clone(),
            self.help.clone(),
            self.unit.clone(),
        )
        .with_samples(samples)
        .unwrap()
    }

    pub fn without_label(&self, label_name: &str) -> Result<Self, ParseError> {
        match self.label_names.iter().position(|n| n == label_name) {
            Some(idx) => {
                let mut label_names = self.label_names.as_ref().clone();
                label_names.remove(idx);
                let mut base = Self::new(
                    self.family_name.clone(),
                    label_names,
                    self.family_type.clone(),
                    self.help.clone(),
                    self.unit.clone(),
                );

                for sample in self.metrics.iter() {
                    let mut label_values = sample.label_values.clone();
                    label_values.remove(idx);
                    let new_sample =
                        Sample::new(label_values, sample.timestamp, sample.value.clone());
                    base.add_sample(new_sample)?;
                }

                Ok(base)
            }
            None => Err(ParseError::InvalidMetric(format!(
                "No label `{}` in metric family",
                label_name
            ))),
        }
    }

    pub fn into_iter_samples(self) -> impl Iterator<Item = Sample<ValueType>> {
        self.metrics.into_iter()
    }

    pub fn iter_samples(&self) -> impl Iterator<Item = &Sample<ValueType>> {
        self.metrics.iter()
    }

    pub fn iter_samples_mut(&mut self) -> impl Iterator<Item = &mut Sample<ValueType>> {
        self.metrics.iter_mut()
    }

    pub fn with_samples<T>(mut self, samples: T) -> Result<Self, ParseError>
    where
        T: IntoIterator<Item = Sample<ValueType>>,
    {
        for sample in samples {
            self.add_sample(sample)?;
        }

        Ok(self)
    }

    pub fn get_sample_matches(&self, sample: &Sample<ValueType>) -> Option<&Sample<ValueType>> {
        return self
            .metrics
            .iter()
            .find(|&s| s.label_values == sample.label_values);
    }

    pub fn get_sample_matches_mut(
        &mut self,
        sample: &Sample<ValueType>,
    ) -> Option<&mut Sample<ValueType>> {
        return self
            .metrics
            .iter_mut()
            .find(|s| s.label_values == sample.label_values);
    }

    pub fn get_sample_by_label_values(
        &self,
        label_values: &[String],
    ) -> Option<&Sample<ValueType>> {
        return self.metrics.iter().find(|s| s.label_values == label_values);
    }

    pub fn get_sample_by_label_values_mut(
        &mut self,
        label_values: &[String],
    ) -> Option<&mut Sample<ValueType>> {
        return self
            .metrics
            .iter_mut()
            .find(|s| s.label_values == label_values);
    }

    pub fn get_sample_by_labelset(&self, labelset: &LabelSet) -> Option<&Sample<ValueType>> {
        return self.metrics.iter().find(|s| labelset.matches_sample(s));
    }

    pub fn get_sample_by_labelset_mut(
        &mut self,
        labelset: &LabelSet,
    ) -> Option<&mut Sample<ValueType>> {
        return self.metrics.iter_mut().find(|s| labelset.matches_sample(s));
    }

    pub fn set_label(&mut self, label_name: &str, label_value: &str) -> Result<(), ParseError> {
        let index = match self.label_names.iter().position(|s| s == label_name) {
            Some(position) => position,
            None => {
                return Err(ParseError::ParseError(format!(
                    "No Label {} on Metric Family",
                    label_name
                )));
            }
        };

        for metric in self.metrics.iter_mut() {
            if index == metric.label_values.len() {
                metric.label_values.push(label_value.to_owned());
            } else {
                metric.label_values[index] = label_value.to_owned();
            }
        }

        Ok(())
    }

    pub fn add_sample(&mut self, mut s: Sample<ValueType>) -> Result<(), ParseError> {
        if s.label_values.len() != self.label_names.len() {
            return Err(ParseError::InvalidMetric(format!(
                "Cannot add a sample with {} labels into a family with {}",
                s.label_values.len(),
                self.label_names.len()
            )));
        }

        if self.get_sample_by_label_values(&s.label_values).is_some() {
            return Err(ParseError::InvalidMetric(format!(
                "Cannot add a duplicate metric to a MetricFamily (Label Values: {:?})",
                s.label_values
            )));
        }

        s.set_label_names(self.label_names.clone());
        self.metrics.push(s);

        Ok(())
    }
}

impl<TypeSet, ValueType> fmt::Display for MetricFamily<TypeSet, ValueType>
where
    TypeSet: fmt::Display + Default + PartialEq,
    ValueType: RenderableMetricValue,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.help.is_empty() {
            writeln!(f, "# HELP {} {}", self.family_name, self.help)?;
        }

        if self.family_type != <TypeSet>::default() {
            writeln!(f, "# TYPE {} {}", self.family_name, self.family_type)?;
        }

        if !self.unit.is_empty() {
            writeln!(f, "# UNIT {} {}", self.family_name, self.unit)?;
        }

        let label_names: Vec<&str> = self.label_names.iter().map(|s| s.as_str()).collect();

        for metric in self.metrics.iter() {
            metric.render(f, &self.family_name, &label_names)?;
        }

        f.write_char('\n')
    }
}

/// Exposition is the top level object of the parser. It's a collection of metric families, indexed by name
#[derive(Debug)]
pub struct MetricsExposition<TypeSet, ValueType> {
    pub families: HashMap<String, MetricFamily<TypeSet, ValueType>>,
}

impl<TypeSet, ValueType> fmt::Display for MetricsExposition<TypeSet, ValueType>
where
    TypeSet: fmt::Display + Default + PartialEq,
    ValueType: RenderableMetricValue,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (_, family) in self.families.iter() {
            writeln!(f, "{}", family)?;
        }

        Ok(())
    }
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

#[derive(Debug, Clone, PartialEq)]
pub struct CounterValue {
    pub value: MetricNumber,
    pub created: Option<Timestamp>,
    pub exemplar: Option<Exemplar>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HistogramBucket {
    pub count: MetricNumber,
    pub upper_bound: f64,
    pub exemplar: Option<Exemplar>,
}

impl RenderableMetricValue for HistogramBucket {
    fn render(
        &self,
        f: &mut fmt::Formatter<'_>,
        metric_name: &str,
        _: Option<&Timestamp>,
        label_names: &[&str],
        label_values: &[&str],
    ) -> fmt::Result {
        let upper_bound_str = format!("{}", self.upper_bound);
        let label_names = {
            let mut names = Vec::from(label_names);
            names.push("le");
            names
        };

        let label_values = {
            let mut values = Vec::from(label_values);
            values.push(&upper_bound_str);
            values
        };

        write!(
            f,
            "{}_bucket{} {}",
            metric_name,
            render_label_values(&label_names, &label_values),
            self.count
        )?;

        if let Some(ex) = self.exemplar.as_ref() {
            write!(f, "{}", ex)?;
        }

        f.write_char('\n')?;

        Ok(())
    }
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct HistogramValue {
    pub sum: Option<MetricNumber>,
    pub count: Option<u64>,
    pub created: Option<Timestamp>,
    pub buckets: Vec<HistogramBucket>,
}

impl RenderableMetricValue for HistogramValue {
    fn render(
        &self,
        f: &mut fmt::Formatter<'_>,
        metric_name: &str,
        timestamp: Option<&Timestamp>,
        label_names: &[&str],
        label_values: &[&str],
    ) -> fmt::Result {
        for bucket in self.buckets.iter() {
            bucket.render(f, metric_name, timestamp, label_names, label_values)?;
        }

        let labels = render_label_values(label_names, label_values);

        if let Some(s) = self.sum {
            writeln!(f, "{}_sum{} {}", metric_name, labels, s)?;
        }

        if let Some(c) = self.count {
            writeln!(f, "{}_count{} {}", metric_name, labels, c)?;
        }

        if let Some(c) = self.created {
            writeln!(f, "{}_created{} {}", metric_name, labels, c)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct State {
    pub name: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Quantile {
    pub quantile: f64,
    pub value: MetricNumber,
}

impl RenderableMetricValue for Quantile {
    fn render(
        &self,
        f: &mut fmt::Formatter<'_>,
        metric_name: &str,
        _: Option<&Timestamp>,
        label_names: &[&str],
        label_values: &[&str],
    ) -> fmt::Result {
        let quantile_str = format!("{}", self.quantile);
        let label_names = {
            let mut names = Vec::from(label_names);
            names.push("quantile");
            names
        };

        let label_values = {
            let mut values = Vec::from(label_values);
            values.push(&quantile_str);
            values
        };

        writeln!(
            f,
            "{}{} {}",
            metric_name,
            render_label_values(&label_names, &label_values),
            self.value
        )
    }
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct SummaryValue {
    pub sum: Option<MetricNumber>,
    pub count: Option<u64>,
    pub created: Option<Timestamp>,
    pub quantiles: Vec<Quantile>,
}

impl RenderableMetricValue for SummaryValue {
    fn render(
        &self,
        f: &mut fmt::Formatter<'_>,
        metric_name: &str,
        timestamp: Option<&Timestamp>,
        label_names: &[&str],
        label_values: &[&str],
    ) -> fmt::Result {
        for q in self.quantiles.iter() {
            q.render(f, metric_name, timestamp, label_names, label_values)?;
        }

        let labels = render_label_values(label_names, label_values);

        if let Some(s) = self.sum {
            writeln!(f, "{}_sum{} {}", metric_name, labels, s)?;
        }

        if let Some(s) = self.count {
            writeln!(f, "{}_count{} {}", metric_name, labels, s)?;
        }

        if let Some(s) = self.created {
            writeln!(f, "{}_created{} {}", metric_name, labels, s)?;
        }

        Ok(())
    }
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

#[derive(Debug, Clone)]
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

impl RenderableMetricValue for OpenMetricsValue {
    fn render(
        &self,
        f: &mut fmt::Formatter<'_>,
        metric_name: &str,
        timestamp: Option<&Timestamp>,
        label_names: &[&str],
        label_values: &[&str],
    ) -> fmt::Result {
        let timestamp_str = timestamp.map(|t| format!(" {}", t)).unwrap_or_default();
        match self {
            OpenMetricsValue::Unknown(n)
            | OpenMetricsValue::Gauge(n)
            | OpenMetricsValue::StateSet(n) => writeln!(
                f,
                "{}{} {}{}",
                metric_name,
                render_label_values(label_names, label_values),
                n,
                timestamp_str
            ),
            OpenMetricsValue::Counter(c) => {
                write!(
                    f,
                    "{}{} {}{}",
                    metric_name,
                    render_label_values(label_names, label_values),
                    c.value,
                    timestamp_str
                )?;
                if let Some(ex) = c.exemplar.as_ref() {
                    write!(f, "{}", ex)?;
                }

                f.write_char('\n')
            }
            OpenMetricsValue::Histogram(h) | OpenMetricsValue::GaugeHistogram(h) => {
                // TODO: This is actually wrong for GaugeHistograms (they should have _gsum and _gcount), but I'm too lazy to fix this at the moment
                h.render(f, metric_name, timestamp, label_names, label_values)
            }
            OpenMetricsValue::Summary(s) => {
                s.render(f, metric_name, timestamp, label_names, label_values)
            }
            OpenMetricsValue::Info => {
                writeln!(
                    f,
                    "{}{} {}{}",
                    metric_name,
                    render_label_values(label_names, label_values),
                    MetricNumber::Int(1),
                    timestamp_str
                )
            }
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum PrometheusType {
    Counter,
    Gauge,
    Histogram,
    Summary,
    Unknown,
}

impl fmt::Display for PrometheusType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let out = match self {
            PrometheusType::Counter => "counter",
            PrometheusType::Gauge => "gauge",
            PrometheusType::Histogram => "histogram",
            PrometheusType::Summary => "summary",
            PrometheusType::Unknown => "unknown",
        };

        f.write_str(out)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PrometheusCounterValue {
    pub value: MetricNumber,
    pub exemplar: Option<Exemplar>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PrometheusValue {
    Unknown(MetricNumber),
    Gauge(MetricNumber),
    Counter(PrometheusCounterValue),
    Histogram(HistogramValue),
    Summary(SummaryValue),
}

impl RenderableMetricValue for PrometheusValue {
    fn render(
        &self,
        f: &mut fmt::Formatter<'_>,
        metric_name: &str,
        timestamp: Option<&Timestamp>,
        label_names: &[&str],
        label_values: &[&str],
    ) -> fmt::Result {
        let timestamp_str = timestamp.map(|t| format!(" {}", t)).unwrap_or_default();
        match self {
            PrometheusValue::Unknown(n) | PrometheusValue::Gauge(n) => writeln!(
                f,
                "{}{} {}{}",
                metric_name,
                render_label_values(label_names, label_values),
                n,
                timestamp_str
            ),
            PrometheusValue::Counter(c) => {
                write!(
                    f,
                    "{}{} {}{}",
                    metric_name,
                    render_label_values(label_names, label_values),
                    c.value,
                    timestamp_str
                )?;
                if let Some(ex) = c.exemplar.as_ref() {
                    write!(f, "{}", ex)?;
                }

                f.write_char('\n')
            }
            PrometheusValue::Histogram(h) => {
                h.render(f, metric_name, timestamp, label_names, label_values)
            }
            PrometheusValue::Summary(s) => {
                s.render(f, metric_name, timestamp, label_names, label_values)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Sample<ValueType> {
    label_names: Option<Arc<Vec<String>>>,
    label_values: Vec<String>,
    pub timestamp: Option<Timestamp>,
    pub value: ValueType,
}

impl<ValueType> Sample<ValueType>
where
    ValueType: RenderableMetricValue,
{
    pub fn new(label_values: Vec<String>, timestamp: Option<Timestamp>, value: ValueType) -> Self {
        Self {
            label_values,
            timestamp,
            value,
            label_names: None,
        }
    }

    fn set_label_names(&mut self, label_names: Arc<Vec<String>>) {
        self.label_names = Some(label_names);
    }

    pub fn get_labelset(&self) -> Result<LabelSet, ParseError> {
        if let Some(label_names) = &self.label_names {
            return LabelSet::new(label_names.clone(), self);
        }

        Err(ParseError::InvalidMetric(
            "Metric has not been bound to a family yet, and thus doesn't have label names"
                .to_string(),
        ))
    }

    fn render(
        &self,
        f: &mut fmt::Formatter<'_>,
        metric_name: &str,
        label_names: &[&str],
    ) -> fmt::Result {
        let values: Vec<&str> = self.label_values.iter().map(|s| s.as_str()).collect();
        self.value.render(
            f,
            metric_name,
            self.timestamp.as_ref(),
            label_names,
            &values,
        )
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum MetricNumber {
    Float(f64),
    Int(i64),
}

impl fmt::Display for MetricNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetricNumber::Float(n) => write!(f, "{}", n),
            MetricNumber::Int(n) => write!(f, "{}", n),
        }
    }
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

impl_op_ex!(+ |a: &MetricNumber, b: &MetricNumber| -> MetricNumber {
    match (a, b) {
        (MetricNumber::Float(f), MetricNumber::Float(f2)) => MetricNumber::Float(f + f2),
        (MetricNumber::Float(f), MetricNumber::Int(i)) => MetricNumber::Float(f + *i as f64),
        (MetricNumber::Int(i), MetricNumber::Float(f)) => MetricNumber::Float(f + *i as f64),
        (MetricNumber::Int(i), MetricNumber::Int(i2)) => MetricNumber::Int(i + i2),
    }
});

impl_op_ex!(-|a: &MetricNumber, b: &MetricNumber| -> MetricNumber {
    match (a, b) {
        (MetricNumber::Float(f), MetricNumber::Float(f2)) => MetricNumber::Float(f - f2),
        (MetricNumber::Float(f), MetricNumber::Int(i)) => MetricNumber::Float(f - *i as f64),
        (MetricNumber::Int(i), MetricNumber::Float(f)) => MetricNumber::Float(f - *i as f64),
        (MetricNumber::Int(i), MetricNumber::Int(i2)) => MetricNumber::Int(i - i2),
    }
});

impl_op_ex!(*|a: &MetricNumber, b: &MetricNumber| -> MetricNumber {
    match (a, b) {
        (MetricNumber::Float(f), MetricNumber::Float(f2)) => MetricNumber::Float(f * f2),
        (MetricNumber::Float(f), MetricNumber::Int(i)) => MetricNumber::Float(f * *i as f64),
        (MetricNumber::Int(i), MetricNumber::Float(f)) => MetricNumber::Float(f * *i as f64),
        (MetricNumber::Int(i), MetricNumber::Int(i2)) => MetricNumber::Int(i * i2),
    }
});

impl_op_ex!(/ |a: &MetricNumber, b: &MetricNumber| -> MetricNumber {
    match (a, b) {
        (MetricNumber::Float(f), MetricNumber::Float(f2)) => MetricNumber::Float(f / f2),
        (MetricNumber::Float(f), MetricNumber::Int(i)) => MetricNumber::Float(f / *i as f64),
        (MetricNumber::Int(i), MetricNumber::Float(f)) => MetricNumber::Float(f / *i as f64),
        (MetricNumber::Int(i), MetricNumber::Int(i2)) => MetricNumber::Int(i / i2),
    }
});

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

pub struct LabelSet<'a> {
    label_names: Arc<Vec<String>>,
    label_values: &'a [String],
}

impl<'a> LabelSet<'a> {
    pub fn new<ValueType>(
        label_names: Arc<Vec<String>>,
        sample: &'a Sample<ValueType>,
    ) -> Result<Self, ParseError> {
        if label_names.len() != sample.label_values.len() {
            return Err(ParseError::InvalidMetric(format!(
                "Cannot create labelset from family with {} labels and sample with {}",
                label_names.len(),
                sample.label_values.len()
            )));
        }

        Ok(Self {
            label_names,
            label_values: &sample.label_values,
        })
    }

    pub fn matches_sample<ValueType>(&self, sample: &Sample<ValueType>) -> bool {
        self.matches_values(&sample.label_values)
    }

    pub fn matches_values(&self, label_values: &[String]) -> bool {
        self.label_values == label_values
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &String)> {
        return self.label_names.iter().zip(self.label_values);
    }

    pub fn iter_names(&self) -> impl Iterator<Item = &String> {
        self.label_names.iter()
    }

    pub fn iter_values(&self) -> impl Iterator<Item = &String> {
        self.label_values.iter()
    }

    pub fn get_label_value(&self, label_name: &str) -> Option<&str> {
        return self
            .label_names
            .iter()
            .position(|s| s == label_name)
            .map(|i| self.label_values[i].as_str());
    }
}
