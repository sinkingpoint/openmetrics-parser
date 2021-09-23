use super::model::*;
use std::{collections::HashMap, convert::TryFrom, fmt};

use pest::Parser;

#[derive(Parser)]
#[grammar = "openmetrics/openmetrics.pest"]
pub struct OpenMetricsParser;

trait MetricsType {
    fn can_have_exemplar(&self, metric_name: &str) -> bool;
    fn is_allowed_units(&self) -> bool;
    fn get_ignored_labels(&self, metric_name: &str) -> &[&str];
    fn get_type_value(&self) -> MetricValueMarshal;
    fn can_have_multiple_lines(&self) -> bool;
}

#[derive(Debug, Default)]
struct CounterValueMarshal {
    value: Option<MetricNumber>,
    created: Option<Timestamp>,
    exemplar: Option<Exemplar>,
}

impl Into<CounterValue> for CounterValueMarshal {
    fn into(self) -> CounterValue {
        return CounterValue {
            value: self.value.unwrap(),
            created: self.created,
            exemplar: self.exemplar,
        };
    }
}

#[derive(Debug)]
enum MetricValueMarshal {
    Unknown(Option<MetricNumber>),
    Gauge(Option<MetricNumber>),
    Counter(CounterValueMarshal),
    Histogram(HistogramValue),
    StateSet(Option<MetricNumber>),
    GaugeHistogram(HistogramValue),
    Info,
    Summary(SummaryValue),
}

impl Into<OpenMetricsValue> for MetricValueMarshal {
    fn into(self) -> OpenMetricsValue {
        match self {
            MetricValueMarshal::Unknown(s) => OpenMetricsValue::Unknown(s.unwrap()),
            MetricValueMarshal::Gauge(s) => OpenMetricsValue::Gauge(s.unwrap()),
            MetricValueMarshal::Counter(s) => OpenMetricsValue::Counter(s.into()),
            MetricValueMarshal::Histogram(s) => OpenMetricsValue::Histogram(s),
            MetricValueMarshal::StateSet(s) => OpenMetricsValue::StateSet(s.unwrap()),
            MetricValueMarshal::GaugeHistogram(s) => OpenMetricsValue::GaugeHistogram(s),
            MetricValueMarshal::Info => OpenMetricsValue::Info,
            MetricValueMarshal::Summary(s) => OpenMetricsValue::Summary(s),
        }
    }
}

#[derive(Debug)]
struct LabelNames<TypeSet> {
    names: Vec<String>,
    metric_type: TypeSet,
}

impl<TypeSet> LabelNames<TypeSet>
where
    TypeSet: MetricsType,
{
    fn new(sample_name: &String, metric_type: TypeSet, labels: Vec<String>) -> LabelNames<TypeSet> {
        let ignored_labels = TypeSet::get_ignored_labels(&metric_type, sample_name);
        let names = labels
            .into_iter()
            .filter(|s| !ignored_labels.contains(&s.as_str()))
            .collect();

        return LabelNames { names, metric_type };
    }

    fn matches(&self, sample_name: &String, other_labels: &LabelNames<TypeSet>) -> bool {
        let ignored_labels = TypeSet::get_ignored_labels(&self.metric_type, sample_name);
        for name in self.names.iter() {
            if !ignored_labels.contains(&name.as_str()) && !other_labels.names.contains(&name) {
                return false;
            }
        }

        return true;
    }
}

impl MetricsType for OpenMetricsType {
    fn can_have_exemplar(&self, metric_name: &str) -> bool {
        match self {
            OpenMetricsType::Counter => metric_name.ends_with("_total"),
            OpenMetricsType::Histogram | OpenMetricsType::GaugeHistogram => {
                metric_name.ends_with("_bucket")
            }
            _ => false,
        }
    }

    fn get_ignored_labels(&self, metric_name: &str) -> &[&str] {
        match self {
            OpenMetricsType::Histogram | OpenMetricsType::GaugeHistogram
                if metric_name.ends_with("bucket") =>
            {
                &["le"]
            }
            _ => &[],
        }
    }

    fn get_type_value(&self) -> MetricValueMarshal {
        match self {
            OpenMetricsType::Histogram => MetricValueMarshal::Histogram(HistogramValue::default()),
            OpenMetricsType::GaugeHistogram => {
                MetricValueMarshal::GaugeHistogram(HistogramValue::default())
            }
            OpenMetricsType::Counter => MetricValueMarshal::Counter(CounterValueMarshal::default()),
            OpenMetricsType::Unknown => MetricValueMarshal::Unknown(None),
            OpenMetricsType::Gauge => MetricValueMarshal::Gauge(None),
            OpenMetricsType::StateSet => MetricValueMarshal::StateSet(None),
            OpenMetricsType::Summary => MetricValueMarshal::Summary(SummaryValue::default()),
            OpenMetricsType::Info => MetricValueMarshal::Info,
        }
    }

    fn is_allowed_units(&self) -> bool {
        match self {
            OpenMetricsType::Counter | OpenMetricsType::Unknown | OpenMetricsType::Gauge => true,
            _ => false,
        }
    }

    fn can_have_multiple_lines(&self) -> bool {
        match self {
            OpenMetricsType::Counter
            | OpenMetricsType::GaugeHistogram
            | OpenMetricsType::Histogram
            | OpenMetricsType::Summary => true,
            _ => false,
        }
    }
}

impl TryFrom<&str> for OpenMetricsType {
    type Error = OpenMetricsParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "counter" => Ok(OpenMetricsType::Counter),
            "gauge" => Ok(OpenMetricsType::Gauge),
            "histogram" => Ok(OpenMetricsType::Histogram),
            "gaugehistogram" => Ok(OpenMetricsType::GaugeHistogram),
            "stateset" => Ok(OpenMetricsType::StateSet),
            "summary" => Ok(OpenMetricsType::Summary),
            "info" => Ok(OpenMetricsType::Info),
            "unknown" => Ok(OpenMetricsType::Unknown),
            _ => Err(OpenMetricsParseError::InvalidMetric(format!(
                "Invalid metric type: {}",
                value
            ))),
        }
    }
}

impl Default for OpenMetricsType {
    fn default() -> Self {
        return OpenMetricsType::Unknown;
    }
}

#[derive(Debug)]
struct MetricMarshal {
    label_values: Vec<String>,
    timestamp: Option<Timestamp>,
    value: MetricValueMarshal,
}

impl From<MetricMarshal> for Metric<OpenMetricsValue> {
    fn from(s: MetricMarshal) -> Metric<OpenMetricsValue> {
        Metric {
            label_values: s.label_values,
            timestamp: s.timestamp,
            value: s.value.into(),
        }
    }
}

impl MetricMarshal {
    fn new(
        label_values: Vec<String>,
        timestamp: Option<Timestamp>,
        value: MetricValueMarshal,
    ) -> MetricMarshal {
        return MetricMarshal {
            label_values,
            timestamp,
            value,
        };
    }

    fn validate<Type>(
        &self,
        family: &MetricFamilyMarshal<Type>,
    ) -> Result<(), OpenMetricsParseError>
    where
        Type: fmt::Debug + Clone + Default,
    {
        // All the labels are right
        if family.label_names.is_none() && self.label_values.len() != 0
            || (family.label_names.as_ref().unwrap().names.len() != self.label_values.len())
        {
            return Err(OpenMetricsParseError::InvalidMetric(format!(
                "Metrics in family have different label sets: {:?} {:?}",
                &family.label_names, self.label_values
            )));
        }

        if family.unit.is_some() && family.metrics.len() == 0 {
            return Err(OpenMetricsParseError::InvalidMetric(
                "Can't have metric with unit and no samples".to_owned(),
            ));
        }

        match &self.value {
            MetricValueMarshal::Histogram(histogram_value)
            | MetricValueMarshal::GaugeHistogram(histogram_value) => {
                let gauge_histogram = if let MetricValueMarshal::GaugeHistogram(_) = &self.value {
                    true
                } else {
                    false
                };

                if histogram_value.buckets.len() == 0 {
                    return Err(OpenMetricsParseError::InvalidMetric(
                        "Histograms must have at least one bucket".to_owned(),
                    ));
                }

                if histogram_value
                    .buckets
                    .iter()
                    .find(|b| b.upper_bound == f64::INFINITY)
                    .is_none()
                {
                    return Err(OpenMetricsParseError::InvalidMetric(format!(
                        "Histograms must have a +INF bucket: {:?}",
                        histogram_value.buckets
                    )));
                }

                let buckets = &histogram_value.buckets;

                let has_negative_bucket = buckets.iter().find(|f| f.upper_bound < 0.).is_some();

                if has_negative_bucket {
                    if histogram_value.sum.is_some() && !gauge_histogram {
                        return Err(OpenMetricsParseError::InvalidMetric(
                            "Histograms cannot have a sum with a negative bucket".to_owned(),
                        ));
                    }
                } else {
                    if histogram_value.sum.is_some()
                        && histogram_value.sum.as_ref().unwrap().as_f64() < 0.
                    {
                        return Err(OpenMetricsParseError::InvalidMetric(
                            "Histograms cannot have a negative sum without a negative bucket"
                                .to_owned(),
                        ));
                    }
                }

                if histogram_value.sum.is_some() && histogram_value.count.is_none() {
                    return Err(OpenMetricsParseError::InvalidMetric(
                        "Count must be present if sum is present".to_owned(),
                    ));
                }

                if histogram_value.sum.is_none() && histogram_value.count.is_some() {
                    return Err(OpenMetricsParseError::InvalidMetric(
                        "Sum must be present if count is present".to_owned(),
                    ));
                }

                let mut last = f64::NEG_INFINITY;
                for bucket in buckets {
                    if bucket.count.as_f64() < last {
                        return Err(OpenMetricsParseError::InvalidMetric(
                            "Histograms must be cumulative".to_owned(),
                        ));
                    }

                    last = bucket.count.as_f64();
                }
            }
            _ => {}
        }

        return Ok(());
    }
}

#[derive(Debug)]
pub enum OpenMetricsParseError {
    ParseError(pest::error::Error<Rule>),
    DuplicateMetric,
    InvalidMetric(String),
}

impl From<pest::error::Error<Rule>> for OpenMetricsParseError {
    fn from(err: pest::error::Error<Rule>) -> Self {
        return OpenMetricsParseError::ParseError(err);
    }
}

impl fmt::Display for OpenMetricsParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OpenMetricsParseError::ParseError(e) => e.fmt(f),
            OpenMetricsParseError::DuplicateMetric => {
                f.write_str("Found two metrics with the same labelset")
            }
            OpenMetricsParseError::InvalidMetric(s) => f.write_str(s),
        }
    }
}

#[derive(Debug)]
struct MetricFamilyMarshal<TypeSet> {
    name: Option<String>,
    label_names: Option<LabelNames<TypeSet>>,
    family_type: Option<TypeSet>,
    help: Option<String>,
    unit: Option<String>,
    metrics: Vec<MetricMarshal>,
}
trait MarshalledMetricFamily {
    type Error;
    fn process_new_metric(
        &mut self,
        metric_name: &str,
        value: MetricNumber,
        label_names: Vec<String>,
        label_values: Vec<String>,
        timestamp: Option<Timestamp>,
        exemplar: Option<Exemplar>,
    ) -> Result<(), Self::Error>;
}

struct MetricProcesser(
    Box<
        dyn Fn(
            &mut MetricMarshal,
            MetricNumber,
            Vec<String>,
            Vec<String>,
            Option<Exemplar>,
            bool,
        ) -> Result<(), OpenMetricsParseError>,
    >,
);

impl MetricProcesser {
    fn new<F>(f: F) -> MetricProcesser
    where
        F: Fn(
                &mut MetricMarshal,
                MetricNumber,
                Vec<String>,
                Vec<String>,
                Option<Exemplar>,
                bool,
            ) -> Result<(), OpenMetricsParseError>
            + 'static,
    {
        MetricProcesser(Box::new(f))
    }
}

impl MarshalledMetricFamily for MetricFamilyMarshal<OpenMetricsType> {
    type Error = OpenMetricsParseError;

    fn process_new_metric(
        &mut self,
        metric_name: &str,
        metric_value: MetricNumber,
        label_names: Vec<String>,
        label_values: Vec<String>,
        timestamp: Option<Timestamp>,
        exemplar: Option<Exemplar>,
    ) -> Result<(), Self::Error> {
        let handlers = vec![
            (
                vec![OpenMetricsType::Histogram],
                vec![
                    (
                        "_bucket",
                        vec!["le"],
                        MetricProcesser::new(
                            |existing_metric: &mut MetricMarshal,
                             metric_value: MetricNumber,
                             label_names: Vec<String>,
                             label_values: Vec<String>,
                             exemplar: Option<Exemplar>,
                             _: bool| {
                                let bucket_bound: f64 = {
                                    let bound_index =
                                        label_names.iter().position(|s| s == "le").unwrap();

                                    let bound = &label_values[bound_index];
                                    match bound.parse() {
                                        Ok(f) => f,
                                        Err(_) => {
                                            return Err(OpenMetricsParseError::InvalidMetric(
                                                format!("Invalid histogram bound: {}", bound),
                                            ));
                                        }
                                    }
                                };

                                let bucket = HistogramBucket {
                                    count: metric_value,
                                    upper_bound: bucket_bound,
                                    exemplar,
                                };

                                if let MetricValueMarshal::Histogram(value) =
                                    &mut existing_metric.value
                                {
                                    value.buckets.push(bucket);
                                } else {
                                    unreachable!();
                                }

                                return Ok(());
                            },
                        ),
                    ),
                    (
                        "_count",
                        vec![],
                        MetricProcesser::new(
                            |existing_metric: &mut MetricMarshal,
                             metric_value: MetricNumber,
                             _: Vec<String>,
                             _: Vec<String>,
                             _: Option<Exemplar>,
                             _: bool| {
                                if let MetricValueMarshal::Histogram(histogram_value) =
                                    &mut existing_metric.value
                                {
                                    let metric_value = if let Some(value) = metric_value.as_i64() {
                                        if value < 0 {
                                            return Err(OpenMetricsParseError::InvalidMetric(
                                                format!(
                                                    "Histogram counts must be positive (got: {})",
                                                    value
                                                ),
                                            ));
                                        }

                                        value as u64
                                    } else {
                                        return Err(OpenMetricsParseError::InvalidMetric(format!(
                                            "Histogram counts must be integers (got: {})",
                                            metric_value.as_f64()
                                        )));
                                    };

                                    match histogram_value.count {
                                        Some(_) => {
                                            return Err(OpenMetricsParseError::DuplicateMetric);
                                        }
                                        None => {
                                            histogram_value.count = Some(metric_value);
                                        }
                                    };
                                } else {
                                    unreachable!();
                                }

                                return Ok(());
                            },
                        ),
                    ),
                    (
                        "_created",
                        vec![],
                        MetricProcesser::new(
                            |existing_metric: &mut MetricMarshal,
                             metric_value: MetricNumber,
                             _: Vec<String>,
                             _: Vec<String>,
                             _: Option<Exemplar>,
                             _: bool| {
                                if let MetricValueMarshal::Histogram(histogram_value) =
                                    &mut existing_metric.value
                                {
                                    match histogram_value.timestamp {
                                        Some(_) => {
                                            return Err(OpenMetricsParseError::DuplicateMetric);
                                        }
                                        None => {
                                            histogram_value.timestamp = Some(metric_value.as_f64());
                                        }
                                    };
                                } else {
                                    unreachable!();
                                }

                                return Ok(());
                            },
                        ),
                    ),
                    (
                        "_sum",
                        vec![],
                        MetricProcesser::new(
                            |existing_metric: &mut MetricMarshal,
                             metric_value: MetricNumber,
                             _: Vec<String>,
                             _: Vec<String>,
                             _: Option<Exemplar>,
                             _: bool| {
                                if let MetricValueMarshal::Histogram(histogram_value) =
                                    &mut existing_metric.value
                                {
                                    if histogram_value.sum.is_some() {
                                        return Err(OpenMetricsParseError::DuplicateMetric);
                                    }

                                    histogram_value.sum = Some(metric_value);

                                    return Ok(());
                                } else {
                                    unreachable!();
                                }
                            },
                        ),
                    ),
                ],
            ),
            (
                vec![OpenMetricsType::GaugeHistogram],
                vec![
                    (
                        "_bucket",
                        vec!["le"],
                        MetricProcesser::new(
                            |existing_metric: &mut MetricMarshal,
                             metric_value: MetricNumber,
                             label_names: Vec<String>,
                             label_values: Vec<String>,
                             exemplar: Option<Exemplar>,
                             _: bool| {
                                let bucket_bound: f64 = {
                                    let bound_index =
                                        label_names.iter().position(|s| s == "le").unwrap();

                                    let bound = &label_values[bound_index];
                                    match bound.parse() {
                                        Ok(f) => f,
                                        Err(_) => {
                                            return Err(OpenMetricsParseError::InvalidMetric(format!("Expected histogram bucket bound to be an f64 (got: {})", bound)));
                                        }
                                    }
                                };

                                let bucket = HistogramBucket {
                                    count: metric_value,
                                    upper_bound: bucket_bound,
                                    exemplar,
                                };

                                if let MetricValueMarshal::GaugeHistogram(value) =
                                    &mut existing_metric.value
                                {
                                    value.buckets.push(bucket);
                                } else {
                                    unreachable!();
                                }

                                return Ok(());
                            },
                        ),
                    ),
                    (
                        "_gcount",
                        vec![],
                        MetricProcesser::new(
                            |existing_metric: &mut MetricMarshal,
                             metric_value: MetricNumber,
                             _: Vec<String>,
                             _: Vec<String>,
                             _: Option<Exemplar>,
                             _: bool| {
                                if let MetricValueMarshal::GaugeHistogram(histogram_value) =
                                    &mut existing_metric.value
                                {
                                    let metric_value = if let Some(value) = metric_value.as_i64() {
                                        if value < 0 {
                                            return Err(OpenMetricsParseError::InvalidMetric(
                                                format!(
                                                    "Histogram counts must be positive (got: {})",
                                                    value
                                                ),
                                            ));
                                        }

                                        value as u64
                                    } else {
                                        return Err(OpenMetricsParseError::InvalidMetric(format!(
                                            "Histogram counts must be integers (got: {})",
                                            metric_value.as_f64()
                                        )));
                                    };

                                    match histogram_value.count {
                                        Some(_) => {
                                            return Err(OpenMetricsParseError::DuplicateMetric);
                                        }
                                        None => {
                                            histogram_value.count = Some(metric_value);
                                        }
                                    };
                                } else {
                                    unreachable!();
                                }

                                return Ok(());
                            },
                        ),
                    ),
                    (
                        "_gsum",
                        vec![],
                        MetricProcesser::new(
                            |existing_metric: &mut MetricMarshal,
                             metric_value: MetricNumber,
                             _: Vec<String>,
                             _: Vec<String>,
                             _: Option<Exemplar>,
                             _: bool| {
                                if let MetricValueMarshal::GaugeHistogram(histogram_value) =
                                    &mut existing_metric.value
                                {
                                    if histogram_value.sum.is_some() {
                                        return Err(OpenMetricsParseError::DuplicateMetric);
                                    }

                                    histogram_value.sum = Some(metric_value);

                                    return Ok(());
                                } else {
                                    unreachable!();
                                }
                            },
                        ),
                    ),
                ],
            ),
            (
                vec![OpenMetricsType::Counter],
                vec![
                    (
                        "_total",
                        vec![],
                        MetricProcesser::new(
                            |existing_metric: &mut MetricMarshal,
                             metric_value: MetricNumber,
                             _: Vec<String>,
                             _: Vec<String>,
                             _: Option<Exemplar>,
                             _: bool| {
                                if let MetricValueMarshal::Counter(counter_value) =
                                    &mut existing_metric.value
                                {
                                    if counter_value.value.is_some() {
                                        return Err(OpenMetricsParseError::DuplicateMetric);
                                    }

                                    let value = metric_value.as_f64();
                                    if value < 0. || value.is_nan() {
                                        return Err(OpenMetricsParseError::InvalidMetric(format!(
                                            "Counter totals must be non negative (got: {})",
                                            metric_value.as_f64()
                                        )));
                                    }

                                    counter_value.value = Some(metric_value);
                                } else {
                                    unreachable!();
                                }

                                return Ok(());
                            },
                        ),
                    ),
                    (
                        "_created",
                        vec![],
                        MetricProcesser::new(
                            |existing_metric: &mut MetricMarshal,
                             metric_value: MetricNumber,
                             _: Vec<String>,
                             _: Vec<String>,
                             _: Option<Exemplar>,
                             _: bool| {
                                if let MetricValueMarshal::Counter(counter_value) =
                                    &mut existing_metric.value
                                {
                                    if counter_value.created.is_some() {
                                        return Err(OpenMetricsParseError::DuplicateMetric);
                                    }

                                    counter_value.created = Some(metric_value.as_f64());
                                    return Ok(());
                                } else {
                                    unreachable!();
                                }
                            },
                        ),
                    ),
                ],
            ),
            (
                vec![OpenMetricsType::Gauge],
                vec![(
                    "",
                    vec![],
                    MetricProcesser::new(
                        |existing_metric: &mut MetricMarshal,
                         metric_value: MetricNumber,
                         _: Vec<String>,
                         _: Vec<String>,
                         _: Option<Exemplar>,
                         _: bool| {
                            if let MetricValueMarshal::Gauge(gauge_value) =
                                &mut existing_metric.value
                            {
                                if gauge_value.is_some() {
                                    return Err(OpenMetricsParseError::DuplicateMetric);
                                }

                                existing_metric.value =
                                    MetricValueMarshal::Gauge(Some(metric_value));
                            } else {
                                unreachable!();
                            }

                            return Ok(());
                        },
                    ),
                )],
            ),
            (
                vec![OpenMetricsType::StateSet],
                vec![(
                    "",
                    vec![],
                    MetricProcesser::new(
                        |existing_metric: &mut MetricMarshal,
                         metric_value: MetricNumber,
                         _: Vec<String>,
                         _: Vec<String>,
                         _: Option<Exemplar>,
                         _: bool| {
                            if let MetricValueMarshal::StateSet(stateset_value) =
                                &mut existing_metric.value
                            {
                                if stateset_value.is_some() {
                                    return Err(OpenMetricsParseError::DuplicateMetric);
                                }

                                if existing_metric.label_values.len() == 0 {
                                    return Err(OpenMetricsParseError::InvalidMetric(format!(
                                        "Stateset must have labels"
                                    )));
                                }

                                if metric_value.as_f64() != 0. && metric_value.as_f64() != 1. {
                                    return Err(OpenMetricsParseError::InvalidMetric(format!(
                                        "Stateset value must be 0 or 1 (got: {})",
                                        metric_value.as_f64()
                                    )));
                                }

                                existing_metric.value =
                                    MetricValueMarshal::StateSet(Some(metric_value));
                            } else {
                                unreachable!();
                            }

                            return Ok(());
                        },
                    ),
                )],
            ),
            (
                vec![OpenMetricsType::Unknown],
                vec![(
                    "",
                    vec![],
                    MetricProcesser::new(
                        |existing_metric: &mut MetricMarshal,
                         metric_value: MetricNumber,
                         _: Vec<String>,
                         _: Vec<String>,
                         _: Option<Exemplar>,
                         _: bool| {
                            if let MetricValueMarshal::Unknown(unknown_value) =
                                &mut existing_metric.value
                            {
                                if unknown_value.is_some() {
                                    return Err(OpenMetricsParseError::DuplicateMetric);
                                }

                                existing_metric.value =
                                    MetricValueMarshal::Unknown(Some(metric_value));
                            } else {
                                unreachable!();
                            }

                            return Ok(());
                        },
                    ),
                )],
            ),
            (
                vec![OpenMetricsType::Info],
                vec![(
                    "_info",
                    vec![],
                    MetricProcesser::new(
                        |_: &mut MetricMarshal,
                         metric_value: MetricNumber,
                         _: Vec<String>,
                         _: Vec<String>,
                         _: Option<Exemplar>,
                         created: bool| {
                            let metric_value = if let Some(value) = metric_value.as_i64() {
                                value as u64
                            } else {
                                return Err(OpenMetricsParseError::InvalidMetric(format!(
                                    "Info values must be integers (got: {})",
                                    metric_value.as_f64()
                                )));
                            };

                            if metric_value != 1 {
                                return Err(OpenMetricsParseError::InvalidMetric(format!(
                                    "Info values must be 1 (got: {})",
                                    metric_value
                                )));
                            }

                            if !created {
                                return Err(OpenMetricsParseError::DuplicateMetric);
                            }

                            return Ok(());
                        },
                    ),
                )],
            ),
            (
                vec![OpenMetricsType::Summary],
                vec![
                    (
                        "_count",
                        vec![],
                        MetricProcesser::new(
                            |existing_metric: &mut MetricMarshal,
                             metric_value: MetricNumber,
                             _: Vec<String>,
                             _: Vec<String>,
                             _: Option<Exemplar>,
                             _: bool| {
                                if let MetricValueMarshal::Summary(summary_value) =
                                    &mut existing_metric.value
                                {
                                    let metric_value = if let Some(value) = metric_value.as_i64() {
                                        if value < 0 {
                                            return Err(OpenMetricsParseError::InvalidMetric(
                                                format!(
                                                    "Summary counts must be positive (got: {})",
                                                    value
                                                ),
                                            ));
                                        }
                                        value as u64
                                    } else {
                                        return Err(OpenMetricsParseError::InvalidMetric(format!(
                                            "Summary counts must be integers (got: {})",
                                            metric_value.as_f64()
                                        )));
                                    };

                                    if let None = summary_value.count {
                                        summary_value.count = Some(metric_value);
                                    } else {
                                        return Err(OpenMetricsParseError::DuplicateMetric);
                                    }
                                } else {
                                    unreachable!();
                                }

                                return Ok(());
                            },
                        ),
                    ),
                    (
                        "_sum",
                        vec![],
                        MetricProcesser::new(
                            |existing_metric: &mut MetricMarshal,
                             metric_value: MetricNumber,
                             _: Vec<String>,
                             _: Vec<String>,
                             _: Option<Exemplar>,
                             _: bool| {
                                let value = metric_value.as_f64();
                                if value < 0. || value.is_nan() {
                                    return Err(OpenMetricsParseError::InvalidMetric(format!(
                                        "Counter totals must be non negative (got: {})",
                                        metric_value.as_f64()
                                    )));
                                }

                                if let MetricValueMarshal::Summary(summary_value) =
                                    &mut existing_metric.value
                                {
                                    if let None = summary_value.sum {
                                        summary_value.sum = Some(metric_value);
                                        return Ok(());
                                    } else {
                                        return Err(OpenMetricsParseError::DuplicateMetric);
                                    }
                                } else {
                                    unreachable!();
                                }
                            },
                        ),
                    ),
                    (
                        "",
                        vec!["quantile"],
                        MetricProcesser::new(
                            |existing_metric: &mut MetricMarshal,
                             metric_value: MetricNumber,
                             label_names: Vec<String>,
                             label_values: Vec<String>,
                             _: Option<Exemplar>,
                             _: bool| {
                                let value = metric_value.as_f64();
                                if !value.is_nan() && value < 0. {
                                    return Err(OpenMetricsParseError::InvalidMetric(
                                        "Summary quantiles can't be negative".to_owned(),
                                    ));
                                }

                                let bucket_bound: f64 = {
                                    let bound_index =
                                        label_names.iter().position(|s| s == "quantile").unwrap();
                                    let bound = &label_values[bound_index];

                                    match bound.parse() {
                                        Ok(f) => f,
                                        Err(_) => {
                                            return Err(OpenMetricsParseError::InvalidMetric(
                                                format!(
                                                    "Summary bounds must be numbers (got: {})",
                                                    bound
                                                ),
                                            ));
                                        }
                                    }
                                };

                                if bucket_bound < 0. || bucket_bound > 1. || bucket_bound.is_nan() {
                                    return Err(OpenMetricsParseError::InvalidMetric(format!(
                                        "Summary bounds must be between 0 and 1 (got: {})",
                                        bucket_bound
                                    )));
                                }

                                let quantile = Quantile {
                                    quantile: bucket_bound,
                                    value: metric_value,
                                };

                                if let MetricValueMarshal::Summary(summary_value) =
                                    &mut existing_metric.value
                                {
                                    summary_value.quantiles.push(quantile);
                                } else {
                                    unreachable!();
                                }

                                return Ok(());
                            },
                        ),
                    ),
                ],
            ),
        ];

        let metric_type = self
            .family_type
            .as_ref()
            .map(|s| s.clone())
            .unwrap_or_default();

        if !metric_type.can_have_exemplar(metric_name) && exemplar.is_some() {
            return Err(OpenMetricsParseError::InvalidMetric(format!(
                "Metric Type {:?} is not allowed exemplars",
                metric_type
            )));
        }

        for (test_type, actions) in handlers {
            if test_type.contains(&metric_type) {
                for (suffix, mandatory_labels, action) in actions {
                    if !metric_name.ends_with(suffix) {
                        continue;
                    }

                    let mut actual_label_names = label_names.clone();
                    let mut actual_label_values = label_values.clone();
                    for label in mandatory_labels {
                        if !label_names.contains(&label.to_owned()) {
                            return Err(OpenMetricsParseError::InvalidMetric(format!(
                                "Missing mandatory label for metric: {}",
                                label
                            )));
                        }

                        let index = actual_label_names.iter().position(|s| s == label).unwrap();

                        actual_label_names.remove(index);
                        actual_label_values.remove(index);
                    }

                    let name = &metric_name.to_owned();
                    self.try_set_label_names(
                        name,
                        LabelNames::new(name, metric_type.clone(), actual_label_names),
                    )?;

                    let metric_name = metric_name.trim_end_matches(suffix);
                    if self.name.is_some() && self.name.as_ref().unwrap() != metric_name {
                        return Err(OpenMetricsParseError::InvalidMetric(format!(
                            "Invalid Name in metric family: {} != {}",
                            metric_name,
                            self.name.as_ref().unwrap()
                        )));
                    } else if self.name.is_none() {
                        self.name = Some(metric_name.to_owned());
                    }

                    let (existing_metric, created) = match self
                        .get_metric_by_labelset_mut(&actual_label_values)
                    {
                        Some(metric) => {
                            println!("Processing: {:?} {:?}", metric, timestamp);
                            match (metric.timestamp.as_ref(), timestamp.as_ref()) {
                                (Some(metric_timestamp), Some(timestamp)) if timestamp < metric_timestamp => return Err(OpenMetricsParseError::InvalidMetric(format!("Timestamps went backwarts in family - saw {} and then saw{}", metric_timestamp, timestamp))),
                                (Some(_), None) | (None, Some(_)) => return Err(OpenMetricsParseError::InvalidMetric(format!("Missing timestamp in family (one metric had a timestamp, another didn't)"))),
                                (Some(metric_timestamp), Some(timestamp)) if timestamp >= metric_timestamp && !metric_type.can_have_multiple_lines() => return Ok(()),
                                _ => (metric, false)
                            }
                        }
                        None => {
                            let new_metric = self
                                .family_type
                                .as_ref()
                                .unwrap_or(&OpenMetricsType::Unknown)
                                .get_type_value();
                            self.add_metric(MetricMarshal::new(
                                actual_label_values.clone(),
                                timestamp,
                                new_metric,
                            ));
                            (
                                self.get_metric_by_labelset_mut(&actual_label_values)
                                    .unwrap(),
                                true,
                            )
                        }
                    };

                    return action.0(
                        existing_metric,
                        metric_value,
                        label_names,
                        label_values,
                        exemplar,
                        created,
                    );
                }
            }
        }

        return Err(OpenMetricsParseError::InvalidMetric(format!(
            "Found weird metric name for type ({:?}): {}",
            metric_type, metric_name
        )));
    }
}

impl<TypeSet> MetricFamilyMarshal<TypeSet>
where
    TypeSet: Default + Clone + fmt::Debug + MetricsType,
{
    fn empty() -> MetricFamilyMarshal<TypeSet> {
        return MetricFamilyMarshal::<TypeSet> {
            name: None,
            label_names: None,
            family_type: None,
            help: None,
            unit: None,
            metrics: Vec::new(),
        };
    }

    fn validate(&self) -> Result<(), OpenMetricsParseError> {
        for metric in self.metrics.iter() {
            metric.validate(self)?;
        }

        return Ok(());
    }

    fn get_metric_by_labelset_mut(
        &mut self,
        label_values: &Vec<String>,
    ) -> Option<&mut MetricMarshal> {
        return self
            .metrics
            .iter_mut()
            .find(|m| &m.label_values == label_values);
    }

    pub fn add_metric(&mut self, metric: MetricMarshal) {
        self.metrics.push(metric);
    }

    fn try_set_label_names(
        &mut self,
        sample_name: &String,
        names: LabelNames<TypeSet>,
    ) -> Result<(), OpenMetricsParseError> {
        if self.label_names.is_none() {
            self.label_names = Some(names);
            return Ok(());
        }

        let old_names = self.label_names.as_ref().unwrap();
        if !old_names.matches(sample_name, &names) {
            return Err(OpenMetricsParseError::InvalidMetric(
                "Labels in metrics have different label sets".to_owned(),
            ));
        }

        return Ok(());
    }

    fn set_or_test_name(&mut self, name: String) -> Result<(), OpenMetricsParseError> {
        let name = Some(name);
        if self.name.is_some() && self.name != name {
            return Err(OpenMetricsParseError::InvalidMetric(format!(
                "Invalid metric name in family. Family name is {}, but got a metric called {}",
                self.name.as_ref().unwrap(),
                name.as_ref().unwrap()
            )));
        }

        self.name = name;
        return Ok(());
    }

    fn try_add_help(&mut self, help: String) -> Result<(), OpenMetricsParseError> {
        if self.help.is_some() {
            return Err(OpenMetricsParseError::InvalidMetric(format!(
                "Got two help lines in the same metric family"
            )));
        }

        self.help = Some(help);

        return Ok(());
    }

    fn try_add_unit(&mut self, unit: String) -> Result<(), OpenMetricsParseError> {
        if self.unit.is_some() {
            return Err(OpenMetricsParseError::InvalidMetric(format!(
                "Got two unit lines in the same metric family"
            )));
        }

        if !self
            .family_type
            .as_ref()
            .map(|s| s.clone())
            .unwrap_or_default()
            .is_allowed_units()
        {
            return Err(OpenMetricsParseError::InvalidMetric(format!(
                "{:?} metrics can't have units",
                self.family_type
            )));
        }

        self.unit = Some(unit);

        return Ok(());
    }

    fn try_add_type(&mut self, family_type: TypeSet) -> Result<(), OpenMetricsParseError> {
        if self.family_type.is_some() {
            return Err(OpenMetricsParseError::InvalidMetric(format!(
                "Got two type lines in the same metric family"
            )));
        }

        self.family_type = Some(family_type);

        return Ok(());
    }
}

impl<TypeSet> From<MetricFamilyMarshal<TypeSet>> for MetricFamily<TypeSet, OpenMetricsValue>
where
    TypeSet: Default,
{
    fn from(marshal: MetricFamilyMarshal<TypeSet>) -> Self {
        assert!(marshal.name.is_some());

        return MetricFamily {
            name: marshal.name.unwrap(),
            label_names: marshal
                .label_names
                .map(|names| names.names)
                .unwrap_or(Vec::new()),
            family_type: marshal.family_type.unwrap_or_default(),
            help: marshal.help,
            unit: marshal.unit,
            metrics: marshal.metrics.into_iter().map(|m| m.into()).collect(),
        };
    }
}

pub fn parse_openmetrics(
    exposition_bytes: &str,
) -> Result<MetricsExposition<OpenMetricsType, OpenMetricsValue>, OpenMetricsParseError> {
    use pest::iterators::Pair;

    fn parse_metric_descriptor(
        pair: Pair<Rule>,
        family: &mut MetricFamilyMarshal<OpenMetricsType>,
    ) -> Result<(), OpenMetricsParseError> {
        assert_eq!(pair.as_rule(), Rule::metricdescriptor);

        let mut descriptor = pair.into_inner();
        let descriptor_type = descriptor.next().unwrap();
        let metric_name = descriptor.next().unwrap().as_str().to_string();

        match descriptor_type.as_rule() {
            Rule::kw_help => {
                let help_text = descriptor.next().unwrap().as_str();
                family.set_or_test_name(metric_name)?;
                family.try_add_help(help_text.to_string())?;
            }
            Rule::kw_type => {
                let family_type = descriptor.next().unwrap().as_str();
                family.set_or_test_name(metric_name)?;
                family.try_add_type(OpenMetricsType::try_from(family_type)?)?;
            }
            Rule::kw_unit => {
                let unit = descriptor.next().unwrap().as_str();
                if family.name.is_none() || &metric_name != family.name.as_ref().unwrap() {
                    return Err(OpenMetricsParseError::InvalidMetric(
                        "UNIT metric name doesn't match family".to_owned(),
                    ));
                }
                let ty = family
                    .family_type
                    .as_ref()
                    .map(|t| t.clone())
                    .unwrap_or_default();
                println!("Can have units: {}", ty.is_allowed_units());
                family.try_add_unit(unit.to_string())?;
            }
            _ => unreachable!(),
        }

        return Ok(());
    }

    fn parse_exemplar(pair: Pair<Rule>) -> Result<Exemplar, OpenMetricsParseError> {
        let mut inner = pair.into_inner();

        let labels = inner.next().unwrap();
        assert_eq!(labels.as_rule(), Rule::labels);

        let labels = parse_labels(labels)?
            .into_iter()
            .map(|(a, b)| (a.to_owned(), b.to_owned()))
            .collect();

        let id = inner.next().unwrap().as_str();
        let id = match id.parse() {
            Ok(i) => i,
            Err(_) => {
                return Err(OpenMetricsParseError::InvalidMetric(format!(
                    "Exemplar value must be a number (got: {})",
                    id
                )))
            }
        };

        let timestamp = match inner.next() {
            Some(timestamp) => match timestamp.as_str().parse() {
                Ok(f) => Some(f),
                Err(_) => {
                    return Err(OpenMetricsParseError::InvalidMetric(format!(
                        "Exemplar timestamp must be a number (got: {})",
                        timestamp.as_str()
                    )))
                }
            },
            None => None,
        };

        return Ok(Exemplar::new(labels, id, timestamp));
    }

    fn parse_labels(pair: Pair<Rule>) -> Result<Vec<(&str, &str)>, OpenMetricsParseError> {
        assert_eq!(pair.as_rule(), Rule::labels);

        let mut label_pairs = pair.into_inner();
        let mut labels: Vec<(&str, &str)> = Vec::new();

        while label_pairs.peek().is_some() && label_pairs.peek().unwrap().as_rule() == Rule::label {
            let mut label = label_pairs.next().unwrap().into_inner();
            let name = label.next().unwrap().as_str();
            let value = label.next().unwrap().as_str();

            if labels.iter().find(|(n, _)| n == &name).is_some() {
                return Err(OpenMetricsParseError::InvalidMetric(format!(
                    "Found label `{}` twice in the same labelset",
                    name
                )));
            }

            labels.push((name, value));
        }

        labels.sort_by_key(|l| l.0);

        return Ok(labels);
    }

    fn parse_sample(
        pair: Pair<Rule>,
        family: &mut MetricFamilyMarshal<OpenMetricsType>,
    ) -> Result<(), OpenMetricsParseError> {
        assert_eq!(pair.as_rule(), Rule::sample);

        let mut descriptor = pair.into_inner();
        let metric_name = descriptor.next().unwrap().as_str();

        let labels = if descriptor.peek().unwrap().as_rule() == Rule::labels {
            parse_labels(descriptor.next().unwrap())?
        } else {
            Vec::new()
        };

        let (label_names, label_values) = {
            let mut names = Vec::new();
            let mut values = Vec::new();
            for (name, value) in labels.into_iter() {
                names.push(name.to_owned());
                values.push(value.to_owned());
            }

            (names, values)
        };

        let value = descriptor.next().unwrap().as_str();
        let value = match value.parse() {
            Ok(f) => MetricNumber::Int(f),
            Err(_) => match value.parse() {
                Ok(f) => MetricNumber::Float(f),
                Err(_) => {
                    return Err(OpenMetricsParseError::InvalidMetric(format!(
                        "Metric Value must be a number (got: {})",
                        value
                    )));
                }
            },
        };

        let mut timestamp = None;
        let mut exemplar = None;

        if descriptor.peek().is_some()
            && descriptor.peek().as_ref().unwrap().as_rule() == Rule::timestamp
        {
            timestamp = Some(descriptor.next().unwrap().as_str().parse().unwrap());
        }

        if descriptor.peek().is_some()
            && descriptor.peek().as_ref().unwrap().as_rule() == Rule::exemplar
        {
            exemplar = Some(parse_exemplar(descriptor.next().unwrap())?);
        }

        family.process_new_metric(
            metric_name,
            value,
            label_names,
            label_values,
            timestamp,
            exemplar,
        )?;

        return Ok(());
    }

    fn parse_metric_family(
        pair: Pair<Rule>,
    ) -> Result<MetricFamily<OpenMetricsType, OpenMetricsValue>, OpenMetricsParseError> {
        assert_eq!(pair.as_rule(), Rule::metricfamily);

        let mut metric_family = MetricFamilyMarshal::empty();

        for child in pair.into_inner() {
            match child.as_rule() {
                Rule::metricdescriptor => {
                    if metric_family.metrics.len() == 0 {
                        parse_metric_descriptor(child, &mut metric_family)?;
                    } else {
                        return Err(OpenMetricsParseError::InvalidMetric(
                            "Metric Descriptor after samples".to_owned(),
                        ));
                    }
                }
                Rule::sample => {
                    parse_sample(child, &mut metric_family)?;
                }
                _ => unreachable!(),
            }
        }

        metric_family.validate()?;

        return Ok(metric_family.into());
    }

    let exposition_marshal = OpenMetricsParser::parse(Rule::exposition, exposition_bytes)?
        .next()
        .unwrap();
    let mut exposition = MetricsExposition::new();

    assert_eq!(exposition_marshal.as_rule(), Rule::exposition);

    let mut found_eof = false;
    for span in exposition_marshal.into_inner() {
        match span.as_rule() {
            Rule::metricfamily => {
                let family = parse_metric_family(span)?;

                if exposition.families.contains_key(&family.name) {
                    return Err(OpenMetricsParseError::InvalidMetric(format!(
                        "Found a metric family called {}, after that family was finalised",
                        family.name
                    )));
                }

                exposition.families.insert(family.name.clone(), family);
            }
            Rule::kw_eof => {
                found_eof = true;

                if span.as_span().end() != exposition_bytes.len()
                    && !(span.as_span().end() == exposition_bytes.len() - 1
                        && exposition_bytes.chars().last() == Some('\n'))
                {
                    return Err(OpenMetricsParseError::InvalidMetric(format!(
                        "Found text after the EOF token"
                    )));
                }
            }
            _ => unreachable!(),
        }
    }

    if !found_eof {
        return Err(OpenMetricsParseError::InvalidMetric(format!(
            "Didn't find an EOF token"
        )));
    }

    return Ok(exposition);
}
