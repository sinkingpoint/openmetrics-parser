use std::{collections::HashMap, convert::TryFrom, fmt};

use pest::{Parser, iterators::Pairs};

#[derive(Parser)]
#[grammar = "openmetrics.pest"]
pub struct OpenMetricsParser;

type Timestamp = f64;

trait MarshalledMetricFamily {
    type Error;
    fn process_new_metric(&mut self, metric_name: &str, value: MetricNumber, label_names: Vec<String>, label_values: Vec<String>, timestamp: Option<Timestamp>, exemplar: Option<Exemplar>) -> Result<(), Self::Error>;
}

trait MetricType {
    fn can_have_exemplar(&self, metric_name: &str) -> bool;
    fn is_allowed_units(&self) -> bool;
    fn get_ignored_labels(&self, metric_name: &str) -> &[&str];
    fn get_type_value(&self) -> MetricValue;
    fn can_have_multiple_lines(&self) -> bool;
}

trait MetricTypeValue{}

#[derive(Debug, Clone)]
pub enum MetricNumber {
    Float(f64),
    Int(i64)
}

impl MetricNumber {
    fn expect_int(&self) -> Result<i64, OpenMetricsParseError> {
        match self {
            MetricNumber::Int(i) => Ok(*i),
            MetricNumber::Float(f) => {
                if f.round() == *f {
                    return Ok(*f as i64);
                }
                Err(OpenMetricsParseError::InvalidValue(format!("Not an Int: {}", f)))
            }
        }
    }

    fn as_f64(&self) -> f64 {
        match self {
            MetricNumber::Int(i) => *i as f64,
            MetricNumber::Float(f) => *f
        }
    }

    fn expect_uint(&self) -> Result<u64, OpenMetricsParseError> {
        let value = self.expect_int()?;
        if value < 0 {
            return Err(OpenMetricsParseError::InvalidValue(format!("Not a UINT: {}", value)));
        }
        return Ok(value as u64);
    }

    fn assert_non_negative(&self) -> Result<(), OpenMetricsParseError> {
        let (non_negative, value) = match self {
            MetricNumber::Float(f) => (*f >= 0., format!("{}", f)),
            MetricNumber::Int(i) => (*i >= 0, format!("{}", i))
        };

        if !non_negative {
            return Err(OpenMetricsParseError::InvalidValue(value));
        }

        return Ok(());
    }
}

#[derive(Debug)]
struct LabelNames<TypeSet> {
    names: Vec<String>,
    metric_type: TypeSet,
}

impl<TypeSet> LabelNames<TypeSet> where TypeSet: MetricType {
    fn new(sample_name: &String, metric_type: TypeSet, labels: Vec<String>) -> LabelNames<TypeSet> {
        let ignored_labels = TypeSet::get_ignored_labels(&metric_type, sample_name);
        let names = labels.into_iter().filter(|s| !ignored_labels.contains(&s.as_str())).collect();

        return LabelNames {
            names,
            metric_type
        }
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

#[derive(Debug, Default)]
pub struct CounterValue {
    value: Option<MetricNumber>,
    created: Option<Timestamp>,
    exemplar: Option<Exemplar>
}
impl MetricTypeValue for CounterValue{}

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
impl MetricTypeValue for HistogramValue{}

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

impl MetricTypeValue for SummaryValue{}

#[derive(Debug)]
pub enum MetricValue {
    Unknown(Option<MetricNumber>),
    Gauge(Option<MetricNumber>),
    Counter(CounterValue),
    Histogram(HistogramValue),
    StateSet(Option<MetricNumber>),
    GaugeHistogram(HistogramValue),
    Info,
    Summary(SummaryValue),
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

impl MetricType for OpenMetricsType {
    fn can_have_exemplar(&self, metric_name: &str) -> bool {
        match self {
            OpenMetricsType::Counter => metric_name.ends_with("_total"),
            OpenMetricsType::Histogram | OpenMetricsType::GaugeHistogram => metric_name.ends_with("_bucket"),
            _ => false
        }
    }

    fn get_ignored_labels(&self, metric_name: &str) -> &[&str] {
        match self {
            OpenMetricsType::Histogram | OpenMetricsType::GaugeHistogram if metric_name.ends_with("bucket") => &["le"],
            _ => &[]
        }
    }

    fn get_type_value(&self) -> MetricValue {
        match self {
            OpenMetricsType::Histogram=> MetricValue::Histogram(HistogramValue::default()),
            OpenMetricsType::GaugeHistogram => MetricValue::GaugeHistogram(HistogramValue::default()),
            OpenMetricsType::Counter => MetricValue::Counter(CounterValue::default()),
            OpenMetricsType::Unknown => MetricValue::Unknown(None),
            OpenMetricsType::Gauge => MetricValue::Gauge(None),
            OpenMetricsType::StateSet => MetricValue::StateSet(None),
            OpenMetricsType::Summary => MetricValue::Summary(SummaryValue::default()),
            OpenMetricsType::Info => MetricValue::Info,
        }
    }

    fn is_allowed_units(&self) -> bool {
        match self {
            OpenMetricsType::Counter | OpenMetricsType::Unknown | OpenMetricsType::Gauge => true,
            _ => false
        }
    }

    fn can_have_multiple_lines(&self) -> bool {
        match self {
            OpenMetricsType::Counter | OpenMetricsType::GaugeHistogram | OpenMetricsType::Histogram | OpenMetricsType::Summary => true,
            _ => false
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
            _ => Err(OpenMetricsParseError::InvalidType(value.to_owned()))
        }
    }
}

impl Default for OpenMetricsType {
    fn default() -> OpenMetricsType {
        return OpenMetricsType::Unknown;
    }
}

#[derive(Debug)]
struct MetricMarshal {
    label_values: Vec<String>,
    timestamp: Option<Timestamp>,
    value: MetricValue
}

impl MetricMarshal {
    fn new(label_values: Vec<String>, timestamp: Option<Timestamp>, value: MetricValue) -> MetricMarshal {
        return MetricMarshal {
            label_values,
            timestamp,
            value
        }
    }

    fn validate<Type>(&self, family: &MetricFamilyMarshal<Type>) -> Result<(), OpenMetricsParseError> where Type: fmt::Debug + Clone + Default {
        // All the labels are right
        if family.label_names.is_none() && self.label_values.len() != 0 || (family.label_names.as_ref().unwrap().names.len() != self.label_values.len()) {
            return Err(OpenMetricsParseError::InvalidMetric(format!("Metrics in family have different label sets: {:?} {:?}", &family.label_names, self.label_values)));
        }

        if family.unit.is_some() && family.metrics.len() == 0 {
            return Err(OpenMetricsParseError::InvalidMetric("Can't have metric with unit and no samples".to_owned()));
        }

        match &self.value {
            MetricValue::Histogram(histogram_value) | MetricValue::GaugeHistogram(histogram_value) => {
                let gauge_histogram = if let MetricValue::GaugeHistogram(_) = &self.value {
                    true
                }
                else {
                    false
                };

                if histogram_value.buckets.len() == 0 {
                    return Err(OpenMetricsParseError::InvalidMetric("Histograms must have at least one bucket".to_owned())); 
                }

                if histogram_value.buckets.iter().find(|b| b.upper_bound == f64::INFINITY).is_none() {
                    return Err(OpenMetricsParseError::InvalidMetric(format!("Histograms must have a +INF bucket: {:?}", histogram_value.buckets))); 
                }

                let buckets = &histogram_value.buckets;
                
                let has_negative_bucket = buckets.iter().find(|f| f.upper_bound < 0.).is_some();

                if has_negative_bucket {
                    if histogram_value.sum.is_some() && !gauge_histogram {
                        return Err(OpenMetricsParseError::InvalidMetric("Histograms cannot have a sum with a negative bucket".to_owned()));
                    }
                }
                else {
                    if histogram_value.sum.is_some() && histogram_value.sum.as_ref().unwrap().as_f64() < 0. {
                        return Err(OpenMetricsParseError::InvalidMetric("Histograms cannot have a negative sum without a negative bucket".to_owned()));
                    }
                }

                if histogram_value.sum.is_some() && histogram_value.count.is_none() {
                    return Err(OpenMetricsParseError::InvalidMetric("Count must be present if sum is present".to_owned()));
                }

                if histogram_value.sum.is_none() && histogram_value.count.is_some() {
                    return Err(OpenMetricsParseError::InvalidMetric("Sum must be present if count is present".to_owned()));
                }

                let mut last = f64::NEG_INFINITY;
                for bucket in buckets {
                    if bucket.count.as_f64() < last {
                        return Err(OpenMetricsParseError::InvalidMetric("Histograms must be cumulative".to_owned()));
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
    DuplicateHelp,
    DuplicateType,
    DuplicateUnit,
    DuplicateMetric,
    DuplicateLabel,
    InterwovenMetricFamily,
    InvalidType(String),
    InvalidLabel,
    InvalidValue(String),
    InvalidTimestamp(String),
    InvalidMetric(String),
    InvalidName,
    UnallowedExemplar,
    MissingBucketBound,
    TextAfterEOF,
    MissingEOF
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
            OpenMetricsParseError::DuplicateHelp => f.write_str("Cannot have multiple help lines for a single metric"),
            OpenMetricsParseError::DuplicateType => f.write_str("Cannot have multiple types for a single metric"),
            OpenMetricsParseError::DuplicateUnit => f.write_str("Cannot have multiple units for a single metric"),
            OpenMetricsParseError::DuplicateMetric => f.write_str("Cannot have multiple metric entries with the same labelset"),
            OpenMetricsParseError::InterwovenMetricFamily => f.write_str("Cannot interweave metric families"),
            OpenMetricsParseError::InvalidType(s) => f.write_str(format!("Invalid Type: {}", s).as_str()),
            OpenMetricsParseError::InvalidLabel => f.write_str("Invalid Label"),
            OpenMetricsParseError::InvalidValue(s) => f.write_str(format!("Invalid Value: {}", s).as_str()),
            OpenMetricsParseError::InvalidTimestamp(s) => f.write_str(format!("Invalid Timestamp: {}", s).as_str()),
            OpenMetricsParseError::UnallowedExemplar => f.write_str("Metric had an exemplar, but its type isn't allowed one"),
            OpenMetricsParseError::MissingBucketBound => f.write_str("Histogram bucket missing `le` label"),
            OpenMetricsParseError::InvalidName => f.write_str("Name doesn't match the rest of the family"),
            OpenMetricsParseError::DuplicateLabel => f.write_str("Can't have two labels with the same name"),
            OpenMetricsParseError::TextAfterEOF => f.write_str("Found text after the EOF token"),
            OpenMetricsParseError::MissingEOF => f.write_str("Didn't find the EOF token"),
            OpenMetricsParseError::InvalidMetric(s) => f.write_str(s)
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
    metrics: Vec<MetricMarshal>
}

struct MetricProcesser(Box<dyn Fn(&mut MetricMarshal, MetricNumber, Vec<String>, Vec<String>, Option<Exemplar>, bool) -> Result<(), OpenMetricsParseError>>);

impl MetricProcesser {
    fn new<F>(f: F) -> MetricProcesser
    where
        F: Fn(&mut MetricMarshal, MetricNumber, Vec<String>, Vec<String>, Option<Exemplar>, bool) -> Result<(), OpenMetricsParseError> + 'static,
    {
        MetricProcesser(Box::new(f))
    }
}


impl MarshalledMetricFamily for MetricFamilyMarshal<OpenMetricsType> {
    type Error = OpenMetricsParseError;

    fn process_new_metric(&mut self, metric_name: &str, metric_value: MetricNumber, label_names: Vec<String>, label_values: Vec<String>, timestamp: Option<Timestamp>, exemplar: Option<Exemplar>) -> Result<(), Self::Error> {
        let handlers = vec![
            (vec![OpenMetricsType::Histogram], vec![
                ("_bucket", vec!["le"], MetricProcesser::new(|existing_metric: &mut MetricMarshal, metric_value: MetricNumber, label_names: Vec<String>, label_values: Vec<String>, exemplar: Option<Exemplar>, _: bool| {
                    let bucket_bound: f64 = {
                        let bound_index = label_names.iter().position(|s| s == "le").unwrap();

                        let bound = &label_values[bound_index];
                        match bound.parse() {
                            Ok(f) => f,
                            Err(_) => {return Err(OpenMetricsParseError::InvalidValue(bound.to_owned()));}
                        }
                    };

                    println!("Processing Bucket: {}", bucket_bound);

                    let bucket = HistogramBucket { count: metric_value, upper_bound: bucket_bound, exemplar };

                    if let MetricValue::Histogram(value) = &mut existing_metric.value {
                        value.buckets.push(bucket);
                    }
                    else {
                        unreachable!();
                    }

                    return Ok(());
                })),
                ("_count", vec![], MetricProcesser::new(|existing_metric: &mut MetricMarshal, metric_value: MetricNumber, _: Vec<String>, _: Vec<String>, _: Option<Exemplar>, _: bool|{
                    if let MetricValue::Histogram(histogram_value) = &mut existing_metric.value {
                        match histogram_value.count {
                            Some(_) => {return Err(OpenMetricsParseError::DuplicateMetric);},
                            None => {histogram_value.count = Some(metric_value.expect_uint()?);},
                        };
                    }
                    else {
                        unreachable!();
                    }

                    return Ok(());
                })),
                ("_created", vec![], MetricProcesser::new(|existing_metric: &mut MetricMarshal, metric_value: MetricNumber, _: Vec<String>, _: Vec<String>, _: Option<Exemplar>, _: bool|{
                    if let MetricValue::Histogram(histogram_value) = &mut existing_metric.value {
                        match histogram_value.timestamp {
                            Some(_) => {return Err(OpenMetricsParseError::DuplicateMetric);},
                            None => {histogram_value.timestamp = Some(metric_value.as_f64());},
                        };
                    }
                    else {
                        unreachable!();
                    }

                    return Ok(());
                })),
                ("_sum", vec![], MetricProcesser::new(|existing_metric: &mut MetricMarshal, metric_value: MetricNumber, _: Vec<String>, _: Vec<String>, _: Option<Exemplar>, _: bool|{
                    if let MetricValue::Histogram(histogram_value) = &mut existing_metric.value {
                        metric_value.assert_non_negative()?;

                        if histogram_value.sum.is_some() {
                            return Err(OpenMetricsParseError::DuplicateMetric);
                        }

                        histogram_value.sum = Some(metric_value);

                        return Ok(());
                    }
                    else {
                        unreachable!();
                    }
                })),
            ]),
            (vec![OpenMetricsType::GaugeHistogram], vec![
                ("_bucket", vec!["le"], MetricProcesser::new(|existing_metric: &mut MetricMarshal, metric_value: MetricNumber, label_names: Vec<String>, label_values: Vec<String>, exemplar: Option<Exemplar>, _: bool| {
                    let bucket_bound: f64 = {
                        let bound_index = label_names.iter().position(|s| s == "le").unwrap();

                        let bound = &label_values[bound_index];
                        match bound.parse() {
                            Ok(f) => f,
                            Err(_) => {return Err(OpenMetricsParseError::InvalidValue(bound.to_owned()));}
                        }
                    };

                    println!("Processing Bucket: {}", bucket_bound);

                    let bucket = HistogramBucket { count: metric_value, upper_bound: bucket_bound, exemplar };

                    if let MetricValue::GaugeHistogram(value) = &mut existing_metric.value {
                        value.buckets.push(bucket);
                    }
                    else {
                        unreachable!();
                    }

                    return Ok(());
                })),
                ("_gcount", vec![], MetricProcesser::new(|existing_metric: &mut MetricMarshal, metric_value: MetricNumber, _: Vec<String>, _: Vec<String>, _: Option<Exemplar>, _: bool|{
                    if let MetricValue::GaugeHistogram(histogram_value) = &mut existing_metric.value {
                        match histogram_value.count {
                            Some(_) => {return Err(OpenMetricsParseError::DuplicateMetric);},
                            None => {histogram_value.count = Some(metric_value.expect_uint()?);},
                        };
                    }
                    else {
                        unreachable!();
                    }

                    return Ok(());
                })),
                ("_gsum", vec![], MetricProcesser::new(|existing_metric: &mut MetricMarshal, metric_value: MetricNumber, _: Vec<String>, _: Vec<String>, _: Option<Exemplar>, _: bool|{
                    if let MetricValue::GaugeHistogram(histogram_value) = &mut existing_metric.value {
                        if histogram_value.sum.is_some() {
                            return Err(OpenMetricsParseError::DuplicateMetric);
                        }

                        histogram_value.sum = Some(metric_value);

                        return Ok(());
                    }
                    else {
                        unreachable!();
                    }
                })),
            ]),
            (vec![OpenMetricsType::Counter], vec![
                ("_total", vec![], MetricProcesser::new(|existing_metric: &mut MetricMarshal, metric_value: MetricNumber, _: Vec<String>, _: Vec<String>, _: Option<Exemplar>, _: bool|{
                    if let MetricValue::Counter(counter_value) = &mut existing_metric.value {
                        if counter_value.value.is_some() {
                            return Err(OpenMetricsParseError::DuplicateMetric);
                        }

                        metric_value.assert_non_negative()?;

                        counter_value.value = Some(metric_value);
                    }
                    else {
                        unreachable!();
                    }

                    return Ok(());
                })),
                ("_created", vec![], MetricProcesser::new(|existing_metric: &mut MetricMarshal, metric_value: MetricNumber, _: Vec<String>, _: Vec<String>, _: Option<Exemplar>, _: bool|{
                    if let MetricValue::Counter(counter_value) = &mut existing_metric.value {
                        if counter_value.created.is_some() {
                            return Err(OpenMetricsParseError::DuplicateMetric);
                        }

                        counter_value.created = Some(metric_value.as_f64());
                        return Ok(());
                    }
                    else {
                        unreachable!();
                    }
                }))
            ]),
            (vec![OpenMetricsType::Gauge], vec![
                ("", vec![], MetricProcesser::new(|existing_metric: &mut MetricMarshal, metric_value: MetricNumber, _: Vec<String>, _: Vec<String>, _: Option<Exemplar>, _: bool|{
                    if let MetricValue::Gauge(gauge_value) = &mut existing_metric.value {
                        if gauge_value.is_some() {
                            return Err(OpenMetricsParseError::DuplicateMetric);
                        }

                        existing_metric.value = MetricValue::Gauge(Some(metric_value));
                    }
                    else {
                        unreachable!();
                    }

                    return Ok(());
                }))
            ]),
            (vec![OpenMetricsType::StateSet], vec![
                ("", vec![], MetricProcesser::new(|existing_metric: &mut MetricMarshal, metric_value: MetricNumber, _: Vec<String>, _: Vec<String>, _: Option<Exemplar>, _: bool|{
                    if let MetricValue::StateSet(stateset_value) = &mut existing_metric.value {
                        if stateset_value.is_some() {
                            return Err(OpenMetricsParseError::DuplicateMetric);
                        }

                        if existing_metric.label_values.len() == 0 {
                            return Err(OpenMetricsParseError::InvalidMetric(format!("Stateset must have labels")));
                        }

                        if metric_value.as_f64() != 0. && metric_value.as_f64() != 1. {
                            return Err(OpenMetricsParseError::InvalidMetric(format!("Stateset value must be 0 or 1 (got: {})", metric_value.as_f64())));
                        }

                        existing_metric.value = MetricValue::StateSet(Some(metric_value));
                    }
                    else {
                        unreachable!();
                    }

                    return Ok(());
                }))
            ]),
            (vec![OpenMetricsType::Unknown], vec![
                ("", vec![], MetricProcesser::new(|existing_metric: &mut MetricMarshal, metric_value: MetricNumber, _: Vec<String>, _: Vec<String>, _: Option<Exemplar>, _: bool|{
                    if let MetricValue::Unknown(unknown_value) = &mut existing_metric.value {
                        if unknown_value.is_some() {
                            return Err(OpenMetricsParseError::DuplicateMetric);
                        }

                        existing_metric.value = MetricValue::Unknown(Some(metric_value));
                    }
                    else {
                        unreachable!();
                    }

                    return Ok(());
                }))
            ]),
            (vec![OpenMetricsType::Info], vec![
                ("_info", vec![], MetricProcesser::new(|_: &mut MetricMarshal, metric_value: MetricNumber, _: Vec<String>, _: Vec<String>, _: Option<Exemplar>, created: bool|{
                    let number = metric_value.expect_uint()?;
                    if number != 1 {
                        return Err(OpenMetricsParseError::InvalidValue(format!("{}", number)));
                    }

                    if !created {
                        return Err(OpenMetricsParseError::DuplicateMetric);
                    }

                    return Ok(());
                }))
            ]),
            (vec![OpenMetricsType::Summary], vec![
                ("_count", vec![], MetricProcesser::new(|existing_metric: &mut MetricMarshal, metric_value: MetricNumber, _: Vec<String>, _: Vec<String>, _: Option<Exemplar>, _: bool|{
                    if let MetricValue::Summary(summary_value) = &mut existing_metric.value {
                        if let None = summary_value.count {
                            summary_value.count = Some(metric_value.expect_uint()?);
                        }
                        else {
                            return Err(OpenMetricsParseError::DuplicateMetric);
                        }
                    }
                    else {
                        unreachable!();
                    }

                    return Ok(());
                })),
                ("_sum", vec![], MetricProcesser::new(|existing_metric: &mut MetricMarshal, metric_value: MetricNumber, _: Vec<String>, _: Vec<String>, _: Option<Exemplar>, _: bool|{
                    metric_value.assert_non_negative()?;
                    if let MetricValue::Summary(summary_value) = &mut existing_metric.value {
                        if let None = summary_value.sum {
                            summary_value.sum = Some(metric_value);
                            return Ok(());
                        }
                        else {
                            return Err(OpenMetricsParseError::DuplicateMetric);
                        }
                    }
                    else {
                        unreachable!();
                    }
                })),
                ("", vec!["quantile"], MetricProcesser::new(|existing_metric: &mut MetricMarshal, metric_value: MetricNumber, label_names: Vec<String>, label_values: Vec<String>, _: Option<Exemplar>, _: bool|{
                    let value = metric_value.as_f64();
                    if !value.is_nan() && value < 0. {
                        return Err(OpenMetricsParseError::InvalidMetric("Summary quantiles can't be negative".to_owned()));
                    }

                    let bucket_bound: f64 = {
                        let bound_index = label_names.iter().position(|s| s == "quantile").unwrap();
                        let bound = &label_values[bound_index];

                        match bound.parse() {
                            Ok(f) => f,
                            Err(_) => {return Err(OpenMetricsParseError::InvalidValue(bound.to_owned()));}
                        }
                    };

                    if bucket_bound < 0. || bucket_bound > 1. || bucket_bound.is_nan() {
                        return Err(OpenMetricsParseError::InvalidValue(format!("{}", bucket_bound)));
                    }

                    let quantile = Quantile{quantile: bucket_bound, value: metric_value};

                    if let MetricValue::Summary(summary_value) = &mut existing_metric.value {
                        summary_value.quantiles.push(quantile);
                    }
                    else {
                        unreachable!();
                    }
                    
                    return Ok(());
                }))
            ]),
        ];

        let metric_type = self.family_type.as_ref().map(|s| s.clone()).unwrap_or_default();

        if !metric_type.can_have_exemplar(metric_name) && exemplar.is_some() {
            return Err(OpenMetricsParseError::UnallowedExemplar);
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
                            return Err(OpenMetricsParseError::MissingBucketBound);
                        }

                        let index = actual_label_names.iter().position(|s| s == label).unwrap();

                        actual_label_names.remove(index);
                        actual_label_values.remove(index);
                    }


                    let name = &metric_name.to_owned();
                    self.try_set_label_names(name, LabelNames::new(name, metric_type.clone(), actual_label_names))?;

                    let metric_name = metric_name.trim_end_matches(suffix);
                    if self.name.is_some() && self.name.as_ref().unwrap() != metric_name {
                        return Err(OpenMetricsParseError::InvalidMetric(format!("Invalid Name in metric family: {} != {}", metric_name, self.name.as_ref().unwrap())));
                    }
                    else if self.name.is_none() {
                        self.name = Some(metric_name.to_owned());
                    }

                    let (existing_metric, created) = match self.get_metric_by_labelset_mut(&actual_label_values) {
                        Some(metric) => {
                            println!("Processing: {:?} {:?}", metric, timestamp);
                            match (metric.timestamp.as_ref(), timestamp.as_ref()) {
                                (Some(metric_timestamp), Some(timestamp)) if timestamp < metric_timestamp => return Err(OpenMetricsParseError::InvalidTimestamp(String::new())),
                                (Some(_), None) | (None, Some(_)) => return Err(OpenMetricsParseError::InvalidTimestamp(String::new())),
                                (Some(metric_timestamp), Some(timestamp)) if timestamp >= metric_timestamp && !metric_type.can_have_multiple_lines() => return Ok(()),
                                _ => (metric, false)
                            }
                        },
                        None => {
                            let new_metric = self.family_type.as_ref().unwrap_or(&OpenMetricsType::Unknown).get_type_value();
                            self.add_metric(MetricMarshal::new(actual_label_values.clone(), timestamp, new_metric));
                            (self.get_metric_by_labelset_mut(&actual_label_values).unwrap(), true)
                        }
                    };

                    return action.0(existing_metric, metric_value, label_names, label_values, exemplar, created);
                }
            }
        }

        return Err(OpenMetricsParseError::InvalidMetric(format!("Found weird metric name for type ({:?}): {}", metric_type, metric_name)));
    }
}

impl<TypeSet> MetricFamilyMarshal<TypeSet> where TypeSet: Default + Clone + fmt::Debug + MetricType {
    fn empty() -> MetricFamilyMarshal<TypeSet> {
        return MetricFamilyMarshal::<TypeSet> {
            name: None,
            label_names: None,
            family_type: None,
            help: None,
            unit: None,
            metrics: Vec::new()
        }
    }

    fn validate(&self) -> Result<(), OpenMetricsParseError>{
        for metric in self.metrics.iter() {
            metric.validate(self)?;
        }

        return Ok(());
    }

    fn get_metric_by_labelset_mut(&mut self, label_values: &Vec<String>) -> Option<&mut MetricMarshal> {
        return self.metrics.iter_mut().find(|m| &m.label_values == label_values);
    }

    pub fn add_metric(&mut self, metric: MetricMarshal) {
        self.metrics.push(metric);
    }

    fn try_set_label_names(&mut self, sample_name: &String, names: LabelNames<TypeSet>) -> Result<(), OpenMetricsParseError> {
        if self.label_names.is_none() {
            self.label_names = Some(names);
            return Ok(());
        }

        let old_names = self.label_names.as_ref().unwrap();
        if !old_names.matches(sample_name, &names) {
            return Err(OpenMetricsParseError::InvalidMetric("Labels in metrics have different label sets".to_owned()));
        }

        return Ok(());
    }

    fn set_or_test_name(&mut self, name: String) -> Result<(), OpenMetricsParseError> {
        let name = Some(name);
        if self.name.is_some() && self.name != name {
            return Err(OpenMetricsParseError::InterwovenMetricFamily);
        }

        self.name = name;
        return Ok(());
    }

    fn try_add_help(&mut self, help: String) -> Result<(), OpenMetricsParseError> {
        if self.help.is_some() {
            return Err(OpenMetricsParseError::DuplicateHelp);
        }

        self.help = Some(help);

        return Ok(());
    }

    fn try_add_unit(&mut self, unit: String) -> Result<(), OpenMetricsParseError> {
        if self.unit.is_some() {
            return Err(OpenMetricsParseError::DuplicateHelp);
        }

        if !self.family_type.as_ref().map(|s| s.clone()).unwrap_or_default().is_allowed_units() {
            return Err(OpenMetricsParseError::InvalidMetric(format!("{:?} metrics can't have units", self.family_type)));
        }

        self.unit = Some(unit);

        return Ok(());
    }

    fn try_add_type(&mut self, family_type: TypeSet) -> Result<(), OpenMetricsParseError> {
        if self.family_type.is_some() {
            return Err(OpenMetricsParseError::DuplicateHelp);
        }

        self.family_type = Some(family_type);

        return Ok(());
    }
}

#[derive(Debug)]
pub struct MetricFamily<TypeSet>  {
    pub name: String,
    pub label_names: Vec<String>,
    pub family_type: TypeSet,
    pub help: Option<String>,
    pub unit: Option<String>,
    metrics: Vec<MetricMarshal>
}

impl<TypeSet> From<MetricFamilyMarshal<TypeSet>> for MetricFamily<TypeSet> where TypeSet: Default {
    fn from(marshal: MetricFamilyMarshal<TypeSet>) -> Self {
        assert!(marshal.name.is_some());

        return MetricFamily {
            name: marshal.name.unwrap(),
            label_names: marshal.label_names.map(|names| names.names).unwrap_or(Vec::new()),
            family_type: marshal.family_type.unwrap_or_default(),
            help: marshal.help,
            unit: marshal.unit,
            metrics: marshal.metrics
        }
    }
}

#[derive(Debug)]
pub struct MetricsExposition<TypeSet> {
    families: HashMap<String, MetricFamily<TypeSet>>
}

impl<TypeSet> MetricsExposition<TypeSet>{
    fn new() -> MetricsExposition<TypeSet> {
        return MetricsExposition::<TypeSet> {
            families: HashMap::new()
        }
    }
}

#[derive(Debug, Clone)]
pub struct Exemplar {
    labels: HashMap<String, String>,
    timestamp: Option<f64>,
    id: f64
}

impl Exemplar {
    fn new(labels: HashMap<String, String>, id: f64, timestamp: Option<f64>) -> Exemplar {
        return Exemplar {
            labels,
            id,
            timestamp
        }
    }
}

pub fn parse_openmetrics(exposition_bytes: &str) -> Result<MetricsExposition<OpenMetricsType>, OpenMetricsParseError> {
    use pest::iterators::Pair;

    fn parse_metric_descriptor(pair: Pair<Rule>, family: &mut MetricFamilyMarshal<OpenMetricsType>) -> Result<(), OpenMetricsParseError> {
        assert_eq!(pair.as_rule(), Rule::metricdescriptor);

        let mut descriptor = pair.into_inner();
        let descriptor_type = descriptor.next().unwrap();
        let metric_name = descriptor.next().unwrap().as_str().to_string();

        match descriptor_type.as_rule() {
            Rule::kw_help => {
                let help_text = descriptor.next().unwrap().as_str();
                family.set_or_test_name(metric_name)?;
                family.try_add_help(help_text.to_string())?;
            },
            Rule::kw_type => {
                let family_type = descriptor.next().unwrap().as_str();
                family.set_or_test_name(metric_name)?;
                family.try_add_type(OpenMetricsType::try_from(family_type)?)?;
            },
            Rule::kw_unit => {
                let unit = descriptor.next().unwrap().as_str();
                if family.name.is_none() || &metric_name != family.name.as_ref().unwrap() {
                    return Err(OpenMetricsParseError::InvalidMetric("UNIT metric name doesn't match family".to_owned()));
                }
                let ty = family.family_type.as_ref().map(|t| t.clone()).unwrap_or_default();
                println!("Can have units: {}", ty.is_allowed_units());
                family.try_add_unit(unit.to_string())?;
            }
            _ => unreachable!()
        }

        return Ok(());
    }

    fn parse_exemplar(pair: Pair<Rule>) -> Result<Exemplar, OpenMetricsParseError> {
        let mut inner = pair.into_inner();

        let labels = inner.next().unwrap();
        assert_eq!(labels.as_rule(), Rule::labels);

        let labels = parse_labels(labels)?.into_iter().map(|(a, b)| (a.to_owned(), b.to_owned())).collect();

        let id = inner.next().unwrap().as_str();
        let id = match id.parse() {
            Ok(i) => i,
            Err(_) => return Err(OpenMetricsParseError::InvalidTimestamp(id.to_owned()))
        };

        let timestamp = match inner.next() {
            Some(timestamp) => {
                match timestamp.as_str().parse() {
                    Ok(f) => Some(f),
                    Err(_) => return Err(OpenMetricsParseError::InvalidTimestamp(timestamp.as_str().to_owned()))
                }
            },
            None => None
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
                return Err(OpenMetricsParseError::DuplicateLabel);
            }

            labels.push((name, value));
        }

        labels.sort_by_key(|l| l.0);

        return Ok(labels);
    }

    fn parse_sample(pair: Pair<Rule>, family: &mut MetricFamilyMarshal<OpenMetricsType>) -> Result<(), OpenMetricsParseError> {
        assert_eq!(pair.as_rule(), Rule::sample);

        let mut descriptor = pair.into_inner();
        let metric_name = descriptor.next().unwrap().as_str();

        let labels = if descriptor.peek().unwrap().as_rule() == Rule::labels {
            parse_labels(descriptor.next().unwrap())?
        }
        else {
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
            Err(_) => {
                match value.parse() {
                    Ok(f) => MetricNumber::Float(f),
                    Err(_) => {return Err(OpenMetricsParseError::InvalidValue(value.to_owned()));}
                }
            }
        };

        let mut timestamp = None;
        let mut exemplar = None;

        if descriptor.peek().is_some() && descriptor.peek().as_ref().unwrap().as_rule() == Rule::timestamp {
            timestamp = Some(descriptor.next().unwrap().as_str().parse().unwrap());
        }

        if descriptor.peek().is_some() && descriptor.peek().as_ref().unwrap().as_rule() == Rule::exemplar {
            exemplar = Some(parse_exemplar(descriptor.next().unwrap())?);
        }

        family.process_new_metric(metric_name, value, label_names, label_values, timestamp, exemplar)?;

        return Ok(());
    }

    fn parse_metric_family(pair: Pair<Rule>) -> Result<MetricFamily<OpenMetricsType>, OpenMetricsParseError> {
        assert_eq!(pair.as_rule(), Rule::metricfamily);

        let mut metric_family = MetricFamilyMarshal::empty();

        for child in pair.into_inner() {
            match child.as_rule() {
                Rule::metricdescriptor => {
                    if metric_family.metrics.len() == 0 {
                        parse_metric_descriptor(child, &mut metric_family)?;
                    }
                    else {
                        return Err(OpenMetricsParseError::InvalidMetric("Metric Descriptor after samples".to_owned()));
                    }
                },
                Rule::sample => {
                    parse_sample(child, &mut metric_family)?;
                },
                _ => unreachable!()
            }
        }

        metric_family.validate()?;

        return Ok(metric_family.into());
    }

    let exposition_marshal = OpenMetricsParser::parse(Rule::exposition, exposition_bytes)?.next().unwrap();
    let mut exposition = MetricsExposition::new();

    assert_eq!(exposition_marshal.as_rule(), Rule::exposition);

    let mut found_eof = false;
    for span in exposition_marshal.into_inner() {
        match span.as_rule() {
            Rule::metricfamily => {
                let family = parse_metric_family(span)?;

                if exposition.families.contains_key(&family.name) {
                    return Err(OpenMetricsParseError::InterwovenMetricFamily);
                }

                exposition.families.insert(family.name.clone(), family);
            },
            Rule::kw_eof => {
                found_eof = true;

                if span.as_span().end() != exposition_bytes.len() && !(span.as_span().end() == exposition_bytes.len() - 1 && exposition_bytes.chars().last() == Some('\n')) {
                    return Err(OpenMetricsParseError::TextAfterEOF);
                }
            },
            _ => unreachable!()
        }
    }

    if !found_eof {
        return Err(OpenMetricsParseError::MissingEOF);
    }

    return Ok(exposition);
}