use std::fmt;

use crate::{Exemplar, MetricNumber, ParseError, Timestamp};

use super::{MetricFamilyMarshal, MetricValueMarshal};

pub trait MetricsType {
    fn can_have_exemplar(&self, metric_name: &str) -> bool;
    fn can_have_units(&self) -> bool;
    fn can_have_multiple_lines(&self) -> bool;
    fn get_ignored_labels(&self, metric_name: &str) -> &[&str];
    fn get_type_value(&self) -> MetricValueMarshal;
}

pub trait MarshalledMetricFamily {
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

    fn validate(&self) -> Result<(), ParseError>;
}

pub trait MarshalledMetric<T> where T: MetricsType {
    fn validate(&self, family: &MetricFamilyMarshal<T>) -> Result<(), ParseError>;
}

pub trait RenderableMetricValue {
    fn render(&self, f: &mut fmt::Formatter<'_>, metric_name: &str, timestamp: Option<&Timestamp>, label_names: &[&str], label_values: &[&str]) -> fmt::Result;
}