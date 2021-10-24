use std::fmt;

use crate::{
    CounterValue, Exemplar, HistogramValue, MetricNumber, ParseError, PrometheusCounterValue,
    SummaryValue, Timestamp,
};

use super::MetricsType;

#[derive(Debug)]
pub enum MetricValueMarshal {
    Unknown(Option<MetricNumber>),
    Gauge(Option<MetricNumber>),
    Counter(CounterValueMarshal),
    Histogram(HistogramValue),
    StateSet(Option<MetricNumber>),
    GaugeHistogram(HistogramValue),
    Info,
    Summary(SummaryValue),
}

#[derive(Debug, Default)]
pub struct CounterValueMarshal {
    pub value: Option<MetricNumber>,
    pub created: Option<Timestamp>,
    pub exemplar: Option<Exemplar>,
}

impl From<CounterValueMarshal> for CounterValue {
    fn from(s: CounterValueMarshal) -> CounterValue {
        CounterValue {
            value: s.value.unwrap(),
            created: s.created,
            exemplar: s.exemplar,
        }
    }
}

impl From<CounterValueMarshal> for PrometheusCounterValue {
    fn from(s: CounterValueMarshal) -> PrometheusCounterValue {
        PrometheusCounterValue {
            value: s.value.unwrap(),
            exemplar: s.exemplar,
        }
    }
}

#[derive(Debug)]
pub struct MetricFamilyMarshal<T>
where
    T: MetricsType,
{
    pub name: Option<String>,
    pub label_names: Option<LabelNames<T>>,
    pub family_type: Option<T>,
    pub help: Option<String>,
    pub unit: Option<String>,
    pub metrics: Vec<MetricMarshal>,
    pub seen_label_sets: Vec<Vec<String>>,
    pub current_label_set: Option<Vec<String>>,
}

impl<T> MetricFamilyMarshal<T>
where
    T: MetricsType + Clone + Default + fmt::Debug,
{
    pub fn empty() -> MetricFamilyMarshal<T> {
        MetricFamilyMarshal {
            name: None,
            label_names: None,
            family_type: None,
            help: None,
            unit: None,
            metrics: Vec::new(),
            seen_label_sets: Vec::new(),
            current_label_set: None,
        }
    }

    pub fn get_metric_by_labelset_mut(
        &mut self,
        label_values: &[String],
    ) -> Option<&mut MetricMarshal> {
        return self
            .metrics
            .iter_mut()
            .find(|m| m.label_values == label_values);
    }

    pub fn add_metric(&mut self, metric: MetricMarshal) {
        self.metrics.push(metric);
    }

    pub fn try_set_label_names(
        &mut self,
        sample_name: &str,
        names: LabelNames<T>,
    ) -> Result<(), ParseError> {
        if self.label_names.is_none() {
            self.label_names = Some(names);
            return Ok(());
        }

        let old_names = self.label_names.as_ref().unwrap();
        if !old_names.matches(sample_name, &names) {
            return Err(ParseError::InvalidMetric(
                "Labels in metrics have different label sets".to_owned(),
            ));
        }

        Ok(())
    }

    pub fn set_or_test_name(&mut self, name: String) -> Result<(), ParseError> {
        let name = Some(name);
        if self.name.is_some() && self.name != name {
            return Err(ParseError::InvalidMetric(format!(
                "Invalid metric name in family. Family name is {}, but got a metric called {}",
                self.name.as_ref().unwrap(),
                name.as_ref().unwrap()
            )));
        }

        self.name = name;
        Ok(())
    }

    pub fn try_add_help(&mut self, help: String) -> Result<(), ParseError> {
        if self.help.is_some() {
            return Err(ParseError::InvalidMetric(
                "Got two help lines in the same metric family".to_string(),
            ));
        }

        self.help = Some(help);

        Ok(())
    }

    pub fn try_add_unit(&mut self, unit: String) -> Result<(), ParseError> {
        if unit.is_empty() {
            return Ok(());
        }

        if self.unit.is_some() {
            return Err(ParseError::InvalidMetric(
                "Got two unit lines in the same metric family".to_string(),
            ));
        }

        if !self
            .family_type
            .as_ref()
            .cloned()
            .unwrap_or_default()
            .can_have_units()
        {
            return Err(ParseError::InvalidMetric(format!(
                "{:?} metrics can't have units",
                self.family_type
            )));
        }

        self.unit = Some(unit);

        Ok(())
    }

    pub fn try_add_type(&mut self, family_type: T) -> Result<(), ParseError> {
        if self.family_type.is_some() {
            return Err(ParseError::InvalidMetric(
                "Got two type lines in the same metric family".to_string(),
            ));
        }

        self.family_type = Some(family_type);

        Ok(())
    }
}

#[derive(Debug)]
pub struct LabelNames<T>
where
    T: MetricsType,
{
    pub names: Vec<String>,
    pub metric_type: T,
}

impl<T> LabelNames<T>
where
    T: MetricsType,
{
    pub fn new(sample_name: &str, metric_type: T, labels: Vec<String>) -> LabelNames<T> {
        let ignored_labels = <T>::get_ignored_labels(&metric_type, sample_name);
        let names = labels
            .into_iter()
            .filter(|s| !ignored_labels.contains(&s.as_str()))
            .collect();

        LabelNames { names, metric_type }
    }

    pub fn matches(&self, sample_name: &str, other_labels: &LabelNames<T>) -> bool {
        let ignored_labels = <T>::get_ignored_labels(&self.metric_type, sample_name);
        for name in self.names.iter() {
            if !ignored_labels.contains(&name.as_str()) && !other_labels.names.contains(name) {
                return false;
            }
        }

        true
    }
}

#[derive(Debug)]
pub struct MetricMarshal {
    pub label_values: Vec<String>,
    pub timestamp: Option<Timestamp>,
    pub value: MetricValueMarshal,
}

impl MetricMarshal {
    pub fn new(
        label_values: Vec<String>,
        timestamp: Option<Timestamp>,
        value: MetricValueMarshal,
    ) -> MetricMarshal {
        MetricMarshal {
            label_values,
            timestamp,
            value,
        }
    }
}

pub struct MetricProcesser(pub Box<MetricProccessFunc>);

type MetricProccessFunc = dyn Fn(
    &mut MetricMarshal,
    MetricNumber,
    Vec<String>,
    Vec<String>,
    Option<Exemplar>,
    bool,
) -> Result<(), ParseError>;

impl MetricProcesser {
    pub fn new<F>(f: F) -> MetricProcesser
    where
        F: Fn(
                &mut MetricMarshal,
                MetricNumber,
                Vec<String>,
                Vec<String>,
                Option<Exemplar>,
                bool,
            ) -> Result<(), ParseError>
            + 'static,
    {
        MetricProcesser(Box::new(f))
    }
}
