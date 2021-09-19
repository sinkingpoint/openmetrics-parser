use std::{collections::{HashMap, HashSet}, convert::TryFrom, fmt, fs};

use pest::Parser;

#[derive(Parser)]
#[grammar = "openmetrics.pest"]
pub struct OpenMetricsParser;

type MetricValue = f64;

#[derive(Debug)]
pub enum OpenMetricsType {
    Counter,
    Gauge,
    Histogram,
    GaugeHistogram,
    StatefulSet,
    Summary,
    Info,
    Unknown
}

impl TryFrom<&str> for OpenMetricsType {
    type Error = OpenMetricsParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "counter" => Ok(OpenMetricsType::Counter),
            "gauge" => Ok(OpenMetricsType::Gauge),
            "histogram" => Ok(OpenMetricsType::Histogram),
            "gaugehistogram" => Ok(OpenMetricsType::GaugeHistogram),
            "statefulset" => Ok(OpenMetricsType::StatefulSet),
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
struct MetricMarshall {
    label_values: Vec<String>,
    value: MetricValue
}

impl MetricMarshall {
    fn new(label_values: Vec<String>, value: MetricValue) -> MetricMarshall {
        return MetricMarshall {
            label_values,
            value
        }
    }
}

#[derive(Debug)]
pub enum OpenMetricsParseError {
    ParseError(pest::error::Error<Rule>),
    DuplicateHelp,
    DuplicateType,
    DuplicateUnit,
    InterwovenMetricFamily,
    InvalidType(String),
    InvalidLabel
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
            OpenMetricsParseError::InterwovenMetricFamily => f.write_str("Cannot interweave metric families"),
            OpenMetricsParseError::InvalidType(s) => f.write_str(format!("Invalid Type: {}", s).as_str()),
            OpenMetricsParseError::InvalidLabel => f.write_str("Invalid Label"),
        }
    }
}

#[derive(Debug)]
struct MetricFamilyMarshal<TypeSet> {
    name: Option<String>,
    label_names: Option<Vec<String>>,
    family_type: Option<TypeSet>,
    help: Option<String>,
    unit: Option<String>,
    metrics: Vec<MetricMarshall>
}

impl<TypeSet> MetricFamilyMarshal<TypeSet> where TypeSet: Default {
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

    fn add_metric(&mut self, metric: MetricMarshall) {
        assert!(self.label_names.is_some());
        assert_eq!(metric.label_values.len(), self.label_names.as_ref().unwrap().len());

        self.metrics.push(metric);
    }

    fn is_valid_label_name(&self, name: &String) -> bool {
        return self.label_names.is_none() || self.label_names.as_ref().unwrap().contains(name);
    }

    fn try_set_label_names(&mut self, names: Vec<String>) -> Result<(), OpenMetricsParseError> {
        if self.label_names.is_none() {
            self.label_names = Some(names);
            return Ok(());
        }

        let old_names = self.label_names.as_ref().unwrap();
        for (i, name) in old_names.into_iter().enumerate() {
            if &names[i] != name {
                return Err(OpenMetricsParseError::InvalidLabel);
            }
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
    metrics: Vec<MetricMarshall>
}

impl<TypeSet> From<MetricFamilyMarshal<TypeSet>> for MetricFamily<TypeSet> where TypeSet: Default {
    fn from(marshal: MetricFamilyMarshal<TypeSet>) -> Self {
        assert!(marshal.name.is_some());
        assert!(marshal.label_names.is_some());
        assert!(marshal.family_type.is_some());

        return MetricFamily {
            name: marshal.name.unwrap(),
            label_names: marshal.label_names.unwrap(),
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

pub fn parse_openmetrics(exposition_bytes: &str) -> Result<MetricsExposition<OpenMetricsType>, OpenMetricsParseError> {
    use pest::iterators::Pair;

    fn parse_metric_descriptor(pair: Pair<Rule>, family: &mut MetricFamilyMarshal<OpenMetricsType>) -> Result<(), OpenMetricsParseError> {
        assert_eq!(pair.as_rule(), Rule::metricdescriptor);

        let mut descriptor = pair.into_inner();
        let descriptor_type = descriptor.next().unwrap();
        let metric_name = descriptor.next().unwrap();
        family.set_or_test_name(metric_name.as_str().to_string())?;

        match descriptor_type.as_rule() {
            Rule::kw_help => {
                let help_text = descriptor.next().unwrap().as_str();
                family.try_add_help(help_text.to_string())?;
            },
            Rule::kw_type => {
                let family_type = descriptor.next().unwrap().as_str();
                family.try_add_type(OpenMetricsType::try_from(family_type)?)?;
            },
            Rule::kw_unit => {
                let unit = descriptor.next().unwrap().as_str();
                family.try_add_unit(unit.to_string())?;
            }
            _ => unreachable!()
        }

        return Ok(());
    }

    fn parse_sample(pair: Pair<Rule>, family: &mut MetricFamilyMarshal<OpenMetricsType>) -> Result<MetricMarshall, OpenMetricsParseError> {
        assert_eq!(pair.as_rule(), Rule::sample);

        let mut descriptor = pair.into_inner();
        let metric_name = descriptor.next().unwrap();
        family.set_or_test_name(metric_name.as_str().to_string())?;

        let mut labels = Vec::new();

        while descriptor.peek().unwrap().as_rule() == Rule::label {
            let mut label = descriptor.next().unwrap().into_inner();
            let name = label.next().unwrap().as_str();
            let value = label.next().unwrap().as_str();

            if !family.is_valid_label_name(&name.to_owned()) {
                return Err(OpenMetricsParseError::InvalidLabel);
            }

            labels.push((name, value));
        }

        labels.sort_by_key(|l| l.0);

        let (label_names, label_values) = {
            let mut names = Vec::new();
            let mut values = Vec::new();
            for (name, value) in labels.into_iter() {
                names.push(name.to_owned());
                values.push(value.to_owned());
            }

            (names, values)
        };

        family.try_set_label_names(label_names)?;

        let value = descriptor.next().unwrap().as_str().parse().unwrap();

        return Ok(MetricMarshall::new(label_values, value));
    }

    fn parse_metric_family(pair: Pair<Rule>) -> Result<MetricFamily<OpenMetricsType>, OpenMetricsParseError> {
        assert_eq!(pair.as_rule(), Rule::metricfamily);

        println!("Parsing metrics family {}", pair.as_str());

        let mut metric_family = MetricFamilyMarshal::empty();

        for child in pair.into_inner() {
            match child.as_rule() {
                Rule::metricdescriptor => {parse_metric_descriptor(child, &mut metric_family)?;},
                Rule::sample => {
                    let sample = parse_sample(child, &mut metric_family)?;
                    metric_family.add_metric(sample);
                },
                _ => { println!("HUH? {:?}", child); }
            }
        }

        return Ok(metric_family.into());
    }

    let exposition_marshal = OpenMetricsParser::parse(Rule::exposition, exposition_bytes)?.next().unwrap();
    let mut exposition = MetricsExposition::new();
    assert_eq!(exposition_marshal.as_rule(), Rule::exposition);

    for span in exposition_marshal.into_inner() {
        match span.as_rule() {
            Rule::metricfamily => {
                let family = parse_metric_family(span)?;
                exposition.families.insert(family.name.clone(), family);
            },
            Rule::kw_eof => break,
            _ => unreachable!()
        }
    }

    return Ok(exposition);
}