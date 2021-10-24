use crate::{
    MetricFamily, OpenMetricsType, OpenMetricsValue, PrometheusType, PrometheusValue, Sample,
};

pub type PrometheusMetricFamily = MetricFamily<PrometheusType, PrometheusValue>;
pub type OpenMetricsMetricFamily = MetricFamily<OpenMetricsType, OpenMetricsValue>;
pub type PrometheusSample = Sample<PrometheusValue>;
pub type OpenMetricsSample = Sample<OpenMetricsValue>;
