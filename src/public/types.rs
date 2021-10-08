use crate::{MetricFamily, OpenMetricsType, OpenMetricsValue, PrometheusType, PrometheusValue};

pub type PrometheusMetricFamily = MetricFamily<PrometheusType, PrometheusValue>;
pub type OpenMetricsMetricFamily = MetricFamily<OpenMetricsType, OpenMetricsValue>;