use crate::{MetricFamily, MetricNumber, PrometheusCounterValue, PrometheusType, PrometheusValue, Sample};

#[test]
fn test_label_sets() {
    let family = MetricFamily {
        name: String::from("test_metric"),
        label_names: vec![String::from("test_label"), String::from("test_label_to_remove")],
        family_type: PrometheusType::Counter,
        help: String::from("HELP!!"),
        unit: String::new(),
        metrics: vec![
            Sample::new(vec![String::from("test1"), String::from("test2")], None, PrometheusValue::Counter(PrometheusCounterValue{
                value: MetricNumber::Int(1),
                exemplar: None
            }))
        ]
    };
}
