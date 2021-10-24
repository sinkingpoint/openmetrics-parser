#[test]
fn test_label_sets() {
    use crate::{
        MetricFamily, MetricNumber, PrometheusCounterValue, PrometheusType, PrometheusValue, Sample,
    };

    let family = MetricFamily::new(
        String::from("test_metric"),
        vec![
            String::from("test_label"),
            String::from("test_label_to_remove"),
        ],
        PrometheusType::Counter,
        String::from("HELP!!"),
        String::new(),
    )
    .with_samples(vec![Sample::new(
        vec![String::from("test1"), String::from("test2")],
        None,
        PrometheusValue::Counter(PrometheusCounterValue {
            value: MetricNumber::Int(1),
            exemplar: None,
        }),
    )])
    .unwrap();

    {
        let metric = family.iter_samples().next().unwrap();
        assert_eq!(
            metric
                .get_labelset()
                .unwrap()
                .get_label_value("test_label")
                .unwrap(),
            "test1"
        );
        assert_eq!(
            metric
                .get_labelset()
                .unwrap()
                .get_label_value("test_label_to_remove")
                .unwrap(),
            "test2"
        );
    }

    let family = family.without_label("test_label_to_remove").unwrap();
    {
        let metric = family.iter_samples().next().unwrap();
        assert_eq!(
            metric
                .get_labelset()
                .unwrap()
                .get_label_value("test_label")
                .unwrap(),
            "test1"
        );
        assert!(metric
            .get_labelset()
            .unwrap()
            .get_label_value("test_label_to_remove")
            .is_none());
    }
}
