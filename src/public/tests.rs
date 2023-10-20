use crate::prometheus::parse_prometheus;

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

#[test]
fn test_render() {
    let test_str = include_str!("../prometheus/testdata/upstream_example.txt");
    let exposition = parse_prometheus(test_str).unwrap();
    let exposition_str = exposition.to_string();
    assert!(parse_prometheus(&exposition_str).is_ok());

    let test_str = include_str!("../prometheus/testdata/gravelgateway#5.txt");
    let exposition = parse_prometheus(test_str).unwrap();
    let exposition_str = exposition.to_string();
    assert!(parse_prometheus(&exposition_str).is_ok());
}

#[test]
fn test_metric_number_operations() {
    use crate::MetricNumber;

    let a = MetricNumber::Int(1);
    let b = MetricNumber::Int(2);
    let c = MetricNumber::Float(0.5);
    let d = MetricNumber::Float(1.5);

    assert_eq!(a + b, MetricNumber::Int(3));
    assert_eq!(a + c, MetricNumber::Float(1.5));
    assert_eq!(a + d, MetricNumber::Float(2.5));
    assert_eq!(b + c, MetricNumber::Float(2.5));
    assert_eq!(b + d, MetricNumber::Float(3.5));
    assert_eq!(c + d, MetricNumber::Float(2.0));

    assert_eq!(a - b, MetricNumber::Int(-1));
    assert_eq!(a - c, MetricNumber::Float(0.5));
    assert_eq!(a - d, MetricNumber::Float(-0.5));
    assert_eq!(b - c, MetricNumber::Float(1.5));
    assert_eq!(b - d, MetricNumber::Float(0.5));
    assert_eq!(c - d, MetricNumber::Float(-1.0));

    assert_eq!(a * b, MetricNumber::Int(2));
    assert_eq!(a * c, MetricNumber::Float(0.5));
    assert_eq!(a * d, MetricNumber::Float(1.5));
    assert_eq!(b * c, MetricNumber::Float(1.0));
    assert_eq!(b * d, MetricNumber::Float(3.0));
    assert_eq!(c * d, MetricNumber::Float(0.75));

    assert_eq!(a / b, MetricNumber::Int(0));
    assert_eq!(a / c, MetricNumber::Float(2.0));
    assert_eq!(a / d, MetricNumber::Float(2.0 / 3.0));
    assert_eq!(b / c, MetricNumber::Float(4.0));
    assert_eq!(b / d, MetricNumber::Float(4.0 / 3.0));
    assert_eq!(c / d, MetricNumber::Float(1.0 / 3.0));

    {
        let mut a = MetricNumber::Int(1);
        a += b;
        assert_eq!(a, MetricNumber::Int(3));
    }
    {
        let mut a = MetricNumber::Int(1);
        a += c;
        assert_eq!(a, MetricNumber::Float(1.5));
    }
    {
        let mut a = MetricNumber::Int(1);
        a += d;
        assert_eq!(a, MetricNumber::Float(2.5));
    }
    {
        let mut a = MetricNumber::Float(0.5);
        a += b;
        assert_eq!(a, MetricNumber::Float(2.5));
    }
    {
        let mut a = MetricNumber::Float(0.5);
        a += c;
        assert_eq!(a, MetricNumber::Float(1.0));
    }
    {
        let mut a = MetricNumber::Float(0.5);
        a += d;
        assert_eq!(a, MetricNumber::Float(2.0));
    }

    {
        let mut a = MetricNumber::Int(1);
        a -= b;
        assert_eq!(a, MetricNumber::Int(-1));
    }
    {
        let mut a = MetricNumber::Int(1);
        a -= c;
        assert_eq!(a, MetricNumber::Float(0.5));
    }
    {
        let mut a = MetricNumber::Int(1);
        a -= d;
        assert_eq!(a, MetricNumber::Float(-0.5));
    }
    {
        let mut a = MetricNumber::Float(0.5);
        a -= b;
        assert_eq!(a, MetricNumber::Float(-1.5));
    }
    {
        let mut a = MetricNumber::Float(0.5);
        a -= c;
    }

    {
        let mut a = MetricNumber::Int(1);
        a *= b;
        assert_eq!(a, MetricNumber::Int(2));
    }
    {
        let mut a = MetricNumber::Int(1);
        a *= c;
        assert_eq!(a, MetricNumber::Float(0.5));
    }
    {
        let mut a = MetricNumber::Int(1);
        a *= d;
        assert_eq!(a, MetricNumber::Float(1.5));
    }
    {
        let mut a = MetricNumber::Float(0.5);
        a *= b;
        assert_eq!(a, MetricNumber::Float(1.0));
    }
    {
        let mut a = MetricNumber::Float(0.5);
        a *= c;
        assert_eq!(a, MetricNumber::Float(0.25));
    }
    {
        let mut a = MetricNumber::Float(0.5);
        a *= d;
        assert_eq!(a, MetricNumber::Float(0.75));
    }

    {
        let mut a = MetricNumber::Int(1);
        a /= b;
        assert_eq!(a, MetricNumber::Int(0));
    }
    {
        let mut a = MetricNumber::Int(1);
        a /= c;
        assert_eq!(a, MetricNumber::Float(2.0));
    }
    {
        let mut a = MetricNumber::Int(1);
        a /= d;
        assert_eq!(a, MetricNumber::Float(2.0 / 3.0));
    }
    {
        let mut a = MetricNumber::Float(0.5);
        a /= b;
        assert_eq!(a, MetricNumber::Float(0.25));
    }
    {
        let mut a = MetricNumber::Float(0.5);
        a /= c;
        assert_eq!(a, MetricNumber::Float(1.0));
    }
    {
        let mut a = MetricNumber::Float(0.5);
        a /= d;
        assert_eq!(a, MetricNumber::Float(1.0 / 3.0));
    }
}
