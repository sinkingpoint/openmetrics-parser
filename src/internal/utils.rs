pub fn render_label_values(label_names: &[&str], label_values: &[&str]) -> String {
    if label_names.is_empty() {
        return String::new();
    }

    let mut build = String::new();

    build.push('{');
    let mut labels = Vec::new();
    for (name, value) in label_names.iter().zip(label_values.iter()) {
        labels.push(format!("{}=\"{}\"", name, value));
    }
    build.push_str(&labels.join(","));
    build.push('}');

    build
}
