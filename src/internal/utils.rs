fn escape_str(s: &str) -> String {
    return s
        .replace("\\", "\\\\")
        .replace("\n", "\\n")
        .replace("\"", "\\\"");
}

pub fn render_label_values(label_names: &[&str], label_values: &[&str]) -> String {
    if label_names.len() == 0 {
        return String::new();
    }

    let mut build = String::new();

    build.push('{');
    let mut labels = Vec::new();
    for (name, value) in label_names.iter().zip(label_values.iter()) {
        labels.push(format!("{}=\"{}\"", name, escape_str(value)));
    }
    build.push_str(&labels.join(","));
    build.push('}');

    return build;
}
