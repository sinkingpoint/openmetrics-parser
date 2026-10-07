/// Resolves the escape sequences (`\\`, `\n`, and `\"`) in a label value or OpenMetrics help string.
/// A backslash that doesn't start one of those sequences is kept as-is.
pub fn unescape(s: &str) -> String {
    unescape_with(s, true)
}

/// Resolves the escape sequences in a Prometheus help string, where only `\\` and `\n` are escapes
pub fn unescape_help(s: &str) -> String {
    unescape_with(s, false)
}

fn unescape_with(s: &str, escaped_quotes: bool) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }

        match chars.peek() {
            Some('\\') => out.push('\\'),
            Some('n') => out.push('\n'),
            Some('"') if escaped_quotes => out.push('"'),
            _ => {
                out.push('\\');
                continue;
            }
        }
        chars.next();
    }

    out
}

/// Escapes a help string for rendering. Quotes don't need escaping in help text.
pub fn escape_help(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\n', "\\n")
}

fn escape_label_value(s: &str) -> String {
    escape_help(s).replace('"', "\\\"")
}

pub fn render_label_values(label_names: &[&str], label_values: &[&str]) -> String {
    if label_names.is_empty() {
        return String::new();
    }

    let mut build = String::new();

    build.push('{');
    let mut labels = Vec::new();
    for (name, value) in label_names.iter().zip(label_values.iter()) {
        labels.push(format!("{}=\"{}\"", name, escape_label_value(value)));
    }
    build.push_str(&labels.join(","));
    build.push('}');

    build
}
