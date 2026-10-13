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

fn is_legacy_name(s: &str, allow_colon: bool) -> bool {
    let valid = |c: char| c.is_ascii_alphabetic() || c == '_' || (allow_colon && c == ':');
    let mut chars = s.chars();
    matches!(chars.next(), Some(c) if valid(c)) && chars.all(|c| valid(c) || c.is_ascii_digit())
}

fn quote(s: &str) -> String {
    format!("\"{}\"", escape_label_value(s))
}

/// Renders a metric name for a HELP, TYPE, or UNIT line, quoting it if it isn't a legacy name
pub fn render_metric_name(name: &str) -> String {
    if is_legacy_name(name, true) {
        name.to_owned()
    } else {
        quote(name)
    }
}

fn render_labels(label_names: &[&str], label_values: &[&str]) -> Vec<String> {
    label_names
        .iter()
        .zip(label_values.iter())
        .map(|(name, value)| {
            let name = if is_legacy_name(name, false) { name.to_string() } else { quote(name) };
            format!("{}=\"{}\"", name, escape_label_value(value))
        })
        .collect()
}

pub fn render_label_values(label_names: &[&str], label_values: &[&str]) -> String {
    if label_names.is_empty() {
        return String::new();
    }

    format!("{{{}}}", render_labels(label_names, label_values).join(","))
}

/// Renders `name{labels}`, or `{"name",labels}` when the name has to be quoted
pub fn render_series(name: &str, label_names: &[&str], label_values: &[&str]) -> String {
    if is_legacy_name(name, true) {
        return format!("{}{}", name, render_label_values(label_names, label_values));
    }

    let mut parts = vec![quote(name)];
    parts.extend(render_labels(label_names, label_values));
    format!("{{{}}}", parts.join(","))
}
