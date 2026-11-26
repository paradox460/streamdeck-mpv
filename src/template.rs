//! Tera templates for Display key titles.
//!
//! A Tera variable is an mpv property name with `-` replaced by `_`
//! (`time_pos` ↔ `time-pos`). Sub-properties are reached by dotting into the
//! parent property (`metadata.title` observes `metadata`).

use std::collections::HashMap;

use serde_json::Value;

pub const TEMPLATE_NAME: &str = "title";

pub struct TitleTemplate {
    tera: tera::Tera,
    /// (Tera variable, mpv property), sorted by variable.
    vars: Vec<(String, String)>,
}

impl TitleTemplate {
    pub fn compile(src: &str) -> Result<Self, String> {
        let mut tera = tera::Tera::new();
        tera.autoescape_on(Vec::<&str>::new());
        tera.register_filter("time", time_filter);
        tera.add_raw_template(TEMPLATE_NAME, src)
            .map_err(|e| e.to_string())?;
        let mut vars: Vec<(String, String)> = tera
            .get_template_variables(TEMPLATE_NAME)
            .expect("template just added")
            .into_iter()
            .map(|var| (var.to_owned(), var.replace('_', "-")))
            .collect();
        vars.sort();
        Ok(Self { tera, vars })
    }

    /// mpv property names the template reads.
    pub fn properties(&self) -> impl Iterator<Item = &str> {
        self.vars.iter().map(|(_, prop)| prop.as_str())
    }

    /// Renders with the latest known property values. Properties absent from
    /// `values` stay undefined so `default` and `time` can handle them.
    pub fn render(&self, values: &HashMap<String, Value>) -> Result<String, String> {
        let mut ctx = tera::Context::new();
        for (var, prop) in &self.vars {
            if let Some(value) = values.get(prop) {
                ctx.insert(var.clone(), value);
            }
        }
        self.tera
            .render(TEMPLATE_NAME, &ctx)
            .map_err(|e| e.to_string())
    }
}

/// Formats seconds as `M:SS`, or `H:MM:SS` from one hour up.
pub fn format_time(seconds: f64) -> String {
    let sign = if seconds < 0.0 { "-" } else { "" };
    let t = seconds.abs().floor() as u64;
    let (h, m, s) = (t / 3600, (t % 3600) / 60, t % 60);
    if t < 3600 {
        format!("{sign}{m}:{s:02}")
    } else {
        format!("{sign}{h}:{m:02}:{s:02}")
    }
}

fn time_filter(value: tera::Value, _: tera::Kwargs, _: &tera::State) -> String {
    if let Some(seconds) = value.as_f64() {
        format_time(seconds)
    } else if value.is_undefined() || value.is_none() {
        "--".to_owned()
    } else if let Some(s) = value.as_str() {
        s.to_owned()
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn values(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), v.clone()))
            .collect()
    }

    fn render(src: &str, pairs: &[(&str, Value)]) -> Result<String, String> {
        TitleTemplate::compile(src).unwrap().render(&values(pairs))
    }

    #[test]
    fn properties_map_underscores_to_dashes() {
        let t = TitleTemplate::compile("{{ time_pos | time }} / {{ duration | time }}").unwrap();
        assert_eq!(t.properties().collect::<Vec<_>>(), ["duration", "time-pos"]);
    }

    #[test]
    fn properties_exclude_loop_locals_and_use_parent_of_dotted() {
        let t = TitleTemplate::compile(
            "{% for t in track_list %}{{ t.id }}{% endfor %}{{ metadata.title }}",
        )
        .unwrap();
        assert_eq!(
            t.properties().collect::<Vec<_>>(),
            ["metadata", "track-list"]
        );
    }

    #[test]
    fn renders_times() {
        assert_eq!(
            render(
                "{{ time_pos | time }} / {{ duration | time }}",
                &[("time-pos", json!(83.4)), ("duration", json!(3723.0))]
            ),
            Ok("1:23 / 1:02:03".to_owned())
        );
    }

    #[test]
    fn time_filter_on_missing_is_placeholder() {
        assert_eq!(render("{{ time_pos | time }}", &[]), Ok("--".to_owned()));
    }

    #[test]
    fn default_filter_on_missing() {
        assert_eq!(
            render("{{ duration | default(value='?') }}", &[]),
            Ok("?".to_owned())
        );
    }

    #[test]
    fn bare_missing_is_render_error() {
        assert!(render("{{ duration }}", &[]).is_err());
    }

    #[test]
    fn int_filter() {
        assert_eq!(
            render("{{ volume | int }}%", &[("volume", json!(42.0))]),
            Ok("42%".to_owned())
        );
    }

    #[test]
    fn syntax_error_fails_compile() {
        assert!(TitleTemplate::compile("{{ x").is_err());
    }

    #[test]
    fn format_time_truncates_and_signs() {
        assert_eq!(format_time(5.9), "0:05");
        assert_eq!(format_time(-5.0), "-0:05");
    }
}
