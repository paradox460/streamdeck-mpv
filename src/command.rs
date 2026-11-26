//! Parsing of the Command action's command setting.

use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub enum CommandInput {
    /// A JSON array sent as `{"command": [...]}`; mpv replies with success or an error.
    Json(Vec<Value>),
    /// input.conf-style text commands, one per line; mpv sends no reply.
    Text(Vec<String>),
}

pub fn parse(input: &str) -> Result<CommandInput, String> {
    let trimmed = input.trim();
    if trimmed.starts_with('[') {
        let args: Vec<Value> =
            serde_json::from_str(trimmed).map_err(|e| format!("invalid JSON command: {e}"))?;
        if args.is_empty() {
            return Err("empty JSON command".to_owned());
        }
        return Ok(CommandInput::Json(args));
    }
    if trimmed.starts_with('{') {
        return Err(
            "object commands are not supported; use a JSON array or input.conf text".to_owned(),
        );
    }
    let lines: Vec<String> = trimmed
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(str::to_owned)
        .collect();
    if lines.is_empty() {
        return Err("command is empty".to_owned());
    }
    Ok(CommandInput::Text(lines))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn text_command() {
        assert_eq!(
            parse("cycle pause"),
            Ok(CommandInput::Text(vec!["cycle pause".into()]))
        );
    }

    #[test]
    fn json_command_keeps_number_args() {
        assert_eq!(
            parse(r#" ["seek", 10] "#),
            Ok(CommandInput::Json(vec![json!("seek"), json!(10)]))
        );
    }

    #[test]
    fn empty_inputs_rejected() {
        assert!(parse("").is_err());
        assert!(parse("  \n # comment\n").is_err());
        assert!(parse("[]").is_err());
    }

    #[test]
    fn malformed_json_rejected() {
        assert!(
            parse("[bad")
                .unwrap_err()
                .starts_with("invalid JSON command")
        );
        assert!(parse(r#"{"command":["stop"]}"#).is_err());
    }

    #[test]
    fn multi_line_text() {
        assert_eq!(
            parse("set pause yes\n\n# note\n  seek 10  \n"),
            Ok(CommandInput::Text(vec![
                "set pause yes".into(),
                "seek 10".into()
            ]))
        );
    }
}
