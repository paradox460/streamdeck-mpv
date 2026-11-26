//! Resolution of the user-supplied mpv IPC socket path.

#[cfg(unix)]
pub const DEFAULT: &str = "/tmp/mpvsocket";
#[cfg(windows)]
pub const DEFAULT: &str = r"\\.\pipe\mpvsocket";

/// Normalizes a socket path from action settings into the path to connect to.
pub fn resolve(raw: &str) -> String {
    let raw = raw.trim();
    if raw.is_empty() {
        return DEFAULT.to_owned();
    }
    platform_resolve(raw)
}

#[cfg(unix)]
fn platform_resolve(raw: &str) -> String {
    match (raw.strip_prefix("~/"), std::env::var_os("HOME")) {
        (Some(rest), Some(home)) => {
            let home = home.to_string_lossy();
            format!("{}/{rest}", home.trim_end_matches('/'))
        }
        _ => raw.to_owned(),
    }
}

#[cfg(windows)]
fn platform_resolve(raw: &str) -> String {
    const PREFIX: &str = r"\\.\pipe\";
    if raw.starts_with(PREFIX) {
        raw.to_owned()
    } else {
        format!("{PREFIX}{raw}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_is_default() {
        assert_eq!(resolve(""), DEFAULT);
        assert_eq!(resolve("   "), DEFAULT);
    }

    #[cfg(unix)]
    #[test]
    fn tilde_expands_to_home() {
        let home = std::env::var("HOME").expect("HOME set in test env");
        assert_eq!(
            resolve("~/x.sock"),
            format!("{}/x.sock", home.trim_end_matches('/'))
        );
    }

    #[cfg(unix)]
    #[test]
    fn absolute_path_unchanged() {
        assert_eq!(resolve("/tmp/a"), "/tmp/a");
        assert_eq!(resolve("  /tmp/a  "), "/tmp/a");
    }

    #[cfg(windows)]
    #[test]
    fn bare_name_gets_pipe_prefix() {
        assert_eq!(resolve("mpvsocket"), r"\\.\pipe\mpvsocket");
        assert_eq!(resolve(r"\\.\pipe\x"), r"\\.\pipe\x");
    }
}
