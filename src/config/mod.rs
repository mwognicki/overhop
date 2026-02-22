use std::fmt;
use std::fs;
use std::path::Path;

use serde::Deserialize;
use toml::Value;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct AppConfig {
    pub logging: LoggingConfig,
    pub heartbeat: HeartbeatConfig,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct LoggingConfig {
    pub level: String,
    pub human_friendly: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
pub struct HeartbeatConfig {
    pub interval_ms: u64,
}

impl AppConfig {
    pub fn load_from_toml_with_args(
        path: impl AsRef<Path>,
        args: impl IntoIterator<Item = String>,
    ) -> Result<Self, ConfigError> {
        let toml_content = fs::read_to_string(path.as_ref()).map_err(|source| ConfigError::Io {
            path: path.as_ref().to_string_lossy().to_string(),
            source,
        })?;

        let mut root_value: Value =
            toml_content
                .parse()
                .map_err(|source| ConfigError::TomlParse {
                    path: path.as_ref().to_string_lossy().to_string(),
                    source,
                })?;

        let overrides = parse_cli_overrides(args)?;
        for (key_path, raw_value) in overrides {
            apply_override(&mut root_value, &key_path, &raw_value)?;
        }

        root_value.try_into().map_err(ConfigError::Deserialize)
    }
}

#[derive(Debug)]
pub enum ConfigError {
    Io {
        path: String,
        source: std::io::Error,
    },
    TomlParse {
        path: String,
        source: toml::de::Error,
    },
    Deserialize(toml::de::Error),
    MissingValueForArg {
        key: String,
    },
    InvalidArgFormat {
        arg: String,
    },
    InvalidPath {
        key: String,
    },
    UnknownPath {
        key: String,
    },
    UnsupportedOverrideType {
        key: String,
    },
    InvalidValueForType {
        key: String,
        expected: &'static str,
        value: String,
    },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { path, source } => {
                write!(f, "failed to read config file '{path}': {source}")
            }
            Self::TomlParse { path, source } => {
                write!(f, "failed to parse TOML config '{path}': {source}")
            }
            Self::Deserialize(source) => write!(f, "failed to deserialize config: {source}"),
            Self::MissingValueForArg { key } => {
                write!(f, "missing value for CLI override '--{key}'")
            }
            Self::InvalidArgFormat { arg } => write!(
                f,
                "invalid CLI argument format '{arg}', expected '--section.key value'"
            ),
            Self::InvalidPath { key } => write!(f, "invalid override key path '{key}'"),
            Self::UnknownPath { key } => write!(f, "unknown override key path '{key}'"),
            Self::UnsupportedOverrideType { key } => {
                write!(f, "override not supported for complex TOML type at '{key}'")
            }
            Self::InvalidValueForType {
                key,
                expected,
                value,
            } => write!(
                f,
                "invalid value '{value}' for '{key}', expected type {expected}"
            ),
        }
    }
}

impl std::error::Error for ConfigError {}

fn parse_cli_overrides(args: impl IntoIterator<Item = String>) -> Result<Vec<(String, String)>, ConfigError> {
    let mut parsed = Vec::new();
    let mut iter = args.into_iter();

    while let Some(arg) = iter.next() {
        let Some(stripped) = arg.strip_prefix("--") else {
            return Err(ConfigError::InvalidArgFormat { arg });
        };

        if stripped.is_empty() {
            return Err(ConfigError::InvalidArgFormat { arg });
        }

        let value = iter.next().ok_or_else(|| ConfigError::MissingValueForArg {
            key: stripped.to_owned(),
        })?;

        parsed.push((stripped.to_owned(), value));
    }

    Ok(parsed)
}

fn apply_override(root: &mut Value, key_path: &str, raw_value: &str) -> Result<(), ConfigError> {
    let parts: Vec<&str> = key_path.split('.').collect();
    if parts.is_empty() || parts.iter().any(|part| part.is_empty()) {
        return Err(ConfigError::InvalidPath {
            key: key_path.to_owned(),
        });
    }

    let mut current = root;
    for section in &parts[..parts.len() - 1] {
        let table = current
            .as_table_mut()
            .ok_or_else(|| ConfigError::UnknownPath {
                key: key_path.to_owned(),
            })?;
        current = table.get_mut(*section).ok_or_else(|| ConfigError::UnknownPath {
            key: key_path.to_owned(),
        })?;
    }

    let final_key = parts[parts.len() - 1];
    let table = current
        .as_table_mut()
        .ok_or_else(|| ConfigError::UnknownPath {
            key: key_path.to_owned(),
        })?;
    let current_value = table
        .get_mut(final_key)
        .ok_or_else(|| ConfigError::UnknownPath {
            key: key_path.to_owned(),
        })?;

    let parsed_value = parse_value_using_current_type(key_path, raw_value, current_value)?;
    *current_value = parsed_value;

    Ok(())
}

fn parse_value_using_current_type(
    key_path: &str,
    raw_value: &str,
    current_value: &Value,
) -> Result<Value, ConfigError> {
    match current_value {
        Value::String(_) => Ok(Value::String(raw_value.to_owned())),
        Value::Integer(_) => {
            let parsed = raw_value
                .parse::<i64>()
                .map_err(|_| ConfigError::InvalidValueForType {
                    key: key_path.to_owned(),
                    expected: "integer",
                    value: raw_value.to_owned(),
                })?;
            Ok(Value::Integer(parsed))
        }
        Value::Float(_) => {
            let parsed = raw_value
                .parse::<f64>()
                .map_err(|_| ConfigError::InvalidValueForType {
                    key: key_path.to_owned(),
                    expected: "float",
                    value: raw_value.to_owned(),
                })?;
            Ok(Value::Float(parsed))
        }
        Value::Boolean(_) => {
            let parsed = raw_value
                .parse::<bool>()
                .map_err(|_| ConfigError::InvalidValueForType {
                    key: key_path.to_owned(),
                    expected: "boolean",
                    value: raw_value.to_owned(),
                })?;
            Ok(Value::Boolean(parsed))
        }
        Value::Datetime(_) | Value::Array(_) | Value::Table(_) => {
            Err(ConfigError::UnsupportedOverrideType {
                key: key_path.to_owned(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use super::{AppConfig, ConfigError};

    fn write_temp_config(content: &str, suffix: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "overhop-config-test-{suffix}-{}.toml",
            std::process::id()
        ));
        fs::write(&path, content).expect("failed to write temp config");
        path
    }

    #[test]
    fn loads_config_from_toml_without_overrides() {
        let path = write_temp_config(
            r#"
[logging]
level = "debug"
human_friendly = false

[heartbeat]
interval_ms = 1000
"#,
            "default",
        );

        let config = AppConfig::load_from_toml_with_args(&path, Vec::<String>::new())
            .expect("config should load");
        fs::remove_file(path).expect("temp config cleanup should succeed");

        assert_eq!(config.logging.level, "debug");
        assert!(!config.logging.human_friendly);
        assert_eq!(config.heartbeat.interval_ms, 1000);
    }

    #[test]
    fn argv_overrides_matching_toml_paths() {
        let path = write_temp_config(
            r#"
[logging]
level = "debug"
human_friendly = false

[heartbeat]
interval_ms = 1000
"#,
            "override",
        );

        let config = AppConfig::load_from_toml_with_args(
            &path,
            vec![
                "--logging.level".to_owned(),
                "info".to_owned(),
                "--logging.human_friendly".to_owned(),
                "true".to_owned(),
                "--heartbeat.interval_ms".to_owned(),
                "250".to_owned(),
            ],
        )
        .expect("config with overrides should load");
        fs::remove_file(path).expect("temp config cleanup should succeed");

        assert_eq!(config.logging.level, "info");
        assert!(config.logging.human_friendly);
        assert_eq!(config.heartbeat.interval_ms, 250);
    }

    #[test]
    fn rejects_unknown_override_path() {
        let path = write_temp_config(
            r#"
[logging]
level = "debug"
human_friendly = false

[heartbeat]
interval_ms = 1000
"#,
            "unknown-path",
        );

        let err = AppConfig::load_from_toml_with_args(
            &path,
            vec!["--logging.nonexistent".to_owned(), "x".to_owned()],
        )
        .expect_err("unknown override key should fail");
        fs::remove_file(path).expect("temp config cleanup should succeed");

        assert!(matches!(err, ConfigError::UnknownPath { .. }));
    }
}
