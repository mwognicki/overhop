use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use toml::Value;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct AppConfig {
    pub logging: LoggingConfig,
    pub heartbeat: HeartbeatConfig,
    pub server: ServerConfig,
    pub wire: WireConfig,
    #[serde(default)]
    pub storage: StorageConfig,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct LoggingConfig {
    pub level: String,
    pub human_friendly: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct HeartbeatConfig {
    pub interval_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub tls_enabled: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireConfig {
    pub max_envelope_size_bytes: u64,
    #[serde(default)]
    pub session: WireSessionConfig,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireSessionConfig {
    pub unhelloed_max_lifetime_seconds: u64,
    pub helloed_unregistered_max_lifetime_seconds: u64,
    pub ident_register_timeout_seconds: u64,
}

impl Default for WireSessionConfig {
    fn default() -> Self {
        Self {
            unhelloed_max_lifetime_seconds: 10,
            helloed_unregistered_max_lifetime_seconds: 10,
            ident_register_timeout_seconds: 2,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct StorageConfig {
    pub engine: String,
    pub path: String,
    pub self_debug_path: Option<String>,
    #[serde(default)]
    pub sled: SledConfig,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            engine: "sled".to_owned(),
            path: "~/.overhop/data".to_owned(),
            self_debug_path: None,
            sled: SledConfig::default(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Default)]
pub struct SledConfig {
    pub cache_capacity: Option<u64>,
    pub mode: Option<String>,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "debug".to_owned(),
            human_friendly: false,
        }
    }
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self { interval_ms: 1000 }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_owned(),
            port: 9876,
            tls_enabled: false,
        }
    }
}

impl Default for WireConfig {
    fn default() -> Self {
        Self {
            max_envelope_size_bytes: 8_388_608,
            session: WireSessionConfig::default(),
        }
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            logging: LoggingConfig::default(),
            heartbeat: HeartbeatConfig::default(),
            server: ServerConfig::default(),
            wire: WireConfig::default(),
            storage: StorageConfig::default(),
        }
    }
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

        let root_value: Value =
            toml_content
                .parse()
                .map_err(|source| ConfigError::TomlParse {
                    path: path.as_ref().to_string_lossy().to_string(),
                    source,
                })?;

        Self::load_from_value_with_args(root_value, args)
    }

    pub fn load_with_discovery(
        args: impl IntoIterator<Item = String>,
    ) -> Result<Self, ConfigError> {
        let parsed_args = args.into_iter().collect::<Vec<_>>();
        match resolve_config_path() {
            Ok(config_path) => Self::load_from_toml_with_args(config_path, parsed_args),
            Err(ConfigError::ConfigNotFound { .. }) => {
                Self::load_from_defaults_with_args(parsed_args)
            }
            Err(error) => Err(error),
        }
    }

    fn load_from_defaults_with_args(
        args: impl IntoIterator<Item = String>,
    ) -> Result<Self, ConfigError> {
        let root = Value::try_from(Self::default()).map_err(ConfigError::SerializeDefaults)?;
        Self::load_from_value_with_args(root, args)
    }

    fn load_from_value_with_args(
        mut root_value: Value,
        args: impl IntoIterator<Item = String>,
    ) -> Result<Self, ConfigError> {
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
    SerializeDefaults(toml::ser::Error),
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
    ConfigNotFound {
        searched: Vec<String>,
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
            Self::SerializeDefaults(source) => {
                write!(f, "failed to construct default config: {source}")
            }
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
            Self::ConfigNotFound { searched } => write!(
                f,
                "config.toml was not found in any expected location: {}",
                searched.join(", ")
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

fn resolve_config_path() -> Result<PathBuf, ConfigError> {
    let candidates = config_candidates();
    if let Some(found) = first_existing_path(&candidates) {
        return Ok(found);
    }

    Err(ConfigError::ConfigNotFound {
        searched: candidates
            .iter()
            .map(|path| path.to_string_lossy().to_string())
            .collect(),
    })
}

fn config_candidates() -> Vec<PathBuf> {
    let mut candidates = Vec::new();

    if let Ok(current_exe) = std::env::current_exe() {
        if let Some(parent) = current_exe.parent() {
            candidates.push(parent.join("config.toml"));
        }
    }

    if let Ok(home) = std::env::var("HOME") {
        candidates.push(Path::new(&home).join(".overhop").join("config.toml"));
    }

    candidates.push(PathBuf::from("/etc/overhop/config.toml"));
    candidates
}

fn first_existing_path(candidates: &[PathBuf]) -> Option<PathBuf> {
    candidates.iter().find(|path| path.exists()).cloned()
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

[server]
host = "0.0.0.0"
port = 9876
tls_enabled = false

[wire]
max_envelope_size_bytes = 8388608

[wire.session]
unhelloed_max_lifetime_seconds = 10
helloed_unregistered_max_lifetime_seconds = 10
ident_register_timeout_seconds = 2
"#,
            "default",
        );

        let config = AppConfig::load_from_toml_with_args(&path, Vec::<String>::new())
            .expect("config should load");
        fs::remove_file(path).expect("temp config cleanup should succeed");

        assert_eq!(config.logging.level, "debug");
        assert!(!config.logging.human_friendly);
        assert_eq!(config.heartbeat.interval_ms, 1000);
        assert_eq!(config.server.host, "0.0.0.0");
        assert_eq!(config.server.port, 9876);
        assert!(!config.server.tls_enabled);
        assert_eq!(config.wire.max_envelope_size_bytes, 8_388_608);
        assert_eq!(config.wire.session.unhelloed_max_lifetime_seconds, 10);
        assert_eq!(
            config.wire.session.helloed_unregistered_max_lifetime_seconds,
            10
        );
        assert_eq!(config.wire.session.ident_register_timeout_seconds, 2);
        assert_eq!(config.storage.engine, "sled");
        assert_eq!(config.storage.path, "~/.overhop/data");
        assert_eq!(config.storage.sled.cache_capacity, None);
        assert_eq!(config.storage.sled.mode, None);
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

[server]
host = "0.0.0.0"
port = 9876
tls_enabled = false

[wire]
max_envelope_size_bytes = 8388608

[wire.session]
unhelloed_max_lifetime_seconds = 10
helloed_unregistered_max_lifetime_seconds = 10
ident_register_timeout_seconds = 2

[storage]
engine = "sled"
path = "~/.overhop/data"

[storage.sled]
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
                "--server.host".to_owned(),
                "127.0.0.1".to_owned(),
                "--server.port".to_owned(),
                "9999".to_owned(),
                "--server.tls_enabled".to_owned(),
                "false".to_owned(),
                "--wire.max_envelope_size_bytes".to_owned(),
                "1048576".to_owned(),
                "--wire.session.unhelloed_max_lifetime_seconds".to_owned(),
                "15".to_owned(),
                "--wire.session.helloed_unregistered_max_lifetime_seconds".to_owned(),
                "20".to_owned(),
                "--wire.session.ident_register_timeout_seconds".to_owned(),
                "3".to_owned(),
                "--storage.path".to_owned(),
                "/tmp/overhop-data".to_owned(),
            ],
        )
        .expect("config with overrides should load");
        fs::remove_file(path).expect("temp config cleanup should succeed");

        assert_eq!(config.logging.level, "info");
        assert!(config.logging.human_friendly);
        assert_eq!(config.heartbeat.interval_ms, 250);
        assert_eq!(config.server.host, "127.0.0.1");
        assert_eq!(config.server.port, 9999);
        assert!(!config.server.tls_enabled);
        assert_eq!(config.wire.max_envelope_size_bytes, 1_048_576);
        assert_eq!(config.wire.session.unhelloed_max_lifetime_seconds, 15);
        assert_eq!(
            config.wire.session.helloed_unregistered_max_lifetime_seconds,
            20
        );
        assert_eq!(config.wire.session.ident_register_timeout_seconds, 3);
        assert_eq!(config.storage.path, "/tmp/overhop-data");
        assert_eq!(config.storage.sled.cache_capacity, None);
        assert_eq!(config.storage.sled.mode, None);
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

[server]
host = "0.0.0.0"
port = 9876
tls_enabled = false

[wire]
max_envelope_size_bytes = 8388608

[wire.session]
unhelloed_max_lifetime_seconds = 10
helloed_unregistered_max_lifetime_seconds = 10
ident_register_timeout_seconds = 2

[storage]
engine = "sled"
path = "~/.overhop/data"

[storage.sled]
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

    #[test]
    fn picks_first_existing_candidate() {
        let base = std::env::temp_dir().join(format!(
            "overhop-config-candidates-{}",
            std::process::id()
        ));
        fs::create_dir_all(&base).expect("failed to create temp candidate directory");

        let missing = base.join("missing.toml");
        let found = base.join("config.toml");
        fs::write(&found, "x").expect("failed to write temp candidate file");

        let selected = super::first_existing_path(&[missing, found.clone()]);
        fs::remove_file(&found).expect("failed to clean temp candidate file");
        fs::remove_dir_all(&base).expect("failed to clean temp candidate directory");

        assert_eq!(selected, Some(found));
    }

    #[test]
    fn defaults_loader_uses_expected_defaults() {
        let config = AppConfig::load_from_defaults_with_args(Vec::<String>::new())
            .expect("defaults loader should return built-in defaults");

        assert_eq!(config.logging.level, "debug");
        assert!(!config.logging.human_friendly);
        assert_eq!(config.heartbeat.interval_ms, 1000);
        assert_eq!(config.server.host, "0.0.0.0");
        assert_eq!(config.server.port, 9876);
        assert!(!config.server.tls_enabled);
        assert_eq!(config.wire.max_envelope_size_bytes, 8_388_608);
        assert_eq!(config.storage.engine, "sled");
        assert_eq!(config.storage.path, "~/.overhop/data");
    }

    #[test]
    fn defaults_loader_applies_overrides() {
        let config = AppConfig::load_from_defaults_with_args(vec![
            "--logging.level".to_owned(),
            "verbose".to_owned(),
            "--server.port".to_owned(),
            "9999".to_owned(),
            "--storage.path".to_owned(),
            "/tmp/overhop-fallback".to_owned(),
        ])
        .expect("defaults mode should still accept overrides");

        assert_eq!(config.logging.level, "verbose");
        assert_eq!(config.server.port, 9999);
        assert_eq!(config.storage.path, "/tmp/overhop-fallback");
    }
}
