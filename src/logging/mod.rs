use std::fmt;
use std::io::{self, Write};
use std::sync::Arc;

use chrono::{SecondsFormat, Utc};
use serde_json::Value;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum LogLevel {
    Error = 1,
    Warn = 2,
    Info = 3,
    Debug = 4,
    Verbose = 5,
}

impl LogLevel {
    fn as_str(self) -> &'static str {
        match self {
            Self::Error => "ERROR",
            Self::Warn => "WARN",
            Self::Info => "INFO",
            Self::Debug => "DEBUG",
            Self::Verbose => "VERBOSE",
        }
    }

    fn as_colored_str(self) -> &'static str {
        match self {
            Self::Error => "\x1b[31mERROR\x1b[0m",
            Self::Warn => "\x1b[33mWARN\x1b[0m",
            Self::Info => "\x1b[32mINFO\x1b[0m",
            Self::Debug => "\x1b[36mDEBUG\x1b[0m",
            Self::Verbose => "\x1b[35mVERBOSE\x1b[0m",
        }
    }
}

#[derive(Clone, Debug)]
pub struct LoggerConfig {
    pub min_level: LogLevel,
    pub human_friendly: bool,
}

impl Default for LoggerConfig {
    fn default() -> Self {
        Self {
            min_level: LogLevel::Debug,
            human_friendly: false,
        }
    }
}

pub trait LogSink: Send + Sync {
    fn write_line(&self, line: &str);
}

#[derive(Default)]
pub struct StdoutSink;

impl LogSink for StdoutSink {
    fn write_line(&self, line: &str) {
        let mut stdout = io::stdout().lock();
        let _ = writeln!(stdout, "{line}");
    }
}

pub struct Logger {
    config: LoggerConfig,
    sink: Arc<dyn LogSink>,
}

impl Logger {
    pub fn new(config: LoggerConfig) -> Self {
        Self::with_sink(config, Arc::new(StdoutSink))
    }

    pub fn with_sink(config: LoggerConfig, sink: Arc<dyn LogSink>) -> Self {
        Self { config, sink }
    }

    pub fn error(&self, context: Option<&str>, message: &str) {
        self.log(LogLevel::Error, context, message, None);
    }

    pub fn warn(&self, context: Option<&str>, message: &str) {
        self.log(LogLevel::Warn, context, message, None);
    }

    pub fn info(&self, context: Option<&str>, message: &str) {
        self.log(LogLevel::Info, context, message, None);
    }

    pub fn debug(&self, context: Option<&str>, message: &str) {
        self.log(LogLevel::Debug, context, message, None);
    }

    pub fn verbose(&self, context: Option<&str>, message: &str) {
        self.log(LogLevel::Verbose, context, message, None);
    }

    pub fn log(
        &self,
        level: LogLevel,
        context: Option<&str>,
        message: &str,
        payload: Option<Value>,
    ) {
        if !self.should_log(level) {
            return;
        }

        let line = self.format_line(level, context, message, payload.as_ref());
        self.sink.write_line(&line);
    }

    fn should_log(&self, level: LogLevel) -> bool {
        level <= self.config.min_level
    }

    fn format_line(
        &self,
        level: LogLevel,
        context: Option<&str>,
        message: &str,
        payload: Option<&Value>,
    ) -> String {
        let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
        let rendered_level = if self.config.human_friendly {
            level.as_colored_str()
        } else {
            level.as_str()
        };

        let context_part = match context {
            Some(ctx) if !ctx.is_empty() => format!(" [{ctx}]"),
            _ => String::new(),
        };

        let payload_part = match payload {
            Some(value) => format!(" payload={value}"),
            None => String::new(),
        };

        format!("{timestamp} [{rendered_level}]{context_part} {message}{payload_part}")
    }
}

impl fmt::Debug for Logger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Logger")
            .field("config", &self.config)
            .field("sink", &"<dyn LogSink>")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use serde_json::json;

    use super::{LogLevel, LogSink, Logger, LoggerConfig};

    #[derive(Default)]
    struct MemorySink {
        lines: Mutex<Vec<String>>,
    }

    impl LogSink for MemorySink {
        fn write_line(&self, line: &str) {
            self.lines
                .lock()
                .expect("memory sink mutex poisoned")
                .push(line.to_owned());
        }
    }

    #[test]
    fn default_config_is_non_human_friendly_and_debug_level() {
        let config = LoggerConfig::default();
        assert_eq!(config.min_level, LogLevel::Debug);
        assert!(!config.human_friendly);
    }

    #[test]
    fn debug_threshold_excludes_verbose_logs() {
        let sink = std::sync::Arc::new(MemorySink::default());
        let logger = Logger::with_sink(LoggerConfig::default(), sink.clone());

        logger.debug(Some("tests::logger"), "debug message");
        logger.verbose(Some("tests::logger"), "verbose message");

        let lines = sink.lines.lock().expect("memory sink mutex poisoned");
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("[DEBUG]"));
        assert!(!lines[0].contains("verbose message"));
    }

    #[test]
    fn log_supports_optional_json_payload() {
        let sink = std::sync::Arc::new(MemorySink::default());
        let logger = Logger::with_sink(LoggerConfig::default(), sink.clone());

        logger.log(
            LogLevel::Info,
            Some("tests::payload"),
            "payload attached",
            Some(json!({"queue":"critical","attempt":2})),
        );

        let lines = sink.lines.lock().expect("memory sink mutex poisoned");
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("[INFO]"));
        assert!(lines[0].contains("[tests::payload]"));
        assert!(lines[0].contains("payload={\"attempt\":2,\"queue\":\"critical\"}"));
        assert!(lines[0].starts_with("20"));
    }

    #[test]
    fn error_and_warn_helpers_emit_lines() {
        let sink = std::sync::Arc::new(MemorySink::default());
        let logger = Logger::with_sink(LoggerConfig::default(), sink.clone());

        logger.warn(Some("tests::warn"), "warn helper");
        logger.error(Some("tests::error"), "error helper");

        let lines = sink.lines.lock().expect("memory sink mutex poisoned");
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("[WARN]"));
        assert!(lines[1].contains("[ERROR]"));
    }
}
