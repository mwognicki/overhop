use std::time::{Duration, Instant};

use crate::logging::Logger;

pub struct ScopedExecutionTimer<'a> {
    task_name: &'a str,
    context: Option<&'a str>,
    started_at: Instant,
    logger: &'a Logger,
}

impl<'a> ScopedExecutionTimer<'a> {
    pub fn new(task_name: &'a str, context: Option<&'a str>, logger: &'a Logger) -> Self {
        Self {
            task_name,
            context,
            started_at: Instant::now(),
            logger,
        }
    }
}

impl Drop for ScopedExecutionTimer<'_> {
    fn drop(&mut self) {
        let elapsed = self.started_at.elapsed();
        self.logger.debug(
            self.context,
            &format!(
                "task '{}' finished in {}",
                self.task_name,
                format_duration_human_friendly(elapsed)
            ),
        );
    }
}

pub fn measure_execution<T, F>(
    task_name: &str,
    context: Option<&str>,
    logger: &Logger,
    task: F,
) -> T
where
    F: FnOnce() -> T,
{
    let timer = ScopedExecutionTimer::new(task_name, context, logger);
    let result = task();
    drop(timer);
    result
}

fn format_duration_human_friendly(duration: Duration) -> String {
    let total_ms = duration.as_secs_f64() * 1000.0;
    if total_ms < 1_000.0 {
        return format!("{total_ms:.2} ms");
    }

    let total_seconds = total_ms / 1_000.0;
    if total_seconds < 60.0 {
        return format!("{total_ms:.2} ms ({total_seconds:.2} s)");
    }

    let minutes = (total_seconds / 60.0).floor();
    let seconds_remainder = total_seconds - (minutes * 60.0);
    format!(
        "{total_ms:.2} ms ({:.0}m {:.2}s)",
        minutes, seconds_remainder
    )
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use crate::logging::{LogLevel, LogSink, Logger, LoggerConfig};

    use super::{format_duration_human_friendly, measure_execution};

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
    fn formats_sub_second_duration_in_milliseconds() {
        let rendered = format_duration_human_friendly(Duration::from_millis(250));
        assert!(rendered.ends_with("ms"));
        assert!(rendered.contains("250"));
    }

    #[test]
    fn formats_seconds_with_ms_and_seconds() {
        let rendered = format_duration_human_friendly(Duration::from_millis(1_500));
        assert!(rendered.contains("ms"));
        assert!(rendered.contains("s"));
    }

    #[test]
    fn measure_execution_logs_debug_with_task_name() {
        let sink = Arc::new(MemorySink::default());
        let logger = Logger::with_sink(
            LoggerConfig {
                min_level: LogLevel::Debug,
                human_friendly: false,
            },
            sink.clone(),
        );

        let value = measure_execution("storage.initialize", Some("tests::timing"), &logger, || 7);
        assert_eq!(value, 7);

        let lines = sink.lines.lock().expect("memory sink mutex poisoned");
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("[DEBUG]"));
        assert!(lines[0].contains("task 'storage.initialize' finished in"));
    }
}
