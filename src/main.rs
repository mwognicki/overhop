mod logging;

use logging::{LogLevel, Logger, LoggerConfig};
use serde_json::json;

fn main() {
    let logger = Logger::new(LoggerConfig::default());
    logger.info(Some("main"), "Starting Overhop");

    println!("{}", greeting());

    logger.log(
        LogLevel::Debug,
        Some("main::greeting"),
        "Rendered greeting",
        Some(json!({"message":"Overhop!"})),
    );
}

fn greeting() -> &'static str {
    "Overhop!"
}

#[cfg(test)]
mod tests {
    use super::greeting;

    #[test]
    fn greeting_is_stable() {
        assert_eq!(greeting(), "Overhop!");
    }
}
