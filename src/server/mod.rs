use std::fmt;
use std::io;
use std::net::{SocketAddr, TcpListener};

use crate::config;

pub const DEFAULT_HOST: &str = "0.0.0.0";
pub const DEFAULT_PORT: u16 = 9876;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: DEFAULT_HOST.to_owned(),
            port: DEFAULT_PORT,
        }
    }
}

impl From<config::ServerConfig> for ServerConfig {
    fn from(value: config::ServerConfig) -> Self {
        Self {
            host: value.host,
            port: value.port,
        }
    }
}

#[derive(Debug)]
pub enum ServerError {
    Bind {
        address: String,
        source: io::Error,
    },
    SetNonBlocking {
        source: io::Error,
    },
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bind { address, source } => {
                write!(f, "failed to bind TCP server on {address}: {source}")
            }
            Self::SetNonBlocking { source } => {
                write!(f, "failed to set TCP server to non-blocking mode: {source}")
            }
        }
    }
}

impl std::error::Error for ServerError {}

pub struct TcpServer {
    listener: TcpListener,
}

impl TcpServer {
    pub fn bind(config: &ServerConfig) -> Result<Self, ServerError> {
        let address = format!("{}:{}", config.host, config.port);
        let listener = TcpListener::bind(&address).map_err(|source| ServerError::Bind {
            address,
            source,
        })?;
        listener
            .set_nonblocking(true)
            .map_err(|source| ServerError::SetNonBlocking { source })?;

        Ok(Self { listener })
    }

    pub fn from_app_config(app_config: &config::AppConfig) -> Result<Self, ServerError> {
        let cfg = ServerConfig::from(app_config.server.clone());
        Self::bind(&cfg)
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub fn try_accept(&self) -> io::Result<(std::net::TcpStream, SocketAddr)> {
        self.listener.accept()
    }
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;

    use crate::config::{AppConfig, HeartbeatConfig, LoggingConfig, ServerConfig as AppServerConfig};

    use super::{ServerConfig, TcpServer, DEFAULT_HOST, DEFAULT_PORT};

    #[test]
    fn default_config_matches_expected_host_and_port() {
        let cfg = ServerConfig::default();
        assert_eq!(cfg.host, DEFAULT_HOST);
        assert_eq!(cfg.port, DEFAULT_PORT);
    }

    #[test]
    fn tcp_listener_is_non_blocking() {
        let cfg = ServerConfig {
            host: "127.0.0.1".to_owned(),
            port: 0,
        };
        let server = TcpServer::bind(&cfg).expect("server should bind");
        let accept_result = server.try_accept();

        assert!(accept_result.is_err());
        assert_eq!(
            accept_result.expect_err("must be error").kind(),
            ErrorKind::WouldBlock
        );
    }

    #[test]
    fn builds_server_from_app_config() {
        let app_config = AppConfig {
            logging: LoggingConfig {
                level: "debug".to_owned(),
                human_friendly: false,
            },
            heartbeat: HeartbeatConfig { interval_ms: 1000 },
            server: AppServerConfig {
                host: "127.0.0.1".to_owned(),
                port: 0,
            },
        };

        let server = TcpServer::from_app_config(&app_config)
            .expect("server should build from app config");
        let bound = server.local_addr().expect("bound address should be present");
        assert_eq!(bound.ip().to_string(), "127.0.0.1");
    }
}
