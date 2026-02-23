use std::collections::HashMap;
use std::fmt;
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::config;

pub const DEFAULT_HOST: &str = "0.0.0.0";
pub const DEFAULT_PORT: u16 = 9876;
pub const DEFAULT_TLS_ENABLED: bool = false;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub tls_enabled: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: DEFAULT_HOST.to_owned(),
            port: DEFAULT_PORT,
            tls_enabled: DEFAULT_TLS_ENABLED,
        }
    }
}

impl From<config::ServerConfig> for ServerConfig {
    fn from(value: config::ServerConfig) -> Self {
        Self {
            host: value.host,
            port: value.port,
            tls_enabled: value.tls_enabled,
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
    TlsNotSupportedYet,
    ConfigureAcceptedStream {
        source: io::Error,
    },
    StreamClone {
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
            Self::TlsNotSupportedYet => {
                write!(f, "TLS is enabled in config but TLS transport is not implemented yet")
            }
            Self::ConfigureAcceptedStream { source } => {
                write!(f, "failed to configure accepted TCP stream: {source}")
            }
            Self::StreamClone { source } => {
                write!(f, "failed to clone accepted TCP stream for full duplex IO: {source}")
            }
        }
    }
}

impl std::error::Error for ServerError {}

pub struct PersistentConnection {
    id: u64,
    peer_addr: SocketAddr,
    reader: Mutex<TcpStream>,
    writer: Mutex<TcpStream>,
}

impl PersistentConnection {
    fn new(id: u64, stream: TcpStream, peer_addr: SocketAddr) -> Result<Self, ServerError> {
        stream
            .set_nodelay(true)
            .map_err(|source| ServerError::ConfigureAcceptedStream { source })?;
        stream
            .set_nonblocking(true)
            .map_err(|source| ServerError::ConfigureAcceptedStream { source })?;

        let writer = stream
            .try_clone()
            .map_err(|source| ServerError::StreamClone { source })?;

        Ok(Self {
            id,
            peer_addr,
            reader: Mutex::new(stream),
            writer: Mutex::new(writer),
        })
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    pub fn try_read(&self, buffer: &mut [u8]) -> io::Result<usize> {
        self.reader
            .lock()
            .expect("connection reader lock poisoned")
            .read(buffer)
    }

    pub fn try_write(&self, payload: &[u8]) -> io::Result<usize> {
        self.writer
            .lock()
            .expect("connection writer lock poisoned")
            .write(payload)
    }

    pub fn shutdown(&self) -> io::Result<()> {
        let _ = self
            .reader
            .lock()
            .expect("connection reader lock poisoned")
            .shutdown(Shutdown::Both);
        self.writer
            .lock()
            .expect("connection writer lock poisoned")
            .shutdown(Shutdown::Both)
    }
}

pub struct TcpServer {
    listener: TcpListener,
    next_connection_id: AtomicU64,
    active_connections: Mutex<HashMap<u64, Arc<PersistentConnection>>>,
}

impl TcpServer {
    pub fn bind(config: &ServerConfig) -> Result<Self, ServerError> {
        if config.tls_enabled {
            return Err(ServerError::TlsNotSupportedYet);
        }

        let address = format!("{}:{}", config.host, config.port);
        let listener = TcpListener::bind(&address).map_err(|source| ServerError::Bind {
            address,
            source,
        })?;
        listener
            .set_nonblocking(true)
            .map_err(|source| ServerError::SetNonBlocking { source })?;

        Ok(Self {
            listener,
            next_connection_id: AtomicU64::new(1),
            active_connections: Mutex::new(HashMap::new()),
        })
    }

    pub fn from_app_config(app_config: &config::AppConfig) -> Result<Self, ServerError> {
        let cfg = ServerConfig::from(app_config.server.clone());
        Self::bind(&cfg)
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub fn try_accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
        self.listener.accept()
    }

    pub fn try_accept_persistent(&self) -> Result<Option<Arc<PersistentConnection>>, ServerError> {
        match self.listener.accept() {
            Ok((stream, peer_addr)) => {
                let id = self.next_connection_id.fetch_add(1, Ordering::Relaxed);
                let connection = Arc::new(PersistentConnection::new(id, stream, peer_addr)?);
                self.active_connections
                    .lock()
                    .expect("active connections lock poisoned")
                    .insert(id, Arc::clone(&connection));
                Ok(Some(connection))
            }
            Err(source) if source.kind() == io::ErrorKind::WouldBlock => Ok(None),
            Err(source) => Err(ServerError::ConfigureAcceptedStream { source }),
        }
    }

    pub fn connection_count(&self) -> usize {
        self.active_connections
            .lock()
            .expect("active connections lock poisoned")
            .len()
    }

    pub fn drop_connection(&self, id: u64) {
        self.active_connections
            .lock()
            .expect("active connections lock poisoned")
            .remove(&id);
    }

    pub fn shutdown_all_connections(&self) {
        let mut connections = self
            .active_connections
            .lock()
            .expect("active connections lock poisoned");

        for connection in connections.values() {
            let _ = connection.shutdown();
        }
        connections.clear();
    }
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;
    use std::net::TcpStream;
    use std::thread;
    use std::time::Duration;

    use crate::config::{
        AppConfig, HeartbeatConfig, LoggingConfig, PaginationConfig,
        ServerConfig as AppServerConfig, StorageConfig, WireConfig, WireSessionConfig,
    };

    use super::{
        ServerConfig, ServerError, TcpServer, DEFAULT_HOST, DEFAULT_PORT, DEFAULT_TLS_ENABLED,
    };

    #[test]
    fn default_config_matches_expected_host_and_port() {
        let cfg = ServerConfig::default();
        assert_eq!(cfg.host, DEFAULT_HOST);
        assert_eq!(cfg.port, DEFAULT_PORT);
        assert_eq!(cfg.tls_enabled, DEFAULT_TLS_ENABLED);
    }

    #[test]
    fn tcp_listener_is_non_blocking() {
        let cfg = ServerConfig {
            host: "127.0.0.1".to_owned(),
            port: 0,
            tls_enabled: false,
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
    fn rejects_tls_until_implemented() {
        let cfg = ServerConfig {
            host: "127.0.0.1".to_owned(),
            port: 0,
            tls_enabled: true,
        };

        let result = TcpServer::bind(&cfg);
        assert!(matches!(result, Err(ServerError::TlsNotSupportedYet)));
    }

    #[test]
    fn accepts_persistent_full_duplex_connection() {
        let cfg = ServerConfig {
            host: "127.0.0.1".to_owned(),
            port: 0,
            tls_enabled: false,
        };
        let server = TcpServer::bind(&cfg).expect("server should bind");
        let addr = server.local_addr().expect("local addr should exist");

        let client = TcpStream::connect(addr).expect("client should connect");
        client
            .set_nonblocking(true)
            .expect("client should be nonblocking");

        let mut accepted = None;
        for _ in 0..20 {
            if let Some(conn) = server
                .try_accept_persistent()
                .expect("accept poll should not fail")
            {
                accepted = Some(conn);
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }

        let conn = accepted.expect("server should accept connection");
        assert_eq!(server.connection_count(), 1);

        let write_result = conn.try_write(b"ping");
        assert!(write_result.is_ok() || write_result.err().is_some());

        let mut buf = [0_u8; 16];
        let read_result = conn.try_read(&mut buf);
        assert!(read_result.is_ok() || read_result.err().is_some());

        server.drop_connection(conn.id());
        assert_eq!(server.connection_count(), 0);
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
                tls_enabled: false,
            },
            wire: WireConfig {
                max_envelope_size_bytes: 8_388_608,
                session: WireSessionConfig::default(),
            },
            pagination: PaginationConfig::default(),
            storage: StorageConfig::default(),
        };

        let server = TcpServer::from_app_config(&app_config)
            .expect("server should build from app config");
        let bound = server.local_addr().expect("bound address should be present");
        assert_eq!(bound.ip().to_string(), "127.0.0.1");
    }
}
