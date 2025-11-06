use std::fmt;

/// Result type alias for Xylem core operations
pub type Result<T> = std::result::Result<T, Error>;

/// Error types for Xylem core operations
#[derive(Debug)]
pub enum Error {
    /// I/O errors from transport layer
    Io(std::io::Error),

    /// Connection errors
    Connection(String),

    /// Protocol errors
    Protocol(String),

    /// Configuration errors
    Config(String),

    /// Statistics calculation errors
    Stats(String),

    /// Other errors
    Other(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::Connection(msg) => write!(f, "Connection error: {msg}"),
            Error::Protocol(msg) => write!(f, "Protocol error: {msg}"),
            Error::Config(msg) => write!(f, "Configuration error: {msg}"),
            Error::Stats(msg) => write!(f, "Statistics error: {msg}"),
            Error::Other(msg) => write!(f, "Error: {msg}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io(err)
    }
}

impl From<anyhow::Error> for Error {
    fn from(err: anyhow::Error) -> Self {
        Error::Other(err.to_string())
    }
}

impl From<xylem_transport::Error> for Error {
    fn from(err: xylem_transport::Error) -> Self {
        match err {
            xylem_transport::Error::Io(e) => Error::Io(e),
            xylem_transport::Error::Connection(msg) => Error::Connection(msg),
            xylem_transport::Error::Other(msg) => Error::Other(msg),
        }
    }
}
