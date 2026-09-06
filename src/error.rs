use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("discovery error: {0}")]
    Discovery(String),

    #[error("connection rejected by peer")]
    ConnectionRejected,

    #[error("connection closed")]
    ConnectionClosed,

    #[error("TLS handshake failed: {0}")]
    HandshakeFailed(String),

    #[error("message too large: {0} bytes")]
    MessageTooLarge(usize),

    #[error(transparent)]
    RemoteFile(#[from] crate::remote_files::RemoteFileCodecError),

    #[error("{0}")]
    Other(String),
}

impl ProtocolError {
    /// Stable machine-readable category for outer transport adapters.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Io(_) | Self::ConnectionClosed => "protocol.unavailable",
            Self::Serialization(_) => "protocol.serialization",
            Self::Discovery(_) => "protocol.discovery",
            Self::ConnectionRejected => "protocol.permission_denied",
            Self::HandshakeFailed(_) => "protocol.handshake_failed",
            Self::MessageTooLarge(_) => "protocol.resource_exhausted",
            Self::RemoteFile(_) => "protocol.remote_file_codec",
            Self::Other(_) => "protocol.operation_failed",
        }
    }
}

pub type Result<T> = std::result::Result<T, ProtocolError>;
