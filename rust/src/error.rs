use thiserror::Error;

/// Error generated while interacting with a transport.
#[derive(Debug, Error, Clone)]
pub enum TransportError {
    /// The transport has already been closed.
    #[error("transport closed")]
    Closed,
    /// Any other transport level failure.
    #[error("transport failure: {0}")]
    Failure(String),
}

/// Error produced when a request is explicitly rejected by the remote peer.
#[derive(Debug, Error, Clone)]
#[error("request rejected with code {code}: {reason}")]
pub struct RequestError {
    /// The error code indicated by the remote peer.
    pub code: u16,
    /// The error reason indicated by the remote peer.
    pub reason: String,
}

impl RequestError {
    pub fn new(code: u16, reason: impl Into<String>) -> Self {
        Self {
            code,
            reason: reason.into(),
        }
    }
}

/// Error produced by peer level operations.
#[derive(Debug, Error, Clone)]
pub enum PeerError {
    /// Operation attempted on a closed peer.
    #[error("peer closed")]
    Closed,
    /// The underlying transport failed.
    #[error("{0}")]
    Transport(#[from] TransportError),
    /// A request timed out waiting for a response.
    #[error("request timeout")]
    Timeout,
    /// The request was explicitly rejected by the remote peer.
    #[error(transparent)]
    Rejected(#[from] RequestError),
}

/// Errors that can happen while manipulating a room.
#[derive(Debug, Error, Clone)]
pub enum RoomError {
    /// The room has already been closed.
    #[error("room closed")]
    Closed,
    /// The provided peer identifier is invalid.
    #[error("peer id cannot be empty")]
    InvalidPeerId,
    /// A peer with the same identifier already exists.
    #[error("peer with id {0} already exists")]
    DuplicatePeer(String),
}
