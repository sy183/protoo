//! A Rust rewrite of the protoo signaling framework.
//!
//! The crate focuses on providing the same high level primitives as the
//! original JavaScript implementation: rooms composed of peers that
//! communicate via transports by exchanging requests, responses and
//! notifications.

mod error;
mod message;
mod peer;
mod room;
mod transport;
mod utils;

pub use error::{PeerError, RequestError, RoomError, TransportError};
pub use message::{Message, Notification, Request, Response};
pub use peer::{Peer, RequestContext};
pub use room::Room;
pub use transport::{InMemoryTransport, Transport, TransportEvent};

#[cfg(test)]
mod tests;
