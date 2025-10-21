use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use parking_lot::Mutex;

use crate::{error::RoomError, peer::Peer, transport::Transport};

pub struct Room {
    inner: Arc<RoomInner>,
}

struct RoomInner {
    closed: AtomicBool,
    peers: Mutex<HashMap<String, Peer>>,
    close_handlers: Mutex<Vec<Arc<dyn Fn() + Send + Sync + 'static>>>,
}

impl Room {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RoomInner {
                closed: AtomicBool::new(false),
                peers: Mutex::new(HashMap::new()),
                close_handlers: Mutex::new(Vec::new()),
            }),
        }
    }

    pub fn closed(&self) -> bool {
        self.inner.closed.load(Ordering::SeqCst)
    }

    pub fn peers(&self) -> Vec<Peer> {
        self.inner.peers.lock().values().cloned().collect()
    }

    pub fn has_peer(&self, peer_id: &str) -> bool {
        self.inner.peers.lock().contains_key(peer_id)
    }

    pub fn get_peer(&self, peer_id: &str) -> Option<Peer> {
        self.inner.peers.lock().get(peer_id).cloned()
    }

    pub fn on_close<F>(&self, handler: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.inner.close_handlers.lock().push(Arc::new(handler));
    }

    pub async fn close(&self) {
        if self.inner.closed.swap(true, Ordering::SeqCst) {
            return;
        }

        let peers = self
            .inner
            .peers
            .lock()
            .drain()
            .map(|(_, peer)| peer)
            .collect::<Vec<_>>();
        for peer in peers {
            peer.close().await;
        }

        self.inner.emit_close();
    }

    pub fn create_peer(
        &self,
        peer_id: impl Into<String>,
        transport: Arc<dyn Transport>,
    ) -> Result<Peer, RoomError> {
        if self.closed() {
            transport.close();
            return Err(RoomError::Closed);
        }

        let peer_id = peer_id.into();
        if peer_id.is_empty() {
            transport.close();
            return Err(RoomError::InvalidPeerId);
        }

        let mut peers = self.inner.peers.lock();
        if peers.contains_key(&peer_id) {
            transport.close();
            return Err(RoomError::DuplicatePeer(peer_id));
        }

        let peer = Peer::new(peer_id.clone(), transport);
        let room_inner = Arc::downgrade(&self.inner);
        peer.on_close({
            let peer_id = peer_id.clone();
            move || {
                if let Some(inner) = room_inner.upgrade() {
                    inner.peers.lock().remove(&peer_id);
                }
            }
        });

        peers.insert(peer_id, peer.clone());
        Ok(peer)
    }
}

impl RoomInner {
    fn emit_close(&self) {
        for handler in self.close_handlers.lock().iter() {
            let handler = handler.clone();
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                handler();
            }));
        }
    }
}

impl Default for Room {
    fn default() -> Self {
        Self::new()
    }
}
