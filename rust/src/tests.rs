use std::sync::Arc;

use parking_lot::Mutex;
use serde_json::{json, Value};
use tokio::sync::oneshot;

use crate::{
    error::PeerError,
    transport::{InMemoryTransport, Transport},
    Peer, Room,
};

fn make_peers() -> (Room, Peer, Peer) {
    let room = Room::new();
    let (left, right) = InMemoryTransport::pair();
    let left_transport: Arc<dyn Transport> = Arc::new(left);
    let right_transport: Arc<dyn Transport> = Arc::new(right);

    let peer_a = room
        .create_peer("alice", left_transport)
        .expect("peer creation must succeed");
    let peer_b = room
        .create_peer("bob", right_transport)
        .expect("peer creation must succeed");

    (room, peer_a, peer_b)
}

#[tokio::test]
async fn request_response_flow() {
    let (_room, peer_a, peer_b) = make_peers();

    peer_b.on_request(|ctx| async move {
        let mut response_data = serde_json::Map::new();
        response_data.insert("echo".into(), ctx.request().data.clone());
        ctx.accept(Some(Value::Object(response_data)))
            .await
            .expect("response must be sent");
    });

    let payload = json!({ "hello": "world" });
    let response = peer_a
        .request("greet", Some(payload.clone()))
        .await
        .expect("request must succeed");

    assert_eq!(response["echo"], payload);
}

#[tokio::test]
async fn notifications_are_delivered() {
    let (_room, peer_a, peer_b) = make_peers();

    let (tx, rx) = oneshot::channel();
    let tx = Arc::new(Mutex::new(Some(tx)));

    peer_a.on_notification({
        let tx = tx.clone();
        move |notification| {
            let tx = tx.clone();
            async move {
                if let Some(sender) = tx.lock().take() {
                    sender
                        .send(notification.method.clone())
                        .expect("receiver alive");
                }
            }
        }
    });

    peer_b
        .notify("joined", Some(json!({ "room": "main" })))
        .await
        .expect("notification should be sent");

    let method = rx.await.expect("notification received");
    assert_eq!(method, "joined");
}

#[tokio::test]
async fn closing_peers_propagates() {
    let (_room, peer_a, peer_b) = make_peers();

    peer_b.close().await;

    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    match peer_a.request("ping", None).await {
        Err(PeerError::Closed) => {}
        Err(PeerError::Transport(_)) => {}
        other => panic!("unexpected result: {:?}", other),
    }
}
