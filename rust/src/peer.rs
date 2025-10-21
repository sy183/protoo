use std::{
    collections::HashMap,
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use futures::future::BoxFuture;
use parking_lot::{RwLock, RwLockWriteGuard};
use serde_json::{Map, Value};
use tokio::{
    sync::{broadcast, oneshot},
    time::{self, Duration},
};

use crate::{
    error::{PeerError, RequestError, TransportError},
    message::{Message, Notification, Request, Response},
    transport::{Transport, TransportEvent},
};

/// Callback invoked whenever a request is received from the remote peer.
pub type RequestHandler =
    Arc<dyn Fn(RequestContext) -> BoxFuture<'static, ()> + Send + Sync + 'static>;
/// Callback invoked whenever a notification is received from the remote peer.
pub type NotificationHandler =
    Arc<dyn Fn(Notification) -> BoxFuture<'static, ()> + Send + Sync + 'static>;
/// Callback invoked when the peer is closed.
pub type CloseHandler = Arc<dyn Fn() + Send + Sync + 'static>;

#[derive(Clone)]
pub struct Peer {
    inner: Arc<PeerInner>,
}

struct PeerInner {
    id: String,
    closed: AtomicBool,
    transport: Arc<dyn Transport>,
    data: RwLock<Map<String, Value>>,
    pending: tokio::sync::Mutex<HashMap<u64, oneshot::Sender<Result<Value, PeerError>>>>,
    request_handlers: RwLock<Vec<RequestHandler>>,
    notification_handlers: RwLock<Vec<NotificationHandler>>,
    close_handlers: RwLock<Vec<CloseHandler>>,
}

impl Peer {
    pub fn new(peer_id: impl Into<String>, transport: Arc<dyn Transport>) -> Self {
        let inner = Arc::new(PeerInner {
            id: peer_id.into(),
            closed: AtomicBool::new(false),
            transport,
            data: RwLock::new(Map::new()),
            pending: tokio::sync::Mutex::new(HashMap::new()),
            request_handlers: RwLock::new(Vec::new()),
            notification_handlers: RwLock::new(Vec::new()),
            close_handlers: RwLock::new(Vec::new()),
        });

        PeerInner::setup_transport(&inner);

        Self { inner }
    }

    pub fn id(&self) -> &str {
        &self.inner.id
    }

    pub fn closed(&self) -> bool {
        self.inner.closed.load(Ordering::SeqCst)
    }

    pub fn data(&self) -> RwLockWriteGuard<'_, Map<String, Value>> {
        self.inner.data.write()
    }

    pub fn on_request<F, Fut>(&self, handler: F)
    where
        F: Fn(RequestContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let handler = Arc::new(move |ctx: RequestContext| -> BoxFuture<'static, ()> {
            Box::pin(handler(ctx))
        });
        self.inner.request_handlers.write().push(handler);
    }

    pub fn on_notification<F, Fut>(&self, handler: F)
    where
        F: Fn(Notification) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let handler = Arc::new(
            move |notification: Notification| -> BoxFuture<'static, ()> {
                Box::pin(handler(notification))
            },
        );
        self.inner.notification_handlers.write().push(handler);
    }

    pub fn on_close<F>(&self, handler: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.inner.close_handlers.write().push(Arc::new(handler));
    }

    pub async fn close(&self) {
        self.inner.close().await;
    }

    pub async fn request(
        &self,
        method: impl Into<String>,
        data: Option<Value>,
    ) -> Result<Value, PeerError> {
        if self.closed() {
            return Err(PeerError::Closed);
        }

        let message = Message::create_request(method, data);
        let request = message.as_request().expect("message must be request");
        let request_id = request.id;

        let (sender, receiver) = oneshot::channel();
        let pending_len = {
            let mut pending = self.inner.pending.lock().await;
            pending.insert(request_id, sender);
            pending.len()
        };

        if let Err(error) = self.inner.transport.send(message).await {
            self.remove_pending(request_id).await;
            return Err(PeerError::Transport(error));
        }

        let base = 2000_f64 * (15_f64 + 0.1 * pending_len as f64);
        let timeout = Duration::from_millis(base as u64);

        match time::timeout(timeout, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(PeerError::Closed),
            Err(_) => {
                self.remove_pending(request_id).await;
                Err(PeerError::Timeout)
            }
        }
    }

    pub async fn notify(
        &self,
        method: impl Into<String>,
        data: Option<Value>,
    ) -> Result<(), TransportError> {
        if self.closed() {
            return Err(TransportError::Closed);
        }

        self.inner
            .transport
            .send(Message::create_notification(method, data))
            .await
    }

    async fn remove_pending(&self, id: u64) {
        let mut pending = self.inner.pending.lock().await;
        pending.remove(&id);
    }
}

impl PeerInner {
    fn setup_transport(inner: &Arc<Self>) {
        let mut receiver = inner.transport.subscribe();
        if inner.transport.is_closed() {
            let inner = inner.clone();
            tokio::spawn(async move {
                inner.on_transport_close().await;
            });
            return;
        }

        let inner_clone = inner.clone();
        tokio::spawn(async move {
            loop {
                match receiver.recv().await {
                    Ok(TransportEvent::Message(message)) => {
                        if let Some(request) = message.as_request() {
                            inner_clone.handle_request(request).await;
                        } else if let Some(response) = message.as_response() {
                            inner_clone.handle_response(response).await;
                        } else if let Some(notification) = message.as_notification() {
                            inner_clone.handle_notification(notification);
                        }
                    }
                    Ok(TransportEvent::Closed) => {
                        inner_clone.on_transport_close().await;
                        break;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        inner_clone.on_transport_close().await;
                        break;
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        continue;
                    }
                }
            }
        });
    }

    async fn handle_request(self: &Arc<Self>, request: Request) {
        let handlers = self.request_handlers.read().clone();

        if handlers.is_empty() {
            let ctx = RequestContext::new(self.clone(), request);
            let _ = ctx.reject(500, "no request handler registered").await;
            return;
        }

        for handler in handlers {
            let context = RequestContext::new(self.clone(), request.clone());
            tokio::spawn(async move {
                handler(context).await;
            });
        }
    }

    async fn handle_response(self: &Arc<Self>, response: Response) {
        let sender = {
            let mut pending = self.pending.lock().await;
            pending.remove(&response.id)
        };

        if let Some(sender) = sender {
            if response.ok {
                let _ = sender.send(Ok(response.data));
            } else {
                let error = RequestError::new(
                    response.error_code.unwrap_or(500),
                    response
                        .error_reason
                        .unwrap_or_else(|| "Unknown error".into()),
                );
                let _ = sender.send(Err(PeerError::Rejected(error)));
            }
        } else {
            log::error!("received unmatched response [id:{}]", response.id);
        }
    }

    fn handle_notification(&self, notification: Notification) {
        let handlers = self.notification_handlers.read().clone();

        for handler in handlers {
            let notification = notification.clone();
            tokio::spawn(async move {
                handler(notification).await;
            });
        }
    }

    async fn on_transport_close(self: &Arc<Self>) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }

        let mut pending = self.pending.lock().await;
        for (_, sender) in pending.drain() {
            let _ = sender.send(Err(PeerError::Closed));
        }
        drop(pending);

        self.emit_close();
    }

    async fn close(&self) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }

        self.transport.close();

        let mut pending = self.pending.lock().await;
        for (_, sender) in pending.drain() {
            let _ = sender.send(Err(PeerError::Closed));
        }
        drop(pending);

        self.emit_close();
    }

    fn emit_close(&self) {
        let handlers = self.close_handlers.read().clone();

        for handler in handlers {
            let handler = handler.clone();
            // Prevent a badly behaved callback from aborting the closing logic.
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                handler();
            }));
        }
    }
}

#[derive(Clone)]
pub struct RequestContext {
    inner: Arc<PeerInner>,
    request: Request,
}

impl RequestContext {
    fn new(inner: Arc<PeerInner>, request: Request) -> Self {
        Self { inner, request }
    }

    pub fn request(&self) -> &Request {
        &self.request
    }

    pub async fn accept(&self, data: Option<Value>) -> Result<(), TransportError> {
        self.inner
            .transport
            .send(Message::create_success_response(&self.request, data))
            .await
    }

    pub async fn reject(
        &self,
        error_code: u16,
        error_reason: impl Into<String>,
    ) -> Result<(), TransportError> {
        self.inner
            .transport
            .send(Message::create_error_response(
                &self.request,
                error_code,
                error_reason,
            ))
            .await
    }
}
