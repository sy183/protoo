use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::utils::generate_random_number;

/// Representation of a request sent between peers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    pub id: u64,
    pub method: String,
    #[serde(default = "default_object")]
    pub data: Value,
}

/// Representation of a response to a request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub id: u64,
    pub ok: bool,
    #[serde(default = "default_object")]
    pub data: Value,
    #[serde(default)]
    pub error_code: Option<u16>,
    #[serde(default)]
    pub error_reason: Option<String>,
}

/// Representation of a notification message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Notification {
    pub method: String,
    #[serde(default = "default_object")]
    pub data: Value,
}

/// Top level message wrapper exchanged through transports.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Message {
    Request(InternalRequest),
    Response(InternalResponse),
    Notification(InternalNotification),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct InternalRequest {
    request: bool,
    id: u64,
    method: String,
    #[serde(default = "default_object")]
    data: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct InternalResponse {
    response: bool,
    id: u64,
    #[serde(default)]
    ok: bool,
    #[serde(default = "default_object")]
    data: Value,
    #[serde(default)]
    error_code: Option<u16>,
    #[serde(default)]
    error_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct InternalNotification {
    notification: bool,
    method: String,
    #[serde(default = "default_object")]
    data: Value,
}

fn default_object() -> Value {
    Value::Object(Map::new())
}

impl Message {
    /// Try to parse a serialized message coming from the transport layer.
    pub fn parse(raw: &str) -> Option<Self> {
        let value: Value = serde_json::from_str(raw).ok()?;

        if !value.is_object() {
            return None;
        }

        if value
            .get("request")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            let request: InternalRequest = serde_json::from_value(value).ok()?;
            return Some(Message::Request(request));
        }

        if value
            .get("response")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            let response: InternalResponse = serde_json::from_value(value).ok()?;
            return Some(Message::Response(response));
        }

        if value
            .get("notification")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            let notification: InternalNotification = serde_json::from_value(value).ok()?;
            return Some(Message::Notification(notification));
        }

        None
    }

    /// Serialize the message to a JSON string.
    pub fn to_json_string(&self) -> String {
        serde_json::to_string(&self).expect("serializing message never fails")
    }

    pub fn as_request(&self) -> Option<Request> {
        match self {
            Message::Request(internal) => Some(Request {
                id: internal.id,
                method: internal.method.clone(),
                data: internal.data.clone(),
            }),
            _ => None,
        }
    }

    pub fn as_response(&self) -> Option<Response> {
        match self {
            Message::Response(internal) => Some(Response {
                id: internal.id,
                ok: internal.ok,
                data: internal.data.clone(),
                error_code: internal.error_code,
                error_reason: internal.error_reason.clone(),
            }),
            _ => None,
        }
    }

    pub fn as_notification(&self) -> Option<Notification> {
        match self {
            Message::Notification(internal) => Some(Notification {
                method: internal.method.clone(),
                data: internal.data.clone(),
            }),
            _ => None,
        }
    }

    pub fn create_request(method: impl Into<String>, data: Option<Value>) -> Self {
        Message::Request(InternalRequest {
            request: true,
            id: generate_random_number(),
            method: method.into(),
            data: data.unwrap_or_else(default_object),
        })
    }

    pub fn create_success_response(request: &Request, data: Option<Value>) -> Self {
        Message::Response(InternalResponse {
            response: true,
            id: request.id,
            ok: true,
            data: data.unwrap_or_else(default_object),
            error_code: None,
            error_reason: None,
        })
    }

    pub fn create_error_response(
        request: &Request,
        error_code: u16,
        error_reason: impl Into<String>,
    ) -> Self {
        Message::Response(InternalResponse {
            response: true,
            id: request.id,
            ok: false,
            data: default_object(),
            error_code: Some(error_code),
            error_reason: Some(error_reason.into()),
        })
    }

    pub fn create_notification(method: impl Into<String>, data: Option<Value>) -> Self {
        Message::Notification(InternalNotification {
            notification: true,
            method: method.into(),
            data: data.unwrap_or_else(default_object),
        })
    }

    pub fn request_id(&self) -> Option<u64> {
        match self {
            Message::Request(req) => Some(req.id),
            Message::Response(res) => Some(res.id),
            _ => None,
        }
    }
}

impl From<Request> for Message {
    fn from(request: Request) -> Self {
        Message::Request(InternalRequest {
            request: true,
            id: request.id,
            method: request.method,
            data: request.data,
        })
    }
}

impl From<Response> for Message {
    fn from(response: Response) -> Self {
        Message::Response(InternalResponse {
            response: true,
            id: response.id,
            ok: response.ok,
            data: response.data,
            error_code: response.error_code,
            error_reason: response.error_reason,
        })
    }
}

impl From<Notification> for Message {
    fn from(notification: Notification) -> Self {
        Message::Notification(InternalNotification {
            notification: true,
            method: notification.method,
            data: notification.data,
        })
    }
}

impl Request {
    pub fn new(id: u64, method: impl Into<String>, data: Value) -> Self {
        Self {
            id,
            method: method.into(),
            data,
        }
    }
}

impl Response {
    pub fn ok(id: u64, data: Value) -> Self {
        Self {
            id,
            ok: true,
            data,
            error_code: None,
            error_reason: None,
        }
    }

    pub fn error(id: u64, code: u16, reason: impl Into<String>) -> Self {
        Self {
            id,
            ok: false,
            data: default_object(),
            error_code: Some(code),
            error_reason: Some(reason.into()),
        }
    }
}

impl Notification {
    pub fn new(method: impl Into<String>, data: Value) -> Self {
        Self {
            method: method.into(),
            data,
        }
    }
}

impl From<&Request> for Request {
    fn from(request: &Request) -> Self {
        Request {
            id: request.id,
            method: request.method.clone(),
            data: request.data.clone(),
        }
    }
}

impl From<&Response> for Response {
    fn from(response: &Response) -> Self {
        Response {
            id: response.id,
            ok: response.ok,
            data: response.data.clone(),
            error_code: response.error_code,
            error_reason: response.error_reason.clone(),
        }
    }
}

impl From<&Notification> for Notification {
    fn from(notification: &Notification) -> Self {
        Notification {
            method: notification.method.clone(),
            data: notification.data.clone(),
        }
    }
}
