use crate::model::{ServerInfo, SessionStats, payload};
use axum::{
    Json, Router,
    body::Body,
    extract::{Path, Request, State},
    http::{HeaderMap, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use bytes::Bytes;
use futures_util::StreamExt;
use std::{
    collections::HashMap,
    convert::Infallible,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

const MAX_BODY: u64 = 64 * 1024 * 1024;
const SESSION_TTL: Duration = Duration::from_secs(15 * 60);

struct Counters {
    bytes: u64,
}
struct Session {
    started: Instant,
    counters: Mutex<Counters>,
}

#[derive(Clone)]
pub struct ServerState {
    name: String,
    token: Option<String>,
    sessions: Arc<Mutex<HashMap<String, Arc<Session>>>>,
    payload: Bytes,
}

impl ServerState {
    pub fn new(name: String, token: Option<String>) -> Self {
        Self {
            name,
            token,
            sessions: Arc::default(),
            payload: payload(),
        }
    }
    fn find(&self, id: &str) -> Result<Arc<Session>, StatusCode> {
        self.sessions
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .filter(|s| s.started.elapsed() < SESSION_TTL)
            .ok_or(StatusCode::NOT_FOUND)
    }
    fn snapshot(id: String, session: &Session) -> SessionStats {
        // Timestamp and byte counter are captured together under the same lock.
        let counters = session.counters.lock().unwrap();
        SessionStats {
            id,
            received_bytes: counters.bytes,
            elapsed_ns: session.started.elapsed().as_nanos() as u64,
        }
    }
}

pub fn router(state: ServerState) -> Router {
    Router::new()
        .route("/v1/info", get(info))
        .route("/v1/ping", get(ping))
        .route("/v1/download", get(download))
        .route("/v1/sessions", post(create_session))
        .route(
            "/v1/sessions/{id}",
            get(session_stats).delete(delete_session),
        )
        .route("/v1/upload/{id}", post(upload))
        .layer(middleware::from_fn_with_state(state.clone(), guard))
        .with_state(state)
}

async fn guard(State(state): State<ServerState>, request: Request, next: Next) -> Response {
    if let Some(token) = &state.token {
        let expected = format!("Bearer {token}");
        let actual = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|h| h.to_str().ok())
            .unwrap_or("");
        // Constant-work comparison for equal-length credentials.
        let different = actual
            .as_bytes()
            .iter()
            .zip(expected.as_bytes())
            .fold(0_u8, |d, (a, b)| d | (a ^ b));
        if actual.len() != expected.len() || different != 0 {
            return StatusCode::UNAUTHORIZED.into_response();
        }
    }
    let mut response = next.run(request).await;
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        "no-store, no-transform".parse().unwrap(),
    );
    response
}

async fn info(State(state): State<ServerState>) -> Json<ServerInfo> {
    Json(ServerInfo {
        protocol_version: 1,
        name: state.name,
    })
}

async fn ping() -> &'static str {
    "pulse"
}

async fn download(State(state): State<ServerState>) -> Response {
    // Finite batches keep each request bounded; the client repeats them.
    let stream = futures_util::stream::iter(
        (0..256).map(move |_| Ok::<_, Infallible>(state.payload.clone())),
    );
    let mut response = Body::from_stream(stream).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        "application/octet-stream".parse().unwrap(),
    );
    response.headers_mut().insert(
        header::CONTENT_LENGTH,
        (256 * 64 * 1024).to_string().parse().unwrap(),
    );
    response
}

async fn create_session(
    State(state): State<ServerState>,
) -> Result<Json<SessionStats>, StatusCode> {
    let mut sessions = state.sessions.lock().unwrap();
    sessions.retain(|_, s| s.started.elapsed() < SESSION_TTL);
    if sessions.len() >= 256 {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    let id = uuid::Uuid::new_v4().to_string();
    let session = Arc::new(Session {
        started: Instant::now(),
        counters: Mutex::new(Counters { bytes: 0 }),
    });
    let stats = ServerState::snapshot(id.clone(), &session);
    sessions.insert(id, session);
    Ok(Json(stats))
}

async fn session_stats(
    State(state): State<ServerState>,
    Path(id): Path<String>,
) -> Result<Json<SessionStats>, StatusCode> {
    let session = state.find(&id)?;
    Ok(Json(ServerState::snapshot(id, &session)))
}

async fn delete_session(State(state): State<ServerState>, Path(id): Path<String>) -> StatusCode {
    state.sessions.lock().unwrap().remove(&id);
    StatusCode::NO_CONTENT
}

async fn upload(
    State(state): State<ServerState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<SessionStats>, StatusCode> {
    if headers.get(header::CONTENT_ENCODING).is_some() {
        return Err(StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }
    if let Some(length) = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        && length > MAX_BODY
    {
        return Err(StatusCode::PAYLOAD_TOO_LARGE);
    }
    let session = state.find(&id)?;
    let mut stream = body.into_data_stream();
    let mut received = 0;
    loop {
        let chunk = tokio::time::timeout(Duration::from_secs(30), stream.next())
            .await
            .map_err(|_| StatusCode::REQUEST_TIMEOUT)?;
        let Some(chunk) = chunk else { break };
        let chunk = chunk.map_err(|_| StatusCode::BAD_REQUEST)?;
        received += chunk.len() as u64;
        if received > MAX_BODY {
            return Err(StatusCode::PAYLOAD_TOO_LARGE);
        }
        session.counters.lock().unwrap().bytes += chunk.len() as u64;
    }
    Ok(Json(ServerState::snapshot(id, &session)))
}
