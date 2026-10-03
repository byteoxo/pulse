use axum::{
    Json, Router,
    body::Body,
    extract::{Query, Request, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use clap::Parser;
use futures_util::StreamExt;
use pulse::{
    config::{Args, Settings},
    engine,
    model::{Phase, Snapshot, mbps, payload},
    provider::{self, Provider},
};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

struct Running {
    url: String,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Running {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn start(app: Router) -> Running {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Running { url, task }
}

#[derive(Clone)]
struct Fixture {
    received: Arc<AtomicU64>,
    saw_token: Arc<AtomicBool>,
    upload_requests: Arc<AtomicU64>,
    rtt: Duration,
    broken_download: bool,
    reject_upload: bool,
}

fn fixture(rtt_ms: u64) -> Fixture {
    Fixture {
        received: Arc::default(),
        saw_token: Arc::default(),
        upload_requests: Arc::default(),
        rtt: Duration::from_millis(rtt_ms),
        broken_download: false,
        reject_upload: false,
    }
}

async fn latency(State(state): State<Fixture>) -> &'static str {
    tokio::time::sleep(state.rtt).await;
    ""
}

async fn download(
    State(state): State<Fixture>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    if state.broken_download {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    assert!(params.contains_key("r"));
    let chunks: usize = params.get("ckSize").unwrap().parse().unwrap();
    let data = payload();
    let stream = futures_util::stream::iter(0..chunks * 16).then(move |_| {
        let data = data.clone();
        async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok::<_, std::io::Error>(data)
        }
    });
    let mut response = Body::from_stream(stream).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        "application/octet-stream".parse().unwrap(),
    );
    response
}

async fn upload(State(state): State<Fixture>, body: Body) -> Result<&'static str, StatusCode> {
    let number = state.upload_requests.fetch_add(1, Ordering::Relaxed);
    // Let the preflight POST pass, then fail actual measurements if requested.
    if state.reject_upload && number > 0 {
        return Err(StatusCode::PAYLOAD_TOO_LARGE);
    }
    let mut stream = body.into_data_stream();
    while let Some(chunk) = stream.next().await {
        let bytes = chunk.map_err(|_| StatusCode::BAD_REQUEST)?;
        tokio::time::sleep(Duration::from_secs_f64(
            bytes.len() as f64 / (2.0 * 1024.0 * 1024.0),
        ))
        .await;
        state
            .received
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
    }
    tokio::time::sleep(state.rtt).await;
    Ok("")
}

fn backend(state: Fixture) -> Router {
    let token_state = state.clone();
    Router::new()
        .route("/custom/latency", get(latency))
        .route("/custom/random", get(download))
        .route("/custom/sink", post(upload))
        .layer(middleware::from_fn(move |request: Request, next: Next| {
            let state = token_state.clone();
            async move {
                if request.headers().contains_key(header::AUTHORIZATION) {
                    state.saw_token.store(true, Ordering::Relaxed);
                }
                next.run(request).await
            }
        }))
        .with_state(state)
}

fn entry(url: &str, id: u64, name: &str) -> serde_json::Value {
    serde_json::json!({ "id": id, "name": name, "server": url, "dlURL": "custom/random", "ulURL": "custom/sink", "pingURL": "custom/latency", "getIpURL": "unused" })
}

async fn directory(entries: Vec<serde_json::Value>, saw_token: Arc<AtomicBool>) -> Running {
    start(Router::new().route(
        "/servers.json",
        get(move |headers: axum::http::HeaderMap| {
            let entries = entries.clone();
            let saw_token = saw_token.clone();
            async move {
                if headers.contains_key(header::AUTHORIZATION) {
                    saw_token.store(true, Ordering::Relaxed);
                }
                Json(entries)
            }
        }),
    ))
    .await
}

fn config(directory: &Running) -> Settings {
    let args = Args::parse_from([
        "pulse",
        "--provider",
        "librespeed",
        "--server-list",
        &format!("{}/servers.json", directory.url),
        "--duration",
        "0.4",
        "--warmup",
        "0.1",
        "--connections",
        "1",
    ]);
    let mut config = Settings::from_args(&args).unwrap();
    config.server_cache = None;
    config.idle_probes = 5;
    config.probe_interval = Duration::from_millis(30);
    config.cooldown = Duration::from_millis(10);
    config
}

#[tokio::test]
async fn public_test_honors_endpoint_mappings_and_counts_only_acked_uploads() {
    let state = fixture(8);
    let server = start(backend(state.clone())).await;
    let directory_token = Arc::new(AtomicBool::new(false));
    let dir = directory(
        vec![entry(&server.url, 101, "Fixture\u{1b} server")],
        directory_token.clone(),
    )
    .await;
    let mut settings = config(&dir);
    settings.token = Some("private-pulse-token".into());
    let (tx, rx) = watch::channel(Snapshot::default());
    let result = engine::run(settings, tx, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(result.provider, Provider::Librespeed);
    assert_eq!(result.server_id, Some(101));
    assert_eq!(result.server_name, "Fixture server");
    assert_eq!(rx.borrow().phase, Phase::Complete);
    assert!(!state.saw_token.load(Ordering::Relaxed));
    assert!(!directory_token.load(Ordering::Relaxed));
    assert!(result.idle_latency.median_ms.unwrap() >= 8.0);
    assert!((10.0..30.0).contains(&result.download.average_mbps));
    assert_eq!(
        result.download.measurement_method,
        "client_received_payload"
    );
    assert_eq!(
        result.upload.measurement_method,
        "acknowledged_http_requests"
    );
    assert!(result.upload.acknowledged_requests.unwrap() > 0);
    assert!(result.upload.measured_seconds >= 0.4);
    assert_eq!(
        result.upload.average_mbps,
        mbps(result.upload.measured_bytes, result.upload.measured_seconds)
    );
    assert!(result.upload.total_bytes > result.upload.measured_bytes);
    assert_eq!(
        result.upload.total_bytes + 16384,
        state.received.load(Ordering::Relaxed)
    );
    assert!(result.upload.loaded_latency.successful > 0);
}

#[tokio::test]
async fn auto_selection_tries_next_server_when_fastest_has_broken_download() {
    let mut bad = fixture(1);
    bad.broken_download = true;
    let fast = start(backend(bad)).await;
    let healthy = start(backend(fixture(25))).await;
    let dir = directory(
        vec![
            entry(&fast.url, 1, "Broken fast"),
            entry(&healthy.url, 2, "Healthy slower"),
        ],
        Arc::default(),
    )
    .await;
    let (tx, _rx) = watch::channel(Snapshot::default());
    let result = engine::run(config(&dir), tx, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(result.server_id, Some(2));
    assert_eq!(result.server_url, healthy.url);
}

#[tokio::test]
async fn public_upload_failure_is_not_saved_as_a_success() {
    let mut state = fixture(3);
    state.reject_upload = true;
    let server = start(backend(state)).await;
    let dir = directory(
        vec![entry(&server.url, 1, "Rejecting uploads")],
        Arc::default(),
    )
    .await;
    let (tx, rx) = watch::channel(Snapshot::default());
    let error = engine::run(config(&dir), tx, CancellationToken::new())
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("413"));
    assert!(rx.borrow().result.is_none());
}

#[tokio::test]
async fn auto_selection_retries_a_full_measurement_on_a_clean_next_server() {
    let mut bad = fixture(1);
    bad.reject_upload = true;
    let fast = start(backend(bad)).await;
    let state = fixture(20);
    let healthy = start(backend(state.clone())).await;
    let dir = directory(
        vec![
            entry(&fast.url, 1, "Failing uploads"),
            entry(&healthy.url, 2, "Working server"),
        ],
        Arc::default(),
    )
    .await;
    let (tx, rx) = watch::channel(Snapshot::default());
    let result = engine::run(config(&dir), tx, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(result.server_id, Some(2));
    assert_eq!(result.failed_servers, ["Failing uploads"]);
    assert_eq!(
        result.upload.total_bytes + 16384,
        state.received.load(Ordering::Relaxed)
    );
    assert_eq!(rx.borrow().result.as_ref().unwrap().server_id, Some(2));
}

#[tokio::test]
async fn public_upload_cancels_without_waiting_for_request_ack() {
    let server = start(backend(fixture(8))).await;
    let dir = directory(vec![entry(&server.url, 1, "Cancellation")], Arc::default()).await;
    let (tx, mut rx) = watch::channel(Snapshot::default());
    let cancel = CancellationToken::new();
    let mut settings = config(&dir);
    settings.duration = Duration::from_secs(10);
    let task = tokio::spawn(engine::run(settings, tx, cancel.clone()));
    tokio::time::timeout(
        Duration::from_secs(15),
        rx.wait_for(|s| s.phase == Phase::Upload),
    )
    .await
    .unwrap()
    .unwrap();
    cancel.cancel();
    let error = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("cancelled"));
}

#[tokio::test]
async fn selected_id_and_invalid_directory_are_handled_explicitly() {
    let a = start(backend(fixture(1))).await;
    let b = start(backend(fixture(2))).await;
    let dir = directory(
        vec![entry(&a.url, 1, "A"), entry(&b.url, 2, "B")],
        Arc::default(),
    )
    .await;
    let mut settings = config(&dir);
    settings.server_id = Some(2);
    let catalog = provider::discover(&settings).await.unwrap();
    assert_eq!(catalog.servers.len(), 1);
    assert_eq!(catalog.servers[0].id, Some(2));
    settings.server_id = Some(9999);
    assert!(provider::discover(&settings).await.is_err());
    let empty = directory(Vec::new(), Arc::default()).await;
    assert!(provider::discover(&config(&empty)).await.is_err());
}
