use axum::{
    Router,
    body::Body,
    extract::Request,
    middleware::{self, Next},
    response::Response,
};
use clap::Parser;
use futures_util::StreamExt;
use pulse::{
    config::{Args, Settings},
    engine,
    model::{Phase, SessionStats, Snapshot, mbps},
    server::{ServerState, router},
};
use std::{sync::Arc, time::Duration};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

struct RunningServer {
    url: String,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for RunningServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn start(app: Router) -> RunningServer {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    RunningServer { url, task }
}

#[tokio::test]
async fn upload_counters_are_server_received_and_sessions_are_isolated() {
    let server = start(router(ServerState::new(
        "Test".into(),
        Some("test-secret".into()),
    )))
    .await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    assert_eq!(
        client
            .get(format!("{}/v1/info", server.url))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    let base = format!("{}/v1/sessions", server.url);
    let a = client
        .post(&base)
        .bearer_auth("test-secret")
        .send()
        .await
        .unwrap()
        .json::<SessionStats>()
        .await
        .unwrap();
    let b = client
        .post(&base)
        .bearer_auth("test-secret")
        .send()
        .await
        .unwrap()
        .json::<SessionStats>()
        .await
        .unwrap();
    let payload = vec![42_u8; 193_713];
    let stats = client
        .post(format!("{}/v1/upload/{}", server.url, a.id))
        .bearer_auth("test-secret")
        .body(payload.clone())
        .send()
        .await
        .unwrap()
        .json::<SessionStats>()
        .await
        .unwrap();
    assert_eq!(stats.received_bytes, payload.len() as u64);
    assert!(stats.elapsed_ns > a.elapsed_ns);
    let untouched = client
        .get(format!("{base}/{}", b.id))
        .bearer_auth("test-secret")
        .send()
        .await
        .unwrap()
        .json::<SessionStats>()
        .await
        .unwrap();
    assert_eq!(untouched.received_bytes, 0);
    client
        .delete(format!("{base}/{}", a.id))
        .bearer_auth("test-secret")
        .send()
        .await
        .unwrap();
    assert_eq!(
        client
            .get(format!("{base}/{}", a.id))
            .bearer_auth("test-secret")
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    assert_eq!(
        client
            .post(format!("{}/v1/upload/{}", server.url, b.id))
            .bearer_auth("test-secret")
            .header("content-encoding", "gzip")
            .body("invalid")
            .send()
            .await
            .unwrap()
            .status(),
        415
    );
}

fn throttled(body: Body, rate: f64) -> Body {
    Body::from_stream(body.into_data_stream().then(move |chunk| async move {
        if let Ok(bytes) = &chunk {
            tokio::time::sleep(Duration::from_secs_f64(bytes.len() as f64 / rate)).await;
        }
        chunk
    }))
}

async fn shape(request: Request, next: Next) -> Response {
    shape_at(request, next, 2.0 * 1024.0 * 1024.0).await
}

async fn shape_at(request: Request, next: Next, rate: f64) -> Response {
    let path = request.uri().path().to_owned();
    let request = if path.starts_with("/v1/upload/") {
        let (parts, body) = request.into_parts();
        Request::from_parts(parts, throttled(body, rate))
    } else {
        request
    };
    if path == "/v1/ping" {
        tokio::time::sleep(Duration::from_millis(8)).await;
    }
    let response = next.run(request).await;
    if path == "/v1/download" {
        let (parts, body) = response.into_parts();
        Response::from_parts(parts, throttled(body, rate))
    } else {
        response
    }
}

fn settings(url: &str) -> Settings {
    let args = Args::parse_from([
        "pulse",
        "--server",
        url,
        "--duration",
        "0.6",
        "--warmup",
        "0.2",
        "--connections",
        "2",
    ]);
    let mut settings = Settings::from_args(&args).unwrap();
    settings.idle_probes = 5;
    settings.probe_interval = Duration::from_millis(40);
    settings.cooldown = Duration::from_millis(20);
    settings
}

#[tokio::test]
async fn full_test_tracks_shaped_bandwidth_and_excludes_warmup() {
    let server = start(
        router(ServerState::new("Shaped server".into(), None)).layer(middleware::from_fn(shape)),
    )
    .await;
    let (tx, rx) = watch::channel(Snapshot::default());
    let result = engine::run(settings(&server.url), tx, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(rx.borrow().phase, Phase::Complete);
    assert_eq!(result.server_name, "Shaped server");
    assert!(!result.simulated);
    assert!(result.idle_latency.median_ms.unwrap() >= 8.0);
    for transfer in [&result.download, &result.upload] {
        // Two connections, each limited to 2 MiB/s: approximately 33.55 Mbps.
        // Scheduling and packet/chunk granularity can lower short-run throughput.
        assert!(
            (15.0..42.0).contains(&transfer.average_mbps),
            "{} Mbps",
            transfer.average_mbps
        );
        assert!(transfer.measured_seconds >= 0.6);
        assert!(transfer.total_bytes > transfer.measured_bytes);
        assert_eq!(
            transfer.average_mbps,
            mbps(transfer.measured_bytes, transfer.measured_seconds)
        );
        assert!(transfer.loaded_latency.successful > 0);
    }
}

#[tokio::test]
async fn cancellation_interrupts_active_transfers() {
    let server = start(
        router(ServerState::new("Cancellation".into(), None)).layer(middleware::from_fn(shape)),
    )
    .await;
    let (tx, mut rx) = watch::channel(Snapshot::default());
    let cancel = CancellationToken::new();
    let mut config = settings(&server.url);
    config.duration = Duration::from_secs(20);
    let task = tokio::spawn(engine::run(config, tx, cancel.clone()));
    tokio::time::timeout(
        Duration::from_secs(5),
        rx.wait_for(|s| s.phase == Phase::Download),
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
async fn failing_transfer_does_not_produce_a_successful_record() {
    let server = start(router(ServerState::new("Failure".into(), None)).layer(
        middleware::from_fn(|request: Request, next: Next| async move {
            if request.uri().path() == "/v1/download" {
                return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
            next.run(request).await
        }),
    ))
    .await;
    use axum::response::IntoResponse;
    let (tx, _rx) = watch::channel(Snapshot::default());
    let error = engine::run(settings(&server.url), tx, CancellationToken::new())
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("503"));
}

#[tokio::test]
async fn cancelled_upload_removes_its_server_session() {
    let created = Arc::new(std::sync::Mutex::new(None::<String>));
    let observer = created.clone();
    let app = router(ServerState::new("Cleanup".into(), None))
        .layer(middleware::from_fn(move |request: Request, next: Next| {
            let observer = observer.clone();
            async move {
                let is_create = request.uri().path() == "/v1/sessions"
                    && request.method() == axum::http::Method::POST;
                let response = next.run(request).await;
                if is_create {
                    let (parts, body) = response.into_parts();
                    let bytes = axum::body::to_bytes(body, 4096).await.unwrap();
                    let stats: SessionStats = serde_json::from_slice(&bytes).unwrap();
                    *observer.lock().unwrap() = Some(stats.id);
                    Response::from_parts(parts, Body::from(bytes))
                } else {
                    response
                }
            }
        }))
        .layer(middleware::from_fn(shape));
    let server = start(app).await;
    let (tx, mut rx) = watch::channel(Snapshot::default());
    let cancel = CancellationToken::new();
    let task = tokio::spawn(engine::run(settings(&server.url), tx, cancel.clone()));
    tokio::time::timeout(
        Duration::from_secs(5),
        rx.wait_for(|s| s.phase == Phase::Upload),
    )
    .await
    .unwrap()
    .unwrap();
    cancel.cancel();
    assert!(task.await.unwrap().is_err());
    let id = created.lock().unwrap().clone().unwrap();
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if client
                .get(format!("{}/v1/sessions/{id}", server.url))
                .send()
                .await
                .unwrap()
                .status()
                == 404
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn slow_upload_is_measured_before_its_request_finishes() {
    let server = start(router(ServerState::new("Slow link".into(), None)).layer(
        middleware::from_fn(|request: Request, next: Next| async move {
            shape_at(request, next, 128.0 * 1024.0).await
        }),
    ))
    .await;
    let mut config = settings(&server.url);
    config.connections = 1;
    config.duration = Duration::from_secs_f64(5.2);
    let (tx, _rx) = watch::channel(Snapshot::default());
    let result = engine::run(config, tx, CancellationToken::new())
        .await
        .unwrap();
    assert!(result.upload.measured_seconds >= 5.2);
    assert!((0.3..1.5).contains(&result.upload.average_mbps));
    // The 8 MiB POST cannot finish on this link during the measurement window.
    assert!(result.upload.total_bytes < 8 * 1024 * 1024);
}
