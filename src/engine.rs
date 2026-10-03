use crate::provider::{self, Endpoint, Provider, client, ping};
use crate::{
    config::Settings,
    model::{Phase, ProbeStats, ResultRecord, SessionStats, Snapshot, Transfer, mbps, payload},
};
use anyhow::{Context, Result, bail};
use futures_util::StreamExt;
use reqwest::{Client, header};
use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

fn emit(tx: &watch::Sender<Snapshot>, state: &Snapshot) {
    tx.send_replace(state.clone());
}

async fn rank_servers(
    settings: &Settings,
    control: &Client,
    probe: &Client,
    tx: &watch::Sender<Snapshot>,
    state: &mut Snapshot,
) -> Result<Vec<Endpoint>> {
    state.message = format!("Loading {} servers", settings.provider.label());
    emit(tx, state);
    let catalog = provider::discover(settings).await?;
    let total = catalog.servers.len();
    let mut ranked = Vec::new();
    let mut candidates =
        futures_util::stream::iter(catalog.servers.into_iter().map(|mut endpoint| {
            let control = control.clone();
            let probe = probe.clone();
            async move {
                let result = tokio::time::timeout(Duration::from_secs(8), async {
                    provider::identify(&control, &mut endpoint).await?;
                    ping(&probe, &endpoint).await?; // Warm DNS/TCP/TLS before ranking.
                    let mut samples = Vec::new();
                    for _ in 0..3 {
                        samples.push(ping(&probe, &endpoint).await.ok());
                    }
                    let stats = ProbeStats::from_samples(&samples);
                    if stats.successful < 2 {
                        bail!("Too few successful latency probes");
                    }
                    Ok::<_, anyhow::Error>(stats.median_ms.unwrap())
                })
                .await
                .context("Server probe deadline exceeded")
                .and_then(|r| r);
                (endpoint, result)
            }
        }))
        .buffer_unordered(6);
    let mut checked = 0;
    while let Some((endpoint, result)) = candidates.next().await {
        checked += 1;
        state.duration = total as f64;
        state.elapsed = checked as f64;
        state.message = if catalog.notice.is_empty() {
            format!("Checked {checked}/{total} servers")
        } else {
            format!("{} · checked {checked}/{total} servers", catalog.notice)
        };
        emit(tx, state);
        match result {
            Ok(rtt) => ranked.push((rtt, endpoint)),
            Err(error) => {
                tracing::warn!(server = %endpoint.base_url, error = %error, "Skipping unavailable server")
            }
        }
    }
    ranked.sort_by(|a, b| a.0.total_cmp(&b.0));
    if ranked.is_empty() {
        let hint = if settings.provider == Provider::Pulse {
            "Start pulse-server or pass --server URL."
        } else {
            "Try another server directory or an explicit --proxy."
        };
        bail!("No reachable {} server. {hint}", settings.provider.label());
    }
    Ok(ranked.into_iter().map(|(_, endpoint)| endpoint).collect())
}

struct UploadCounters {
    bytes: AtomicU64,
    requests: AtomicU64,
    last_ack_ns: AtomicU64,
    batch_size: AtomicU64,
}

impl UploadCounters {
    fn new() -> Self {
        Self {
            bytes: AtomicU64::new(0),
            requests: AtomicU64::new(0),
            last_ack_ns: AtomicU64::new(0),
            batch_size: AtomicU64::new(32 * 1024),
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn public_upload(
    settings: &Settings,
    probe: &Client,
    endpoint: &Endpoint,
    tx: &watch::Sender<Snapshot>,
    state: &mut Snapshot,
    test_start: Instant,
) -> Result<Transfer> {
    state.phase = Phase::Upload;
    state.loaded = ProbeStats::default();
    state.warming_up = true;
    state.duration = settings.warmup.as_secs_f64();
    state.elapsed = 0.0;
    state.message = "Warming up acknowledged uploads".into();
    emit(tx, state);
    // Keep each worker's HTTP connection pool across the warm-up boundary.
    // Warm-up requests finish before new measured requests use these clients.
    let clients = (0..settings.connections)
        .map(|_| client(settings))
        .collect::<Result<Vec<_>>>()?;
    let counters = Arc::new(UploadCounters::new());
    if !settings.warmup.is_zero() {
        public_upload_window(
            &clients,
            endpoint,
            tx,
            state,
            counters.clone(),
            settings.warmup,
            None,
        )
        .await?;
    }
    let warmup_bytes = counters.bytes.load(Ordering::Relaxed);
    counters.bytes.store(0, Ordering::Relaxed);
    counters.requests.store(0, Ordering::Relaxed);
    counters.last_ack_ns.store(0, Ordering::Relaxed);
    state.warming_up = false;
    state.duration = settings.duration.as_secs_f64();
    state.elapsed = 0.0;
    state.upload_mbps = 0.0;
    state.message = "Measuring acknowledged requests · includes HTTP response time".into();
    emit(tx, state);
    let (probe_tx, mut probe_rx) = mpsc::channel(16);
    let mut probes = JoinSet::new();
    let probe = probe.clone();
    let endpoint_clone = endpoint.clone();
    let interval = settings.probe_interval;
    probes.spawn(async move {
        let mut timer = tokio::time::interval(interval);
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            timer.tick().await;
            let sample = ping(&probe, &endpoint_clone).await.ok();
            if probe_tx
                .send((test_start.elapsed().as_secs_f64(), sample))
                .await
                .is_err()
            {
                break;
            }
        }
    });
    let (seconds, mut samples) = public_upload_window(
        &clients,
        endpoint,
        tx,
        state,
        counters.clone(),
        settings.duration,
        Some(&mut probe_rx),
    )
    .await?;
    probes.abort_all();
    while probes.join_next().await.is_some() {}
    while let Ok((time, sample)) = probe_rx.try_recv() {
        samples.push(sample);
        state.current_latency = sample;
        if let Some(rtt) = sample {
            state.latency_points.push((time, rtt));
        }
    }
    let measured_bytes = counters.bytes.load(Ordering::Relaxed);
    let result = Transfer {
        measurement_method: "acknowledged_http_requests".into(),
        acknowledged_requests: Some(counters.requests.load(Ordering::Relaxed)),
        average_mbps: mbps(measured_bytes, seconds),
        measured_bytes,
        total_bytes: warmup_bytes + measured_bytes,
        measured_seconds: seconds,
        loaded_latency: ProbeStats::from_samples(&samples),
    };
    state.upload_mbps = result.average_mbps;
    state.upload_bytes = result.total_bytes;
    state.loaded = result.loaded_latency.clone();
    state.elapsed = seconds;
    emit(tx, state);
    Ok(result)
}

#[allow(clippy::too_many_arguments)]
async fn public_upload_window(
    clients: &[Client],
    endpoint: &Endpoint,
    tx: &watch::Sender<Snapshot>,
    state: &mut Snapshot,
    counters: Arc<UploadCounters>,
    duration: Duration,
    mut probe_rx: Option<&mut mpsc::Receiver<(f64, Option<f64>)>>,
) -> Result<(f64, Vec<Option<f64>>)> {
    let started = Instant::now();
    let deadline = started + duration;
    let initial_total = state.upload_bytes;
    let mut workers = JoinSet::new();
    for client in clients {
        let client = client.clone();
        let endpoint = endpoint.clone();
        let counters = counters.clone();
        workers.spawn(async move {
            let payload = payload();
            while Instant::now() < deadline {
                let size = counters.batch_size.load(Ordering::Relaxed) as usize;
                let chunk = payload.clone();
                let produced = Arc::new(AtomicU64::new(0));
                let produced_clone = produced.clone();
                let body = futures_util::stream::unfold((0, chunk), move |(sent, chunk)| {
                    let produced = produced_clone.clone();
                    async move {
                        if sent == size {
                            return None;
                        }
                        let len = (size - sent).min(chunk.len());
                        produced.fetch_add(len as u64, Ordering::Relaxed);
                        Some((
                            Ok::<_, std::io::Error>(chunk.slice(..len)),
                            (sent + len, chunk),
                        ))
                    }
                });
                let request_start = Instant::now();
                let response = client
                    .post(provider::uncached(&endpoint.upload_url)?)
                    .header(header::CONTENT_TYPE, "application/octet-stream")
                    .header(header::CONTENT_LENGTH, size)
                    .body(reqwest::Body::wrap_stream(body))
                    .send()
                    .await?
                    .error_for_status()?;
                let response_body = response.bytes().await?;
                if !response_body.is_empty() || produced.load(Ordering::Relaxed) != size as u64 {
                    bail!("Upload endpoint did not acknowledge a complete request");
                }
                counters.bytes.fetch_add(size as u64, Ordering::Relaxed);
                counters.requests.fetch_add(1, Ordering::Relaxed);
                counters
                    .last_ack_ns
                    .fetch_max(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
                let factor =
                    (0.5 / request_start.elapsed().as_secs_f64().max(0.001)).clamp(0.5, 4.0);
                let next = ((size as f64 * factor) as u64 / 16384 * 16384)
                    .clamp(16 * 1024, 4 * 1024 * 1024);
                counters.batch_size.store(next, Ordering::Relaxed);
            }
            Ok::<_, anyhow::Error>(())
        });
    }
    let mut samples = Vec::new();
    let mut recent = VecDeque::from([(0_u64, 0.0_f64)]);
    loop {
        while let Some(result) = workers.try_join_next() {
            result.context("Public upload worker stopped")??;
        }
        let elapsed = started.elapsed().as_secs_f64();
        let bytes = counters.bytes.load(Ordering::Relaxed);
        recent.push_back((bytes, elapsed));
        while recent.len() > 2 && elapsed - recent[1].1 >= 1.0 {
            recent.pop_front();
        }
        let first = recent.front().unwrap();
        if let Some(rx) = &mut probe_rx {
            while let Ok((time, sample)) = rx.try_recv() {
                samples.push(sample);
                state.current_latency = sample;
                if let Some(rtt) = sample {
                    state.latency_points.push((time, rtt));
                }
            }
            state.loaded = ProbeStats::from_samples(&samples);
        }
        state.upload_bytes = initial_total + bytes;
        state.elapsed = elapsed;
        if !state.warming_up {
            state.upload_mbps = mbps(bytes - first.0, elapsed - first.1);
            state.upload_points.push((elapsed, state.upload_mbps));
        }
        if Instant::now() >= deadline && !workers.is_empty() {
            state.message =
                "Finishing in-flight upload requests · waiting for acknowledgements".into();
        }
        emit(tx, state);
        if workers.is_empty() {
            break;
        }
        if Instant::now() > deadline + Duration::from_secs(5) {
            bail!(
                "Public upload acknowledgements exceeded the 5 s completion allowance; no result saved"
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if counters.bytes.load(Ordering::Relaxed) == 0 {
        bail!("No upload requests were acknowledged");
    }
    Ok((
        counters.last_ack_ns.load(Ordering::Relaxed) as f64 / 1e9,
        samples,
    ))
}

pub async fn run(
    settings: Settings,
    tx: watch::Sender<Snapshot>,
    cancel: CancellationToken,
) -> Result<ResultRecord> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => bail!("Test cancelled"),
        result = run_inner(settings, tx) => result,
    }
}

async fn run_inner(settings: Settings, tx: watch::Sender<Snapshot>) -> Result<ResultRecord> {
    let mut state = Snapshot {
        phase: Phase::Selecting,
        route: settings.route(),
        connections: settings.connections,
        simulated: settings.demo,
        duration: settings.duration.as_secs_f64(),
        ..Default::default()
    };
    emit(&tx, &state);
    if settings.demo {
        return demo(settings, tx, state).await;
    }
    let control = client(&settings)?;
    let probe = client(&settings)?;
    let ranked = rank_servers(&settings, &control, &probe, &tx, &mut state).await?;
    let maximum_attempts = if settings.provider == Provider::Librespeed {
        3
    } else {
        1
    };
    let mut attempts = 0;
    let mut failed_servers = Vec::new();
    let mut last_error = None;
    for endpoint in ranked {
        state = Snapshot {
            phase: Phase::Selecting,
            route: settings.route(),
            connections: settings.connections,
            server_name: endpoint.name.clone(),
            server_url: endpoint.base_url.clone(),
            ..Default::default()
        };
        state.message = if attempts == 0 {
            "Checking download and upload endpoints".into()
        } else {
            format!("Previous server failed · trying {}", endpoint.name)
        };
        emit(&tx, &state);
        if let Err(error) = provider::preflight(&control, &endpoint).await {
            tracing::warn!(server = %endpoint.base_url, error = %error, "Skipping failed transfer endpoints");
            last_error = Some(error);
            continue;
        }
        attempts += 1;
        match measure(&settings, &control, &probe, &endpoint, &tx, &mut state).await {
            Ok(mut record) => {
                record.failed_servers = failed_servers;
                if !record.failed_servers.is_empty() {
                    state.message = format!(
                        "Finished · recovered after {} failed server attempt(s)",
                        record.failed_servers.len()
                    );
                }
                state.result = Some(record.clone());
                emit(&tx, &state);
                return Ok(record);
            }
            Err(error) => {
                tracing::warn!(server = %endpoint.base_url, error = %error, "Server measurement failed");
                failed_servers.push(endpoint.name.clone());
                last_error = Some(error);
                if attempts >= maximum_attempts {
                    break;
                }
            }
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("No usable server"))).context(format!("No successful {} measurement after {attempts} attempt(s). Select another server or try again.", settings.provider.label()))
}

async fn measure(
    settings: &Settings,
    control: &Client,
    probe: &Client,
    endpoint: &Endpoint,
    tx: &watch::Sender<Snapshot>,
    state: &mut Snapshot,
) -> Result<ResultRecord> {
    state.server_url = endpoint.base_url.clone();
    state.server_name = endpoint.name.clone();
    state.phase = Phase::Latency;
    state.duration = settings.idle_probes as f64 * settings.probe_interval.as_secs_f64();
    state.message = "Measuring idle HTTP round-trip time".into();
    let test_start = Instant::now();
    let mut idle_samples = Vec::new();
    let mut timer = tokio::time::interval(settings.probe_interval);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let idle_start = Instant::now();
    for _ in 0..settings.idle_probes {
        timer.tick().await;
        let sample = ping(probe, endpoint).await.ok();
        idle_samples.push(sample);
        state.current_latency = sample;
        state.idle = ProbeStats::from_samples(&idle_samples);
        state.elapsed = idle_start.elapsed().as_secs_f64();
        if let Some(rtt) = sample {
            state
                .latency_points
                .push((test_start.elapsed().as_secs_f64(), rtt));
        }
        emit(tx, state);
    }
    if state.idle.successful == 0 {
        bail!("All idle latency probes failed");
    }
    let download = transfer(
        false, settings, control, probe, endpoint, tx, state, test_start,
    )
    .await?;
    state.phase = Phase::Cooldown;
    state.elapsed = 0.0;
    state.duration = settings.cooldown.as_secs_f64();
    state.message = "Letting the connection settle".into();
    emit(tx, state);
    tokio::time::sleep(settings.cooldown).await;
    let upload = if settings.provider == Provider::Librespeed {
        public_upload(settings, probe, endpoint, tx, state, test_start).await?
    } else {
        transfer(
            true, settings, control, probe, endpoint, tx, state, test_start,
        )
        .await?
    };
    let mut record = make_record(
        settings,
        endpoint.name.clone(),
        endpoint.base_url.clone(),
        state.idle.clone(),
        download,
        upload,
    );
    record.server_id = endpoint.id;
    state.phase = Phase::Complete;
    state.download_mbps = record.download.average_mbps;
    state.upload_mbps = record.upload.average_mbps;
    state.message = "Finished · average speeds exclude warm-up · totals include warm-up".into();
    state.result = Some(record.clone());
    emit(tx, state);
    Ok(record)
}

struct SessionLease {
    client: Client,
    url: String,
}
impl Drop for SessionLease {
    fn drop(&mut self) {
        let client = self.client.clone();
        let url = self.url.clone();
        // Also runs when a test is cancelled or a worker fails.
        tokio::spawn(async move {
            let _ = client
                .delete(url)
                .timeout(Duration::from_secs(2))
                .send()
                .await;
        });
    }
}

fn check_workers(workers: &mut JoinSet<Result<()>>) -> Result<()> {
    if let Some(result) = workers.try_join_next() {
        result.context("Transfer task stopped unexpectedly")??;
        bail!("Transfer task stopped unexpectedly");
    }
    Ok(())
}

async fn counter(
    control: &Client,
    session: Option<&str>,
    bytes: &AtomicU64,
    started: Instant,
) -> Result<(u64, u64)> {
    if let Some(url) = session {
        let stats = control
            .get(url)
            .timeout(Duration::from_secs(2))
            .send()
            .await?
            .error_for_status()?
            .json::<SessionStats>()
            .await?;
        Ok((stats.received_bytes, stats.elapsed_ns))
    } else {
        Ok((
            bytes.load(Ordering::Relaxed),
            started.elapsed().as_nanos() as u64,
        ))
    }
}

#[allow(clippy::too_many_arguments)]
async fn transfer(
    upload: bool,
    settings: &Settings,
    control: &Client,
    probe: &Client,
    endpoint: &Endpoint,
    tx: &watch::Sender<Snapshot>,
    state: &mut Snapshot,
    test_start: Instant,
) -> Result<Transfer> {
    let url = &endpoint.base_url;
    let mut workers = JoinSet::new();
    let bytes = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let lease = if upload {
        let session = control
            .post(format!("{url}/v1/sessions"))
            .timeout(Duration::from_secs(3))
            .send()
            .await?
            .error_for_status()?
            .json::<SessionStats>()
            .await?;
        Some(SessionLease {
            client: control.clone(),
            url: format!("{url}/v1/sessions/{}", session.id),
        })
    } else {
        None
    };
    for _ in 0..settings.connections {
        let client = client(settings)?;
        let bytes = bytes.clone();
        let target = if let Some(lease) = &lease {
            lease.url.replace("/v1/sessions/", "/v1/upload/")
        } else {
            endpoint.download_url.clone()
        };
        let worker_endpoint = endpoint.clone();
        workers.spawn(async move {
            let data = payload();
            loop {
                if upload {
                    let data = data.clone();
                    let body = futures_util::stream::iter(
                        (0..128).map(move |_| Ok::<_, std::io::Error>(data.clone())),
                    );
                    client
                        .post(&target)
                        .header(header::CONTENT_TYPE, "application/octet-stream")
                        .header(header::CONTENT_LENGTH, 128 * 64 * 1024)
                        .body(reqwest::Body::wrap_stream(body))
                        .send()
                        .await?
                        .error_for_status()?
                        .bytes()
                        .await?;
                } else {
                    let response = provider::download_request(&client, &worker_endpoint, 16)?
                        .send()
                        .await?
                        .error_for_status()?;
                    provider::validate_download(&response, &worker_endpoint)?;
                    let mut stream = response.bytes_stream();
                    let mut batch = 0;
                    while let Some(chunk) = stream.next().await {
                        let len = chunk?.len() as u64;
                        batch += len;
                        bytes.fetch_add(len, Ordering::Relaxed);
                    }
                    if batch == 0 {
                        bail!("Server returned an empty download");
                    }
                }
                tokio::task::yield_now().await;
            }
            #[allow(unreachable_code)]
            Ok(())
        });
    }
    state.phase = if upload {
        Phase::Upload
    } else {
        Phase::Download
    };
    state.loaded = ProbeStats::default();
    state.warming_up = true;
    state.duration = settings.warmup.as_secs_f64();
    state.elapsed = 0.0;
    state.message = "Warming up · traffic counts toward totals".into();
    emit(tx, state);
    let session_url = lease.as_ref().map(|l| l.url.as_str());
    while started.elapsed() < settings.warmup {
        tokio::time::sleep(
            Duration::from_millis(150).min(settings.warmup.saturating_sub(started.elapsed())),
        )
        .await;
        check_workers(&mut workers)?;
        let (total, _) = counter(control, session_url, &bytes, started).await?;
        if upload {
            state.upload_bytes = total;
        } else {
            state.download_bytes = total;
        }
        state.elapsed = started.elapsed().as_secs_f64();
        emit(tx, state);
    }
    let baseline = counter(control, session_url, &bytes, started).await?;
    let mut last = baseline;
    let mut recent = VecDeque::new();
    recent.push_back(baseline);
    let mut samples = Vec::new();
    let (probe_tx, mut probe_rx) = mpsc::channel(16);
    // Owned JoinSet aborts this probe when the transfer future is dropped.
    let mut probe_tasks = JoinSet::new();
    let probe_client = probe.clone();
    let probe_endpoint = endpoint.clone();
    let interval = settings.probe_interval;
    probe_tasks.spawn(async move {
        let mut timer = tokio::time::interval(interval);
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            timer.tick().await;
            let sample = ping(&probe_client, &probe_endpoint).await.ok();
            if probe_tx
                .send((test_start.elapsed().as_secs_f64(), sample))
                .await
                .is_err()
            {
                break;
            }
        }
    });
    state.warming_up = false;
    state.duration = settings.duration.as_secs_f64();
    state.elapsed = 0.0;
    state.message = if upload {
        "Measuring server-confirmed upload"
    } else {
        "Measuring received download payload"
    }
    .into();
    emit(tx, state);
    loop {
        let elapsed = (last.1 - baseline.1) as f64 / 1e9;
        if elapsed >= settings.duration.as_secs_f64() {
            break;
        }
        tokio::time::sleep(Duration::from_secs_f64(
            (settings.duration.as_secs_f64() - elapsed).min(0.15),
        ))
        .await;
        check_workers(&mut workers)?;
        last = counter(control, session_url, &bytes, started).await?;
        if last.0 < baseline.0 || last.1 < baseline.1 {
            bail!("Server measurement counters moved backwards");
        }
        recent.push_back(last);
        while recent.len() > 2 && last.1 - recent[1].1 >= 1_000_000_000 {
            recent.pop_front();
        }
        let first = recent.front().unwrap();
        let speed = mbps(last.0 - first.0, (last.1 - first.1) as f64 / 1e9);
        state.elapsed = (last.1 - baseline.1) as f64 / 1e9;
        if upload {
            state.upload_mbps = speed;
            state.upload_bytes = last.0;
            state.upload_points.push((state.elapsed, speed));
        } else {
            state.download_mbps = speed;
            state.download_bytes = last.0;
            state.download_points.push((state.elapsed, speed));
        }
        while let Ok((time, sample)) = probe_rx.try_recv() {
            samples.push(sample);
            state.current_latency = sample;
            if let Some(rtt) = sample {
                state.latency_points.push((time, rtt));
            }
        }
        state.loaded = ProbeStats::from_samples(&samples);
        emit(tx, state);
    }
    check_workers(&mut workers)?;
    workers.abort_all();
    while workers.join_next().await.is_some() {}
    probe_tasks.abort_all();
    while probe_tasks.join_next().await.is_some() {}
    while let Ok((time, sample)) = probe_rx.try_recv() {
        samples.push(sample);
        if let Some(rtt) = sample {
            state.latency_points.push((time, rtt));
        }
    }
    let seconds = (last.1 - baseline.1) as f64 / 1e9;
    if last.0 == baseline.0 {
        bail!("No payload received during the measurement window");
    }
    let measured_bytes = last.0 - baseline.0;
    let total_bytes = if upload {
        // Capture any final bytes received during cancellation, for honest totals.
        counter(control, session_url, &bytes, started).await?.0
    } else {
        bytes.load(Ordering::Relaxed)
    };
    let result = Transfer {
        measurement_method: if upload {
            "server_received_counter"
        } else {
            "client_received_payload"
        }
        .into(),
        acknowledged_requests: None,
        average_mbps: mbps(measured_bytes, seconds),
        measured_bytes,
        total_bytes,
        measured_seconds: seconds,
        loaded_latency: ProbeStats::from_samples(&samples),
    };
    if upload {
        state.upload_mbps = result.average_mbps;
        state.upload_bytes = total_bytes;
    } else {
        state.download_mbps = result.average_mbps;
        state.download_bytes = total_bytes;
    }
    state.loaded = result.loaded_latency.clone();
    emit(tx, state);
    drop(lease);
    Ok(result)
}

fn make_record(
    settings: &Settings,
    name: String,
    url: String,
    idle: ProbeStats,
    download: Transfer,
    upload: Transfer,
) -> ResultRecord {
    ResultRecord {
        provider: settings.provider,
        server_id: None,
        failed_servers: Vec::new(),
        schema_version: 1,
        id: uuid::Uuid::new_v4().to_string(),
        timestamp_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
        server_name: name,
        transport: if settings.demo {
            "SIMULATED"
        } else if url.starts_with("https://") {
            "HTTPS / HTTP/1.1"
        } else {
            "HTTP / HTTP/1.1"
        }
        .into(),
        server_url: url,
        route: settings.route(),
        probe_method: "HTTP request/response RTT (warm persistent connection)".into(),
        simulated: settings.demo,
        connections: settings.connections,
        warmup_seconds: settings.warmup.as_secs_f64(),
        idle_latency: idle,
        download,
        upload,
    }
}

async fn demo(
    settings: Settings,
    tx: watch::Sender<Snapshot>,
    mut state: Snapshot,
) -> Result<ResultRecord> {
    state.server_name = "Demo · simulated network".into();
    state.server_url = "demo://pulse".into();
    state.route = "No network traffic".into();
    state.idle =
        ProbeStats::from_samples(&[Some(17.4), Some(18.6), Some(19.2), Some(18.1), Some(20.0)]);
    state.phase = Phase::Latency;
    state.message = "SIMULATION · these are illustrative metrics".into();
    emit(&tx, &state);
    tokio::time::sleep(Duration::from_millis(400)).await;
    let test_start = Instant::now();
    let mut results = Vec::new();
    for upload in [false, true] {
        state.phase = if upload {
            Phase::Upload
        } else {
            Phase::Download
        };
        state.duration = settings.duration.as_secs_f64();
        let started = Instant::now();
        let mut total = 0.0;
        let mut previous = 0.0;
        let mut samples = Vec::new();
        while started.elapsed() < settings.duration {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let t = started.elapsed().as_secs_f64();
            let base = if upload { 82.4 } else { 245.6 };
            let speed = base * (1.0 - (-t * 3.0).exp()) * (1.0 + (t * 4.1).sin() * 0.04);
            total += speed * 1_000_000.0 / 8.0 * (t - previous);
            previous = t;
            let rtt = 18.6 + 16.0 * (1.0 - (-t * 2.0).exp()) + (t * 6.0).sin().abs() * 9.0;
            samples.push(Some(rtt));
            state.current_latency = Some(rtt);
            state.loaded = ProbeStats::from_samples(&samples);
            state.elapsed = t;
            state
                .latency_points
                .push((test_start.elapsed().as_secs_f64(), rtt));
            if upload {
                state.upload_mbps = speed;
                state.upload_bytes = total as u64;
                state.upload_points.push((t, speed));
            } else {
                state.download_mbps = speed;
                state.download_bytes = total as u64;
                state.download_points.push((t, speed));
            }
            emit(&tx, &state);
        }
        results.push(Transfer {
            measurement_method: "simulated".into(),
            acknowledged_requests: None,
            average_mbps: mbps(total as u64, previous),
            measured_bytes: total as u64,
            total_bytes: total as u64,
            measured_seconds: previous,
            loaded_latency: ProbeStats::from_samples(&samples),
        });
    }
    let upload = results.pop().unwrap();
    let download = results.pop().unwrap();
    let mut record = make_record(
        &settings,
        state.server_name.clone(),
        state.server_url.clone(),
        state.idle.clone(),
        download,
        upload,
    );
    record.warmup_seconds = 0.0;
    record.route = state.route.clone();
    state.phase = Phase::Complete;
    state.download_mbps = record.download.average_mbps;
    state.upload_mbps = record.upload.average_mbps;
    state.message = "SIMULATION complete · no network traffic · not saved to history".into();
    state.result = Some(record.clone());
    emit(&tx, &state);
    Ok(record)
}
