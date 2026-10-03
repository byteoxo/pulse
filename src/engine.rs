use crate::{
    config::Settings,
    model::{
        Phase, ProbeStats, ResultRecord, ServerInfo, SessionStats, Snapshot, Transfer, mbps,
        payload,
    },
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

fn client(settings: &Settings) -> Result<Client> {
    let mut headers = header::HeaderMap::new();
    headers.insert(
        header::ACCEPT_ENCODING,
        header::HeaderValue::from_static("identity"),
    );
    headers.insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-cache"),
    );
    if let Some(token) = &settings.token {
        let mut value = header::HeaderValue::from_str(&format!("Bearer {token}"))
            .context("Invalid bearer token")?;
        value.set_sensitive(true);
        headers.insert(header::AUTHORIZATION, value);
    }
    let mut builder = Client::builder()
        .no_proxy()
        .http1_only()
        .default_headers(headers)
        .connect_timeout(Duration::from_secs(3))
        // An upload request can still be streaming when the phase ends. A short
        // response-read timeout would incorrectly reject slow connections.
        .read_timeout(settings.warmup + settings.duration + Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::none())
        .user_agent(concat!("pulse/", env!("CARGO_PKG_VERSION")));
    if let Some(proxy) = &settings.proxy {
        builder = builder.proxy(reqwest::Proxy::all(proxy).context("Invalid proxy URL")?);
    }
    Ok(builder.build()?)
}

async fn ping(client: &Client, url: &str) -> Result<f64> {
    let started = Instant::now();
    let body = client
        .get(format!("{url}/v1/ping"))
        .timeout(Duration::from_secs(2))
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    if body.as_ref() != b"pulse" {
        bail!("Unexpected ping response; this endpoint is not a Pulse server");
    }
    Ok(started.elapsed().as_secs_f64() * 1000.0)
}

fn emit(tx: &watch::Sender<Snapshot>, state: &Snapshot) {
    tx.send_replace(state.clone());
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
    let mut best: Option<(f64, String, String)> = None;
    let mut failures = Vec::new();
    for url in &settings.servers {
        state.server_url = url.clone();
        state.message = format!("Checking {url}");
        emit(&tx, &state);
        let candidate = async {
            let info = control
                .get(format!("{url}/v1/info"))
                .timeout(Duration::from_secs(3))
                .send()
                .await?
                .error_for_status()?
                .json::<ServerInfo>()
                .await?;
            if info.protocol_version != 1 {
                bail!("Unsupported Pulse protocol {}", info.protocol_version);
            }
            let _ = ping(&probe, url).await?; // Exclude DNS/TCP/TLS establishment.
            let mut samples = Vec::new();
            for _ in 0..3 {
                samples.push(ping(&probe, url).await.ok());
            }
            let rtt = ProbeStats::from_samples(&samples)
                .median_ms
                .context("No successful probes")?;
            let name: String = info
                .name
                .chars()
                .filter(|c| !c.is_control())
                .take(80)
                .collect();
            Ok::<_, anyhow::Error>((rtt, url.clone(), name))
        }
        .await;
        match candidate {
            Ok(candidate) if best.as_ref().is_none_or(|b| candidate.0 < b.0) => {
                best = Some(candidate)
            }
            Ok(_) => {}
            Err(error) => failures.push(format!("{url}: {error}")),
        }
    }
    let (_, url, name) = best.with_context(|| {
        format!(
            "No reachable Pulse server. Start pulse-server or pass --server URL. {}",
            failures.join("; ")
        )
    })?;
    state.server_url = url.clone();
    state.server_name = name.clone();
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
        let sample = ping(&probe, &url).await.ok();
        idle_samples.push(sample);
        state.current_latency = sample;
        state.idle = ProbeStats::from_samples(&idle_samples);
        state.elapsed = idle_start.elapsed().as_secs_f64();
        if let Some(rtt) = sample {
            state
                .latency_points
                .push((test_start.elapsed().as_secs_f64(), rtt));
        }
        emit(&tx, &state);
    }
    if state.idle.successful == 0 {
        bail!("All idle latency probes failed");
    }
    let download = transfer(
        false, &settings, &control, &probe, &url, &tx, &mut state, test_start,
    )
    .await?;
    state.phase = Phase::Cooldown;
    state.elapsed = 0.0;
    state.duration = settings.cooldown.as_secs_f64();
    state.message = "Letting the connection settle".into();
    emit(&tx, &state);
    tokio::time::sleep(settings.cooldown).await;
    let upload = transfer(
        true, &settings, &control, &probe, &url, &tx, &mut state, test_start,
    )
    .await?;
    let record = make_record(&settings, name, url, state.idle.clone(), download, upload);
    state.phase = Phase::Complete;
    state.download_mbps = record.download.average_mbps;
    state.upload_mbps = record.upload.average_mbps;
    state.message = "Finished · average speeds exclude warm-up · totals include warm-up".into();
    state.result = Some(record.clone());
    emit(&tx, &state);
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
    url: &str,
    tx: &watch::Sender<Snapshot>,
    state: &mut Snapshot,
    test_start: Instant,
) -> Result<Transfer> {
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
            format!("{url}/v1/download")
        };
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
                    let response = client.get(&target).send().await?.error_for_status()?;
                    if response
                        .headers()
                        .get(header::CONTENT_ENCODING)
                        .is_some_and(|v| v != "identity")
                    {
                        bail!("Compressed downloads are not valid measurements");
                    }
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
    let probe_url = url.to_owned();
    let interval = settings.probe_interval;
    probe_tasks.spawn(async move {
        let mut timer = tokio::time::interval(interval);
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            timer.tick().await;
            let sample = ping(&probe_client, &probe_url).await.ok();
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
