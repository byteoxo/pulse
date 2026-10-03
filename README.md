# Pulse

A Rust terminal dashboard for download speed, upload speed, latency, and jitter.

Pulse displays **Mbps**, **MB/s**, and total transferred **MB** together, with live throughput and latency graphs. The dashboard uses Ratatui; the asynchronous measurement engine uses Tokio and Reqwest. A companion Axum server supplies streaming payloads and server-received upload counters.

![Pulse terminal dashboard](docs/preview.png)

*A terminal capture at 120 × 42. Metrics shown are from the explicitly labeled demo mode.*

## Quick start

Install Rust 1.89 or newer, then build both binaries:

```sh
cargo build --release --locked
```

For a local smoke test, start the server in one terminal:

```sh
./target/release/pulse-server
```

Open the dashboard in another:

```sh
./target/release/pulse
```

Press **Enter** to start. The default endpoint is `http://127.0.0.1:8787`. A loopback test measures the local machine and networking stack; it does **not** measure your internet connection.

Try the interface without any server or network traffic:

```sh
./target/release/pulse --demo
```

Demo metrics are explicitly labeled and are never added to measurement history. Demo mode skips warm-up and uses the configured duration for each simulated direction.

## Test a remote connection

Run the companion server on another machine:

```sh
PULSE_SERVER_TOKEN=your-token ./target/release/pulse-server \
  --bind 0.0.0.0:8787 --name "Singapore 01"
```

Then connect from your client:

```sh
PULSE_TOKEN=your-token ./target/release/pulse \
  --server http://your-server:8787
```

For HTTPS, put the server behind a TLS reverse proxy and use its `https://` URL. Disable response compression, caching, request buffering, and response buffering on the measurement routes. Allow streaming uploads of at least 8 MiB and request durations longer than the test. Ensure the server's network capacity exceeds the connection you are measuring.

Pulse speaks its own versioned protocol. Arbitrary download URLs, Ookla endpoints, and LibreSpeed endpoints are not interchangeable with a Pulse server.

Repeat `--server` to automatically choose the reachable candidate with the lowest median RTT:

```sh
./target/release/pulse \
  --server https://speed-a.example.com \
  --server https://speed-b.example.com \
  --duration 10 --warmup 2 --connections 4
```

## Controls

| Key | Action |
| --- | --- |
| Enter | Start or repeat a full test |
| Esc | Cancel a test or close a dialog |
| S | Choose a server; A adds a URL for this session |
| H | Browse the last 100 completed tests |
| E | Export the current result or selected history entry |
| Up / Down, J / K | Navigate server and history lists |
| ? | Show help |
| Q / Ctrl-C | Quit and stop active transfers |

The dashboard adapts to terminal size. A 120 × 42 terminal displays three separate graphs; smaller windows combine the throughput graphs. Very small windows use a compact metrics view. Use `--ascii` for ASCII borders and simple graph markers. No icon font is required.

Color defaults to `auto` and respects `NO_COLOR`. Use `--color always` to enable the full cyan/violet/amber theme, or `--color never` for monochrome output.

## Measurement definitions

The normal sequence is server selection → idle probes → download → cooldown → upload → results. Download and upload run separately, with four independent HTTP/1.1 transfer connections by default. A separate connection probes latency during each measured transfer phase.

| Metric | Definition |
| --- | --- |
| Idle latency | Median HTTP(S) request/response RTT over 20 probes, on a warm persistent connection |
| P95 | Nearest-rank 95th percentile of successful probes |
| Jitter | Mean absolute difference between consecutive successful RTT probes; failures break adjacency |
| Probe failures | HTTP errors and timeouts; these are not IP packet-loss measurements |
| Loaded latency | Separate probe statistics collected during download and upload |
| Current speed | Payload throughput over approximately the most recent second |
| Final speed | Measured payload bytes divided by actual measurement elapsed time |
| Total MB | Payload transferred during warm-up, measurement, and final transfer shutdown |

Speed conversions use decimal units:

```text
Mbps = bytes / elapsed_seconds × 8 / 1,000,000
MB/s = Mbps / 8
MB   = total_bytes / 1,000,000
100 Mbps = 12.5 MB/s
```

Upload throughput comes from server-received byte counters and server monotonic timestamps sampled together. It does not count data merely queued in client buffers. Download throughput counts payload delivered to the client. Neither direction includes HTTP/TLS headers or link-layer overhead.

Connection setup and the configurable warm-up are excluded from final speeds. The default measurement window is 10 seconds per direction. HTTP polling and scheduling can make the actual window slightly longer; the result records the actual elapsed time. HTTP RTT includes server processing and differs from ICMP ping or one-way latency.

## Headless results and configuration

Run once and print machine-readable JSON:

```sh
./target/release/pulse --server https://speed.example.com --json
./target/release/pulse --json --output result.json
```

Completed real tests are saved locally. Cancelled, failed, and demo runs are not saved. `--output` writes a completed result to a specified path; E exports into the application's `exports` directory by default. The JSON includes units in field names, actual measurement durations, byte totals, probe statistics, route, transport, concurrency, and schema version.

On macOS the default history directory is `~/Library/Application Support/dev.byteoxo.pulse/history`; on Linux it follows XDG data-directory conventions, and on Windows it uses the application local-data directory. `--history-dir PATH` overrides it. Logs are written to `pulse.log` alongside the history directory. Corrupt or unsupported history entries are skipped and logged.

Use [pulse.example.toml](pulse.example.toml) with `--config`:

```sh
./target/release/pulse --config pulse.example.toml
```

Explicit command-line options override the file. Credentials belong in `PULSE_TOKEN`; result files do not contain bearer tokens or proxy credentials.

Connections are **direct by default**, regardless of system proxy settings. Use an explicit proxy to measure that route:

```sh
./target/release/pulse --server https://speed.example.com \
  --proxy socks5h://127.0.0.1:1080
```

## Server protocol

| Endpoint | Purpose |
| --- | --- |
| `GET /v1/info` | Server name and protocol version |
| `GET /v1/ping` | Small uncached response for RTT probes |
| `GET /v1/download` | Streaming 16 MiB payload batch; repeated by the client |
| `POST /v1/sessions` | Create an isolated upload session |
| `POST /v1/upload/{id}` | Stream and discard payload; increment received byte counter |
| `GET /v1/sessions/{id}` | Atomically sample received bytes and monotonic elapsed nanoseconds |
| `DELETE /v1/sessions/{id}` | Clean up a session |

Payloads reuse a precomputed pseudorandom 64 KiB block and are not compressed. Memory use is bounded rather than proportional to transfer size. Upload requests are limited to 64 MiB, stalled body reads time out after 30 seconds, and inactive session entries expire after 15 minutes. The server permits up to 256 sessions. Server bearer authentication is optional and applies to every endpoint when configured.

## Development

```sh
cargo fmt --all --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```

Tests exercise decimal conversions, percentile/jitter definitions, responsive rendering, authenticated server counters, session isolation, a full test with shaped bandwidth and latency, transfer failure, cancellation, and cancelled-upload cleanup.

```text
src/config.rs       CLI and TOML settings
src/model.rs        Metrics, statistics, protocol types
src/engine.rs       Async probes and transfer workers
src/ui.rs           Responsive dashboard and dialogs
src/history.rs      Atomic JSON history and exports
src/server.rs       Companion HTTP service
src/main.rs         TUI lifecycle and keyboard handling
src/bin/server.rs   Server entry point
```

Licensed under MIT.
