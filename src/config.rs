use anyhow::{Context, Result, bail};
use clap::{Parser, ValueEnum};
use serde::Deserialize;

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
pub enum ColorMode {
    #[default]
    Auto,
    Always,
    Never,
}
use std::{path::PathBuf, time::Duration};

#[derive(Clone, Debug, Parser)]
#[command(
    name = "pulse",
    version,
    about = "Beautiful network measurements in your terminal"
)]
pub struct Args {
    /// Pulse server base URL. Repeat to automatically select the lowest RTT.
    #[arg(short, long)]
    pub server: Vec<String>,
    /// Measurement seconds per direction, excluding warm-up.
    #[arg(long)]
    pub duration: Option<f64>,
    /// Warm-up seconds per direction.
    #[arg(long)]
    pub warmup: Option<f64>,
    /// Independent HTTP/1.1 transfer connections.
    #[arg(short, long)]
    pub connections: Option<usize>,
    /// Explicit HTTP(S) or SOCKS5 proxy; otherwise measurements are direct.
    #[arg(long)]
    pub proxy: Option<String>,
    /// Server bearer token; prefer the environment variable to command-line arguments.
    #[arg(long, env = "PULSE_TOKEN", hide_env_values = true)]
    pub token: Option<String>,
    /// Run once without the TUI and print the result as JSON.
    #[arg(long)]
    pub json: bool,
    /// Simulated metrics, clearly labeled; no network traffic or history writes.
    #[arg(long)]
    pub demo: bool,
    /// Optional TOML configuration file.
    #[arg(long)]
    pub config: Option<PathBuf>,
    #[arg(long)]
    pub history_dir: Option<PathBuf>,
    /// Write the completed result to this JSON file.
    #[arg(long)]
    pub output: Option<PathBuf>,
    /// Use ASCII borders and graph markers.
    #[arg(long)]
    pub ascii: bool,
    /// Color policy. Auto respects NO_COLOR; always enables the dashboard theme.
    #[arg(long, value_enum, default_value = "auto")]
    pub color: ColorMode,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    servers: Option<Vec<String>>,
    duration: Option<f64>,
    warmup: Option<f64>,
    connections: Option<usize>,
    proxy: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Settings {
    pub servers: Vec<String>,
    pub duration: Duration,
    pub warmup: Duration,
    pub connections: usize,
    pub proxy: Option<String>,
    pub token: Option<String>,
    pub demo: bool,
    pub idle_probes: usize,
    pub probe_interval: Duration,
    pub cooldown: Duration,
}

pub fn normalize_server(value: &str) -> Result<String> {
    let mut url = reqwest::Url::parse(value.trim())
        .context("Server must be a full http:// or https:// URL")?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        bail!("Server must use HTTP or HTTPS");
    }
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        bail!("Server URLs cannot include credentials, a query, or a fragment");
    }
    let path = url.path().trim_end_matches('/').to_owned();
    url.set_path(&path);
    Ok(url.as_str().trim_end_matches('/').to_owned())
}

impl Settings {
    pub fn from_args(args: &Args) -> Result<Self> {
        let file: FileConfig = match &args.config {
            Some(path) => toml::from_str(
                &std::fs::read_to_string(path)
                    .with_context(|| format!("Reading {}", path.display()))?,
            )?,
            None => FileConfig::default(),
        };
        let duration = args.duration.or(file.duration).unwrap_or(10.0);
        let warmup = args.warmup.or(file.warmup).unwrap_or(2.0);
        let connections = args.connections.or(file.connections).unwrap_or(4);
        if !duration.is_finite() || !(0.2..=120.0).contains(&duration) {
            bail!("Duration must be 0.2–120 seconds");
        }
        if !warmup.is_finite() || !(0.0..=30.0).contains(&warmup) {
            bail!("Warm-up must be 0–30 seconds");
        }
        if !(1..=32).contains(&connections) {
            bail!("Connections must be 1–32");
        }
        let servers = if args.server.is_empty() {
            file.servers
                .unwrap_or_else(|| vec!["http://127.0.0.1:8787".into()])
        } else {
            args.server.clone()
        };
        if servers.is_empty() || servers.len() > 32 {
            bail!("Configure between 1 and 32 servers");
        }
        Ok(Self {
            servers: servers
                .iter()
                .map(|s| normalize_server(s))
                .collect::<Result<_>>()?,
            duration: Duration::from_secs_f64(duration),
            warmup: Duration::from_secs_f64(warmup),
            connections,
            proxy: args.proxy.clone().or(file.proxy),
            token: args.token.clone(),
            demo: args.demo,
            idle_probes: 20,
            probe_interval: Duration::from_millis(200),
            cooldown: Duration::from_secs(1),
        })
    }
    pub fn route(&self) -> String {
        // Never display a proxy URL, which may contain credentials.
        if self.proxy.is_some() {
            "Explicit proxy"
        } else {
            "Direct"
        }
        .into()
    }
}
