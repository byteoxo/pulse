use crate::provider::{Endpoint, Provider};
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_public_and_existing_server_commands_remain_pulse() {
        let default = Settings::from_args(&Args::parse_from(["pulse"])).unwrap();
        assert_eq!(default.provider, Provider::Librespeed);
        assert!(default.servers.is_empty());
        let legacy = Settings::from_args(&Args::parse_from([
            "pulse",
            "--server",
            "http://localhost:8787",
        ]))
        .unwrap();
        assert_eq!(legacy.provider, Provider::Pulse);
        assert_eq!(legacy.servers[0].ping_url, "http://localhost:8787/v1/ping");
        let public = Settings::from_args(&Args::parse_from([
            "pulse",
            "--provider",
            "librespeed",
            "--server",
            "https://example.com/backend",
        ]))
        .unwrap();
        assert_eq!(
            public.servers[0].download_url,
            "https://example.com/backend/garbage.php"
        );
        let local =
            Settings::from_args(&Args::parse_from(["pulse", "--provider", "pulse"])).unwrap();
        assert_eq!(local.servers[0].base_url, "http://127.0.0.1:8787");
    }
}

use std::{path::PathBuf, time::Duration};

#[derive(Clone, Debug, Parser)]
#[command(
    name = "pulse",
    version,
    about = "Beautiful network measurements in your terminal"
)]
pub struct Args {
    /// Provider. Default: LibreSpeed; an explicit --server without this flag preserves Pulse compatibility.
    #[arg(long, value_enum)]
    pub provider: Option<Provider>,
    /// Server base URL for the chosen provider. Repeat to select the lowest RTT.
    #[arg(short, long)]
    pub server: Vec<String>,
    /// LibreSpeed directory URL or local JSON file, using standard endpoint mappings.
    #[arg(long)]
    pub server_list: Option<String>,
    /// Select a public LibreSpeed server ID from its directory.
    #[arg(long, conflicts_with = "server")]
    pub server_id: Option<u64>,
    /// Print available servers without running a speed test; --json returns JSON.
    #[arg(long, conflicts_with = "demo")]
    pub list_servers: bool,
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
    provider: Option<Provider>,
    servers: Option<Vec<String>>,
    server_list: Option<String>,
    server_id: Option<u64>,
    duration: Option<f64>,
    warmup: Option<f64>,
    connections: Option<usize>,
    proxy: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Settings {
    pub provider: Provider,
    pub servers: Vec<Endpoint>,
    pub server_list: Option<String>,
    pub server_id: Option<u64>,
    pub server_cache: Option<PathBuf>,
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
        let mut servers = if args.server.is_empty() {
            file.servers.unwrap_or_default()
        } else {
            args.server.clone()
        };
        let server_list = args.server_list.clone().or(file.server_list);
        let server_id = args.server_id.or(file.server_id);
        let provider = args.provider.or(file.provider).unwrap_or_else(|| {
            if !servers.is_empty() && server_list.is_none() && server_id.is_none() {
                Provider::Pulse
            } else {
                Provider::Librespeed
            }
        });
        if servers.len() > 32 {
            bail!("Configure at most 32 explicit servers");
        }
        if !servers.is_empty() && (server_list.is_some() || server_id.is_some()) {
            bail!("Choose explicit servers or a LibreSpeed directory/ID");
        }
        if provider == Provider::Pulse {
            if server_list.is_some() || server_id.is_some() {
                bail!("Server lists and IDs require the LibreSpeed provider");
            }
            if servers.is_empty() {
                servers.push("http://127.0.0.1:8787".into());
            }
        }
        let server_cache = args
            .history_dir
            .as_ref()
            .map(|p| p.join("librespeed-directory.cache"))
            .or_else(crate::provider::default_cache);
        Ok(Self {
            provider,
            servers: servers
                .iter()
                .map(|s| Endpoint::from_base(provider, s))
                .collect::<Result<_>>()?,
            server_list,
            server_id,
            server_cache,
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
