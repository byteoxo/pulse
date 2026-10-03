use crate::{
    config::{Settings, normalize_server},
    history,
    model::ServerInfo,
};
use anyhow::{Context, Result, bail};
use clap::ValueEnum;
use futures_util::StreamExt;
use reqwest::{Client, Url, header};
use serde::{Deserialize, Serialize};
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

pub const PUBLIC_DIRECTORY: &str = "https://librespeed.org/backend-servers/servers.php";
const MAX_DIRECTORY_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Provider {
    #[default]
    Librespeed,
    Pulse,
}

impl Provider {
    pub fn label(self) -> &'static str {
        match self {
            Self::Librespeed => "LibreSpeed",
            Self::Pulse => "Pulse",
        }
    }
    pub fn pulse() -> Self {
        Self::Pulse
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Endpoint {
    pub provider: Provider,
    pub id: Option<u64>,
    pub name: String,
    pub base_url: String,
    pub ping_url: String,
    pub download_url: String,
    pub upload_url: String,
}

impl Endpoint {
    pub fn from_base(provider: Provider, base: &str) -> Result<Self> {
        let base = normalize_server(base)?;
        let (ping, download, upload) = match provider {
            Provider::Pulse => ("v1/ping", "v1/download", "v1/upload"),
            Provider::Librespeed => ("empty.php", "garbage.php", "empty.php"),
        };
        Ok(Self {
            provider,
            id: None,
            name: base.clone(),
            ping_url: endpoint_url(&base, ping)?,
            download_url: endpoint_url(&base, download)?,
            upload_url: endpoint_url(&base, upload)?,
            base_url: base,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PublicServer {
    #[serde(default)]
    pub id: Option<u64>,
    pub name: String,
    pub server: String,
    #[serde(rename = "dlURL")]
    pub download: String,
    #[serde(rename = "ulURL")]
    pub upload: String,
    #[serde(rename = "pingURL")]
    pub ping: String,
}

impl PublicServer {
    fn endpoint(&self) -> Result<Endpoint> {
        let server = if self.server.starts_with("//") {
            format!("https:{}", self.server)
        } else {
            self.server.clone()
        };
        let base = normalize_server(&server)?;
        let name: String = self
            .name
            .chars()
            .filter(|c| !c.is_control())
            .take(100)
            .collect();
        Ok(Endpoint {
            provider: Provider::Librespeed,
            id: self.id,
            name,
            ping_url: endpoint_url(&base, &self.ping)?,
            download_url: endpoint_url(&base, &self.download)?,
            upload_url: endpoint_url(&base, &self.upload)?,
            base_url: base,
        })
    }
}

fn endpoint_url(base: &str, path: &str) -> Result<String> {
    if path.is_empty() {
        bail!("Missing measurement endpoint");
    }
    let base = Url::parse(&format!("{base}/"))?;
    let url = base.join(path)?;
    if url.origin() != base.origin()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        bail!("Measurement endpoint must belong to its server origin");
    }
    Ok(url.to_string())
}

pub fn default_cache() -> Option<PathBuf> {
    directories::ProjectDirs::from("dev", "byteoxo", "pulse")
        .map(|p| p.data_local_dir().join("librespeed-servers.json"))
}

pub fn client(settings: &Settings) -> Result<Client> {
    let mut headers = header::HeaderMap::new();
    headers.insert(
        header::ACCEPT_ENCODING,
        header::HeaderValue::from_static("identity"),
    );
    headers.insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-cache"),
    );
    // PULSE_TOKEN is never forwarded to the public directory or public servers.
    if settings.provider == Provider::Pulse
        && let Some(token) = &settings.token
    {
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
        .read_timeout(settings.warmup + settings.duration + Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::none())
        .user_agent(concat!("pulse/", env!("CARGO_PKG_VERSION")));
    if let Some(proxy) = &settings.proxy {
        builder = builder.proxy(reqwest::Proxy::all(proxy).context("Invalid proxy URL")?);
    }
    Ok(builder.build()?)
}

pub struct Catalog {
    pub servers: Vec<Endpoint>,
    pub notice: String,
}

fn parse_directory(data: &[u8]) -> Result<(Vec<PublicServer>, Vec<Endpoint>)> {
    if data.len() > MAX_DIRECTORY_BYTES {
        bail!("Server directory exceeds 1 MiB");
    }
    let entries: Vec<PublicServer> =
        serde_json::from_slice(data).context("Invalid LibreSpeed server directory JSON")?;
    if entries.len() > 256 {
        bail!("Server directory has more than 256 entries");
    }
    let servers = entries
        .iter()
        .filter_map(|entry| match entry.endpoint() {
            Ok(endpoint) => Some(endpoint),
            Err(error) => {
                tracing::warn!(error = %error, "Skipping invalid public server entry");
                None
            }
        })
        .collect::<Vec<_>>();
    if servers.is_empty() {
        bail!("Server directory contains no valid endpoints");
    }
    Ok((entries, servers))
}

async fn read_directory(settings: &Settings, source: &str) -> Result<Vec<u8>> {
    if source.starts_with("https://") || source.starts_with("http://") {
        let url = Url::parse(source)?;
        if !url.username().is_empty() || url.password().is_some() {
            bail!("Directory URLs cannot contain credentials");
        }
        let mut response = client(settings)?
            .get(url)
            .timeout(Duration::from_secs(5))
            .send()
            .await?
            .error_for_status()?;
        let mut data = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            if data.len() + chunk.len() > MAX_DIRECTORY_BYTES {
                bail!("Server directory exceeds 1 MiB");
            }
            data.extend_from_slice(&chunk);
        }
        Ok(data)
    } else {
        if tokio::fs::metadata(source).await?.len() > MAX_DIRECTORY_BYTES as u64 {
            bail!("Server directory exceeds 1 MiB");
        }
        Ok(tokio::fs::read(source).await?)
    }
}

fn bundled() -> Vec<PublicServer> {
    // Small fallback list from the public directory, verified 2026-10-03.
    // Each candidate is re-probed and preflighted before any speed measurement.
    [
        (
            54,
            "Los Angeles, US (Clouvider)",
            "https://la.speedtest.clouvider.net/backend",
            "garbage.php",
            "empty.php",
        ),
        (
            49,
            "London, UK (Clouvider)",
            "https://lon.speedtest.clouvider.net/backend",
            "garbage.php",
            "empty.php",
        ),
        (
            79,
            "Prague, CZ (CESNET)",
            "https://speedtest.cesnet.cz",
            "backend/garbage.php",
            "backend/empty.php",
        ),
    ]
    .into_iter()
    .map(|(id, name, server, download, ping)| PublicServer {
        id: Some(id),
        name: name.into(),
        server: server.into(),
        download: download.into(),
        upload: ping.into(),
        ping: ping.into(),
    })
    .collect()
}

pub async fn discover(settings: &Settings) -> Result<Catalog> {
    if !settings.servers.is_empty() {
        return Ok(Catalog {
            servers: settings.servers.clone(),
            notice: String::new(),
        });
    }
    let source = settings.server_list.as_deref().unwrap_or(PUBLIC_DIRECTORY);
    let fetched = match read_directory(settings, source).await {
        Ok(data) => parse_directory(&data),
        Err(error) => Err(error),
    };
    let (mut servers, notice) = match fetched {
        Ok((entries, servers)) => {
            if settings.server_list.is_none()
                && let Some(cache) = &settings.server_cache
                && let Err(error) = history::write_json(cache, &entries)
            {
                tracing::warn!(error = %error, "Could not cache public directory");
            }
            (servers, "Public server directory loaded".into())
        }
        Err(error) if settings.server_list.is_some() => {
            return Err(error).context("Loading the configured LibreSpeed server list");
        }
        Err(error) => {
            tracing::warn!(error = %error, "Public directory unavailable");
            let fallback = fallback_catalog(settings).await?;
            (fallback.servers, fallback.notice)
        }
    };
    if let Some(id) = settings.server_id {
        servers.retain(|s| s.id == Some(id));
        if servers.is_empty() {
            bail!("LibreSpeed server ID {id} is not in the available directory");
        }
    }
    Ok(Catalog { servers, notice })
}

async fn fallback_catalog(settings: &Settings) -> Result<Catalog> {
    if let Some(path) = &settings.server_cache
        && let Ok(data) = read_directory(settings, &path.to_string_lossy()).await
        && let Ok((_, servers)) = parse_directory(&data)
    {
        return Ok(Catalog {
            servers,
            notice: "Directory unavailable · using cached public servers".into(),
        });
    }
    Ok(Catalog {
        servers: bundled()
            .iter()
            .map(PublicServer::endpoint)
            .collect::<Result<_>>()?,
        notice: "Directory unavailable · using bundled public servers".into(),
    })
}

pub fn uncached(url: &str) -> Result<Url> {
    let mut url = Url::parse(url)?;
    url.query_pairs_mut()
        .append_pair("r", &uuid::Uuid::new_v4().to_string());
    Ok(url)
}

pub fn download_request(
    client: &Client,
    endpoint: &Endpoint,
    chunks: u32,
) -> Result<reqwest::RequestBuilder> {
    let mut url = uncached(&endpoint.download_url)?;
    if endpoint.provider == Provider::Librespeed {
        url.query_pairs_mut()
            .append_pair("ckSize", &chunks.to_string());
    }
    Ok(client.get(url))
}

pub fn validate_download(response: &reqwest::Response, endpoint: &Endpoint) -> Result<()> {
    if response
        .headers()
        .get(header::CONTENT_ENCODING)
        .is_some_and(|v| v != "identity")
    {
        bail!("Compressed downloads are not valid measurements");
    }
    if endpoint.provider == Provider::Librespeed {
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if !content_type.starts_with("application/octet-stream") {
            bail!(
                "LibreSpeed download endpoint returned {content_type:?}, expected binary payload"
            );
        }
    }
    Ok(())
}

pub async fn ping(client: &Client, endpoint: &Endpoint) -> Result<f64> {
    let started = Instant::now();
    let response = client
        .get(uncached(&endpoint.ping_url)?)
        .timeout(Duration::from_secs(2))
        .send()
        .await?
        .error_for_status()?;
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if body.len() + chunk.len() > 4096 {
            bail!("Latency endpoint returned an unexpected response");
        }
        body.extend_from_slice(&chunk);
    }
    match endpoint.provider {
        Provider::Pulse if body.as_slice() != b"pulse" => bail!("Endpoint is not a Pulse server"),
        Provider::Librespeed if !body.is_empty() => {
            bail!("LibreSpeed latency endpoint must return an empty body")
        }
        _ => {}
    }
    Ok(started.elapsed().as_secs_f64() * 1000.0)
}

pub async fn identify(client: &Client, endpoint: &mut Endpoint) -> Result<()> {
    if endpoint.provider == Provider::Pulse {
        let info = client
            .get(format!("{}/v1/info", endpoint.base_url))
            .timeout(Duration::from_secs(3))
            .send()
            .await?
            .error_for_status()?
            .json::<ServerInfo>()
            .await?;
        if info.protocol_version != 1 {
            bail!("Unsupported Pulse protocol {}", info.protocol_version);
        }
        endpoint.name = info
            .name
            .chars()
            .filter(|c| !c.is_control())
            .take(100)
            .collect();
    }
    Ok(())
}

pub async fn preflight(client: &Client, endpoint: &Endpoint) -> Result<()> {
    if endpoint.provider == Provider::Pulse {
        return Ok(());
    }
    let response = download_request(client, endpoint, 1)?
        .timeout(Duration::from_secs(3))
        .send()
        .await?
        .error_for_status()?;
    validate_download(&response, endpoint)?;
    let first = response
        .bytes_stream()
        .next()
        .await
        .context("Empty download response")??;
    if first.is_empty() {
        bail!("Empty download payload");
    }
    let response = client
        .post(uncached(&endpoint.upload_url)?)
        .timeout(Duration::from_secs(3))
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(vec![0_u8; 16 * 1024])
        .send()
        .await?
        .error_for_status()?;
    let body = response.bytes().await?;
    if !body.is_empty() {
        bail!("LibreSpeed upload endpoint returned an unexpected response");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn directory_paths_are_resolved_and_cross_origin_paths_are_rejected() {
        let data = br#"[{"id":79,"name":"Prague","server":"//speedtest.example.com","dlURL":"backend/random?size=1","ulURL":"backend/sink","pingURL":"backend/ping"}]"#;
        let (_, endpoints) = parse_directory(data).unwrap();
        assert_eq!(
            endpoints[0].download_url,
            "https://speedtest.example.com/backend/random?size=1"
        );
        assert!(endpoint_url("https://example.com", "https://other.example.com/upload").is_err());
        assert!(
            endpoint_url(
                "https://example.com",
                "https://user:secret@example.com/upload"
            )
            .is_err()
        );
        let url = uncached(&endpoints[0].download_url).unwrap();
        assert!(url.query_pairs().any(|(k, v)| k == "size" && v == "1"));
        assert!(url.query_pairs().any(|(k, _)| k == "r"));
    }

    #[tokio::test]
    async fn offline_directory_uses_valid_cache_then_bundled_fallback() {
        use clap::Parser;
        let folder =
            std::env::temp_dir().join(format!("pulse-catalog-test-{}", uuid::Uuid::new_v4()));
        struct Scratch(PathBuf);
        impl Drop for Scratch {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let _scratch = Scratch(folder.clone());
        let path = folder.join("catalog.cache");
        let mut settings =
            Settings::from_args(&crate::config::Args::parse_from(["pulse"])).unwrap();
        settings.server_cache = Some(path.clone());
        let fallback = fallback_catalog(&settings).await.unwrap();
        assert_eq!(fallback.servers.len(), 3);
        assert!(fallback.notice.contains("bundled"));
        let entries = vec![PublicServer {
            id: Some(7),
            name: "Cached".into(),
            server: "https://example.com/backend".into(),
            download: "random".into(),
            upload: "sink".into(),
            ping: "ping".into(),
        }];
        history::write_json(&path, &entries).unwrap();
        let cached = fallback_catalog(&settings).await.unwrap();
        assert_eq!(cached.servers[0].id, Some(7));
        assert_eq!(
            cached.servers[0].download_url,
            "https://example.com/backend/random"
        );
        assert!(cached.notice.contains("cached"));
        std::fs::write(path, b"invalid JSON").unwrap();
        assert_eq!(fallback_catalog(&settings).await.unwrap().servers.len(), 3);
    }
}
