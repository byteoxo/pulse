use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Phase {
    #[default]
    Ready,
    Selecting,
    Latency,
    Download,
    Cooldown,
    Upload,
    Complete,
    Cancelled,
    Failed,
}

impl Phase {
    pub fn label(self) -> &'static str {
        match self {
            Self::Ready => "Ready",
            Self::Selecting => "Selecting server",
            Self::Latency => "Idle latency",
            Self::Download => "Download",
            Self::Cooldown => "Cooldown",
            Self::Upload => "Upload",
            Self::Complete => "Complete",
            Self::Cancelled => "Cancelled",
            Self::Failed => "Failed",
        }
    }
    pub fn running(self) -> bool {
        !matches!(
            self,
            Self::Ready | Self::Complete | Self::Cancelled | Self::Failed
        )
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ProbeStats {
    pub median_ms: Option<f64>,
    pub p95_ms: Option<f64>,
    pub jitter_ms: Option<f64>,
    pub successful: usize,
    pub failed: usize,
}

impl ProbeStats {
    pub fn from_samples(samples: &[Option<f64>]) -> Self {
        let mut values: Vec<f64> = samples.iter().flatten().copied().collect();
        values.sort_by(f64::total_cmp);
        let median_ms = if values.is_empty() {
            None
        } else if values.len().is_multiple_of(2) {
            Some((values[values.len() / 2 - 1] + values[values.len() / 2]) / 2.0)
        } else {
            Some(values[values.len() / 2])
        };
        let p95_ms =
            (!values.is_empty()).then(|| values[(values.len() as f64 * 0.95).ceil() as usize - 1]);
        // Never bridge a timeout: only consecutive successful probes are paired.
        let differences: Vec<f64> = samples
            .windows(2)
            .filter_map(|p| Some((p[1]? - p[0]?).abs()))
            .collect();
        let jitter_ms = (!differences.is_empty())
            .then(|| differences.iter().sum::<f64>() / differences.len() as f64);
        Self {
            median_ms,
            p95_ms,
            jitter_ms,
            successful: values.len(),
            failed: samples.len() - values.len(),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Transfer {
    #[serde(default)]
    pub measurement_method: String,
    #[serde(default)]
    pub acknowledged_requests: Option<u64>,
    pub average_mbps: f64,
    pub measured_bytes: u64,
    pub total_bytes: u64,
    pub measured_seconds: f64,
    pub loaded_latency: ProbeStats,
}

pub fn mbps(bytes: u64, seconds: f64) -> f64 {
    if seconds > 0.0 {
        bytes as f64 * 8.0 / 1_000_000.0 / seconds
    } else {
        0.0
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResultRecord {
    #[serde(default = "crate::provider::Provider::pulse")]
    pub provider: crate::provider::Provider,
    #[serde(default)]
    pub server_id: Option<u64>,
    #[serde(default)]
    pub failed_servers: Vec<String>,
    pub schema_version: u32,
    pub id: String,
    pub timestamp_unix_ms: u64,
    pub server_name: String,
    pub server_url: String,
    pub transport: String,
    pub route: String,
    pub probe_method: String,
    pub simulated: bool,
    pub connections: usize,
    pub warmup_seconds: f64,
    pub idle_latency: ProbeStats,
    pub download: Transfer,
    pub upload: Transfer,
}

#[derive(Clone, Debug, Default)]
pub struct Snapshot {
    pub phase: Phase,
    pub server_name: String,
    pub server_url: String,
    pub route: String,
    pub simulated: bool,
    pub warming_up: bool,
    pub elapsed: f64,
    pub duration: f64,
    pub connections: usize,
    pub idle: ProbeStats,
    pub loaded: ProbeStats,
    pub current_latency: Option<f64>,
    pub download_mbps: f64,
    pub upload_mbps: f64,
    pub download_bytes: u64,
    pub upload_bytes: u64,
    pub download_points: Vec<(f64, f64)>,
    pub upload_points: Vec<(f64, f64)>,
    pub latency_points: Vec<(f64, f64)>,
    pub message: String,
    pub result: Option<ResultRecord>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServerInfo {
    pub protocol_version: u32,
    pub name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionStats {
    pub id: String,
    pub received_bytes: u64,
    pub elapsed_ns: u64,
}

pub fn payload() -> bytes::Bytes {
    let mut state = 0x9e3779b97f4a7c15_u64;
    let mut data = vec![0_u8; 64 * 1024];
    for chunk in data.as_chunks_mut::<8>().0 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        chunk.copy_from_slice(&state.to_le_bytes());
    }
    bytes::Bytes::from(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_units_and_elapsed_time() {
        assert_eq!(mbps(12_500_000, 1.0), 100.0);
        assert_eq!(mbps(25_000_000, 2.0) / 8.0, 12.5);
        assert_eq!(mbps(100, 0.0), 0.0);
    }

    #[test]
    fn jitter_does_not_bridge_failed_probes() {
        let stats =
            ProbeStats::from_samples(&[Some(10.0), Some(14.0), None, Some(100.0), Some(102.0)]);
        assert_eq!(stats.median_ms, Some(57.0));
        assert_eq!(stats.p95_ms, Some(102.0));
        assert_eq!(stats.jitter_ms, Some(3.0));
        assert_eq!(stats.failed, 1);
        assert_eq!(ProbeStats::from_samples(&[None, None]).median_ms, None);
        assert_eq!(ProbeStats::from_samples(&[Some(3.0)]).jitter_ms, None);
    }

    #[test]
    fn previous_history_records_keep_their_pulse_provider() {
        let probes = serde_json::json!({"median_ms":10.0,"p95_ms":12.0,"jitter_ms":2.0,"successful":20,"failed":0});
        let transfer = serde_json::json!({"average_mbps":100.0,"measured_bytes":125000000,"total_bytes":150000000,"measured_seconds":10.0,"loaded_latency":probes.clone()});
        let legacy = serde_json::json!({"schema_version":1,"id":"legacy","timestamp_unix_ms":1,"server_name":"Local Pulse","server_url":"http://localhost:8787","transport":"HTTP / HTTP/1.1","route":"Direct","probe_method":"HTTP RTT","simulated":false,"connections":4,"warmup_seconds":2.0,"idle_latency":probes,"download":transfer.clone(),"upload":transfer});
        let record: ResultRecord = serde_json::from_value(legacy).unwrap();
        assert_eq!(record.provider, crate::provider::Provider::Pulse);
        assert_eq!(record.server_id, None);
        assert_eq!(record.upload.acknowledged_requests, None);
        assert_eq!(record.upload.average_mbps, 100.0);
    }
}
