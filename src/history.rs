use crate::model::ResultRecord;
use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

pub fn default_dir() -> PathBuf {
    directories::ProjectDirs::from("dev", "byteoxo", "pulse")
        .map(|p| p.data_local_dir().join("history"))
        .unwrap_or_else(|| PathBuf::from(".pulse/history"))
}

pub fn write_json(path: &Path, record: &ResultRecord) -> Result<()> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(".pulse-{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        use std::io::Write;
        file.write_all(&serde_json::to_vec_pretty(record)?)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        std::fs::rename(&temporary, path)?;
        Ok::<_, anyhow::Error>(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result.with_context(|| format!("Writing {}", path.display()))
}

pub fn save(dir: &Path, record: &ResultRecord) -> Result<PathBuf> {
    let path = dir.join(format!("{}-{}.json", record.timestamp_unix_ms, record.id));
    write_json(&path, record)?;
    Ok(path)
}

pub fn load(dir: &Path) -> Result<Vec<ResultRecord>> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut records = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().is_some_and(|s| s == "json") {
            match std::fs::read(&path)
                .ok()
                .and_then(|data| serde_json::from_slice::<ResultRecord>(&data).ok())
            {
                Some(record) if record.schema_version == 1 => records.push(record),
                _ => {
                    tracing::warn!(file = %path.display(), "Skipping unreadable or unsupported history entry")
                }
            }
        }
    }
    records.sort_by_key(|r| std::cmp::Reverse(r.timestamp_unix_ms));
    records.truncate(100);
    Ok(records)
}
