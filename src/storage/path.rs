use std::path::PathBuf;

use super::StorageError;

pub(crate) fn expand_home_path(raw_path: &str) -> Result<PathBuf, StorageError> {
    if raw_path.starts_with("~/") {
        let home = std::env::var("HOME").map_err(|_| StorageError::HomeDirectoryUnavailable)?;
        return Ok(PathBuf::from(home).join(raw_path.trim_start_matches("~/")));
    }

    if raw_path == "$HOME" || raw_path.starts_with("$HOME/") {
        let home = std::env::var("HOME").map_err(|_| StorageError::HomeDirectoryUnavailable)?;
        let suffix = raw_path.strip_prefix("$HOME").unwrap_or_default();
        return Ok(PathBuf::from(format!("{home}{suffix}")));
    }

    Ok(PathBuf::from(raw_path))
}
