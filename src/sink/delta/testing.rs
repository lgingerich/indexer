//! A directory for one test's lake.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

/// A fresh directory for one test's lake, removed when the guard drops.
pub(crate) struct Dir(pub(crate) PathBuf);

impl Dir {
    pub(crate) fn new() -> Self {
        static NEXT: AtomicU32 = AtomicU32::new(0);
        Self(std::env::temp_dir().join(format!(
            "indexer-lake-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )))
    }

    pub(crate) fn uri(&self) -> String {
        self.0.display().to_string()
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
