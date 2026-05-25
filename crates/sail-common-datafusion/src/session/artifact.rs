use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::extension::SessionExtension;

/// Session-scoped store for artifact bytes (Arrow IPC blobs uploaded by the Spark Connect client).
/// Used to resolve `CachedLocalRelation { hash }` plan nodes.
#[derive(Clone)]
pub struct ArtifactStore {
    inner: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl SessionExtension for ArtifactStore {
    fn name() -> &'static str {
        "ArtifactStore"
    }
}

impl ArtifactStore {
    pub fn new() -> Self {
        Self { inner: Arc::new(Mutex::new(HashMap::new())) }
    }

    pub fn store(&self, name: String, data: Vec<u8>) {
        if let Ok(mut map) = self.inner.lock() {
            map.insert(name, data);
        }
    }

    pub fn exists(&self, name: &str) -> bool {
        self.inner.lock().map(|m| m.contains_key(name)).unwrap_or(false)
    }

    pub fn get(&self, name: &str) -> Option<Vec<u8>> {
        self.inner.lock().ok()?.get(name).cloned()
    }
}

impl Default for ArtifactStore {
    fn default() -> Self {
        Self::new()
    }
}
