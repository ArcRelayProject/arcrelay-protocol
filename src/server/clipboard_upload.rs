use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};

const MAX_BLOB_BYTES: usize = 20 * 1024 * 1024;
const MAX_TOTAL_BYTES: usize = 64 * 1024 * 1024;
const UPLOAD_TTL: Duration = Duration::from_secs(2 * 60);

#[derive(Clone, Default)]
pub(super) struct ClipboardUploadStore {
    inner: Arc<Mutex<ClipboardUploadStoreInner>>,
}

#[derive(Default)]
struct ClipboardUploadStoreInner {
    uploads: HashMap<(Vec<u8>, String), ClipboardUpload>,
    total_bytes: usize,
}

struct ClipboardUpload {
    bytes: Vec<u8>,
    sha256: Vec<u8>,
    created_at: Instant,
}

impl ClipboardUploadStore {
    pub fn insert(
        &self,
        upload_id: Vec<u8>,
        bytes: Vec<u8>,
        expected_sha256: &[u8],
        media_type: &str,
    ) -> Result<(), &'static str> {
        if upload_id.len() != 32
            || bytes.is_empty()
            || bytes.len() > MAX_BLOB_BYTES
            || !matches!(
                media_type,
                "image/png" | "text/html; charset=utf-8" | "text/rtf"
            )
        {
            return Err("invalid clipboard blob upload");
        }
        let digest = Sha256::digest(&bytes).to_vec();
        if expected_sha256.len() != 32 || digest != expected_sha256 {
            return Err("clipboard blob digest mismatch");
        }
        let mut inner = lock(&self.inner);
        inner.prune();
        let key = (upload_id, media_type.to_string());
        let previous = inner
            .uploads
            .get(&key)
            .map(|upload| upload.bytes.len())
            .unwrap_or_default();
        let next_total = inner
            .total_bytes
            .saturating_sub(previous)
            .saturating_add(bytes.len());
        if next_total > MAX_TOTAL_BYTES {
            return Err("clipboard blob upload cache is full");
        }
        inner.total_bytes = next_total;
        inner.uploads.insert(
            key,
            ClipboardUpload {
                bytes,
                sha256: digest,
                created_at: Instant::now(),
            },
        );
        Ok(())
    }

    pub fn get(
        &self,
        upload_id: &[u8],
        expected_sha256: &[u8],
        expected_media_type: &str,
    ) -> Option<Vec<u8>> {
        let mut inner = lock(&self.inner);
        inner.prune();
        inner
            .uploads
            .get(&(upload_id.to_vec(), expected_media_type.to_string()))
            .filter(|upload| upload.sha256 == expected_sha256)
            .map(|upload| upload.bytes.clone())
    }
}

impl ClipboardUploadStoreInner {
    fn prune(&mut self) {
        let now = Instant::now();
        let expired = self
            .uploads
            .iter()
            .filter(|(_, upload)| now.duration_since(upload.created_at) >= UPLOAD_TTL)
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in expired {
            if let Some(upload) = self.uploads.remove(&key) {
                self.total_bytes = self.total_bytes.saturating_sub(upload.bytes.len());
            }
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stores_same_digest_under_distinct_allowed_media_types() {
        let store = ClipboardUploadStore::default();
        let bytes = b"same source bytes".to_vec();
        let digest = Sha256::digest(&bytes).to_vec();

        store
            .insert(
                digest.clone(),
                bytes.clone(),
                &digest,
                "text/html; charset=utf-8",
            )
            .unwrap();
        store
            .insert(digest.clone(), bytes.clone(), &digest, "text/rtf")
            .unwrap();

        assert_eq!(
            store.get(&digest, &digest, "text/html; charset=utf-8"),
            Some(bytes.clone())
        );
        assert_eq!(store.get(&digest, &digest, "text/rtf"), Some(bytes));
        assert_eq!(store.get(&digest, &digest, "image/png"), None);
    }

    #[test]
    fn rejects_unapproved_media_types() {
        let store = ClipboardUploadStore::default();
        let bytes = b"payload".to_vec();
        let digest = Sha256::digest(&bytes).to_vec();
        assert_eq!(
            store.insert(digest.clone(), bytes, &digest, "text/plain"),
            Err("invalid clipboard blob upload")
        );
    }
}
