use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};

#[derive(Debug, Clone)]
struct ActiveConnection {
    disconnect: Arc<Notify>,
    transport: Option<quinn::Connection>,
    negotiated_features: HashMap<i32, u32>,
}

impl ActiveConnection {
    fn supports_feature(&self, feature: i32, version: u32) -> bool {
        self.negotiated_features.get(&feature) == Some(&version)
    }
}

/// Tracks active connections, allowing the UI to force-disconnect a device
/// and feature clients to reuse an authenticated inbound transport.
#[derive(Debug, Clone, Default)]
pub struct ConnectionRegistry {
    inner: Arc<Mutex<HashMap<String, ActiveConnection>>>,
}

impl ConnectionRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Register the unique live connection for a device. A newly authenticated
    /// replacement closes the previous connection immediately.
    pub async fn register(&self, device_id: &str) -> Arc<Notify> {
        self.register_inner(device_id, None, HashMap::new()).await
    }

    /// Register a live authenticated transport. The transport can be reused by
    /// features whose streams are symmetric over the control connection.
    pub async fn register_transport(
        &self,
        device_id: &str,
        transport: quinn::Connection,
    ) -> Arc<Notify> {
        self.register_transport_with_features(device_id, transport, HashMap::new())
            .await
    }

    /// Register a transport together with the exact feature versions selected
    /// during control-protocol negotiation.
    pub async fn register_transport_with_features(
        &self,
        device_id: &str,
        transport: quinn::Connection,
        negotiated_features: HashMap<i32, u32>,
    ) -> Arc<Notify> {
        self.register_inner(device_id, Some(transport), negotiated_features)
            .await
    }

    async fn register_inner(
        &self,
        device_id: &str,
        transport: Option<quinn::Connection>,
        negotiated_features: HashMap<i32, u32>,
    ) -> Arc<Notify> {
        let notify = Arc::new(Notify::new());
        if let Some(previous) = self.inner.lock().await.insert(
            device_id.to_string(),
            ActiveConnection {
                disconnect: notify.clone(),
                transport,
                negotiated_features,
            },
        ) {
            previous.disconnect.notify_one();
        }
        notify
    }

    /// Unregister a device (called when connection ends naturally).
    pub async fn unregister(&self, device_id: &str, notify: &Arc<Notify>) {
        let mut connections = self.inner.lock().await;
        if connections
            .get(device_id)
            .is_some_and(|active| Arc::ptr_eq(&active.disconnect, notify))
        {
            connections.remove(device_id);
        }
    }

    /// Force-disconnect a device by signaling its Notify.
    pub async fn disconnect(&self, device_id: &str) -> bool {
        if let Some(connection) = self.inner.lock().await.get(device_id) {
            connection.disconnect.notify_one();
            true
        } else {
            false
        }
    }

    /// Return a clone of the authenticated transport for a connected device.
    pub async fn transport(&self, device_id: &str) -> Option<quinn::Connection> {
        self.inner
            .lock()
            .await
            .get(device_id)
            .and_then(|connection| connection.transport.clone())
    }

    /// Return a transport only when the requested feature version was
    /// negotiated on that connection.
    pub async fn transport_for_feature(
        &self,
        device_id: &str,
        feature: i32,
        version: u32,
    ) -> Option<quinn::Connection> {
        self.inner
            .lock()
            .await
            .get(device_id)
            .filter(|connection| connection.supports_feature(feature, version))
            .and_then(|connection| connection.transport.clone())
    }

    /// List currently connected device ids.
    pub async fn list_connected(&self) -> Vec<String> {
        self.inner.lock().await.keys().cloned().collect()
    }

    /// List devices whose live transport negotiated the requested feature
    /// version.
    pub async fn list_connected_with_feature(&self, feature: i32, version: u32) -> Vec<String> {
        self.inner
            .lock()
            .await
            .iter()
            .filter(|(_, connection)| {
                connection.transport.is_some() && connection.supports_feature(feature, version)
            })
            .map(|(device_id, _)| device_id.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn register_and_list() {
        let reg = ConnectionRegistry::new();
        let _notify = reg.register("iPad").await;
        let list = reg.list_connected().await;
        assert_eq!(list.len(), 1);
        assert!(list.contains(&"iPad".to_string()));
    }

    #[tokio::test]
    async fn unregister_removes_device() {
        let reg = ConnectionRegistry::new();
        let notify = reg.register("iPad").await;
        reg.unregister("iPad", &notify).await;
        assert!(reg.list_connected().await.is_empty());
    }

    #[tokio::test]
    async fn disconnect_returns_false_for_unknown() {
        let reg = ConnectionRegistry::new();
        assert!(!reg.disconnect("nonexistent").await);
    }

    #[tokio::test]
    async fn disconnect_signals_notify() {
        let reg = ConnectionRegistry::new();
        let notify = reg.register("iPad").await;
        assert!(reg.disconnect("iPad").await);
        // notify should be signaled — notified() completes immediately after notify_one
        tokio::time::timeout(std::time::Duration::from_millis(50), notify.notified())
            .await
            .expect("notify should fire");
    }

    #[tokio::test]
    async fn a_replacement_disconnects_the_previous_connection() {
        let reg = ConnectionRegistry::new();
        let first = reg.register("device-id").await;
        let second = reg.register("device-id").await;

        tokio::time::timeout(std::time::Duration::from_millis(50), first.notified())
            .await
            .expect("replaced connection should be signaled");
        assert!(reg.disconnect("device-id").await);
        tokio::time::timeout(std::time::Duration::from_millis(50), second.notified())
            .await
            .expect("active connection should be signaled");

        reg.unregister("device-id", &first).await;
        assert_eq!(reg.list_connected().await, vec!["device-id".to_string()]);
        reg.unregister("device-id", &second).await;
        assert!(reg.list_connected().await.is_empty());
    }

    #[test]
    fn feature_versions_must_match_exactly() {
        let active = ActiveConnection {
            disconnect: Arc::new(Notify::new()),
            transport: None,
            negotiated_features: HashMap::from([(12, 2)]),
        };

        assert!(active.supports_feature(12, 2));
        assert!(!active.supports_feature(12, 1));
        assert!(!active.supports_feature(13, 2));
    }
}
