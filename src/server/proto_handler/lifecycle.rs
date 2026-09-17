use std::future::Future;
use std::pin::Pin;

use tokio::task::{AbortHandle, JoinError, JoinSet};

/// Every connection task belongs to its session, including setup and requests.
/// Cleanup stays owned while it is awaited, so cancellation during shutdown
/// transfers the remaining cleanup to the same long-lived runtime.
pub(super) struct SessionTasks {
    tasks: JoinSet<()>,
    cleanup: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl SessionTasks {
    pub(super) fn new(cleanup: impl Future<Output = ()> + Send + 'static) -> Self {
        Self {
            tasks: JoinSet::new(),
            cleanup: Some(Box::pin(cleanup)),
        }
    }

    pub(super) fn spawn(&mut self, task: impl Future<Output = ()> + Send + 'static) -> AbortHandle {
        self.tasks.spawn(task)
    }

    pub(super) fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    pub(super) async fn join_next(&mut self) -> Option<Result<(), JoinError>> {
        self.tasks.join_next().await
    }

    pub(super) async fn shutdown(&mut self) {
        self.tasks.shutdown().await;
        if let Some(cleanup) = self.cleanup.as_mut() {
            cleanup.await;
        }
        self.cleanup = None;
    }
}

impl Drop for SessionTasks {
    fn drop(&mut self) {
        self.tasks.abort_all();
        if let Some(cleanup) = self.cleanup.take() {
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(cleanup);
            }
        }
    }
}

pub(in crate::server) struct TransportCloseGuard(pub(in crate::server) quinn::Connection);

impl Drop for TransportCloseGuard {
    fn drop(&mut self) {
        self.0.close(0_u32.into(), b"control session ended");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::{
        connection_registry::ConnectionRegistry, input_session::InputSessionManager,
    };
    use std::sync::Arc;
    use tokio::sync::{oneshot, Notify};

    #[tokio::test]
    async fn cancelling_session_aborts_children_and_releases_registration_and_lease() {
        let sessions = InputSessionManager::new();
        let lease = sessions.acquire("phone", "Phone").await.unwrap();
        let registry = ConnectionRegistry::new();
        let registration = registry.register("phone").await;
        let (cleaned, completion) = oneshot::channel();
        let cleanup_sessions = sessions.clone();
        let cleanup_registry = registry.clone();
        let mut tasks = SessionTasks::new(async move {
            cleanup_sessions.release(lease).await;
            cleanup_registry.unregister("phone", &registration).await;
            let _ = cleaned.send(());
        });
        let (child_dropped, child_completion) = oneshot::channel();
        struct SignalOnDrop(Option<oneshot::Sender<()>>);
        impl Drop for SignalOnDrop {
            fn drop(&mut self) {
                let _ = self.0.take().unwrap().send(());
            }
        }
        let signal = SignalOnDrop(Some(child_dropped));
        tasks.spawn(async move {
            let _signal = signal;
            std::future::pending::<()>().await;
        });
        drop(tasks);
        completion.await.unwrap();
        child_completion.await.unwrap();
        assert!(registry.list_connected().await.is_empty());
        assert!(sessions.acquire("tablet", "Tablet").await.is_ok());
    }

    #[tokio::test]
    async fn cancellation_during_shutdown_preserves_in_progress_cleanup() {
        let started = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        let (done, completion) = oneshot::channel();
        let cleanup_started = started.clone();
        let cleanup_resume = resume.clone();
        let mut tasks = SessionTasks::new(async move {
            cleanup_started.notify_one();
            cleanup_resume.notified().await;
            let _ = done.send(());
        });
        let owner = tokio::spawn(async move { tasks.shutdown().await });
        started.notified().await;
        owner.abort();
        let _ = owner.await;
        resume.notify_one();
        completion.await.unwrap();
    }
}
