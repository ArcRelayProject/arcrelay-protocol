use super::*;
use arcrelay_core::domain::{
    device::*, input_control::*, media_control::*, process::*, system_monitor::*, window_manager::*,
};
use arcrelay_core::infrastructure::clipboard_test_support::TestClipboardRepository;
use arcrelay_network::{
    DeviceIdentity, DeviceMetadata, NetworkRuntime, NetworkRuntimeConfig, PeerAdvertisement,
    SessionKind,
};
use std::sync::atomic::AtomicUsize;

#[derive(Default)]
struct TestHost {
    releases: AtomicUsize,
    release_blocked: AtomicBool,
    release_started: Notify,
    resume_release: Notify,
}

#[async_trait::async_trait]
impl DeviceRepository for TestHost {
    async fn local_info(&self) -> arcrelay_core::error::Result<LocalDeviceInfo> {
        unreachable!("session tests must not call unrelated host adapters")
    }
    async fn scan_devices(&self) -> arcrelay_core::error::Result<Vec<DeviceInfo>> {
        unreachable!("session tests must not call unrelated host adapters")
    }
    async fn get_config(&self) -> arcrelay_core::error::Result<ConnectionConfig> {
        unreachable!("session tests must not call unrelated host adapters")
    }
    async fn set_config(&self, _config: ConnectionConfig) -> arcrelay_core::error::Result<()> {
        unreachable!("session tests must not call unrelated host adapters")
    }
}

#[async_trait::async_trait]
impl MediaControlRepository for TestHost {
    async fn playback_info(&self) -> arcrelay_core::error::Result<Option<PlaybackInfo>> {
        unreachable!("session tests must not call unrelated host adapters")
    }
    async fn playback_action(&self, _action: PlaybackAction) -> arcrelay_core::error::Result<()> {
        unreachable!("session tests must not call unrelated host adapters")
    }
    async fn volume_info(&self) -> arcrelay_core::error::Result<VolumeInfo> {
        unreachable!("session tests must not call unrelated host adapters")
    }
    async fn set_system_volume(&self, _volume: u8) -> arcrelay_core::error::Result<()> {
        unreachable!("session tests must not call unrelated host adapters")
    }
    async fn set_system_muted(&self, _muted: bool) -> arcrelay_core::error::Result<()> {
        unreachable!("session tests must not call unrelated host adapters")
    }
    async fn app_volumes(&self) -> arcrelay_core::error::Result<Vec<AppVolume>> {
        unreachable!("session tests must not call unrelated host adapters")
    }
    async fn set_app_volume(
        &self,
        _app_name: &str,
        _volume: u8,
    ) -> arcrelay_core::error::Result<()> {
        unreachable!("session tests must not call unrelated host adapters")
    }
    async fn is_microphone_active(&self) -> arcrelay_core::error::Result<bool> {
        unreachable!("session tests must not call unrelated host adapters")
    }
    async fn set_microphone_active(&self, _active: bool) -> arcrelay_core::error::Result<()> {
        unreachable!("session tests must not call unrelated host adapters")
    }
    async fn is_dnd_active(&self) -> arcrelay_core::error::Result<bool> {
        unreachable!("session tests must not call unrelated host adapters")
    }
    async fn set_dnd_active(&self, _active: bool) -> arcrelay_core::error::Result<()> {
        unreachable!("session tests must not call unrelated host adapters")
    }
}

#[async_trait::async_trait]
impl ProcessRepository for TestHost {
    async fn list(
        &self,
        _sort_by: ProcessSortBy,
    ) -> arcrelay_core::error::Result<Vec<ProcessInfo>> {
        unreachable!("session tests must not call unrelated host adapters")
    }
    async fn kill(&self, _pid: u32) -> arcrelay_core::error::Result<()> {
        unreachable!("session tests must not call unrelated host adapters")
    }
}

#[async_trait::async_trait]
impl SystemMonitorRepository for TestHost {
    async fn snapshot(&self) -> arcrelay_core::error::Result<SystemSnapshot> {
        unreachable!("session tests must not call unrelated host adapters")
    }
    async fn cpu_info(&self) -> arcrelay_core::error::Result<CpuInfo> {
        unreachable!("session tests must not call unrelated host adapters")
    }
    async fn memory_info(&self) -> arcrelay_core::error::Result<MemoryInfo> {
        unreachable!("session tests must not call unrelated host adapters")
    }
    async fn gpu_info(&self) -> arcrelay_core::error::Result<Vec<GpuInfo>> {
        unreachable!("session tests must not call unrelated host adapters")
    }
    async fn disk_info(&self) -> arcrelay_core::error::Result<Vec<DiskInfo>> {
        unreachable!("session tests must not call unrelated host adapters")
    }
    async fn network_stats(&self) -> arcrelay_core::error::Result<NetworkStats> {
        unreachable!("session tests must not call unrelated host adapters")
    }
}

#[async_trait::async_trait]
impl WindowManagerRepository for TestHost {
    fn focused_app_name_now(&self) -> arcrelay_core::error::Result<String> {
        unreachable!("session tests must not call unrelated host adapters")
    }
    async fn list_windows(&self) -> arcrelay_core::error::Result<Vec<WindowInfo>> {
        unreachable!("session tests must not call unrelated host adapters")
    }
    async fn list_windows_meta(&self) -> arcrelay_core::error::Result<Vec<WindowInfo>> {
        unreachable!("session tests must not call unrelated host adapters")
    }
    async fn focused_app_name(&self) -> arcrelay_core::error::Result<String> {
        unreachable!("session tests must not call unrelated host adapters")
    }
    async fn focus_window(&self, _window_id: u32) -> arcrelay_core::error::Result<()> {
        unreachable!("session tests must not call unrelated host adapters")
    }
    async fn list_spaces(&self) -> arcrelay_core::error::Result<Vec<SpaceInfo>> {
        unreachable!("session tests must not call unrelated host adapters")
    }
    async fn switch_space(&self, _space_id: u64) -> arcrelay_core::error::Result<()> {
        unreachable!("session tests must not call unrelated host adapters")
    }
    async fn on_screen_window_ids(&self) -> arcrelay_core::error::Result<Vec<u32>> {
        unreachable!("session tests must not call unrelated host adapters")
    }
}

#[async_trait::async_trait]
impl InputControlRepository for TestHost {
    fn permission_state(&self) -> InputPermissionState {
        InputPermissionState::Granted
    }
    fn open_permission_settings(&self) -> arcrelay_core::Result<()> {
        Ok(())
    }
    fn validate_events(&self, _events: &[InputEvent]) -> arcrelay_core::Result<()> {
        Ok(())
    }
    async fn apply_events(&self, _events: &[InputEvent]) -> arcrelay_core::Result<()> {
        Ok(())
    }
    async fn paste_clipboard(&self, _is_text: bool) -> arcrelay_core::Result<()> {
        Ok(())
    }
    async fn type_text_as_keys(&self, _text: &str) -> arcrelay_core::Result<()> {
        Ok(())
    }
    async fn release_all(&self) -> arcrelay_core::Result<()> {
        if self.release_blocked.load(Ordering::SeqCst) {
            self.release_started.notify_one();
            self.resume_release.notified().await;
        }
        self.releases.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct Fixture {
    _root: tempfile::TempDir,
    runtimes: Vec<Arc<NetworkRuntime>>,
    send: quinn::SendStream,
    recv: quinn::RecvStream,
    connection: quinn::Connection,
    task: Option<tokio::task::JoinHandle<Result<()>>>,
    registry: ConnectionRegistry,
    sessions: InputSessionManager,
    events: mpsc::Receiver<ServerEvent>,
    host: Arc<TestHost>,
    lease: InputLease,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
        for runtime in &self.runtimes {
            runtime.shutdown("session test complete");
        }
    }
}
impl Fixture {
    async fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let mut runtimes = Vec::new();
        for name in ["left", "right"] {
            let mut config = NetworkRuntimeConfig::new(
                root.path().join(name),
                DeviceMetadata {
                    name: name.into(),
                    platform: "test".into(),
                    model: "test".into(),
                },
                Arc::new(arcrelay_peer::InMemoryPeerRepository::default()),
            );
            config.listen_address = "127.0.0.1".parse().unwrap();
            config.listen_port = 0;
            runtimes.push(NetworkRuntime::bind(config).await.unwrap());
        }
        let identity = DeviceIdentity::load_or_create(&root.path().join("right")).unwrap();
        let right = &runtimes[1];
        let mut incoming = right.subscribe();
        let session = runtimes[0]
            .connect(
                &PeerAdvertisement {
                    device_id: right.device_id(),
                    public_key: identity.public_key_value(),
                    metadata: right.metadata(),
                    addresses: vec!["127.0.0.1".parse().unwrap()],
                    connection_addresses: vec![(
                        std::net::Ipv4Addr::LOCALHOST,
                        right.local_port().unwrap(),
                    )
                        .into()],
                    port: right.local_port().unwrap(),
                    certificate_sha256: identity.certificate_sha256(),
                    last_seen_at_ms: 0,
                },
                SessionKind::Pairing,
            )
            .await
            .unwrap();
        let remote = incoming.recv().await.unwrap();
        let connection = session.transport_handle();
        let server_connection = remote.transport_handle();
        let (send, recv) = connection.open_bi().await.unwrap();
        let host = Arc::new(TestHost::default());
        let clipboard = TestClipboardRepository::open(None).await.unwrap().service();
        let coordinator = StateCoordinator::new(Arc::new(arcrelay_core::ArcRelayService::compose(
            host.clone(),
            host.clone(),
            host.clone(),
            clipboard,
            host.clone(),
            host.clone(),
            host.clone(),
        )));
        let registry = ConnectionRegistry::new();
        let sessions = InputSessionManager::new();
        let (event_tx, events) = mpsc::channel(32);
        let grants = vec![Grant {
            peer_id: runtimes[0].device_id(),
            capability: CapabilityId::RemoteInputInject,
            direction: GrantDirection::Inbound,
            constraints: GrantConstraints::None,
            granted_at_ms: 0,
        }];
        let services = ControlSessionServices {
            coordinator,
            event_tx,
            action_provider: None,
            remote_file_provider: None,
            registry: registry.clone(),
            input_sessions: sessions.clone(),
            command_cache: Arc::new(Mutex::new(CommandCache::default())),
            command_limit: Arc::new(Semaphore::new(MAX_BACKGROUND_COMMANDS)),
            blob_store: BlobStore::new(),
            clipboard_uploads: ClipboardUploadStore::default(),
            workspace_input_router: None,
        };
        let peer = ControlSessionPeer {
            device_name: "Phone".into(),
            device_id: "phone".into(),
            device_public_key: vec![1; 32],
            server_features: vec![feature(proto::Feature::InputReliable)],
            remote_file_access: RemoteFileAccess::from_grants(&grants, GrantDirection::Inbound),
            grants,
        };
        let task = tokio::spawn(async move {
            let streams = server_connection.accept_bi().await.unwrap();
            handle_quic_connection(server_connection, streams, services, peer).await
        });
        let mut fixture = Self {
            _root: root,
            runtimes,
            send,
            recv,
            connection,
            task: Some(task),
            registry,
            sessions,
            events,
            host,
            lease: InputLease {
                session_id: 0,
                epoch: 0,
            },
        };
        fixture
            .send_frame(proto::client_control_frame::Body::Hello(
                proto::ControlHello {
                    features: vec![feature(proto::Feature::InputReliable)],
                    receive_limits: Some(protocol_limits()),
                },
            ))
            .await;
        assert!(matches!(
            fixture.recv_frame().await.body,
            Some(proto::server_control_frame::Body::Welcome(_))
        ));
        fixture
            .send_frame(proto::client_control_frame::Body::BeginInputSession(
                proto::BeginInputSessionRequest {
                    request_id: 1,
                    ..Default::default()
                },
            ))
            .await;
        let Some(proto::server_control_frame::Body::InputSessionResult(result)) =
            fixture.recv_frame().await.body
        else {
            panic!("expected input result")
        };
        assert_eq!(
            result.status.as_ref().unwrap().code,
            proto::ErrorCode::Ok as i32,
            "input acquisition failed: {result:?}"
        );
        fixture.lease = InputLease {
            session_id: result.session_id,
            epoch: result.epoch,
        };
        fixture
    }
    async fn send_frame(&mut self, body: proto::client_control_frame::Body) {
        send_message(
            &mut self.send,
            &proto::ClientControlFrame { body: Some(body) },
            MAX_CONTROL_FRAME_SIZE,
        )
        .await
        .unwrap();
    }
    async fn recv_frame(&mut self) -> proto::ServerControlFrame {
        tokio::time::timeout(
            Duration::from_secs(5),
            recv_message(&mut self.recv, MAX_CONTROL_FRAME_SIZE),
        )
        .await
        .unwrap()
        .unwrap()
    }
    async fn assert_released(&mut self) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if matches!(
                    self.events.recv().await,
                    Some(ServerEvent::DeviceDisconnected { .. })
                ) {
                    break;
                }
            }
        })
        .await
        .unwrap();
        assert!(self.registry.list_connected().await.is_empty());
        assert!(self.sessions.acquire("tablet", "Tablet").await.is_ok());
        assert!(self.host.releases.load(Ordering::SeqCst) > 0);
        tokio::time::timeout(Duration::from_secs(2), self.connection.closed())
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn malformed_control_frame_releases_input_and_unregisters_transport() {
    let mut fixture = Fixture::new().await;
    arcrelay_transport::write_frame(&mut fixture.send, &[0xff], MAX_CONTROL_FRAME_SIZE)
        .await
        .unwrap();
    let task = fixture.task.take().unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .is_err());
    fixture.assert_released().await;
}

#[tokio::test]
async fn aborting_control_session_releases_input_and_unregisters_transport() {
    let mut fixture = Fixture::new().await;
    let task = fixture.task.take().unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    fixture.assert_released().await;
}

#[tokio::test]
async fn fragmented_control_frame_survives_lease_ticks_and_clean_eof_releases_input() {
    let mut fixture = Fixture::new().await;
    let ping = proto::ClientControlFrame {
        body: Some(proto::client_control_frame::Body::Ping(proto::Ping {
            request_id: 2,
            monotonic_elapsed_us: 0,
        })),
    }
    .encode_to_vec();
    fixture
        .send
        .write_all(&(ping.len() as u32).to_be_bytes())
        .await
        .unwrap();
    fixture.send.write_all(&ping[..1]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    fixture.send.write_all(&ping[1..]).await.unwrap();
    assert!(matches!(
        fixture.recv_frame().await.body,
        Some(proto::server_control_frame::Body::Pong(_))
    ));
    fixture.send.finish().unwrap();
    let task = fixture.task.take().unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .is_ok());
    fixture.assert_released().await;
}

#[tokio::test]
async fn cancellation_during_native_release_keeps_lease_owned_until_cleanup_finishes() {
    let mut fixture = Fixture::new().await;
    fixture.host.release_blocked.store(true, Ordering::SeqCst);
    fixture
        .send_frame(proto::client_control_frame::Body::EndInputSession(
            proto::EndInputSessionRequest {
                request_id: 2,
                session_id: fixture.lease.session_id,
                epoch: fixture.lease.epoch,
            },
        ))
        .await;
    tokio::time::timeout(
        Duration::from_secs(5),
        fixture.host.release_started.notified(),
    )
    .await
    .unwrap();
    let task = fixture.task.take().unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(fixture.sessions.acquire("tablet", "Tablet").await.is_err());
    fixture.host.release_blocked.store(false, Ordering::SeqCst);
    fixture.host.resume_release.notify_one();
    fixture.assert_released().await;
}
