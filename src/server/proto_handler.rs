use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use prost::Message;
use rand::RngCore;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{broadcast, mpsc, watch, Mutex, Notify, Semaphore};
use tracing::{error, warn};

use arcrelay_core::application::state_coordinator::StateCoordinator;
use arcrelay_core::domain::input_control::{
    InputEvent as DomainInputEvent, InputPermissionState as DomainInputPermissionState,
    MouseButton as DomainMouseButton, ScrollGesturePhase as DomainScrollGesturePhase,
};
use arcrelay_core::Error;
use arcrelay_peer::{CapabilityId, Grant, GrantConstraints, GrantDirection};

use crate::error::{ProtocolError, Result};
use crate::message::{
    MAX_CONTROL_FRAME_SIZE, STREAM_KIND_BLOB_DOWNLOAD, STREAM_KIND_CLIPBOARD_BLOB_UPLOAD,
    STREAM_KIND_RELIABLE_INPUT,
};
use crate::proto_msg::{
    clipboard_kind_to_proto, clipboard_policy_to_proto, clipboard_summary_to_proto,
    connection_config_to_proto, local_device_info_to_proto, playback_action_from_proto,
    playback_info_to_proto, process_sort_from_proto, proto, system_snapshot_to_proto,
};
use crate::remote_files::{
    read_remote_message, write_remote_message, RemoteFileAccess, RemoteFileError,
    RemoteFileErrorCode, RemoteFileKind, RemoteFileProvider, RemoteFileRequest, RemoteFileResponse,
};

use super::blob_store::{BlobStore, IssueTicketError};
use super::clipboard_upload::ClipboardUploadStore;
use super::command_cache::{CommandCache, CommandCacheKey, InFlightCommand};
use super::connection_registry::ConnectionRegistry;
use super::input_session::{
    InputFeedbackSnapshot, InputLease, InputSessionManager, INPUT_LEASE_TIMEOUT,
    MAX_RELIABLE_EVENTS_PER_FRAME, MAX_RELIABLE_FRAMES_PER_SECOND,
};
use super::types::*;
use super::wire::{
    now_ms, read_stream_kind, recv_control, recv_control_buffered, recv_message,
    recv_reliable_input, send_control, send_message,
};

mod lifecycle;
#[cfg(test)]
mod session_tests;
use lifecycle::SessionTasks;
pub(super) use lifecycle::TransportCloseGuard;

const STREAM_SETUP_TIMEOUT: Duration = Duration::from_secs(5);
const BLOB_TRANSFER_TIMEOUT: Duration = Duration::from_secs(2 * 60);
const CRITICAL_SEND_TIMEOUT: Duration = Duration::from_secs(5);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(45);
const MAX_CONCURRENT_REQUESTS: usize = 16;

struct InputMetricsAccumulator {
    window_started: Instant,
    last_motion_received: Option<Instant>,
    last_client_elapsed_us: Option<u64>,
    motion_received: u64,
    quartz_submitted: u64,
    gap_samples: u64,
    gap_total_us: u64,
    gap_max_us: u64,
    source_gap_max_us: u64,
    transport_stall_max_us: u64,
    apply_total_us: u64,
    apply_max_us: u64,
}

#[derive(Default)]
struct LatestStateQueue {
    frames: std::sync::Mutex<HashMap<u64, proto::ServerControlFrame>>,
    changed: Notify,
}

impl LatestStateQueue {
    fn replace(&self, frame: proto::ServerControlFrame) {
        if frame.encoded_len() > MAX_CONTROL_FRAME_SIZE {
            return;
        }
        let subscription_id = match frame.body.as_ref() {
            Some(proto::server_control_frame::Body::Event(event)) => event.subscription_id,
            _ => return,
        };
        self.frames
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(subscription_id, frame);
        self.changed.notify_one();
    }

    async fn recv(&self) -> proto::ServerControlFrame {
        loop {
            let notified = self.changed.notified();
            if let Some(frame) = self
                .frames
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .extract_if(|_, _| true)
                .next()
                .map(|(_, frame)| frame)
            {
                return frame;
            }
            notified.await;
        }
    }
}

impl InputMetricsAccumulator {
    fn new() -> Self {
        Self {
            window_started: Instant::now(),
            last_motion_received: None,
            last_client_elapsed_us: None,
            motion_received: 0,
            quartz_submitted: 0,
            gap_samples: 0,
            gap_total_us: 0,
            gap_max_us: 0,
            source_gap_max_us: 0,
            transport_stall_max_us: 0,
            apply_total_us: 0,
            apply_max_us: 0,
        }
    }

    fn observe_motion(&mut self, now: Instant, client_elapsed_us: u64) {
        self.motion_received += 1;
        if let Some(previous) = self.last_motion_received.replace(now) {
            let gap = now
                .duration_since(previous)
                .as_micros()
                .min(u64::MAX as u128) as u64;
            let source_gap = self
                .last_client_elapsed_us
                .map(|value| client_elapsed_us.saturating_sub(value))
                .unwrap_or(0);
            self.gap_samples += 1;
            self.gap_total_us = self.gap_total_us.saturating_add(gap);
            self.gap_max_us = self.gap_max_us.max(gap);
            self.source_gap_max_us = self.source_gap_max_us.max(source_gap);
            self.transport_stall_max_us = self
                .transport_stall_max_us
                .max(gap.saturating_sub(source_gap));
        }
        self.last_client_elapsed_us = Some(client_elapsed_us);
    }

    fn observe_quartz(&mut self, elapsed: Duration) {
        let apply_us = elapsed.as_micros().min(u64::MAX as u128) as u64;
        self.quartz_submitted += 1;
        self.apply_total_us = self.apply_total_us.saturating_add(apply_us);
        self.apply_max_us = self.apply_max_us.max(apply_us);
    }

    fn take(&mut self, session_id: String) -> Option<ServerEvent> {
        let elapsed = self.window_started.elapsed();
        if elapsed < Duration::from_secs(1) {
            return None;
        }
        let seconds = elapsed.as_secs_f32().max(0.001);
        let event = ServerEvent::InputMetrics {
            session_id,
            receive_hz: self.motion_received as f32 / seconds,
            quartz_hz: self.quartz_submitted as f32 / seconds,
            average_gap_us: self.gap_total_us / self.gap_samples.max(1),
            maximum_gap_us: self.gap_max_us,
            maximum_source_gap_us: self.source_gap_max_us,
            maximum_transport_stall_us: self.transport_stall_max_us,
            average_apply_us: self.apply_total_us / self.quartz_submitted.max(1),
            maximum_apply_us: self.apply_max_us,
        };
        *self = Self::new();
        Some(event)
    }
}
const CRITICAL_CONTROL_QUEUE_CAPACITY: usize = 32;
const STATE_CONTROL_QUEUE_CAPACITY: usize = 16;
pub(super) const MAX_BACKGROUND_COMMANDS: usize = 32;
const MAX_STATUS_MESSAGE_BYTES: usize = 8 * 1024;
const MAX_ACTION_OUTPUT_LINE_BYTES: usize = 16 * 1024;
const MAX_ACTION_OUTPUT_LINES: usize = 500;
const MAX_ACTION_OUTPUT_SNAPSHOT_BYTES: usize = 512 * 1024;
const MAX_COMMAND_OUTPUT_BYTES: usize = 256 * 1024;
const MAX_PROCESS_ITEMS: usize = 4096;
const MAX_APP_VOLUME_ITEMS: usize = 2048;
const MAX_GPU_ITEMS: usize = 64;
const MAX_DISK_ITEMS: usize = 256;
const MAX_WINDOW_ITEMS: usize = 1024;
const MAX_THUMBNAIL_WINDOW_IDS: usize = 64;
const MAX_SPACE_ITEMS: usize = 256;
const MAX_CLIPBOARD_ITEMS: usize = 200;
const MAX_CLIPBOARD_TEXT_BYTES: usize = 512 * 1024;
const MAX_CLIPBOARD_PREVIEW_BYTES: usize = 2 * 1024;
const MAX_ACTION_ITEMS: usize = 512;
const MAX_AUTOMATION_ITEMS: usize = 256;
const MAX_ACTION_GAP_RESOURCE_IDS: usize = 1024;
const MAX_ACTION_GAP_RESOURCE_BYTES: usize = 256 * 1024;

type SystemVersioned = arcrelay_core::application::state_coordinator::Versioned<
    arcrelay_core::domain::system_monitor::SystemSnapshot,
>;
type MediaVersioned = arcrelay_core::application::state_coordinator::Versioned<
    arcrelay_core::application::state_coordinator::MediaState,
>;
type WindowVersioned = arcrelay_core::application::state_coordinator::Versioned<
    Vec<arcrelay_core::domain::window_manager::WindowInfo>,
>;
type ClipboardVersioned = arcrelay_core::application::state_coordinator::Versioned<
    arcrelay_core::application::state_coordinator::ClipboardState,
>;

struct StateSubscription<T: Clone> {
    id: u64,
    sequence: u64,
    receiver: watch::Receiver<Option<Arc<T>>>,
}

struct EventSubscription<T> {
    id: u64,
    sequence: u64,
    receiver: broadcast::Receiver<T>,
    action_gap: ActionGap,
}

struct PassiveSubscription {
    id: u64,
    sequence: u64,
}

#[derive(Default)]
struct ActionGap {
    first_missing_sequence: Option<u64>,
    resource_ids: HashSet<String>,
    resource_bytes: usize,
    resync_all: bool,
}

impl ActionGap {
    fn remember_sequence(&mut self, sequence: u64) {
        self.first_missing_sequence.get_or_insert(sequence);
    }

    fn remember_resource(&mut self, sequence: u64, resource_id: String) {
        self.remember_sequence(sequence);
        if self.resync_all || self.resource_ids.contains(&resource_id) {
            return;
        }
        if resource_id.is_empty()
            || self.resource_ids.len() >= MAX_ACTION_GAP_RESOURCE_IDS
            || self.resource_bytes.saturating_add(resource_id.len()) > MAX_ACTION_GAP_RESOURCE_BYTES
        {
            self.resource_ids.clear();
            self.resource_bytes = 0;
            self.resync_all = true;
            return;
        }
        self.resource_bytes = self.resource_bytes.saturating_add(resource_id.len());
        self.resource_ids.insert(resource_id);
    }

    fn remember_gap(&mut self, gap: proto::EventGap) {
        self.remember_sequence(gap.first_missing_sequence);
        if gap.resource_ids.is_empty() {
            self.resource_ids.clear();
            self.resource_bytes = 0;
            self.resync_all = true;
            return;
        }
        for resource_id in gap.resource_ids {
            self.remember_resource(gap.first_missing_sequence, resource_id);
        }
    }

    fn take_resource_ids(&mut self) -> Vec<String> {
        let mut resource_ids = if self.resync_all {
            Vec::new()
        } else {
            self.resource_ids.drain().collect()
        };
        resource_ids.sort_unstable();
        self.resource_ids.clear();
        self.resource_bytes = 0;
        self.resync_all = false;
        resource_ids
    }
}

struct ReliableInputStreamGuard(Arc<AtomicBool>);

impl Drop for ReliableInputStreamGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

struct TaskAbortGuard(tokio::task::AbortHandle);

impl Drop for TaskAbortGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl<T: Clone> StateSubscription<T> {
    fn next_sequence(&mut self) -> u64 {
        self.sequence = self.sequence.saturating_add(1);
        self.sequence
    }
}

impl<T> EventSubscription<T> {
    fn next_sequence(&mut self) -> u64 {
        self.sequence = self.sequence.saturating_add(1);
        self.sequence
    }
}

pub(super) struct ControlSessionServices {
    pub coordinator: Arc<StateCoordinator>,
    pub event_tx: mpsc::Sender<ServerEvent>,
    pub action_provider: Option<Arc<dyn HostCapabilityProvider>>,
    pub remote_file_provider: Option<Arc<dyn RemoteFileProvider>>,
    pub registry: ConnectionRegistry,
    pub input_sessions: InputSessionManager,
    pub command_cache: Arc<Mutex<CommandCache>>,
    pub command_limit: Arc<Semaphore>,
    pub blob_store: BlobStore,
    pub clipboard_uploads: ClipboardUploadStore,
    pub workspace_input_router: Option<Arc<dyn WorkspaceInputRouter>>,
}

pub(super) struct ControlSessionPeer {
    pub device_name: String,
    pub device_id: String,
    pub device_public_key: Vec<u8>,
    pub server_features: Vec<proto::FeatureVersion>,
    pub grants: Vec<Grant>,
    pub remote_file_access: RemoteFileAccess,
}

pub(super) async fn handle_quic_connection(
    connection: quinn::Connection,
    streams: (quinn::SendStream, quinn::RecvStream),
    services: ControlSessionServices,
    peer: ControlSessionPeer,
) -> Result<()> {
    let _transport_guard = TransportCloseGuard(connection.clone());
    let (mut control_send, mut control_recv) = streams;
    let ControlSessionServices {
        coordinator,
        event_tx,
        action_provider,
        remote_file_provider,
        registry,
        input_sessions,
        command_cache,
        command_limit,
        blob_store,
        clipboard_uploads,
        workspace_input_router,
    } = services;
    let ControlSessionPeer {
        device_name,
        device_id,
        device_public_key,
        server_features,
        grants,
        remote_file_access,
    } = peer;
    let hello = tokio::time::timeout(STREAM_SETUP_TIMEOUT, recv_control(&mut control_recv))
        .await
        .map_err(|_| ProtocolError::Other("control hello timed out".into()))??;
    let hello = match hello.body {
        Some(proto::client_control_frame::Body::Hello(hello)) => hello,
        _ => {
            return Err(ProtocolError::Other(
                "first control frame must be hello".into(),
            ))
        }
    };
    let negotiated_feature_versions = negotiate_features(&hello.features, &server_features)?;
    let negotiated_features = negotiated_feature_versions
        .keys()
        .copied()
        .collect::<HashSet<_>>();
    let granted_capabilities = grants
        .iter()
        .filter(|grant| grant.direction == GrantDirection::Inbound)
        .map(|grant| grant.capability)
        .collect::<HashSet<_>>();
    let limits = protocol_limits();
    validate_receive_limits(hello.receive_limits.as_ref(), &hello.features)?;
    let welcome = proto::ServerControlFrame {
        body: Some(proto::server_control_frame::Body::Welcome(
            proto::ControlWelcome {
                features: selected_feature_versions(&negotiated_feature_versions),
                limits: Some(limits),
                grants: grants.iter().map(grant_to_proto).collect(),
                authorization_epoch: random_nonzero_u64(),
                server_time_ms: now_ms(),
            },
        )),
    };
    tokio::time::timeout(
        CRITICAL_SEND_TIMEOUT,
        send_control(&mut control_send, &welcome),
    )
    .await
    .map_err(|_| ProtocolError::Other("control welcome timed out".into()))??;

    let active_input = Arc::new(Mutex::new(None::<InputLease>));
    let input_apply_gate = input_sessions.apply_gate();
    let disconnect_signal = registry
        .register_transport_with_features(
            &device_id,
            connection.clone(),
            negotiated_feature_versions.clone(),
        )
        .await;
    let input_context = InputSessionContext {
        coordinator: coordinator.clone(),
        input_sessions: input_sessions.clone(),
        active_input: active_input.clone(),
        input_apply_gate: input_apply_gate.clone(),
        event_tx: event_tx.clone(),
        device_id: device_id.clone(),
        device_name: device_name.clone(),
        workspace_input_router: workspace_input_router.clone(),
    };
    let cleanup_registry = registry.clone();
    let cleanup_signal = disconnect_signal.clone();
    let cleanup_device_id = device_id.clone();
    let cleanup_device_name = device_name.clone();
    let cleanup_input = input_context.clone();
    let mut tasks = SessionTasks::new(async move {
        // Stop advertising this transport even if native input cleanup is slow.
        cleanup_registry
            .unregister(&cleanup_device_id, &cleanup_signal)
            .await;
        let _apply_guard = cleanup_input.input_apply_gate.lock().await;
        let lease = *cleanup_input.active_input.lock().await;
        if let Some(lease) = lease {
            end_input_lease_locked(lease, &cleanup_input).await;
        }
        drop(_apply_guard);
        let _ = tokio::time::timeout(
            CRITICAL_SEND_TIMEOUT,
            cleanup_input
                .event_tx
                .send(ServerEvent::DeviceDisconnected {
                    device_id: cleanup_device_id,
                    device_name: cleanup_device_name,
                }),
        )
        .await;
    });
    let _ = event_tx
        .send(ServerEvent::DeviceConnected {
            device_id: device_id.clone(),
            device_name: device_name.clone(),
        })
        .await;

    let (critical_tx, mut critical_rx) =
        mpsc::channel::<proto::ServerControlFrame>(CRITICAL_CONTROL_QUEUE_CAPACITY);
    let (event_tx_out, mut event_rx_out) =
        mpsc::channel::<proto::ServerControlFrame>(STATE_CONTROL_QUEUE_CAPACITY);
    let latest_state = Arc::new(LatestStateQueue::default());
    let writer_latest_state = latest_state.clone();
    let writer_failed = Arc::new(Notify::new());
    let writer_failed_signal = writer_failed.clone();
    let writer_connection = connection.clone();
    tasks.spawn(async move {
        loop {
            let frame = tokio::select! {
                biased;
                frame = critical_rx.recv() => frame,
                frame = event_rx_out.recv() => frame,
                frame = writer_latest_state.recv() => Some(frame),
            };
            let Some(frame) = frame else { break };
            if let Err(error) = send_control(&mut control_send, &frame).await {
                error!(%error, "QUIC control writer failed");
                writer_connection.close(4_u32.into(), b"control write failed");
                writer_failed_signal.notify_one();
                break;
            }
        }
    });

    let replica_limit = Arc::new(Semaphore::new(4));
    let request_limit = Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS));
    let (request_done_tx, mut request_done_rx) = mpsc::channel::<u64>(32);
    let mut request_tasks: HashMap<u64, tokio::task::AbortHandle> = HashMap::new();
    let reliable_input_stream_open = Arc::new(AtomicBool::new(false));
    let mut input_lease_tick = tokio::time::interval(Duration::from_secs(1));
    let mut heartbeat_tick = tokio::time::interval(Duration::from_secs(5));
    let mut last_client_control_activity = Instant::now();
    let mut workspace_input_events = negotiated_features
        .contains(&(proto::Feature::WorkspaceInput as i32))
        .then(|| {
            workspace_input_router
                .as_ref()
                .map(|router| router.subscribe())
        })
        .flatten();

    let mut subscriptions = ConnectionSubscriptions::default();

    let request_context = RequestContext {
        coordinator: coordinator.clone(),
        action_provider: action_provider.clone(),
        features: Arc::new(negotiated_features.clone()),
        capabilities: Arc::new(granted_capabilities.clone()),
        blob_store: blob_store.clone(),
        clipboard_uploads: clipboard_uploads.clone(),
        command_cache,
        command_limit,
        device_public_key: device_public_key.clone(),
        device_id: device_id.clone(),
        device_name: device_name.clone(),
    };
    let mut control_decoder = arcrelay_transport::FrameReader::new(MAX_CONTROL_FRAME_SIZE);
    let result: Result<()> = async {
    loop {
        tokio::select! {
            completed = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(Err(error)) = completed {
                    tracing::warn!(%error, "control session child task failed");
                }
            }
            _ = writer_failed.notified() => break,
            _ = disconnect_signal.notified() => {
                connection.close(5_u32.into(), b"disconnected by desktop");
                break;
            }
            _ = connection.closed() => break,
            focus = recv_workspace_input_change(&mut workspace_input_events) => {
                let Some(focus) = focus else { continue };
                let lease = { *active_input.lock().await };
                let owned_workspace_lease = if focus.owner_device_id.as_deref() == Some(device_id.as_str()) {
                    match lease {
                        Some(lease) if input_sessions.is_workspace_routed(lease, &device_id).await => Some(lease),
                        _ => None,
                    }
                } else {
                    None
                };
                if matches!(focus.state, WorkspaceInputRouteState::Suspended | WorkspaceInputRouteState::Ended) {
                    if let Some(lease) = owned_workspace_lease {
                        let _apply_guard = input_apply_gate.lock().await;
                        if active_input.lock().await.as_ref().is_some_and(|active| *active == lease) {
                            let mut active = active_input.lock().await;
                            let _ = input_sessions.release(lease).await;
                            *active = None;
                            drop(active);
                            emit_input_ended(&event_tx, lease, &device_id, &device_name).await;
                        }
                    }
                }
                send_critical(
                    &critical_tx,
                    input_focus_changed_frame(owned_workspace_lease, &focus),
                ).await?;
            }
            Some(request_id) = request_done_rx.recv() => {
                request_tasks.remove(&request_id);
            }
            _ = input_lease_tick.tick() => {
                let _apply_guard = input_apply_gate.lock().await;
                let lease = { *active_input.lock().await };
                if let Some(lease) = lease {
                    if input_sessions.is_expired(lease, &device_id).await {
                        end_input_lease_locked(lease, &input_context).await;
                    } else {
                        let cancellations = input_sessions.cancel_idle_system_gesture(lease, &device_id).await;
                        if !cancellations.is_empty() {
                            let result = if input_sessions.is_workspace_routed(lease, &device_id).await {
                                match workspace_input_router.as_ref() {
                                    Some(router) => router.apply(&device_id, &cancellations).await.map(|_| ()),
                                    None => Err(HostCapabilityError::new(
                                        HostCapabilityErrorCode::Unavailable,
                                        "workspace input router is unavailable",
                                    )),
                                }
                            } else {
                                coordinator.service().input_control.apply_events(&cancellations).await.map_err(|error| {
                                    HostCapabilityError::new(HostCapabilityErrorCode::Internal, error.to_string())
                                })
                            };
                            if let Err(error) = result {
                                tracing::warn!(%error, "failed to cancel idle remote system gesture");
                            }
                        }
                    }
                }
            }
            _ = heartbeat_tick.tick() => {
                if last_client_control_activity.elapsed() >= HEARTBEAT_TIMEOUT {
                    connection.close(7_u32.into(), b"application heartbeat timed out");
                    break;
                }
                if let Some(subscription) = subscriptions.action_output_sub.as_mut() {
                    if let Some(first_missing_sequence) =
                        subscription.action_gap.first_missing_sequence.take()
                    {
                        let sequence = subscription.next_sequence();
                        let resource_ids = subscription.action_gap.take_resource_ids();
                        send_critical(
                            &critical_tx,
                            event_frame(
                                subscription.id,
                                sequence,
                                now_ms(),
                                proto::event::Data::Gap(proto::EventGap {
                                    first_missing_sequence,
                                    next_available_sequence: sequence.saturating_add(1),
                                    resync_required: true,
                                    resource_ids,
                                }),
                            ),
                        )
                        .await?;
                    }
                }
            }
            stream = connection.accept_uni() => {
                match stream {
                    Ok(stream) => {
                        let context = input_context.clone();
                        let critical_tx = critical_tx.clone();
                        let reliable_input_stream_open = reliable_input_stream_open.clone();
                        tasks.spawn(async move {
                            if let Err(error) = handle_uni_stream(
                                stream, context, critical_tx, reliable_input_stream_open,
                            ).await {
                                warn!(%error, "QUIC unidirectional stream ended");
                            }
                        });
                    }
                    Err(_) => break,
                }
            }
            stream = connection.accept_bi() => {
                match stream {
                    Ok((send, recv)) => {
                        let replica_service = (negotiated_feature_versions.get(&(proto::Feature::ClipboardSync as i32)) == Some(&crate::clipboard_replication::VERSION)
                            && granted_capabilities.contains(&CapabilityId::ClipboardSync))
                            .then(|| coordinator.service().clipboard.clone());
                        let replica_limit = replica_limit.clone();
                        let blob_store = blob_store.clone();
                        let clipboard_uploads = clipboard_uploads.clone();
                        let remote_file_provider = remote_file_provider.clone();
                        let remote_file_access = remote_file_access.clone();
                        let blob_owner = device_public_key.clone();
                        let clipboard_upload_allowed = negotiated_features.contains(
                            &(proto::Feature::ClipboardSync as i32),
                        ) && granted_capabilities.contains(&CapabilityId::ClipboardSync);
                        tasks.spawn(async move {
                            if let Err(error) = handle_auxiliary_stream(
                                send, recv, AuxiliaryContext {
                                    blob_store, clipboard_uploads, blob_owner,
                                    clipboard_upload_allowed, replica_service, replica_limit,
                                    remote_file_provider, remote_file_access,
                                },
                            ).await {
                                warn!(%error, "QUIC bidirectional stream ended");
                            }
                        });
                    }
                    Err(_) => break,
                }
            }
            frame = recv_control_buffered(&mut control_recv, &mut control_decoder) => {
                let frame = match frame {
                    Ok(frame) => frame,
                    Err(ProtocolError::Io(error)) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(error) => return Err(error),
                };
                last_client_control_activity = Instant::now();
                // A completed task may have queued its response and completion
                // notification in the same scheduler turn. Prune it here so a
                // client cannot be disconnected merely because the completion
                // channel lost the select race to the next control frame.
                request_tasks.retain(|_, task| !task.is_finished());
                match frame.body {
                    Some(proto::client_control_frame::Body::Hello(_)) => {
                        connection.close(6_u32.into(), b"control hello already received");
                        break;
                    }
                    Some(proto::client_control_frame::Body::Request(request)) => {
                        if request.request_id == 0 {
                            send_critical(&critical_tx, response_error(
                                request.request_id,
                                proto::ErrorCode::InvalidArgument,
                                "request id must be non-zero",
                            )).await?;
                            continue;
                        }
                        if request_tasks.contains_key(&request.request_id) {
                            connection.close(6_u32.into(), b"request id reused while outstanding");
                            break;
                        }
                        let permit = match request_limit.clone().try_acquire_owned() {
                            Ok(permit) => permit,
                            Err(_) => {
                                send_critical(&critical_tx, response_error(
                                    request.request_id,
                                    proto::ErrorCode::ResourceExhausted,
                                    "too many concurrent requests",
                                )).await?;
                                continue;
                            }
                        };
                        let request_id = request.request_id;
                        let command_request = matches!(
                            request.body.as_ref(),
                            Some(proto::request::Body::Command(_))
                        );
                        let context = request_context.clone();
                        let critical = critical_tx.clone();
                        let done = request_done_tx.clone();
                        let task = tasks.spawn(async move {
                            let _permit = permit;
                            let timeout = Duration::from_millis(request.timeout_ms.clamp(100, 120_000) as u64);
                            let mut operation = tokio::spawn(handle_request(request, context));
                            let _abort_on_drop = TaskAbortGuard(operation.abort_handle());
                            let frame = match tokio::time::timeout(timeout, &mut operation).await {
                                Ok(Ok(frame)) => frame,
                                Ok(Err(error)) => response_error(
                                    request_id,
                                    proto::ErrorCode::Internal,
                                    format!("request task failed: {error}"),
                                ),
                                Err(_) => {
                                    operation.abort();
                                    response_error(
                                        request_id,
                                        proto::ErrorCode::DeadlineExceeded,
                                        if command_request {
                                            "command may continue in the background; retry with the same idempotency key"
                                        } else {
                                            "request deadline exceeded"
                                        },
                                    )
                                }
                            };
                            let _ = send_critical(&critical, frame).await;
                            let _ = done.send(request_id).await;
                        });
                        request_tasks.insert(request_id, task);
                    }
                    Some(proto::client_control_frame::Body::CancelRequest(cancel)) => {
                        if cancel.request_id == 0 {
                            connection.close(6_u32.into(), b"invalid cancellation request id");
                            break;
                        }
                        if let Some(task) = request_tasks.remove(&cancel.request_id) {
                            if !task.is_finished() {
                                task.abort();
                                send_critical(&critical_tx, response_error(
                                    cancel.request_id,
                                    proto::ErrorCode::Cancelled,
                                    "request cancelled",
                                )).await?;
                            }
                        }
                    }
                    Some(proto::client_control_frame::Body::Subscribe(request)) => {
                        if request.request_id == 0 {
                            send_subscription_result(
                                &critical_tx,
                                0,
                                0,
                                proto::SubscriptionTopic::Unspecified,
                                0,
                                status(proto::ErrorCode::InvalidArgument, "request id must be non-zero"),
                            )
                            .await?;
                            continue;
                        }
                        if request_tasks.contains_key(&request.request_id) {
                            connection.close(6_u32.into(), b"request id reused while outstanding");
                            break;
                        }
                        handle_subscribe(
                            request,
                            &coordinator,
                            &action_provider,
                            &negotiated_features,
                            &granted_capabilities,
                            &critical_tx,
                            &mut subscriptions,
                        ).await?;
                    }
                    Some(proto::client_control_frame::Body::Unsubscribe(request)) => {
                        if request.request_id == 0 {
                            send_subscription_result(
                                &critical_tx,
                                0,
                                request.subscription_id,
                                proto::SubscriptionTopic::Unspecified,
                                0,
                                status(proto::ErrorCode::InvalidArgument, "request id must be non-zero"),
                            )
                            .await?;
                            continue;
                        }
                        if request_tasks.contains_key(&request.request_id) {
                            connection.close(6_u32.into(), b"request id reused while outstanding");
                            break;
                        }
                        handle_unsubscribe(
                            request,
                            &critical_tx,
                            &mut subscriptions,
                        ).await?;
                    }
                    Some(proto::client_control_frame::Body::Ping(ping)) => {
                        if ping.request_id == 0 || request_tasks.contains_key(&ping.request_id) {
                            connection.close(6_u32.into(), b"invalid or reused request id");
                            break;
                        }
                        send_critical(&critical_tx, server_frame(
                            proto::server_control_frame::Body::Pong(proto::Pong {
                                request_id: ping.request_id,
                                client_monotonic_elapsed_us: ping.monotonic_elapsed_us,
                                server_time_ms: now_ms(),
                            })
                        )).await?;
                    }
                    Some(proto::client_control_frame::Body::BeginInputSession(request)) => {
                        if request.request_id == 0 {
                            send_critical(
                                &critical_tx,
                                server_frame(proto::server_control_frame::Body::InputSessionResult(
                                    input_session_result(
                                        0,
                                        false,
                                        InputLease { session_id: 0, epoch: 0 },
                                        coordinator.service().input_control.permission_state(),
                                        "request id must be non-zero",
                                        proto::ErrorCode::InvalidArgument,
                                    ),
                                )),
                            )
                            .await?;
                            continue;
                        }
                        if request_tasks.contains_key(&request.request_id) {
                            connection.close(6_u32.into(), b"request id reused while outstanding");
                            break;
                        }
                        let frame = begin_input_session(
                            request, &negotiated_features, &granted_capabilities, &input_context,
                        ).await;
                        send_critical(&critical_tx, frame).await?;
                    }
                    Some(proto::client_control_frame::Body::EndInputSession(request)) => {
                        if request.request_id == 0 {
                            send_critical(
                                &critical_tx,
                                server_frame(proto::server_control_frame::Body::InputSessionResult(
                                    input_session_result(
                                        0,
                                        false,
                                        InputLease { session_id: 0, epoch: 0 },
                                        coordinator.service().input_control.permission_state(),
                                        "request id must be non-zero",
                                        proto::ErrorCode::InvalidArgument,
                                    ),
                                )),
                            )
                            .await?;
                            continue;
                        }
                        if request_tasks.contains_key(&request.request_id) {
                            connection.close(6_u32.into(), b"request id reused while outstanding");
                            break;
                        }
                        let _apply_guard = input_apply_gate.lock().await;
                        let lease = InputLease { session_id: request.session_id, epoch: request.epoch };
                        let matches = active_input.lock().await.as_ref().is_some_and(|active| *active == lease);
                        if matches {
                            end_input_lease_locked(lease, &input_context).await;
                        }
                        send_critical(&critical_tx, server_frame(
                            proto::server_control_frame::Body::InputSessionResult(
                                input_session_result(
                                    request.request_id,
                                    matches,
                                    lease,
                                    coordinator.service().input_control.permission_state(),
                                    if matches { "" } else { "input session is not active" },
                                    proto::ErrorCode::FailedPrecondition,
                                )
                            )
                        )).await?;
                    }
                    Some(proto::client_control_frame::Body::TransferOffer(request)) => {
                        if request.request_id == 0 || request_tasks.contains_key(&request.request_id) {
                            connection.close(6_u32.into(), b"invalid or reused request id");
                            break;
                        }
                        send_critical(&critical_tx, server_frame(
                            proto::server_control_frame::Body::TransferCancel(
                                proto::TransferCancel {
                                    request_id: request.request_id,
                                    transfer_id: request.transfer_id,
                                    status: Some(status(
                                        proto::ErrorCode::Unsupported,
                                        "file transfers are not enabled",
                                    )),
                                },
                            ),
                        )).await?;
                    }
                    Some(proto::client_control_frame::Body::TransferAccept(_))
                    | Some(proto::client_control_frame::Body::TransferCancel(_))
                    | Some(proto::client_control_frame::Body::TransferProgress(_)) => {
                        connection.close(6_u32.into(), b"file transfers are not negotiated");
                        break;
                    }
                    None => {
                        connection.close(6_u32.into(), b"invalid control frame");
                        break;
                    }
                }
            }
            update = recv_system(&mut subscriptions.system_sub) => {
                if let Some(update) = update {
                    push_system_event(&latest_state, subscriptions.system_sub.as_mut().unwrap(), update);
                }
            }
            update = recv_media(&mut subscriptions.media_sub) => {
                if let Some(update) = update {
                    push_media_event(&latest_state, subscriptions.media_sub.as_mut().unwrap(), update);
                }
            }
            update = recv_windows(&mut subscriptions.window_sub) => {
                if let Some(update) = update {
                    let spaces = current_spaces(&coordinator).await;
                    push_window_event(&latest_state, subscriptions.window_sub.as_mut().unwrap(), update, spaces);
                }
            }
            update = recv_clipboard(&mut subscriptions.clipboard_sub) => {
                if let Some(update) = update {
                    push_clipboard_event(&latest_state, subscriptions.clipboard_sub.as_mut().unwrap(), update);
                }
            }
            output = recv_action_output(&mut subscriptions.action_output_sub) => {
                if let Some(output) = output {
                    push_action_output_event(&event_tx_out, subscriptions.action_output_sub.as_mut().unwrap(), output);
                }
            }
            change = recv_notification_change(&mut subscriptions.notification_sub) => {
                if change.is_some() {
                    push_notification_event(
                        &latest_state,
                        subscriptions.notification_sub.as_mut().unwrap(),
                        action_provider.as_ref(),
                    ).await;
                }
            }
            change = recv_clipboard_sync(&mut subscriptions.clipboard_sync_sub) => {
                if let Some(record) = change.filter(|record| {
                    coordinator.service().clipboard.should_send_sync_record(record)
                }) {
                    push_clipboard_sync_event(
                        &event_tx_out,
                        subscriptions.clipboard_sync_sub.as_mut().unwrap(),
                        record,
                        &blob_store,
                        &device_public_key,
                    );
                }
            }
        }
    }

    Ok(())
    }.await;
    connection.close(0_u32.into(), b"control session ended");
    tasks.shutdown().await;
    result
}

async fn recv_workspace_input_change(
    receiver: &mut Option<broadcast::Receiver<WorkspaceInputSnapshot>>,
) -> Option<WorkspaceInputSnapshot> {
    loop {
        match receiver.as_mut() {
            Some(events) => match events.recv().await {
                Ok(change) => return Some(change),
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => *receiver = None,
            },
            None => std::future::pending::<()>().await,
        }
    }
}

include!("proto_handler/requests/control.rs");
include!("proto_handler/requests/queries.rs");
include!("proto_handler/requests/commands.rs");
include!("proto_handler/requests/snapshots.rs");
include!("proto_handler/subscriptions.rs");
include!("proto_handler/input.rs");

include!("proto_handler/tests.rs");
