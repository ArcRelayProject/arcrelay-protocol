mod blob_store;
mod clipboard_upload;
mod command_cache;
mod connection_registry;
mod input_session;
mod proto_handler;
mod types;
mod wire;

// Re-export public API
pub use connection_registry::ConnectionRegistry;
pub use proto_handler::handle_remote_file_stream as serve_remote_file_stream;
pub use types::*;

use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::warn;

use arcrelay_core::application::state_coordinator::StateCoordinator;
use arcrelay_network::{NetworkRuntime, Session, SessionKind};
use arcrelay_peer::{CapabilityId, Grant, GrantConstraints, GrantDirection};
use prost::Message as _;

use crate::error::Result;
use input_session::InputSessionManager;

pub const CONTROL_TYPE_URL: &str = "arcrelay.control";
const PAIRING_APPROVAL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(90);

/// v1 control-plane adapter. It contains business protocol state only; device
/// identity, trust, discovery, sockets and QUIC authentication are owned by
/// `NetworkRuntime`.
pub struct DesktopControlService {
    connection_registry: ConnectionRegistry,
    input_sessions: InputSessionManager,
    pairing_gate: Arc<tokio::sync::Semaphore>,
    command_cache: Arc<tokio::sync::Mutex<command_cache::CommandCache>>,
    command_limit: Arc<tokio::sync::Semaphore>,
    blob_store: blob_store::BlobStore,
    clipboard_uploads: clipboard_upload::ClipboardUploadStore,
    workspace_input_router: Option<Arc<dyn WorkspaceInputRouter>>,
}

impl Default for DesktopControlService {
    fn default() -> Self {
        Self::new()
    }
}

impl DesktopControlService {
    pub fn new() -> Self {
        Self {
            connection_registry: ConnectionRegistry::new(),
            input_sessions: InputSessionManager::new(),
            pairing_gate: Arc::new(tokio::sync::Semaphore::new(1)),
            command_cache: Arc::new(tokio::sync::Mutex::new(
                command_cache::CommandCache::default(),
            )),
            command_limit: Arc::new(tokio::sync::Semaphore::new(
                proto_handler::MAX_BACKGROUND_COMMANDS,
            )),
            blob_store: blob_store::BlobStore::new(),
            clipboard_uploads: clipboard_upload::ClipboardUploadStore::default(),
            workspace_input_router: None,
        }
    }

    pub fn with_workspace_input_router(mut self, router: Arc<dyn WorkspaceInputRouter>) -> Self {
        self.workspace_input_router = Some(router);
        self
    }

    pub fn connection_registry(&self) -> ConnectionRegistry {
        self.connection_registry.clone()
    }

    pub async fn start(
        self: Arc<Self>,
        network: Arc<NetworkRuntime>,
        coordinator: Arc<StateCoordinator>,
        event_tx: mpsc::Sender<ServerEvent>,
        pairing_tx: mpsc::Sender<PairingRequestForApproval>,
        action_provider: Option<Arc<dyn HostCapabilityProvider>>,
        remote_file_provider: Option<Arc<dyn crate::remote_files::RemoteFileProvider>>,
    ) -> Result<()> {
        let incoming = network.subscribe();
        self.start_with_incoming(
            network,
            incoming,
            coordinator,
            event_tx,
            pairing_tx,
            action_provider,
            remote_file_provider,
        )
        .await
    }

    /// Starts the control service with a receiver subscribed by the caller.
    /// Desktop startup uses this to subscribe immediately after the network
    /// endpoint is bound, so authenticated sessions arriving during other
    /// backend initialization remain buffered for the control service.
    #[allow(clippy::too_many_arguments)]
    pub async fn start_with_incoming(
        self: Arc<Self>,
        network: Arc<NetworkRuntime>,
        mut incoming: tokio::sync::broadcast::Receiver<Arc<Session>>,
        coordinator: Arc<StateCoordinator>,
        event_tx: mpsc::Sender<ServerEvent>,
        pairing_tx: mpsc::Sender<PairingRequestForApproval>,
        action_provider: Option<Arc<dyn HostCapabilityProvider>>,
        remote_file_provider: Option<Arc<dyn crate::remote_files::RemoteFileProvider>>,
    ) -> Result<()> {
        tracing::info!(
            event = "desktop.control_service.ready",
            "desktop control service is ready for incoming sessions"
        );
        loop {
            let session = match incoming.recv().await {
                Ok(session) => session,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!(skipped, "control service lagged behind incoming sessions");
                    continue;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
            };
            match session.kind() {
                SessionKind::Pairing => {
                    let network = network.clone();
                    let event_tx = event_tx.clone();
                    let pairing_tx = pairing_tx.clone();
                    let gate = self.pairing_gate.clone();
                    tokio::spawn(async move {
                        if let Err(error) =
                            handle_v1_pairing(network, session, event_tx, pairing_tx, gate).await
                        {
                            warn!(%error, "v1 pairing session failed");
                        }
                    });
                }
                SessionKind::Control => {
                    tracing::info!(
                        event = "desktop.control_service.session_received",
                        session_id = session.id(),
                        "desktop control service received an authenticated session"
                    );
                    let service = self.clone();
                    let network = network.clone();
                    let coordinator = coordinator.clone();
                    let event_tx = event_tx.clone();
                    let action_provider = action_provider.clone();
                    let remote_file_provider = remote_file_provider.clone();
                    tokio::spawn(async move {
                        if let Err(error) = service
                            .serve_control_session(
                                network,
                                session,
                                coordinator,
                                event_tx,
                                action_provider,
                                remote_file_provider,
                            )
                            .await
                        {
                            warn!(%error, "v1 control session ended");
                        }
                    });
                }
                _ => {}
            }
        }
    }

    async fn serve_control_session(
        &self,
        network: Arc<NetworkRuntime>,
        session: Arc<Session>,
        coordinator: Arc<StateCoordinator>,
        event_tx: mpsc::Sender<ServerEvent>,
        action_provider: Option<Arc<dyn HostCapabilityProvider>>,
        remote_file_provider: Option<Arc<dyn crate::remote_files::RemoteFileProvider>>,
    ) -> Result<()> {
        let stream = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            session.accept_feature_stream(),
        )
        .await
        .map_err(|_| crate::error::ProtocolError::Other("control stream timed out".into()))?
        .map_err(|error| crate::error::ProtocolError::Other(error.to_string()))?;
        if stream.feature_id != CONTROL_TYPE_URL
            || stream.negotiate_minor(1, 0, 0).is_err()
            || !stream.opening_payload.is_empty()
        {
            return Err(crate::error::ProtocolError::Other(
                "invalid v1 control stream header".into(),
            ));
        }
        let grants = network
            .grants(&session.peer().device_id)
            .await
            .map_err(|error| crate::error::ProtocolError::Other(error.to_string()))?;
        let server_features = proto_handler::server_features(
            &coordinator,
            action_provider.is_some(),
            action_provider
                .as_ref()
                .is_some_and(|provider| provider.notifications_available()),
            remote_file_provider.is_some(),
            self.workspace_input_router
                .as_ref()
                .is_some_and(|router| router.available()),
        );
        let remote_file_access =
            crate::remote_files::RemoteFileAccess::from_grants(&grants, GrantDirection::Inbound);
        proto_handler::handle_quic_connection(
            session.transport_handle(),
            coordinator,
            event_tx,
            action_provider,
            remote_file_provider,
            self.connection_registry.clone(),
            self.input_sessions.clone(),
            self.command_cache.clone(),
            self.command_limit.clone(),
            self.blob_store.clone(),
            self.clipboard_uploads.clone(),
            self.workspace_input_router.clone(),
            stream.send,
            stream.receive,
            session.peer().metadata.name.clone(),
            session.peer().device_id.to_string(),
            session.peer().public_key.as_bytes().to_vec(),
            server_features,
            grants,
            remote_file_access,
        )
        .await
    }
}

async fn handle_v1_pairing(
    network: Arc<NetworkRuntime>,
    session: Arc<Session>,
    event_tx: mpsc::Sender<ServerEvent>,
    pairing_tx: mpsc::Sender<PairingRequestForApproval>,
    gate: Arc<tokio::sync::Semaphore>,
) -> Result<()> {
    let _permit = gate
        .acquire_owned()
        .await
        .map_err(|_| crate::error::ProtocolError::Other("pairing service stopped".into()))?;
    let mut stream = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        session.accept_feature_stream(),
    )
    .await
    .map_err(|_| crate::error::ProtocolError::Other("pairing stream timed out".into()))?
    .map_err(|error| crate::error::ProtocolError::Other(error.to_string()))?;
    if stream.feature_id != "arcrelay.pairing" || stream.negotiate_minor(1, 0, 0).is_err() {
        return Err(crate::error::ProtocolError::Other(
            "invalid pairing stream type".into(),
        ));
    }
    let request = arcrelay_wire::common::PairingRequest::decode(stream.opening_payload.as_ref())
        .map_err(|error| crate::error::ProtocolError::Other(error.to_string()))?;
    if request.requested_grants.len() > 64 {
        return Err(crate::error::ProtocolError::Other(
            "too many requested capabilities".into(),
        ));
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    let mut requested = Vec::with_capacity(request.requested_grants.len());
    let mut seen = std::collections::BTreeSet::new();
    for wire in &request.requested_grants {
        if wire.capability.is_empty() || wire.capability.len() > 128 || wire.granted_at_ms != 0 {
            return Err(crate::error::ProtocolError::Other(
                "invalid pairing capability request".into(),
            ));
        }
        let capability = CapabilityId::parse_token(&wire.capability).ok_or_else(|| {
            crate::error::ProtocolError::Other(format!("unknown capability {}", wire.capability))
        })?;
        let direction = match arcrelay_wire::common::GrantDirection::try_from(wire.direction) {
            Ok(arcrelay_wire::common::GrantDirection::Inbound) => GrantDirection::Inbound,
            Ok(arcrelay_wire::common::GrantDirection::Outbound) => GrantDirection::Outbound,
            _ => {
                return Err(crate::error::ProtocolError::Other(
                    "invalid pairing grant direction".into(),
                ))
            }
        };
        if !seen.insert((capability, direction)) {
            return Err(crate::error::ProtocolError::Other(
                "duplicate pairing capability request".into(),
            ));
        }
        let constraints = decode_pairing_constraints(wire.constraints.clone())?;
        validate_grant_constraints(capability, &constraints)?;
        requested.push(Grant {
            peer_id: session.peer().device_id.clone(),
            capability,
            direction,
            constraints,
            granted_at_ms: now,
        });
    }
    let peer = session.peer();
    let already_paired = network
        .paired_peers()
        .await
        .map_err(|error| crate::error::ProtocolError::Other(error.to_string()))?
        .into_iter()
        .any(|stored| stored.device_id == peer.device_id && stored.public_key == peer.public_key);
    let decision = if already_paired && requested.is_empty() {
        PairingApproval {
            accepted: true,
            approved_grants: std::collections::BTreeSet::new(),
        }
    } else {
        let (respond, mut response) = mpsc::channel(1);
        pairing_tx
            .send(PairingRequestForApproval {
                device_name: peer.metadata.name.clone(),
                device_id: peer.device_id.to_string(),
                pairing_code: session.verification_code().to_owned(),
                requested_grants: requested.clone(),
                respond,
            })
            .await
            .map_err(|_| crate::error::ProtocolError::Other("pairing UI stopped".into()))?;
        tokio::time::timeout(PAIRING_APPROVAL_TIMEOUT, response.recv())
            .await
            .ok()
            .flatten()
            .unwrap_or(PairingApproval {
                accepted: false,
                approved_grants: std::collections::BTreeSet::new(),
            })
    };
    let (accepted, approved) = if decision.accepted {
        let approved = requested
            .into_iter()
            .filter(|grant| {
                decision
                    .approved_grants
                    .contains(&(grant.capability, grant.direction))
            })
            .collect::<Vec<_>>();
        (true, approved)
    } else {
        (false, Vec::new())
    };
    if accepted {
        network
            .confirm_pairing_with_grants(&session, approved.clone())
            .await
            .map_err(|error| crate::error::ProtocolError::Other(error.to_string()))?;
    }
    let _ = event_tx
        .send(ServerEvent::PairingResult {
            device_name: peer.metadata.name.clone(),
            accepted,
        })
        .await;
    let response = arcrelay_wire::common::PairingResponse {
        result: Some(arcrelay_wire::common::PairingResult {
            paired: accepted,
            peer_id: network.device_id().to_string(),
            error: if accepted {
                String::new()
            } else {
                "pairing rejected".into()
            },
        }),
        granted_grants: approved
            .iter()
            .map(|grant| arcrelay_wire::common::CapabilityGrant {
                capability: grant.capability.token().to_string(),
                direction: match grant.direction {
                    GrantDirection::Inbound => {
                        arcrelay_wire::common::GrantDirection::Inbound as i32
                    }
                    GrantDirection::Outbound => {
                        arcrelay_wire::common::GrantDirection::Outbound as i32
                    }
                },
                constraints: encode_pairing_constraints(&grant.constraints),
                granted_at_ms: grant.granted_at_ms,
            })
            .collect(),
    };
    arcrelay_transport::write_frame(
        &mut stream.send,
        &response.encode_to_vec(),
        arcrelay_wire::MAX_CONTROL_FRAME_SIZE,
    )
    .await
    .map_err(|error| crate::error::ProtocolError::Other(error.to_string()))?;
    stream
        .send
        .finish()
        .map_err(|error| crate::error::ProtocolError::Other(error.to_string()))?;
    if !accepted {
        session.close("pairing rejected");
    }
    Ok(())
}

fn validate_grant_constraints(
    capability: CapabilityId,
    constraints: &GrantConstraints,
) -> Result<()> {
    let GrantConstraints::RemoteFileShares {
        share_ids,
        writable,
    } = constraints
    else {
        return Ok(());
    };
    if !matches!(
        capability,
        CapabilityId::RemoteFilesRead | CapabilityId::RemoteFilesWrite
    ) || (capability == CapabilityId::RemoteFilesRead && *writable)
        || (capability == CapabilityId::RemoteFilesWrite && !writable)
        || share_ids.len() > 128
    {
        return Err(crate::error::ProtocolError::Other(
            "invalid remote-file grant constraints".into(),
        ));
    }
    let mut seen = std::collections::HashSet::with_capacity(share_ids.len());
    if share_ids
        .iter()
        .any(|id| id.is_empty() || id.len() > 256 || !seen.insert(id))
    {
        return Err(crate::error::ProtocolError::Other(
            "invalid remote-file share constraint".into(),
        ));
    }
    Ok(())
}

fn decode_pairing_constraints(
    constraints: Option<arcrelay_wire::common::GrantConstraints>,
) -> Result<GrantConstraints> {
    match constraints {
        None => Ok(GrantConstraints::None),
        Some(arcrelay_wire::common::GrantConstraints { kind: None }) => Err(
            crate::error::ProtocolError::Other("unknown or empty grant constraints".into()),
        ),
        Some(arcrelay_wire::common::GrantConstraints {
            kind:
                Some(arcrelay_wire::common::grant_constraints::Kind::RemoteFileShares(constraints)),
        }) => Ok(GrantConstraints::RemoteFileShares {
            share_ids: constraints.share_ids,
            writable: constraints.writable,
        }),
    }
}

fn encode_pairing_constraints(
    constraints: &GrantConstraints,
) -> Option<arcrelay_wire::common::GrantConstraints> {
    let kind = match constraints {
        GrantConstraints::None => return None,
        GrantConstraints::RemoteFileShares {
            share_ids,
            writable,
        } => arcrelay_wire::common::grant_constraints::Kind::RemoteFileShares(
            arcrelay_wire::common::RemoteFileShareConstraints {
                share_ids: share_ids.clone(),
                writable: *writable,
            },
        ),
    };
    Some(arcrelay_wire::common::GrantConstraints { kind: Some(kind) })
}
