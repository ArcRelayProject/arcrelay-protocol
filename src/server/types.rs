use tokio::sync::{broadcast, mpsc};

use arcrelay_core::domain::input_control::InputEvent;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostCapabilityErrorCode {
    InvalidArgument,
    NotFound,
    Conflict,
    FailedPrecondition,
    Unavailable,
    Unsupported,
    Internal,
}

impl HostCapabilityErrorCode {
    pub const fn protocol_code(self) -> crate::proto_msg::proto::ErrorCode {
        use crate::proto_msg::proto::ErrorCode;
        match self {
            Self::InvalidArgument => ErrorCode::InvalidArgument,
            Self::NotFound => ErrorCode::NotFound,
            Self::Conflict => ErrorCode::Conflict,
            Self::FailedPrecondition => ErrorCode::FailedPrecondition,
            Self::Unavailable => ErrorCode::Unavailable,
            Self::Unsupported => ErrorCode::Unsupported,
            Self::Internal => ErrorCode::Internal,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct HostCapabilityError {
    pub code: HostCapabilityErrorCode,
    pub message: String,
}

impl HostCapabilityError {
    pub fn new(code: HostCapabilityErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

pub type HostCapabilityResult<T> = std::result::Result<T, HostCapabilityError>;

// ── Quick Action Provider Trait ─────────────────────────────────────

/// Info about a quick action exposed to mobile clients.
#[derive(Debug, Clone)]
pub struct QuickActionInfo {
    pub id: String,
    pub name: String,
    pub icon_id: String,
    pub icon_svg: String,
    pub color: String,
    pub group: String,
    pub action_type_label: String,
    pub sort_order: u32,
    pub is_toggle: bool,
    pub is_running: bool,
    pub requires_confirmation: bool,
}

/// A single line of output from an action process (for broadcast).
#[derive(Debug, Clone)]
pub struct AutomationInfo {
    pub id: String,
    pub name: String,
    pub summary: String,
    pub enabled: bool,
    pub latest_activity_id: Option<String>,
    pub latest_status: Option<String>,
    pub reason: Option<String>,
    pub next_run_at_ms: Option<i64>,
    pub completed_steps: u32,
    pub total_steps: u32,
}

/// A single line of output from an action process (for broadcast).
#[derive(Debug, Clone)]
pub struct OutputLine {
    pub action_id: String,
    pub text: String,
}

/// A durable Host notification exposed to trusted mobile clients.
#[derive(Debug, Clone)]
pub struct HostNotificationInfo {
    pub id: String,
    pub title: String,
    pub body: String,
    pub source: String,
    pub kind: i32,
    pub reference: Option<String>,
    pub created_at_ms: i64,
    pub read_at_ms: Option<i64>,
    pub read_by_device_name: Option<String>,
}

#[async_trait::async_trait]
pub trait ActionProvider: Send + Sync {
    async fn list_actions(&self) -> Vec<QuickActionInfo>;
    async fn execute_action(&self, action_id: &str) -> HostCapabilityResult<String>;

    fn subscribe_output(&self) -> Option<broadcast::Receiver<OutputLine>> {
        None
    }
    async fn get_action_output(&self, _action_id: &str) -> Vec<String> {
        vec![]
    }
}

#[async_trait::async_trait]
pub trait WorkflowProvider: Send + Sync {
    async fn list_automations(&self) -> Vec<AutomationInfo> {
        vec![]
    }
    async fn run_automation(&self, _id: &str) -> HostCapabilityResult<String> {
        Err(HostCapabilityError::new(
            HostCapabilityErrorCode::Unsupported,
            "automations unavailable",
        ))
    }
    async fn set_automation_enabled(&self, _id: &str, _enabled: bool) -> HostCapabilityResult<()> {
        Err(HostCapabilityError::new(
            HostCapabilityErrorCode::Unsupported,
            "automations unavailable",
        ))
    }
    async fn cancel_automation(&self, _id: &str) -> HostCapabilityResult<()> {
        Err(HostCapabilityError::new(
            HostCapabilityErrorCode::Unsupported,
            "automations unavailable",
        ))
    }
}

#[async_trait::async_trait]
pub trait NotificationProvider: Send + Sync {
    fn notifications_available(&self) -> bool {
        false
    }

    fn subscribe_notifications(&self) -> Option<broadcast::Receiver<()>> {
        None
    }

    async fn list_notifications(
        &self,
        _include_read: bool,
        _limit: usize,
    ) -> HostCapabilityResult<Vec<HostNotificationInfo>> {
        Err(HostCapabilityError::new(
            HostCapabilityErrorCode::Unsupported,
            "notifications are unavailable",
        ))
    }

    async fn mark_notification_read(
        &self,
        _notification_id: &str,
        _device_id: &str,
        _device_name: &str,
    ) -> HostCapabilityResult<HostNotificationInfo> {
        Err(HostCapabilityError::new(
            HostCapabilityErrorCode::Unsupported,
            "notifications are unavailable",
        ))
    }
}

/// Capability bundle accepted by the protocol server. Each feature is kept in
/// a separate trait so hosts can compose adapters without one oversized port.
pub trait HostCapabilityProvider:
    ActionProvider + WorkflowProvider + NotificationProvider + Send + Sync
{
}

impl<T> HostCapabilityProvider for T where
    T: ActionProvider + WorkflowProvider + NotificationProvider + Send + Sync
{
}

// ── Workspace Input Router ────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceInputRouteState {
    Active,
    Suspended,
    Ended,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceInputFocusCause {
    SessionStarted,
    MobilePortal,
    DesktopPortal,
    PhysicalActivity,
    SessionEnded,
    GatewayUnavailable,
}

/// Current workspace-level input focus as observed by a control connection.
/// `owner_device_id` is present only while a mobile-originated input session
/// owns the desktop gateway. Observer-only desktop focus changes leave it
/// empty so every connected mobile may follow the logical target without
/// accidentally treating the event as its own lease.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceInputSnapshot {
    pub owner_device_id: Option<String>,
    pub controller_device_id: String,
    pub logical_target_device_id: String,
    pub target_display_id: String,
    pub control_epoch: u64,
    pub supports_system_gestures: bool,
    pub state: WorkspaceInputRouteState,
    pub cause: WorkspaceInputFocusCause,
    pub message: String,
}

/// Host adapter that feeds authenticated mobile events into the same ArcInput
/// ownership, topology and handoff path used by physical desktop input.
#[async_trait::async_trait]
pub trait WorkspaceInputRouter: Send + Sync {
    fn available(&self) -> bool;

    fn subscribe(&self) -> broadcast::Receiver<WorkspaceInputSnapshot>;

    async fn begin(&self, owner_device_id: &str) -> HostCapabilityResult<WorkspaceInputSnapshot>;

    async fn apply(
        &self,
        owner_device_id: &str,
        events: &[InputEvent],
    ) -> HostCapabilityResult<WorkspaceInputSnapshot>;

    async fn end(&self, owner_device_id: &str, cause: WorkspaceInputFocusCause);
}

// ── Server Events ───────────────────────────────────────────────────

/// Events emitted by the server to the UI
#[derive(Debug, Clone)]
pub enum ServerEvent {
    /// A new device wants to pair.
    PairingRequest {
        device_name: String,
        device_id: String,
        pairing_code: String,
    },
    /// Pairing was approved/rejected
    PairingResult { device_name: String, accepted: bool },
    /// A pending pairing request was rejected, cancelled, or timed out.
    PairingCancelled {
        device_id: String,
        device_name: String,
    },
    /// A desktop client is connecting outward and shows the same verification
    /// code as the receiving desktop's approval prompt.
    OutgoingPairingCode {
        device_id: String,
        device_name: String,
        pairing_code: String,
    },
    /// Clears an outward desktop pairing code after setup succeeds or ends.
    OutgoingPairingFinished { device_id: String },
    /// A device connected
    DeviceConnected {
        device_id: String,
        device_name: String,
    },
    /// A device disconnected
    DeviceDisconnected {
        device_id: String,
        device_name: String,
    },
    /// A trusted device attempted to start remote input while the host's
    /// operating-system input permission was unavailable.
    InputPermissionRequired { device_name: String },
    /// A trusted device acquired a remote-input lease.
    InputSessionStarted {
        session_id: String,
        device_id: String,
        device_name: String,
    },
    /// A remote-input lease ended or timed out.
    InputSessionEnded {
        session_id: String,
        device_id: String,
        device_name: String,
    },
    /// Temporary end-to-end input diagnostics, refreshed about once per second.
    InputMetrics {
        session_id: String,
        receive_hz: f32,
        quartz_hz: f32,
        average_gap_us: u64,
        maximum_gap_us: u64,
        maximum_source_gap_us: u64,
        maximum_transport_stall_us: u64,
        average_apply_us: u64,
        maximum_apply_us: u64,
    },
}

#[derive(Debug, Clone)]
pub struct PairingApproval {
    pub accepted: bool,
    pub approved_grants:
        std::collections::BTreeSet<(arcrelay_peer::CapabilityId, arcrelay_peer::GrantDirection)>,
}

/// Channel for the UI to reject pairing or approve a least-privilege subset.
pub type PairingApprovalTx = mpsc::Sender<PairingApproval>;
pub type PairingApprovalRx = mpsc::Receiver<PairingApproval>;

/// Pairing request sent to UI for approval
#[derive(Debug)]
pub struct PairingRequestForApproval {
    pub device_name: String,
    pub device_id: String,
    pub pairing_code: String,
    pub requested_grants: Vec<arcrelay_peer::Grant>,
    pub respond: PairingApprovalTx,
}
