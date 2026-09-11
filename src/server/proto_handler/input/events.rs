pub(super) fn convert_reliable_events(
    events: &[proto::ReliableInputEvent],
) -> std::result::Result<Vec<DomainInputEvent>, String> {
    let mut converted = Vec::with_capacity(events.len());
    for event in events {
        converted.push(match event.event.as_ref() {
            Some(proto::reliable_input_event::Event::PointerButton(button)) => {
                if !(1..=3).contains(&button.click_count) {
                    return Err("pointer click count must be between 1 and 3".into());
                }
                let button = match proto::MouseButton::try_from(button.button)
                    .unwrap_or(proto::MouseButton::Unspecified)
                {
                    proto::MouseButton::Left => DomainMouseButton::Left,
                    proto::MouseButton::Right => DomainMouseButton::Right,
                    proto::MouseButton::Middle => DomainMouseButton::Middle,
                    proto::MouseButton::Unspecified => return Err("invalid mouse button".into()),
                };
                DomainInputEvent::PointerButton {
                    button,
                    down: button_down(event),
                    click_count: match event.event.as_ref() {
                        Some(proto::reliable_input_event::Event::PointerButton(value)) => {
                            value.click_count as u8
                        }
                        _ => unreachable!(),
                    },
                }
            }
            Some(proto::reliable_input_event::Event::Key(key)) => {
                if key.hid_usage == 0 || key.hid_usage > 0xE7 {
                    return Err("invalid HID keyboard usage".into());
                }
                if key.repeat && !key.down {
                    return Err("key repeat is only valid for key-down events".into());
                }
                DomainInputEvent::Key {
                    hid_usage: key.hid_usage as u16,
                    down: key.down,
                    repeat: key.repeat,
                }
            }
            Some(proto::reliable_input_event::Event::TextCommit(text)) => {
                if text.text.is_empty() {
                    return Err("text commit is empty".into());
                }
                if text.text.len() > 4096 {
                    return Err("text commit is too large".into());
                }
                DomainInputEvent::TextCommit(text.text.clone())
            }
            Some(proto::reliable_input_event::Event::ReleaseAll(_)) => DomainInputEvent::ReleaseAll,
            Some(proto::reliable_input_event::Event::SystemGesture(gesture)) => {
                use arcrelay_core::domain::input_control::{SystemGestureEvent, SYSTEM_GESTURE_FORMAT_VERSION};
                if gesture.format_version != SYSTEM_GESTURE_FORMAT_VERSION {
                    return Err("unsupported system gesture format".into());
                }
                let gesture = SystemGestureEvent {
                    axis: gesture.axis,
                    phase: gesture.phase,
                    progress: gesture.progress,
                    velocity_x: gesture.velocity_x,
                    velocity_y: gesture.velocity_y,
                    // The mobile control-session protocol remains DockSwipe v1.
                    inverted_from_device: false,
                    finger_count: 0,
                }.validate_format(gesture.format_version).map_err(str::to_string)?;
                DomainInputEvent::SystemGesture(gesture)
            }
            Some(proto::reliable_input_event::Event::ScrollGesture(gesture)) => {
                let phase = match proto::ScrollGesturePhase::try_from(gesture.phase)
                    .unwrap_or(proto::ScrollGesturePhase::Unspecified)
                {
                    proto::ScrollGesturePhase::Began => DomainScrollGesturePhase::Began,
                    proto::ScrollGesturePhase::Ended => DomainScrollGesturePhase::Ended,
                    proto::ScrollGesturePhase::Cancelled => DomainScrollGesturePhase::Cancelled,
                    proto::ScrollGesturePhase::MomentumBegan => {
                        DomainScrollGesturePhase::MomentumBegan
                    }
                    proto::ScrollGesturePhase::MomentumEnded => {
                        DomainScrollGesturePhase::MomentumEnded
                    }
                    proto::ScrollGesturePhase::Unspecified => {
                        return Err("invalid scroll gesture phase".into());
                    }
                };
                DomainInputEvent::ScrollGesture { phase }
            }
            None => return Err("empty reliable input event".into()),
        });
    }
    Ok(converted)
}

pub(super) fn button_down(event: &proto::ReliableInputEvent) -> bool {
    match event.event.as_ref() {
        Some(proto::reliable_input_event::Event::PointerButton(button)) => button.down,
        _ => false,
    }
}

pub(super) fn input_feedback_frame(
    lease: InputLease,
    feedback: InputFeedbackSnapshot,
    result: proto::Status,
) -> proto::ServerControlFrame {
    server_frame(proto::server_control_frame::Body::InputFeedback(
        proto::InputFeedback {
            session_id: lease.session_id,
            epoch: lease.epoch,
            reliable_sequence: feedback.reliable_sequence,
            motion_packet_sequence: feedback.motion_sequence,
            server_time_ms: now_ms(),
            status: Some(result),
            motion_datagrams_received: feedback.motion_datagrams_received,
            motion_datagrams_missing: feedback.motion_datagrams_missing,
        },
    ))
}

pub(super) fn input_focus_changed_frame(
    lease: Option<InputLease>,
    focus: &WorkspaceInputSnapshot,
) -> proto::ServerControlFrame {
    let state = match focus.state {
        WorkspaceInputRouteState::Active => proto::InputRouteState::Active,
        WorkspaceInputRouteState::Suspended => proto::InputRouteState::Suspended,
        WorkspaceInputRouteState::Ended => proto::InputRouteState::Ended,
    };
    let cause = match focus.cause {
        WorkspaceInputFocusCause::SessionStarted => proto::InputFocusCause::SessionStarted,
        WorkspaceInputFocusCause::MobilePortal => proto::InputFocusCause::MobilePortal,
        WorkspaceInputFocusCause::DesktopPortal => proto::InputFocusCause::DesktopPortal,
        WorkspaceInputFocusCause::PhysicalActivity => proto::InputFocusCause::PhysicalActivity,
        WorkspaceInputFocusCause::SessionEnded => proto::InputFocusCause::SessionEnded,
        WorkspaceInputFocusCause::GatewayUnavailable => {
            proto::InputFocusCause::GatewayUnavailable
        }
    };
    server_frame(proto::server_control_frame::Body::InputFocusChanged(
        proto::InputFocusChanged {
            session_id: lease.map_or(0, |lease| lease.session_id),
            epoch: lease.map_or(0, |lease| lease.epoch),
            state: state as i32,
            cause: cause as i32,
            controller_device_id: focus.controller_device_id.clone(),
            logical_target_device_id: focus.logical_target_device_id.clone(),
            target_display_id: focus.target_display_id.clone(),
            control_epoch: focus.control_epoch,
            message: focus.message.clone(),
            supports_system_gestures: focus.supports_system_gestures,
        },
    ))
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn revoke_input_with_feedback(
    lease: InputLease,
    message: &str,
    active_input: &Arc<Mutex<Option<InputLease>>>,
    input_sessions: &InputSessionManager,
    coordinator: &StateCoordinator,
    critical_tx: &mpsc::Sender<proto::ServerControlFrame>,
    event_tx: &mpsc::Sender<ServerEvent>,
    device_id: &str,
    device_name: &str,
    workspace_input_router: Option<&Arc<dyn WorkspaceInputRouter>>,
) {
    end_input_lease_locked(
        lease,
        active_input,
        input_sessions,
        coordinator,
        event_tx,
        device_id,
        device_name,
        workspace_input_router,
    )
    .await;
    let _ = send_critical(
        critical_tx,
        input_feedback_frame(
            lease,
            InputFeedbackSnapshot::default(),
            status(proto::ErrorCode::FailedPrecondition, message),
        ),
    )
    .await;
}

pub(super) async fn end_input_lease_locked(
    lease: InputLease,
    active_input: &Arc<Mutex<Option<InputLease>>>,
    input_sessions: &InputSessionManager,
    coordinator: &StateCoordinator,
    event_tx: &mpsc::Sender<ServerEvent>,
    device_id: &str,
    device_name: &str,
    workspace_input_router: Option<&Arc<dyn WorkspaceInputRouter>>,
) {
    let mut active = active_input.lock().await;
    if active.as_ref().is_some_and(|current| *current == lease) {
        *active = None;
        drop(active);
        let workspace_routed = input_sessions.is_workspace_routed(lease, device_id).await;
        let release_events = input_sessions.release(lease).await;
        if workspace_routed {
            if let Some(router) = workspace_input_router {
                router
                    .end(device_id, WorkspaceInputFocusCause::SessionEnded)
                    .await;
            }
        } else if !release_events.is_empty() {
            let _ = coordinator
                .service()
                .input_control
                .apply_events(&release_events)
                .await;
        }
        emit_input_ended(event_tx, lease, device_id, device_name).await;
    }
}

pub(super) async fn emit_input_ended(
    event_tx: &mpsc::Sender<ServerEvent>,
    lease: InputLease,
    device_id: &str,
    device_name: &str,
) {
    let _ = event_tx
        .send(ServerEvent::InputSessionEnded {
            session_id: lease_label(lease),
            device_id: device_id.to_string(),
            device_name: device_name.to_string(),
        })
        .await;
}

pub(super) fn lease_label(lease: InputLease) -> String {
    format!("{:016x}:{:08x}", lease.session_id, lease.epoch)
}

pub(super) fn random_nonzero_u64() -> u64 {
    let mut value = rand::rngs::OsRng.next_u64() & i64::MAX as u64;
    if value == 0 {
        value = 1;
    }
    value
}
