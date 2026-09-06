pub(super) fn server_features(
    coordinator: &StateCoordinator,
    actions_available: bool,
    notifications_available: bool,
    remote_files_available: bool,
    workspace_input_available: bool,
) -> Vec<proto::FeatureVersion> {
    let mut features = vec![
        feature(proto::Feature::Subscriptions),
        feature(proto::Feature::System),
        feature(proto::Feature::Processes),
        feature(proto::Feature::Media),
        feature(proto::Feature::Clipboard),
        feature(proto::Feature::ClipboardSync),
        feature(proto::Feature::Windows),
        feature(proto::Feature::Blobs),
    ];
    if actions_available {
        features.push(feature(proto::Feature::Actions));
    }
    if notifications_available {
        features.push(feature(proto::Feature::Notifications));
    }
    if remote_files_available {
        features.push(feature(proto::Feature::RemoteFiles));
    }
    if coordinator.service().input_control.permission_state()
        != DomainInputPermissionState::Unsupported
    {
        features.push(feature(proto::Feature::InputReliable));
        if workspace_input_available {
            features.push(feature(proto::Feature::WorkspaceInput));
        }
    }
    features.sort_unstable_by_key(|feature| feature.feature);
    features
}

fn feature(feature: proto::Feature) -> proto::FeatureVersion {
    let version = if feature == proto::Feature::RemoteFiles {
        crate::remote_files::REMOTE_FILE_PROTOCOL_VERSION
    } else {
        1
    };
    proto::FeatureVersion {
        feature: feature as i32,
        min_version: version,
        max_version: version,
    }
}

fn negotiate_features(
    client: &[proto::FeatureVersion],
    server: &[proto::FeatureVersion],
) -> Result<HashMap<i32, u32>> {
    if client.len() > 64 {
        return Err(ProtocolError::Other("too many client features".into()));
    }
    let mut client_ranges = HashMap::new();
    for feature in client {
        if proto::Feature::try_from(feature.feature).is_err()
            || feature.feature == proto::Feature::Unspecified as i32
            || feature.min_version == 0
            || feature.min_version > feature.max_version
            || client_ranges
                .insert(feature.feature, (feature.min_version, feature.max_version))
                .is_some()
        {
            return Err(ProtocolError::Other("invalid client feature range".into()));
        }
    }
    let mut selected = HashMap::new();
    for feature in server {
        let Some((client_min, client_max)) = client_ranges.get(&feature.feature).copied() else {
            continue;
        };
        let minimum = client_min.max(feature.min_version);
        let maximum = client_max.min(feature.max_version);
        if minimum <= maximum {
            selected.insert(feature.feature, maximum);
        }
    }
    Ok(selected)
}

fn selected_feature_versions(features: &HashMap<i32, u32>) -> Vec<proto::FeatureVersion> {
    let mut selected = features
        .iter()
        .map(|(&feature, &version)| proto::FeatureVersion {
            feature,
            min_version: version,
            max_version: version,
        })
        .collect::<Vec<_>>();
    selected.sort_unstable_by_key(|feature| feature.feature);
    selected
}

fn protocol_limits() -> proto::ProtocolLimits {
    proto::ProtocolLimits {
        max_control_frame_bytes: arcrelay_wire::MAX_CONTROL_FRAME_SIZE as u32,
        max_reliable_input_frame_bytes: arcrelay_wire::MAX_RELIABLE_INPUT_FRAME_SIZE as u32,
        max_datagram_bytes: arcrelay_wire::MAX_DATAGRAM_SIZE as u32,
        max_concurrent_requests: MAX_CONCURRENT_REQUESTS as u32,
        heartbeat_interval_ms: 15_000,
        heartbeat_timeout_ms: HEARTBEAT_TIMEOUT.as_millis() as u32,
        max_blob_chunk_bytes: arcrelay_wire::MAX_BLOB_CHUNK_SIZE as u32,
        max_remote_file_message_bytes: crate::remote_files::MAX_REMOTE_FILE_MESSAGE_SIZE as u32,
        max_remote_file_content_bytes: crate::remote_files::MAX_REMOTE_FILE_CONTENT_SIZE,
        max_print_chunk_bytes: arcrelay_wire::MAX_PRINT_DOCUMENT_CHUNK_SIZE as u32,
        max_print_document_bytes: arcrelay_wire::MAX_PRINT_DOCUMENT_SIZE,
    }
}

#[allow(deprecated)]
fn validate_receive_limits(
    limits: Option<&proto::ProtocolLimits>,
    features: &[proto::FeatureVersion],
) -> Result<()> {
    let Some(limits) = limits else {
        return Err(ProtocolError::Other("client receive limits are required".into()));
    };

    let offers = |feature: proto::Feature| {
        features
            .iter()
            .any(|candidate| candidate.feature == feature as i32)
    };
    let invalid = |field: &str| {
        Err(ProtocolError::Other(format!(
            "invalid client receive limit: {field}"
        )))
    };

    if !(4096..=arcrelay_wire::MAX_CONTROL_FRAME_SIZE as u32)
        .contains(&limits.max_control_frame_bytes)
    {
        return invalid("max_control_frame_bytes");
    }
    if limits.max_reliable_input_frame_bytes
        > arcrelay_wire::MAX_RELIABLE_INPUT_FRAME_SIZE as u32
        || (offers(proto::Feature::InputReliable)
            && limits.max_reliable_input_frame_bytes == 0)
    {
        return invalid("max_reliable_input_frame_bytes");
    }
    if limits.max_datagram_bytes > arcrelay_wire::MAX_DATAGRAM_SIZE as u32
        || (offers(proto::Feature::InputDatagram) && limits.max_datagram_bytes == 0)
    {
        return invalid("max_datagram_bytes");
    }
    if !(1..=1024).contains(&limits.max_concurrent_requests) {
        return invalid("max_concurrent_requests");
    }
    if limits.max_blob_chunk_bytes > arcrelay_wire::MAX_BLOB_CHUNK_SIZE as u32
        || (offers(proto::Feature::Blobs) && limits.max_blob_chunk_bytes == 0)
    {
        return invalid("max_blob_chunk_bytes");
    }
    if !(1_000..=120_000).contains(&limits.heartbeat_interval_ms) {
        return invalid("heartbeat_interval_ms");
    }
    if limits.heartbeat_timeout_ms < limits.heartbeat_interval_ms.saturating_mul(2)
        || limits.heartbeat_timeout_ms > 300_000
    {
        return invalid("heartbeat_timeout_ms");
    }
    if limits.max_remote_file_message_bytes
        > crate::remote_files::MAX_REMOTE_FILE_MESSAGE_SIZE as u32
        || (offers(proto::Feature::RemoteFiles)
            && limits.max_remote_file_message_bytes < 4096)
    {
        return invalid("max_remote_file_message_bytes");
    }
    if limits.max_remote_file_content_bytes > crate::remote_files::MAX_REMOTE_FILE_CONTENT_SIZE
        || (offers(proto::Feature::RemoteFiles) && limits.max_remote_file_content_bytes == 0)
    {
        return invalid("max_remote_file_content_bytes");
    }
    if limits.max_print_chunk_bytes > arcrelay_wire::MAX_PRINT_DOCUMENT_CHUNK_SIZE as u32
        || (offers(proto::Feature::Printing) && limits.max_print_chunk_bytes == 0)
    {
        return invalid("max_print_chunk_bytes");
    }
    if limits.max_print_document_bytes > arcrelay_wire::MAX_PRINT_DOCUMENT_SIZE
        || (offers(proto::Feature::Printing) && limits.max_print_document_bytes == 0)
    {
        return invalid("max_print_document_bytes");
    }
    Ok(())
}

fn grant_to_proto(grant: &Grant) -> proto::GrantedCapability {
    proto::GrantedCapability {
        capability: grant.capability.token().to_string(),
        direction: match grant.direction {
            GrantDirection::Inbound => proto::CapabilityDirection::Inbound as i32,
            GrantDirection::Outbound => proto::CapabilityDirection::Outbound as i32,
        },
        constraints: match &grant.constraints {
            GrantConstraints::None => None,
            GrantConstraints::RemoteFileShares {
                share_ids,
                writable,
            } => Some(proto::CapabilityConstraints {
                kind: Some(proto::capability_constraints::Kind::RemoteFileShares(
                    proto::RemoteFileCapabilityConstraints {
                        share_ids: share_ids.clone(),
                        writable: *writable,
                    },
                )),
            }),
        },
        granted_at_ms: grant.granted_at_ms,
    }
}



fn ok_status() -> proto::Status {
    proto::Status {
        code: proto::ErrorCode::Ok as i32,
        message: String::new(),
        retryable: false,
        retry_after_ms: 0,
        recovery_action: proto::RecoveryAction::None as i32,
        error_id: String::new(),
    }
}

fn status(code: proto::ErrorCode, message: impl Into<String>) -> proto::Status {
    let recovery_action = match code {
        proto::ErrorCode::Busy
        | proto::ErrorCode::ResourceExhausted
        | proto::ErrorCode::DeadlineExceeded
        | proto::ErrorCode::Unavailable => proto::RecoveryAction::RetryWithBackoff,
        proto::ErrorCode::Conflict => proto::RecoveryAction::Refresh,
        proto::ErrorCode::InvalidArgument | proto::ErrorCode::FailedPrecondition => {
            proto::RecoveryAction::ChangeRequest
        }
        _ => proto::RecoveryAction::None,
    };
    let mut message = truncate_utf8(message.into(), MAX_STATUS_MESSAGE_BYTES);
    let error_id = if code == proto::ErrorCode::Internal {
        let error_id = format!("{:032x}", rand::random::<u128>());
        tracing::error!(
            event = "protocol.request.failed",
            error_id,
            error_code = "protocol.internal",
            error = %message,
            "protocol request failed internally"
        );
        message = "operation failed internally".into();
        error_id
    } else {
        String::new()
    };
    proto::Status {
        code: code as i32,
        message,
        retryable: matches!(
            recovery_action,
            proto::RecoveryAction::Retry | proto::RecoveryAction::RetryWithBackoff
        ),
        retry_after_ms: 0,
        recovery_action: recovery_action as i32,
        error_id,
    }
}

fn server_frame(body: proto::server_control_frame::Body) -> proto::ServerControlFrame {
    proto::ServerControlFrame { body: Some(body) }
}

fn response_error(
    request_id: u64,
    code: proto::ErrorCode,
    message: impl Into<String>,
) -> proto::ServerControlFrame {
    server_frame(proto::server_control_frame::Body::Response(
        proto::Response {
            request_id,
            status: Some(status(code, message)),
            body: None,
        },
    ))
}

#[cfg(test)]
mod feature_negotiation_tests {
    use super::*;

    #[test]
    fn selects_highest_overlapping_feature_version() {
        let client = [proto::FeatureVersion {
            feature: proto::Feature::Clipboard as i32,
            min_version: 1,
            max_version: 4,
        }];
        let server = [proto::FeatureVersion {
            feature: proto::Feature::Clipboard as i32,
            min_version: 2,
            max_version: 3,
        }];
        let selected = negotiate_features(&client, &server).unwrap();
        assert_eq!(selected.get(&(proto::Feature::Clipboard as i32)), Some(&3));
    }

    #[test]
    fn rejects_duplicate_client_feature_ranges() {
        let feature = proto::FeatureVersion {
            feature: proto::Feature::Clipboard as i32,
            min_version: 1,
            max_version: 1,
        };
        assert!(negotiate_features(&[feature, feature], &[]).is_err());
    }

    #[test]
    fn optional_feature_limits_may_be_zero_when_the_feature_is_not_offered() {
        let mut limits = protocol_limits();
        limits.max_datagram_bytes = 0;
        limits.max_print_chunk_bytes = 0;
        limits.max_print_document_bytes = 0;
        let features = [feature(proto::Feature::Clipboard)];

        validate_receive_limits(Some(&limits), &features).unwrap();
    }

    #[test]
    fn remote_files_advertise_the_typed_error_protocol_version() {
        let feature = feature(proto::Feature::RemoteFiles);
        assert_eq!(feature.min_version, 2);
        assert_eq!(feature.max_version, 2);
    }

    #[test]
    fn offered_features_require_a_usable_receive_limit() {
        let mut limits = protocol_limits();
        limits.max_blob_chunk_bytes = 0;
        let features = [feature(proto::Feature::Blobs)];

        let error = validate_receive_limits(Some(&limits), &features).unwrap_err();
        assert!(error.to_string().contains("max_blob_chunk_bytes"));
    }
}

async fn send_critical(
    sender: &mpsc::Sender<proto::ServerControlFrame>,
    frame: proto::ServerControlFrame,
) -> Result<()> {
    tokio::time::timeout(CRITICAL_SEND_TIMEOUT, sender.send(frame))
        .await
        .map_err(|_| ProtocolError::Other("control writer is backpressured".into()))?
        .map_err(|_| ProtocolError::ConnectionClosed)
}

async fn handle_request(
    request: proto::Request,
    coordinator: Arc<StateCoordinator>,
    action_provider: Option<Arc<dyn HostCapabilityProvider>>,
    features: HashSet<i32>,
    capabilities: HashSet<CapabilityId>,
    blob_store: BlobStore,
    clipboard_uploads: ClipboardUploadStore,
    command_cache: Arc<Mutex<CommandCache>>,
    command_limit: Arc<Semaphore>,
    device_public_key: Vec<u8>,
    device_id: String,
    device_name: String,
) -> proto::ServerControlFrame {
    let request_id = request.request_id;
    let idempotency_key = request.idempotency_key;
    match request.body {
        Some(proto::request::Body::Query(query)) => {
            if !idempotency_key.is_empty() {
                return response_error(
                    request_id,
                    proto::ErrorCode::InvalidArgument,
                    "queries must not include an idempotency key",
                );
            }
            if let Err(frame) = authorize_query(request_id, &query, &features, &capabilities) {
                return frame;
            }
            handle_query(
                request_id,
                query,
                coordinator,
                action_provider,
                blob_store,
                device_public_key.clone(),
                (capabilities.contains(&CapabilityId::ClipboardRead)
                    || capabilities.contains(&CapabilityId::ClipboardSync))
                    && features.contains(&(proto::Feature::Blobs as i32)),
            )
            .await
        }
        Some(proto::request::Body::Command(command)) => {
            if !(16..=64).contains(&idempotency_key.len()) {
                return response_error(
                    request_id,
                    proto::ErrorCode::InvalidArgument,
                    "commands require a 16-64 byte idempotency key",
                );
            }
            if let Err(frame) = authorize_command(request_id, &command, &features, &capabilities) {
                return frame;
            }
            let encoded_command = command.encode_to_vec();
            let cache_key = CommandCacheKey::new(&device_public_key, &idempotency_key);
            {
                let mut cache = command_cache.lock().await;
                cache.prune();
                match cached_command_frame(&cache, &cache_key, &encoded_command, request_id) {
                    Ok(Some(frame)) | Err(frame) => return frame,
                    Ok(None) => {}
                }
            }

            // Only the leader for an idempotency key consumes background
            // command capacity. Duplicate callers merely watch its result and
            // can time out or cancel without leaking a global permit.
            let mut result_rx = {
                let mut cache = command_cache.lock().await;
                cache.prune();
                match cached_command_frame(&cache, &cache_key, &encoded_command, request_id) {
                    Ok(Some(frame)) | Err(frame) => return frame,
                    Ok(None) => {}
                }
                if let Some(in_flight) = cache.in_flight.get(&cache_key) {
                    if in_flight.command != encoded_command {
                        return response_error(
                            request_id,
                            proto::ErrorCode::InvalidArgument,
                            "idempotency key was reused for a different command",
                        );
                    }
                    in_flight.result_tx.subscribe()
                } else {
                    let command_permit = match command_limit.try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) => {
                            return response_error(
                                request_id,
                                proto::ErrorCode::ResourceExhausted,
                                "too many background commands are active",
                            )
                        }
                    };
                    let (result_tx, result_rx) = watch::channel::<Option<proto::Response>>(None);
                    cache.in_flight.insert(
                        cache_key.clone(),
                        InFlightCommand {
                            command: encoded_command.clone(),
                            result_tx: result_tx.clone(),
                        },
                    );
                    let command_cache = command_cache.clone();
                    let cache_key = cache_key.clone();
                    let cached_command = encoded_command.clone();
                    tokio::spawn(async move {
                        let _command_permit = command_permit;
                        let frame = match tokio::spawn(handle_command(
                            request_id,
                            command,
                            coordinator,
                            action_provider,
                            clipboard_uploads,
                            device_id,
                            device_name,
                        ))
                        .await
                        {
                            Ok(frame) => frame,
                            Err(error) => response_error(
                                request_id,
                                proto::ErrorCode::Internal,
                                format!("command task failed: {error}"),
                            ),
                        };
                        let response = match frame.body {
                            Some(proto::server_control_frame::Body::Response(response)) => response,
                            _ => proto::Response {
                                request_id,
                                status: Some(status(
                                    proto::ErrorCode::Internal,
                                    "command handler returned an invalid frame",
                                )),
                                body: None,
                            },
                        };
                        {
                            let mut cache = command_cache.lock().await;
                            cache.in_flight.remove(&cache_key);
                            if response
                                .status
                                .as_ref()
                                .is_some_and(|status| !status.retryable)
                            {
                                cache.insert(cache_key, cached_command, response.clone());
                            }
                        }
                        let _ = result_tx.send(Some(response));
                    });
                    result_rx
                }
            };

            if result_rx.borrow().is_none() {
                let _ = result_rx.changed().await;
            }
            let result = result_rx.borrow().clone();
            match result {
                Some(response) => response_frame_for_request(response, request_id),
                None => response_error(
                    request_id,
                    proto::ErrorCode::Unavailable,
                    "the original command ended without a result",
                ),
            }
        }
        Some(proto::request::Body::GetBlob(request)) => {
            if !idempotency_key.is_empty() {
                return response_error(
                    request_id,
                    proto::ErrorCode::InvalidArgument,
                    "Blob requests must not include an idempotency key",
                );
            }
            if request.blob_id.len() != 32 {
                return response_error(
                    request_id,
                    proto::ErrorCode::InvalidArgument,
                    "Blob IDs must be 32-byte SHA-256 digests",
                );
            }
            if let Err(frame) = require_feature(request_id, &features, proto::Feature::Blobs) {
                return frame;
            }
            if let Err(frame) =
                require_any_capability(
                    request_id,
                    &capabilities,
                    &[CapabilityId::ClipboardRead, CapabilityId::ClipboardSync],
                )
            {
                return frame;
            }
            match blob_store.issue_ticket(&device_public_key, &request.blob_id) {
                Ok(ticket) => server_frame(proto::server_control_frame::Body::Response(
                    proto::Response {
                        request_id,
                        status: Some(ok_status()),
                        body: Some(proto::response::Body::BlobTicket(ticket)),
                    },
                )),
                Err(IssueTicketError::NotFound) => response_error(
                    request_id,
                    proto::ErrorCode::NotFound,
                    "blob is not available",
                ),
                Err(IssueTicketError::ResourceExhausted) => response_error(
                    request_id,
                    proto::ErrorCode::ResourceExhausted,
                    "too many Blob tickets are outstanding",
                ),
            }
        }
        None => response_error(
            request_id,
            proto::ErrorCode::InvalidArgument,
            "empty request",
        ),
    }
}

fn cached_command_frame(
    cache: &CommandCache,
    cache_key: &CommandCacheKey,
    encoded_command: &[u8],
    request_id: u64,
) -> std::result::Result<Option<proto::ServerControlFrame>, proto::ServerControlFrame> {
    let Some(cached) = cache.entries.get(cache_key) else {
        return Ok(None);
    };
    if cached.command != encoded_command {
        return Err(response_error(
            request_id,
            proto::ErrorCode::InvalidArgument,
            "idempotency key was reused for a different command",
        ));
    }
    Ok(Some(response_frame_for_request(
        cached.response.clone(),
        request_id,
    )))
}

fn response_frame_for_request(
    mut response: proto::Response,
    request_id: u64,
) -> proto::ServerControlFrame {
    response.request_id = request_id;
    server_frame(proto::server_control_frame::Body::Response(response))
}

fn authorize_query(
    request_id: u64,
    query: &proto::Query,
    features: &HashSet<i32>,
    capabilities: &HashSet<CapabilityId>,
) -> std::result::Result<(), proto::ServerControlFrame> {
    if let Err(message) = validate_query(query) {
        return Err(response_error(
            request_id,
            proto::ErrorCode::InvalidArgument,
            message,
        ));
    }
    let (feature, capability) = match query.body.as_ref() {
        Some(proto::query::Body::GetSystem(_)) => {
            (proto::Feature::System, CapabilityId::SystemRead)
        }
        Some(proto::query::Body::ListProcesses(_)) => (
            proto::Feature::Processes,
            CapabilityId::ProcessRead,
        ),
        Some(proto::query::Body::GetMedia(_)) => {
            (proto::Feature::Media, CapabilityId::MediaRead)
        }
        Some(proto::query::Body::GetClipboard(_)) => (
            proto::Feature::Clipboard,
            CapabilityId::ClipboardRead,
        ),
        Some(proto::query::Body::GetDevices(_)) => {
            (proto::Feature::System, CapabilityId::SystemRead)
        }
        Some(proto::query::Body::ListWindows(_)) => {
            (proto::Feature::Windows, CapabilityId::WindowRead)
        }
        Some(proto::query::Body::ListActions(_)) | Some(proto::query::Body::GetActionOutput(_)) => {
            (proto::Feature::Actions, CapabilityId::ActionRead)
        }
        Some(proto::query::Body::GetNotifications(_)) => (
            proto::Feature::Notifications,
            CapabilityId::NotificationRead,
        ),
        Some(proto::query::Body::GetClipboardSync(_)) => (
            proto::Feature::ClipboardSync,
            CapabilityId::ClipboardSync,
        ),
        None => {
            return Err(response_error(
                request_id,
                proto::ErrorCode::InvalidArgument,
                "empty query",
            ))
        }
    };
    require_feature(request_id, features, feature)?;
    require_capability(request_id, capabilities, capability)
}

fn validate_query(query: &proto::Query) -> std::result::Result<(), &'static str> {
    match query.body.as_ref() {
        Some(proto::query::Body::ListProcesses(request)) => {
            if proto::ProcessSortBy::try_from(request.sort_by)
                .map_or(true, |sort| sort == proto::ProcessSortBy::Unspecified)
            {
                return Err("invalid process sort order");
            }
        }
        Some(proto::query::Body::GetClipboard(request))
            if request.history_limit as usize > MAX_CLIPBOARD_ITEMS =>
        {
            return Err("clipboard history limit must not exceed 200");
        }
        Some(proto::query::Body::GetClipboard(request)) => {
            if request.search.len() > 512 || request.search.chars().any(char::is_control) {
                return Err("invalid clipboard search query");
            }
            if request.cursor_timestamp_ms.is_some() != request.cursor_id.is_some()
                || request.cursor_id == Some(0)
            {
                return Err("clipboard cursor is incomplete");
            }
            if request.kinds.iter().any(|kind| {
                proto::ClipboardContentKind::try_from(*kind).map_or(true, |kind| {
                    kind == proto::ClipboardContentKind::Unspecified
                })
            }) {
                return Err("invalid clipboard content kind");
            }
        }
        Some(proto::query::Body::GetActionOutput(request)) => {
            if !valid_control_identifier(&request.action_id) {
                return Err("invalid quick action id");
            }
        }
        Some(proto::query::Body::GetNotifications(request)) if request.limit > 100 => {
            return Err("notification limit must not exceed 100");
        }
        Some(proto::query::Body::GetClipboardSync(request)) if request.limit > 200 => {
            return Err("clipboard sync page limit must not exceed 200");
        }
        Some(
            proto::query::Body::GetSystem(_)
            | proto::query::Body::GetMedia(_)
            | proto::query::Body::GetDevices(_)
            | proto::query::Body::ListWindows(_)
            | proto::query::Body::ListActions(_)
            | proto::query::Body::GetNotifications(_)
            | proto::query::Body::GetClipboardSync(_),
        ) => {}
        None => return Err("empty query"),
    }
    Ok(())
}

fn authorize_command(
    request_id: u64,
    command: &proto::Command,
    features: &HashSet<i32>,
    capabilities: &HashSet<CapabilityId>,
) -> std::result::Result<(), proto::ServerControlFrame> {
    if let Err(message) = validate_command(command) {
        if matches!(command.action, Some(proto::command::Action::ApplyClipboardSyncRecord(_))) {
            tracing::warn!(
                event = "clipboard.sync.record_validation_failed",
                request_id,
                reason = message,
                "rejected invalid clipboard sync record"
            );
        }
        return Err(response_error(
            request_id,
            proto::ErrorCode::InvalidArgument,
            message,
        ));
    }
    if matches!(
        command.action.as_ref(),
        Some(proto::command::Action::PasteClipboardRecord(_))
    ) {
        require_feature(request_id, features, proto::Feature::Clipboard)?;
        require_capability(request_id, capabilities, CapabilityId::ClipboardRead)?;
        require_capability(request_id, capabilities, CapabilityId::ClipboardWrite)?;
        return require_capability(request_id, capabilities, CapabilityId::RemoteInputInject);
    }
    let (feature, capability) = match command.action.as_ref() {
        Some(proto::command::Action::PlaybackAction(_))
        | Some(proto::command::Action::SetSystemVolume(_))
        | Some(proto::command::Action::SetAppVolume(_))
        | Some(proto::command::Action::SetMicrophone(_))
        | Some(proto::command::Action::SetDnd(_)) => {
            (proto::Feature::Media, CapabilityId::MediaControl)
        }
        Some(proto::command::Action::KillProcess(_)) => (
            proto::Feature::Processes,
            CapabilityId::ProcessManage,
        ),
        Some(proto::command::Action::SetClipboard(_))
        | Some(proto::command::Action::ClearClipboardHistory(_))
        | Some(proto::command::Action::PasteClipboardRecord(_))
        | Some(proto::command::Action::DeleteClipboardRecord(_))
        | Some(proto::command::Action::SetClipboardFavorite(_))
        | Some(proto::command::Action::CreateClipboardLabel(_))
        | Some(proto::command::Action::UpdateClipboardLabel(_))
        | Some(proto::command::Action::DeleteClipboardLabel(_))
        | Some(proto::command::Action::SetClipboardLabels(_))
        | Some(proto::command::Action::UpdateClipboardPolicy(_)) => (
            proto::Feature::Clipboard,
            CapabilityId::ClipboardWrite,
        ),
        Some(proto::command::Action::FocusWindow(_))
        | Some(proto::command::Action::SwitchSpace(_)) => (
            proto::Feature::Windows,
            CapabilityId::WindowControl,
        ),
        Some(proto::command::Action::ExecuteQuickAction(_))
        | Some(proto::command::Action::RunAutomation(_))
        | Some(proto::command::Action::SetAutomationEnabled(_))
        | Some(proto::command::Action::CancelAutomation(_))
        => (
            proto::Feature::Actions,
            CapabilityId::ActionExecute,
        ),
        Some(proto::command::Action::MarkNotificationRead(_)) => (
            proto::Feature::Notifications,
            CapabilityId::NotificationAcknowledge,
        ),
        Some(proto::command::Action::ApplyClipboardSyncRecord(_)) => (
            proto::Feature::ClipboardSync,
            CapabilityId::ClipboardSync,
        ),
        None => {
            return Err(response_error(
                request_id,
                proto::ErrorCode::InvalidArgument,
                "empty command",
            ))
        }
    };
    require_feature(request_id, features, feature)?;
    require_capability(request_id, capabilities, capability)
}

fn validate_command(command: &proto::Command) -> std::result::Result<(), &'static str> {
    match command.action.as_ref() {
        Some(proto::command::Action::PlaybackAction(command)) => {
            if proto::PlaybackAction::try_from(command.action)
                .map_or(true, |action| action == proto::PlaybackAction::Unspecified)
            {
                return Err("invalid playback action");
            }
        }
        Some(proto::command::Action::SetSystemVolume(command)) => {
            if command.volume > 100 {
                return Err("system volume must be between 0 and 100");
            }
        }
        Some(proto::command::Action::SetAppVolume(command)) => {
            if !valid_control_identifier(&command.app_name) {
                return Err("invalid application name");
            }
            if command.volume > 100 {
                return Err("application volume must be between 0 and 100");
            }
        }
        Some(proto::command::Action::KillProcess(command)) if command.pid == 0 => {
            return Err("process id must be non-zero");
        }
        Some(proto::command::Action::SetClipboard(command)) => {
            if command.content.len() > MAX_CLIPBOARD_TEXT_BYTES {
                return Err("clipboard content is too large");
            }
        }
        Some(proto::command::Action::PasteClipboardRecord(command)) if command.record_id == 0 => {
            return Err("clipboard record id must be non-zero");
        }
        Some(proto::command::Action::DeleteClipboardRecord(command)) if command.record_id == 0 => {
            return Err("clipboard record id must be non-zero");
        }
        Some(proto::command::Action::SetClipboardFavorite(command)) if command.record_id == 0 => {
            return Err("clipboard record id must be non-zero");
        }
        Some(proto::command::Action::CreateClipboardLabel(command))
            if command.name.trim().is_empty() =>
        {
            return Err("clipboard label name is required");
        }
        Some(proto::command::Action::UpdateClipboardLabel(command))
            if command.label_id.is_empty() || command.name.trim().is_empty() =>
        {
            return Err("clipboard label id and name are required");
        }
        Some(proto::command::Action::DeleteClipboardLabel(command))
            if command.label_id.is_empty() =>
        {
            return Err("clipboard label id is required");
        }
        Some(proto::command::Action::SetClipboardLabels(command)) if command.record_id == 0 => {
            return Err("clipboard record id must be non-zero");
        }
        Some(proto::command::Action::UpdateClipboardPolicy(command)) => {
            let Some(policy) = command.policy.as_ref() else {
                return Err("clipboard policy is required");
            };
            if policy.max_items == 0
                || policy.max_items > 5000
                || policy.max_bytes < 1024 * 1024
                || policy.max_bytes > 1024 * 1024 * 1024
                || policy.retention_days > 3650
            {
                return Err("invalid clipboard retention policy");
            }
        }
        Some(proto::command::Action::FocusWindow(command)) if command.window_id == 0 => {
            return Err("window id must be non-zero");
        }
        Some(proto::command::Action::SwitchSpace(command)) if command.space_id == 0 => {
            return Err("space id must be non-zero");
        }
        Some(proto::command::Action::ExecuteQuickAction(command)) => {
            if !valid_control_identifier(&command.action_id) {
                return Err("invalid quick action id");
            }
        }
        Some(proto::command::Action::RunAutomation(command)) => {
            if !valid_control_identifier(&command.automation_id) { return Err("invalid automation id"); }
        }
        Some(proto::command::Action::SetAutomationEnabled(command)) => {
            if !valid_control_identifier(&command.automation_id) { return Err("invalid automation id"); }
        }
        Some(proto::command::Action::CancelAutomation(command)) => {
            if !valid_control_identifier(&command.activity_id) { return Err("invalid automation activity id"); }
        }
        Some(proto::command::Action::MarkNotificationRead(command)) => {
            if !valid_control_identifier(&command.notification_id) {
                return Err("invalid notification id");
            }
        }
        Some(proto::command::Action::ApplyClipboardSyncRecord(command)) => {
            let Some(record) = command.record.as_ref() else {
                return Err("clipboard sync record is required");
            };
            validate_clipboard_sync_record(record)?;
        }
        Some(
            proto::command::Action::SetMicrophone(_)
            | proto::command::Action::SetDnd(_)
            | proto::command::Action::ClearClipboardHistory(_)
            | proto::command::Action::PasteClipboardRecord(_)
            | proto::command::Action::DeleteClipboardRecord(_)
            | proto::command::Action::SetClipboardFavorite(_)
            | proto::command::Action::CreateClipboardLabel(_)
            | proto::command::Action::UpdateClipboardLabel(_)
            | proto::command::Action::DeleteClipboardLabel(_)
            | proto::command::Action::SetClipboardLabels(_)
            | proto::command::Action::KillProcess(_)
            | proto::command::Action::FocusWindow(_)
            | proto::command::Action::SwitchSpace(_),
        ) => {}
        None => return Err("empty command"),
    }
    Ok(())
}

fn valid_control_identifier(value: &str) -> bool {
    let value = value.trim();
    !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
}

fn validate_clipboard_sync_record(
    record: &proto::ClipboardSyncRecord,
) -> std::result::Result<(), &'static str> {
    use proto::clipboard_sync_record::Payload;
    if record.sync_id.len() != 64 || !record.sync_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("invalid clipboard sync identifier");
    }
    if record.favorite_revision == 0 || record.favorite_revision > i64::MAX as u64 {
        return Err("invalid clipboard sync favorite revision");
    }
    if !record.labels.is_empty() || !record.label_memberships.is_empty() {
        return Err("clipboard sync labels are not supported");
    }
    if record.preview.len() > 2 * 1024 {
        return Err("clipboard sync preview exceeds limit");
    }
    if record.revision == 0 || record.revision > i64::MAX as u64 {
        return Err("invalid clipboard sync revision");
    }
    if record.captured_at_ms <= 0 || record.captured_at_ms > now_ms().saturating_add(5 * 60 * 1000) {
        return Err("invalid clipboard sync capture timestamp");
    }
    let kind = proto::ClipboardContentKind::try_from(record.kind)
        .unwrap_or(proto::ClipboardContentKind::Unspecified);
    let change_kind = proto::ClipboardSyncChangeKind::try_from(record.change_kind)
        .unwrap_or(proto::ClipboardSyncChangeKind::Unspecified);
    if change_kind == proto::ClipboardSyncChangeKind::Unspecified
        || !matches!(
            kind,
            proto::ClipboardContentKind::Text
                | proto::ClipboardContentKind::Html
                | proto::ClipboardContentKind::Image
                | proto::ClipboardContentKind::Files
        )
    {
        return Err("invalid clipboard sync kind");
    }
    if record.deleted {
        if record.payload.is_some() {
            return Err("deleted clipboard sync records cannot include payloads");
        }
        return Ok(());
    }
    match kind {
        proto::ClipboardContentKind::Text => {
            if !matches!(record.payload.as_ref(), Some(Payload::Text(text)) if !text.is_empty() && text.len() <= 768 * 1024) {
                return Err("invalid synchronized text payload");
            }
        }
        proto::ClipboardContentKind::Html => {
            let Some(Payload::RichText(rich_text)) = record.payload.as_ref() else {
                return Err("synchronized rich text is required");
            };
            if rich_text.plain_text.len() > 1024 * 1024
                || !valid_clipboard_blob_ref(rich_text.html.as_ref(), "text/html; charset=utf-8", 16 * 1024 * 1024)
                || rich_text.rtf.as_ref().is_some_and(|rtf| !valid_clipboard_blob_ref(Some(rtf), "text/rtf", 16 * 1024 * 1024))
            {
                return Err("invalid synchronized rich-text payload");
            }
        }
        proto::ClipboardContentKind::Image => {
            let Some(Payload::Image(image)) = record.payload.as_ref() else {
                return Err("synchronized image reference is required");
            };
            if !valid_clipboard_blob_ref(Some(image), "image/png", 20 * 1024 * 1024) {
                return Err("invalid synchronized image reference");
            }
        }
        proto::ClipboardContentKind::Files => {
            if record.payload.is_some() {
                return Err("synchronized file metadata cannot include payloads");
            }
        }
        _ => return Err("invalid clipboard sync kind"),
    }
    Ok(())
}

fn valid_clipboard_blob_ref(reference: Option<&proto::BlobRef>, media_type: &str, maximum: u64) -> bool {
    reference.is_some_and(|reference| {
        reference.blob_id.len() == 32
            && reference.sha256.len() == 32
            && reference.blob_id == reference.sha256
            && reference.size > 0
            && reference.size <= maximum
            && reference.media_type == media_type
    })
}

fn require_feature(
    request_id: u64,
    features: &HashSet<i32>,
    feature: proto::Feature,
) -> std::result::Result<(), proto::ServerControlFrame> {
    if features.contains(&(feature as i32)) {
        Ok(())
    } else {
        Err(response_error(
            request_id,
            proto::ErrorCode::Unsupported,
            format!("feature {:?} was not negotiated", feature),
        ))
    }
}

fn require_capability(
    request_id: u64,
    capabilities: &HashSet<CapabilityId>,
    capability: CapabilityId,
) -> std::result::Result<(), proto::ServerControlFrame> {
    if capabilities.contains(&capability) {
        Ok(())
    } else {
        Err(response_error(
            request_id,
            proto::ErrorCode::PermissionDenied,
            format!("capability {} was not granted", capability.token()),
        ))
    }
}

fn require_any_capability(
    request_id: u64,
    capabilities: &HashSet<CapabilityId>,
    required: &[CapabilityId],
) -> std::result::Result<(), proto::ServerControlFrame> {
    if required.iter().any(|capability| capabilities.contains(capability)) {
        Ok(())
    } else {
        Err(response_error(
            request_id,
            proto::ErrorCode::PermissionDenied,
            "required capability was not granted",
        ))
    }
}
