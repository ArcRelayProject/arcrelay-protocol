async fn begin_input_session(
    request: proto::BeginInputSessionRequest,
    features: &HashSet<i32>,
    capabilities: &HashSet<CapabilityId>,
    coordinator: &Arc<StateCoordinator>,
    input_sessions: &InputSessionManager,
    active_input: &Arc<Mutex<Option<InputLease>>>,
    input_apply_gate: &Arc<Mutex<()>>,
    event_tx: &mpsc::Sender<ServerEvent>,
    device_id: &str,
    device_name: &str,
    workspace_input_router: Option<&Arc<dyn WorkspaceInputRouter>>,
) -> proto::ServerControlFrame {
    let permission = coordinator.service().input_control.permission_state();
    if !features.contains(&(proto::Feature::InputReliable as i32)) {
        return server_frame(proto::server_control_frame::Body::InputSessionResult(
            input_session_result(
                request.request_id,
                false,
                InputLease {
                    session_id: 0,
                    epoch: 0,
                },
                permission,
                "remote input features were not negotiated",
                proto::ErrorCode::Unsupported,
            ),
        ));
    }
    if !capabilities.contains(&CapabilityId::RemoteInputInject) {
        return server_frame(proto::server_control_frame::Body::InputSessionResult(
            input_session_result(
                request.request_id,
                false,
                InputLease {
                    session_id: 0,
                    epoch: 0,
                },
                permission,
                "remote-input.inject capability was not granted",
                proto::ErrorCode::PermissionDenied,
            ),
        ));
    }
    let workspace_routed = request.workspace_routing;
    if workspace_routed
        && (!features.contains(&(proto::Feature::WorkspaceInput as i32))
            || workspace_input_router.is_none_or(|router| !router.available()))
    {
        return server_frame(proto::server_control_frame::Body::InputSessionResult(
            input_session_result(
                request.request_id,
                false,
                InputLease {
                    session_id: 0,
                    epoch: 0,
                },
                permission,
                "workspace input routing is unavailable",
                proto::ErrorCode::Unsupported,
            ),
        ));
    }
    if permission != DomainInputPermissionState::Granted {
        let _ = event_tx
            .send(ServerEvent::InputPermissionRequired {
                device_name: device_name.to_string(),
            })
            .await;
        return server_frame(proto::server_control_frame::Body::InputSessionResult(
            input_session_result(
                request.request_id,
                false,
                InputLease {
                    session_id: 0,
                    epoch: 0,
                },
                permission,
                "desktop input permission is not granted",
                proto::ErrorCode::PermissionDenied,
            ),
        ));
    }
    let _apply_guard = input_apply_gate.lock().await;
    let previous = *active_input.lock().await;
    if let Some(previous) = previous {
        end_input_lease_locked(
            previous,
            active_input,
            input_sessions,
            coordinator,
            event_tx,
            device_id,
            device_name,
            workspace_input_router,
        )
        .await;
    }
    match input_sessions
        .acquire_with_routing(device_id, device_name, workspace_routed)
        .await
    {
        Ok(lease) => {
            let route = if workspace_routed {
                match workspace_input_router
                    .expect("workspace router was validated")
                    .begin(device_id)
                    .await
                {
                    Ok(route) => Some(route),
                    Err(error) => {
                        let _ = input_sessions.release(lease).await;
                        return server_frame(proto::server_control_frame::Body::InputSessionResult(
                            input_session_result(
                                request.request_id,
                                false,
                                InputLease { session_id: 0, epoch: 0 },
                                permission,
                                &error.message,
                                error.code.protocol_code(),
                            ),
                        ));
                    }
                }
            } else {
                None
            };
            *active_input.lock().await = Some(lease);
            let _ = event_tx
                .send(ServerEvent::InputSessionStarted {
                    session_id: lease_label(lease),
                    device_id: device_id.to_string(),
                    device_name: device_name.to_string(),
                })
                .await;
            server_frame(proto::server_control_frame::Body::InputSessionResult(
                proto::InputSessionResult {
                    supports_system_gestures: route.as_ref().map_or_else(
                        || coordinator.service().input_control.supports_system_gestures(),
                        |route| route.supports_system_gestures,
                    ),
                    workspace_routing_active: workspace_routed,
                    gateway_device_id: route.as_ref().map_or_else(String::new, |route| route.controller_device_id.clone()),
                    logical_target_device_id: route.as_ref().map_or_else(String::new, |route| route.logical_target_device_id.clone()),
                    target_display_id: route.as_ref().map_or_else(String::new, |route| route.target_display_id.clone()),
                    control_epoch: route.as_ref().map_or(0, |route| route.control_epoch),
                    ..input_session_result(
                    request.request_id,
                    true,
                    lease,
                    permission,
                    "",
                    proto::ErrorCode::Ok,
                    )
                },
            ))
        }
        Err(error) => server_frame(proto::server_control_frame::Body::InputSessionResult(
            input_session_result(
                request.request_id,
                false,
                InputLease {
                    session_id: 0,
                    epoch: 0,
                },
                permission,
                &error,
                proto::ErrorCode::Busy,
            ),
        )),
    }
}

#[allow(deprecated)]
fn input_session_result(
    request_id: u64,
    accepted: bool,
    lease: InputLease,
    permission: DomainInputPermissionState,
    message: &str,
    failure_code: proto::ErrorCode,
) -> proto::InputSessionResult {
    proto::InputSessionResult {
        request_id,
        status: Some(if accepted {
            ok_status()
        } else {
            status(failure_code, message)
        }),
        session_id: lease.session_id,
        epoch: lease.epoch,
        permission_state: match permission {
            DomainInputPermissionState::Granted => proto::InputPermissionState::Granted as i32,
            DomainInputPermissionState::Denied => proto::InputPermissionState::Denied as i32,
            DomainInputPermissionState::Unsupported => {
                proto::InputPermissionState::Unsupported as i32
            }
        },
        lease_timeout_ms: INPUT_LEASE_TIMEOUT.as_millis() as u32,
        max_datagram_rate: 0,
        max_reliable_events_per_frame: MAX_RELIABLE_EVENTS_PER_FRAME as u32,
        max_reliable_frame_rate: MAX_RELIABLE_FRAMES_PER_SECOND as u32,
        supports_system_gestures: false,
        workspace_routing_active: false,
        gateway_device_id: String::new(),
        logical_target_device_id: String::new(),
        target_display_id: String::new(),
        control_epoch: 0,
    }
}

async fn handle_uni_stream(
    mut stream: quinn::RecvStream,
    coordinator: Arc<StateCoordinator>,
    input_sessions: InputSessionManager,
    critical_tx: mpsc::Sender<proto::ServerControlFrame>,
    active_input: Arc<Mutex<Option<InputLease>>>,
    input_apply_gate: Arc<Mutex<()>>,
    reliable_input_stream_open: Arc<AtomicBool>,
    event_tx: mpsc::Sender<ServerEvent>,
    device_id: String,
    device_name: String,
    workspace_input_router: Option<Arc<dyn WorkspaceInputRouter>>,
) -> Result<()> {
    let kind = tokio::time::timeout(STREAM_SETUP_TIMEOUT, read_stream_kind(&mut stream))
        .await
        .map_err(|_| ProtocolError::Other("input stream preface timed out".into()))??;
    if kind != STREAM_KIND_RELIABLE_INPUT {
        return Err(ProtocolError::Other(
            "unsupported client stream kind".into(),
        ));
    }
    if reliable_input_stream_open.swap(true, Ordering::AcqRel) {
        return Err(ProtocolError::Other(
            "a reliable input stream is already open".into(),
        ));
    }
    let _stream_guard = ReliableInputStreamGuard(reliable_input_stream_open);
    let mut metrics = InputMetricsAccumulator::new();
    loop {
        let frame = match recv_reliable_input(&mut stream).await {
            Ok(frame) => frame,
            Err(error) => {
                let clean_end = matches!(
                    &error,
                    ProtocolError::Io(io_error)
                        if io_error.kind() == std::io::ErrorKind::UnexpectedEof
                );
                let _apply_guard = input_apply_gate.lock().await;
                let lease = { *active_input.lock().await };
                if let Some(lease) = lease {
                    end_input_lease_locked(
                        lease,
                        &active_input,
                        &input_sessions,
                        &coordinator,
                        &event_tx,
                        &device_id,
                        &device_name,
                        workspace_input_router.as_ref(),
                    )
                    .await;
                }
                return if clean_end { Ok(()) } else { Err(error) };
            }
        };
        let lease = InputLease {
            session_id: frame.session_id,
            epoch: frame.epoch,
        };
        let is_motion_frame = frame.motion.is_some();
        if is_motion_frame {
            metrics.observe_motion(Instant::now(), frame.elapsed_us);
        }
        if frame.events.len() > MAX_RELIABLE_EVENTS_PER_FRAME {
            let _apply_guard = input_apply_gate.lock().await;
            revoke_input_with_feedback(
                lease,
                "invalid reliable input event count",
                &active_input,
                &input_sessions,
                &coordinator,
                &critical_tx,
                &event_tx,
                &device_id,
                &device_name,
                workspace_input_router.as_ref(),
            )
            .await;
            continue;
        }
        let events = match convert_reliable_events(&frame.events) {
            Ok(events) => events,
            Err(error) => {
                let _apply_guard = input_apply_gate.lock().await;
                revoke_input_with_feedback(
                    lease,
                    &error,
                    &active_input,
                    &input_sessions,
                    &coordinator,
                    &critical_tx,
                    &event_tx,
                    &device_id,
                    &device_name,
                    workspace_input_router.as_ref(),
                )
                .await;
                continue;
            }
        };
        if frame.motion.is_some() && !events.is_empty() {
            let _apply_guard = input_apply_gate.lock().await;
            revoke_input_with_feedback(
                lease,
                "ordered input frame mixes motion and discrete events",
                &active_input,
                &input_sessions,
                &coordinator,
                &critical_tx,
                &event_tx,
                &device_id,
                &device_name,
                workspace_input_router.as_ref(),
            )
            .await;
            continue;
        }
        let apply_guard = input_apply_gate.lock().await;
        if let Err(error) = coordinator.service().input_control.validate_events(&events) {
            revoke_input_with_feedback(
                lease,
                &error.to_string(),
                &active_input,
                &input_sessions,
                &coordinator,
                &critical_tx,
                &event_tx,
                &device_id,
                &device_name,
                workspace_input_router.as_ref(),
            )
            .await;
            continue;
        }
        let sequences = match input_sessions
            .accept_reliable(lease, &device_id, frame.sequence, frame.elapsed_us, 0)
            .await
        {
            Ok(sequences) => sequences,
            Err(error) => {
                revoke_input_with_feedback(
                    lease,
                    &error,
                    &active_input,
                    &input_sessions,
                    &coordinator,
                    &critical_tx,
                    &event_tx,
                    &device_id,
                    &device_name,
                    workspace_input_router.as_ref(),
                )
                .await;
                continue;
            }
        };
        let mut input_events = match input_sessions
            .normalize_reliable_events(lease, &device_id, events)
            .await
        {
            Ok(events) => events,
            Err(error) => {
                revoke_input_with_feedback(
                    lease,
                    &error,
                    &active_input,
                    &input_sessions,
                    &coordinator,
                    &critical_tx,
                    &event_tx,
                    &device_id,
                    &device_name,
                    workspace_input_router.as_ref(),
                )
                .await;
                continue;
            }
        };
        let feedback = if let Some(motion) = frame.motion.as_ref() {
            let delta = match input_sessions
                .accept_ordered_motion(
                    lease,
                    &device_id,
                    motion.pointer_total_x_256,
                    motion.pointer_total_y_256,
                    motion.scroll_total_x_256,
                    motion.scroll_total_y_256,
                    motion.precise_scroll,
                )
                .await
            {
                Ok(delta) => delta,
                Err(error) => {
                    revoke_input_with_feedback(
                        lease,
                        &error,
                        &active_input,
                        &input_sessions,
                        &coordinator,
                        &critical_tx,
                        &event_tx,
                        &device_id,
                        &device_name,
                        workspace_input_router.as_ref(),
                    )
                    .await;
                    continue;
                }
            };
            if delta.pointer_x_256 != 0 || delta.pointer_y_256 != 0 {
                input_events.push(DomainInputEvent::PointerMove {
                    delta_x: delta.pointer_x_256 as f32 / 256.0,
                    delta_y: delta.pointer_y_256 as f32 / 256.0,
                });
            }
            if delta.scroll_x_256 != 0 || delta.scroll_y_256 != 0 {
                input_events.push(DomainInputEvent::Scroll {
                    delta_x: delta.scroll_x_256 as f32 / 256.0,
                    delta_y: delta.scroll_y_256 as f32 / 256.0,
                    precise: delta.precise_scroll,
                });
            }
            delta.feedback
        } else {
            sequences
        };
        if !input_events.is_empty() {
            let apply_started = Instant::now();
            let apply_result = if input_sessions
                .is_workspace_routed(lease, &device_id)
                .await
            {
                match workspace_input_router.as_ref() {
                    Some(router) => router
                        .apply(&device_id, &input_events)
                        .await
                        .map(|_| ()),
                    None => Err(HostCapabilityError::new(
                        HostCapabilityErrorCode::Unavailable,
                        "workspace input router became unavailable",
                    )),
                }
            } else {
                coordinator
                    .service()
                    .input_control
                    .apply_events(&input_events)
                    .await
                    .map_err(|error| {
                        HostCapabilityError::new(
                            HostCapabilityErrorCode::Internal,
                            error.to_string(),
                        )
                    })
            };
            if let Err(error) = apply_result {
                revoke_input_with_feedback(
                    lease,
                    &error.to_string(),
                    &active_input,
                    &input_sessions,
                    &coordinator,
                    &critical_tx,
                    &event_tx,
                    &device_id,
                    &device_name,
                    workspace_input_router.as_ref(),
                )
                .await;
                continue;
            }
            if is_motion_frame {
                metrics.observe_quartz(apply_started.elapsed());
            }
        }
        drop(apply_guard);
        if let Some(event) = metrics.take(lease_label(lease)) {
            let _ = event_tx.try_send(event);
        }
        if frame.motion.is_none() || frame.sequence % 15 == 0 {
            let _ = send_critical(
                &critical_tx,
                input_feedback_frame(lease, feedback, ok_status()),
            )
            .await;
        }
    }
}

async fn handle_auxiliary_stream(
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
    blob_store: BlobStore,
    clipboard_uploads: ClipboardUploadStore,
    blob_owner: Vec<u8>,
    clipboard_upload_allowed: bool,
    replica_service: Option<Arc<arcrelay_core::application::clipboard_service::ClipboardApplicationService>>,
    replica_limit: Arc<Semaphore>,
    remote_file_provider: Option<Arc<dyn RemoteFileProvider>>,
    remote_file_access: RemoteFileAccess,
) -> Result<()> {
    let kind = tokio::time::timeout(STREAM_SETUP_TIMEOUT, read_stream_kind(&mut recv))
        .await
        .map_err(|_| ProtocolError::Other("auxiliary stream preface timed out".into()))??;
    match kind {
        arcrelay_wire::STREAM_KIND_CLIPBOARD_REPLICA => {
            let service = replica_service.ok_or_else(|| ProtocolError::Other("clipboard replication is not authorized or negotiated".into()))?;
            let _permit = replica_limit.try_acquire_owned().map_err(|_| ProtocolError::Other("too many clipboard replication streams".into()))?;
            let result = crate::clipboard_replication::serve_stream(&mut send, &mut recv, &service).await.map_err(ProtocolError::Other);
            let _ = send.finish();
            result
        }
        STREAM_KIND_BLOB_DOWNLOAD => tokio::time::timeout(
            BLOB_TRANSFER_TIMEOUT,
            handle_blob_stream(&mut send, &mut recv, blob_store, blob_owner),
        )
        .await
        .map_err(|_| ProtocolError::Other("blob transfer timed out".into()))?,
        STREAM_KIND_CLIPBOARD_BLOB_UPLOAD if clipboard_upload_allowed => tokio::time::timeout(
            BLOB_TRANSFER_TIMEOUT,
            handle_clipboard_blob_upload(&mut send, &mut recv, clipboard_uploads),
        )
        .await
        .map_err(|_| ProtocolError::Other("clipboard blob upload timed out".into()))?,
        crate::message::STREAM_KIND_REMOTE_FILES => {
            let result = tokio::time::timeout(
                BLOB_TRANSFER_TIMEOUT,
                handle_remote_file_stream(
                    &mut send,
                    &mut recv,
                    remote_file_provider,
                    remote_file_access,
                ),
            )
            .await
            .map_err(|_| ProtocolError::Other("remote file operation timed out".into()))?;
            let _ = send.finish();
            result
        }
        _ => Err(ProtocolError::Other(
            "unsupported bidirectional stream kind".into(),
        )),
    }
}

pub async fn handle_remote_file_stream<W, R>(
    send: &mut W,
    recv: &mut R,
    provider: Option<Arc<dyn RemoteFileProvider>>,
    access: RemoteFileAccess,
) -> Result<()>
where
    W: AsyncWrite + Unpin,
    R: AsyncRead + Unpin,
{
    let request: RemoteFileRequest = read_remote_message(recv).await?;
    let Some(provider) = provider else {
        write_remote_message(
            send,
            &RemoteFileResponse::failure(
                RemoteFileErrorCode::Unavailable,
                "remote files are unavailable",
            ),
        )
            .await?;
        return Ok(());
    };

    let denied = |message: &str| {
        RemoteFileResponse::failure(RemoteFileErrorCode::PermissionDenied, message)
    };

    // Reuse the bounded upload stream, but commit system-folder saves through
    // the domain service so revision checks and publication share one lock.
    let (request, expected_revision) = match request {
        RemoteFileRequest::ConditionalUpload { share_id, relative_path, name, size, expected_revision } => (
            RemoteFileRequest::Upload {
                share_id, relative_path, name, size,
                overwrite: !expected_revision.is_empty(), expected_modified_at_ms: None,
            },
            Some(expected_revision),
        ),
        request => (request, None),
    };

    match request {
        RemoteFileRequest::ConditionalUpload { .. } => unreachable!("normalized above"),
        RemoteFileRequest::Stat { share_id, relative_path } => {
            let response = if !access.can_read(&share_id) {
                denied("read access to this share is not permitted")
            } else {
                match provider.stat(&share_id, &relative_path).await {
                    Ok((entry, revision)) => RemoteFileResponse { entry: Some(entry), revision, ..RemoteFileResponse::success() },
                    Err(error) => RemoteFileResponse::from_error(error),
                }
            };
            write_remote_message(send, &response).await?;
        }
        RemoteFileRequest::ConditionalDelete { share_id, relative_path, expected_revision, recursive } => {
            let response = if !access.can_write(&share_id) {
                denied("write access to this share is not permitted")
            } else {
                match provider.conditional_delete(&share_id, &relative_path, &expected_revision, recursive).await {
                    Ok(()) => RemoteFileResponse::success(),
                    Err(error) => RemoteFileResponse::from_error(error),
                }
            };
            write_remote_message(send, &response).await?;
        }
        RemoteFileRequest::Move { share_id, relative_path, destination_path, overwrite, expected_revision } => {
            let response = if !access.can_write(&share_id) {
                denied("write access to this share is not permitted")
            } else {
                match provider.move_entry(&share_id, &relative_path, &destination_path, overwrite, &expected_revision).await {
                    Ok(entry) => RemoteFileResponse { entry: Some(entry), ..RemoteFileResponse::success() },
                    Err(error) => RemoteFileResponse::from_error(error),
                }
            };
            write_remote_message(send, &response).await?;
        }
        RemoteFileRequest::ReadRange { share_id, relative_path, offset, length, revision } => {
            let response = if !access.can_read(&share_id) {
                denied("read access to this share is not permitted")
            } else {
                match provider.read_range(&share_id, &relative_path, offset, length, &revision).await {
                    Ok(range_data) => RemoteFileResponse { range_data, revision, ..RemoteFileResponse::success() },
                    Err(error) => RemoteFileResponse::from_error(error),
                }
            };
            write_remote_message(send, &response).await?;
        }
        RemoteFileRequest::ListShares => {
            if !access.can_list_shares() {
                write_remote_message(send, &denied("remote file access is not permitted"))
                    .await?;
                return Ok(());
            }
            let response = match provider.list_shares().await {
                Ok(shares) => RemoteFileResponse {
                    shares: shares
                        .into_iter()
                        .filter_map(|mut share| {
                            (access.can_read(&share.id) || access.can_write(&share.id)).then(|| {
                                share.writable &= access.can_write(&share.id);
                                share
                            })
                        })
                        .collect(),
                    ..RemoteFileResponse::success()
                },
                Err(error) => RemoteFileResponse::from_error(error),
            };
            write_remote_message(send, &response)
                .await?;
        }
        RemoteFileRequest::ListDirectory {
            share_id,
            relative_path,
            cursor,
            limit,
            search,
            sort_key,
            sort_direction,
        } => {
            if !access.can_read(&share_id) {
                write_remote_message(send, &denied("read access to this share is not permitted"))
                    .await?;
                return Ok(());
            }
            let limit = if limit == 0 {
                crate::remote_files::DEFAULT_REMOTE_DIRECTORY_PAGE_SIZE
            } else {
                limit.clamp(1, crate::remote_files::MAX_REMOTE_DIRECTORY_PAGE_SIZE)
            };
            let response = match provider
                .list_directory(
                    &share_id,
                    &relative_path,
                    cursor.as_deref(),
                    limit,
                    search.as_deref(),
                    sort_key,
                    sort_direction,
                )
                .await
            {
                Ok(page) => RemoteFileResponse {
                    entries: page.entries,
                    next_cursor: page.next_cursor,
                    ..RemoteFileResponse::success()
                },
                Err(error) => RemoteFileResponse::from_error(error),
            };
            write_remote_message(send, &response)
                .await?;
        }
        RemoteFileRequest::CreateDirectory {
            share_id,
            relative_path,
            name,
        } => {
            if !access.can_write(&share_id) {
                write_remote_message(send, &denied("write access to this share is not permitted"))
                    .await?;
                return Ok(());
            }
            let response = match provider
                .create_directory(&share_id, &relative_path, &name)
                .await
            {
                Ok(entry) => RemoteFileResponse {
                    entry: Some(entry),
                    ..RemoteFileResponse::success()
                },
                Err(error) => RemoteFileResponse::from_error(error),
            };
            write_remote_message(send, &response)
                .await?;
        }
        RemoteFileRequest::Rename {
            share_id,
            relative_path,
            new_name,
        } => {
            if !access.can_write(&share_id) {
                write_remote_message(send, &denied("write access to this share is not permitted"))
                    .await?;
                return Ok(());
            }
            let response = match provider.rename(&share_id, &relative_path, &new_name).await {
                Ok(entry) => RemoteFileResponse {
                    entry: Some(entry),
                    ..RemoteFileResponse::success()
                },
                Err(error) => RemoteFileResponse::from_error(error),
            };
            write_remote_message(send, &response)
                .await?;
        }
        RemoteFileRequest::Delete {
            share_id,
            relative_path,
        } => {
            if !access.can_write(&share_id) {
                write_remote_message(send, &denied("write access to this share is not permitted"))
                    .await?;
                return Ok(());
            }
            let response = match provider.delete(&share_id, &relative_path).await {
                Ok(()) => RemoteFileResponse::success(),
                Err(error) => RemoteFileResponse::from_error(error),
            };
            write_remote_message(send, &response)
                .await?;
        }
        RemoteFileRequest::Download {
            share_id,
            relative_path,
        } => {
            if !access.can_read(&share_id) {
                write_remote_message(send, &denied("read access to this share is not permitted"))
                    .await?;
                return Ok(());
            }
            let download = match provider.prepare_download(&share_id, &relative_path).await {
                Ok(download) => download,
                Err(error) => {
                    write_remote_message(send, &RemoteFileResponse::from_error(error))
                        .await?;
                    return Ok(());
                }
            };
            if download.entry.kind != RemoteFileKind::File {
                write_remote_message(
                    send,
                    &RemoteFileResponse::failure(
                        RemoteFileErrorCode::FailedPrecondition,
                        "only files can be downloaded",
                    ),
                )
                    .await?;
                return Ok(());
            }
            let mut file = match tokio::fs::File::open(&download.path).await {
                Ok(file) => file,
                Err(error) => {
                    write_remote_message(
                        send,
                        &RemoteFileResponse::failure(
                            RemoteFileErrorCode::Unavailable,
                            format!("failed to open remote file: {error}"),
                        ),
                    )
                        .await?;
                    return Ok(());
                }
            };
            write_remote_message(
                send,
                &RemoteFileResponse {
                    entry: Some(download.entry.clone()),
                    ..RemoteFileResponse::success()
                },
            )
            .await?;
            let sent = tokio::io::copy(&mut file, send).await?;
            if sent != download.entry.size {
                return Err(ProtocolError::Other("remote file changed during download".into()));
            }
        }
        RemoteFileRequest::Thumbnail {
            share_id,
            relative_path,
            max_dimension,
        } => {
            if !access.can_read(&share_id) {
                write_remote_message(send, &denied("read access to this share is not permitted"))
                    .await?;
                return Ok(());
            }
            let thumbnail = match provider
                .prepare_thumbnail(&share_id, &relative_path, max_dimension)
                .await
            {
                Ok(Some(thumbnail)) => thumbnail,
                Ok(None) => {
                    write_remote_message(
                        send,
                        &RemoteFileResponse::failure(
                            RemoteFileErrorCode::NotFound,
                            "no thumbnail is available for this file",
                        ),
                    )
                    .await?;
                    return Ok(());
                }
                Err(error) => {
                    write_remote_message(send, &RemoteFileResponse::from_error(error))
                        .await?;
                    return Ok(());
                }
            };
            write_remote_message(
                send,
                &RemoteFileResponse {
                    thumbnail_size: thumbnail.bytes.len() as u64,
                    thumbnail_media_type: Some(thumbnail.media_type),
                    ..RemoteFileResponse::success()
                },
            )
            .await?;
            send.write_all(&thumbnail.bytes).await?;
        }
        RemoteFileRequest::Upload {
            share_id,
            relative_path,
            name,
            size,
            overwrite,
            expected_modified_at_ms,
        } => {
            if !access.can_write(&share_id) {
                write_remote_message(send, &denied("write access to this share is not permitted"))
                    .await?;
                return Ok(());
            }
            if size > crate::remote_files::MAX_REMOTE_FILE_CONTENT_SIZE {
                write_remote_message(
                    send,
                    &RemoteFileResponse::failure(
                        RemoteFileErrorCode::ResourceExhausted,
                        "remote file size exceeds the limit",
                    ),
                )
                    .await?;
                return Ok(());
            }
            let upload = match provider
                .prepare_upload(
                    &share_id,
                    &relative_path,
                    &name,
                    size,
                    overwrite,
                    expected_modified_at_ms,
                )
                .await
            {
                Ok(upload) => upload,
                Err(error) => {
                    write_remote_message(send, &RemoteFileResponse::from_error(error))
                        .await?;
                    return Ok(());
                }
            };
            write_remote_message(
                send,
                &RemoteFileResponse {
                    entry: Some(upload.entry.clone()),
                    ..RemoteFileResponse::success()
                },
            )
            .await?;

            let parent = upload
                .destination
                .parent()
                .ok_or_else(|| ProtocolError::Other("invalid upload target directory".into()))?;
            let temporary = parent.join(format!(".arcrelay-upload-{}", uuid::Uuid::new_v4()));
            let mut committed = None;
            let result: std::result::Result<(), RemoteFileError> = async {
                let mut file = tokio::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&temporary)
                    .await
                    .map_err(|error| {
                        if error.kind() == std::io::ErrorKind::PermissionDenied {
                            RemoteFileError::new(
                                RemoteFileErrorCode::PermissionDenied,
                                "upload failed: target directory is read-only or the current user lacks write permission",
                            )
                        } else {
                            RemoteFileError::new(
                                RemoteFileErrorCode::Unavailable,
                                format!("failed to create upload file: {error}"),
                            )
                        }
                    })?;
                let mut limited = (&mut *recv).take(size);
                let received = tokio::io::copy(&mut limited, &mut file)
                    .await
                    .map_err(|error| RemoteFileError::new(RemoteFileErrorCode::Unavailable, format!("upload stream failed: {error}")))?;
                drop(limited);
                if expected_revision.is_some() && received == size {
                    let mut extra = [0u8; 1];
                    let trailing = tokio::time::timeout(std::time::Duration::from_secs(30), recv.read(&mut extra)).await
                        .map_err(|_| RemoteFileError::new(RemoteFileErrorCode::Unavailable, "upload did not finish"))?
                        .map_err(|e| RemoteFileError::new(RemoteFileErrorCode::Unavailable, e.to_string()))?;
                    if trailing != 0 { return Err(RemoteFileError::new(RemoteFileErrorCode::InvalidArgument, "upload exceeds its declared size")); }
                }
                file.flush().await.map_err(|error| RemoteFileError::new(RemoteFileErrorCode::Unavailable, format!("failed to flush upload: {error}")))?;
                file.sync_all().await.map_err(|error| RemoteFileError::new(RemoteFileErrorCode::Unavailable, format!("failed to synchronize upload: {error}")))?;
                drop(file);
                if received != size {
                    return Err(RemoteFileError::new(
                        RemoteFileErrorCode::Unavailable,
                        format!("upload ended early: expected {size} bytes, received {received}"),
                    ));
                }
                if let Some(expected) = &expected_revision {
                    committed = Some(provider.commit_system_upload(&share_id, &relative_path, &name, &temporary, expected).await?);
                    return Ok(());
                }
                if let Some(expected) = upload.expected_modified_at_ms {
                    let metadata = tokio::fs::metadata(&upload.destination)
                        .await
                        .map_err(|_| RemoteFileError::new(
                            RemoteFileErrorCode::NotFound,
                            "the remote file was deleted; the local edited copy was retained",
                        ))?;
                    let actual = metadata
                        .modified()
                        .ok()
                        .and_then(|value| value.duration_since(std::time::UNIX_EPOCH).ok())
                        .map(|value| value.as_millis().min(i64::MAX as u128) as i64)
                        .unwrap_or_default();
                    if actual != expected {
                        return Err(RemoteFileError::new(
                            RemoteFileErrorCode::Conflict,
                            "the remote file was modified elsewhere; the local edited copy was retained",
                        ));
                    }
                }
                // Never remove the old file before the new contents have been
                // committed. rename replaces atomically on Unix and Windows.
                if upload.overwrite {
                    tokio::fs::rename(&temporary, &upload.destination).await
                } else {
                    // An existence check followed by rename would clobber a
                    // destination concurrently created by another client.
                    tokio::fs::hard_link(&temporary, &upload.destination).await
                }
                .map_err(|error| RemoteFileError::new(
                    if error.kind() == std::io::ErrorKind::AlreadyExists {
                        RemoteFileErrorCode::Conflict
                    } else {
                        RemoteFileErrorCode::Unavailable
                    },
                    format!("failed to commit uploaded file: {error}"),
                ))?;
                if !upload.overwrite {
                    let _ = tokio::fs::remove_file(&temporary).await;
                }
                Ok(())
            }
            .await;
            if result.is_err() {
                let _ = tokio::fs::remove_file(&temporary).await;
            }
            let response = match result {
                Ok(()) if committed.is_some() => {
                    let (entry, revision) = committed.unwrap();
                    RemoteFileResponse { entry: Some(entry), revision, ..RemoteFileResponse::success() }
                }
                Ok(()) => RemoteFileResponse {
                    entry: Some(upload.entry),
                    ..RemoteFileResponse::success()
                },
                Err(error) => RemoteFileResponse::from_error(error),
            };
            write_remote_message(send, &response)
                .await?;
        }
    }
    Ok(())
}

async fn handle_clipboard_blob_upload(
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
    uploads: ClipboardUploadStore,
) -> Result<()> {
    const MAX_UPLOAD_BYTES: usize = 20 * 1024 * 1024;
    let open: proto::ClipboardBlobUploadOpen =
        recv_message(recv, MAX_CONTROL_FRAME_SIZE).await?;
    let expected_size = usize::try_from(open.size).unwrap_or(usize::MAX);
    if open.upload_id.len() != 32
        || open.sha256.len() != 32
        || open.upload_id != open.sha256
        || expected_size == 0
        || expected_size > MAX_UPLOAD_BYTES
        || !matches!(
            open.media_type.as_str(),
            "image/png" | "text/html; charset=utf-8" | "text/rtf"
        )
    {
        send_message(
            send,
            &proto::ClipboardBlobUploadHeader {
                status: Some(status(
                    proto::ErrorCode::InvalidArgument,
                    "invalid clipboard blob upload",
                )),
                upload_id: open.upload_id,
                chunk_size: crate::message::MAX_BLOB_CHUNK_SIZE as u32,
            },
            MAX_CONTROL_FRAME_SIZE,
        )
        .await?;
        return Ok(());
    }
    send_message(
        send,
        &proto::ClipboardBlobUploadHeader {
            status: Some(ok_status()),
            upload_id: open.upload_id.clone(),
            chunk_size: crate::message::MAX_BLOB_CHUNK_SIZE as u32,
        },
        MAX_CONTROL_FRAME_SIZE,
    )
    .await?;
    let mut bytes = Vec::with_capacity(expected_size);
    loop {
        let chunk: proto::ClipboardBlobUploadChunk =
            recv_message(recv, crate::message::MAX_BLOB_CHUNK_SIZE + 1024).await?;
        if chunk.upload_id != open.upload_id || chunk.offset as usize != bytes.len() {
            return Err(ProtocolError::Other(
                "invalid clipboard blob upload chunk".into(),
            ));
        }
        if bytes.len().saturating_add(chunk.data.len()) > expected_size {
            return Err(ProtocolError::Other(
                "clipboard blob upload exceeds declared size".into(),
            ));
        }
        bytes.extend_from_slice(&chunk.data);
        if chunk.end_of_upload {
            break;
        }
    }
    let received_size = bytes.len();
    let result = if received_size == expected_size {
        uploads.insert(
            open.upload_id.clone(),
            bytes,
            &open.sha256,
            &open.media_type,
        )
    } else {
        Err("clipboard blob upload ended early")
    };
    send_message(
        send,
        &proto::ClipboardBlobUploadResult {
            status: Some(match result {
                Ok(()) => ok_status(),
                Err(message) => status(proto::ErrorCode::InvalidArgument, message),
            }),
            upload_id: open.upload_id,
            received_size: received_size as u64,
            sha256: open.sha256,
        },
        MAX_CONTROL_FRAME_SIZE,
    )
    .await?;
    let _ = send.finish();
    Ok(())
}

async fn handle_blob_stream(
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
    blob_store: BlobStore,
    blob_owner: Vec<u8>,
) -> Result<()> {
    let request: proto::BlobStreamOpen = tokio::time::timeout(
        STREAM_SETUP_TIMEOUT,
        recv_message(recv, MAX_CONTROL_FRAME_SIZE),
    )
    .await
    .map_err(|_| ProtocolError::Other("blob stream open timed out".into()))??;
    let redeemed = match blob_store.redeem(&blob_owner, request.transfer_id, &request.ticket) {
        Ok(blob) => blob,
        Err(message) => {
            send_message(
                send,
                &proto::BlobStreamHeader {
                    status: Some(status(proto::ErrorCode::Unauthenticated, message)),
                    blob: None,
                    accepted_offset: 0,
                    chunk_size: crate::message::MAX_BLOB_CHUNK_SIZE as u32,
                },
                MAX_CONTROL_FRAME_SIZE,
            )
            .await?;
            let _ = send.finish();
            return Ok(());
        }
    };
    let offset = match usize::try_from(request.offset) {
        Ok(offset) if offset <= redeemed.bytes.len() => offset,
        _ => {
            send_message(
                send,
                &proto::BlobStreamHeader {
                    status: Some(status(
                        proto::ErrorCode::InvalidArgument,
                        "blob offset is outside the payload",
                    )),
                    blob: Some(redeemed.reference),
                    accepted_offset: 0,
                    chunk_size: crate::message::MAX_BLOB_CHUNK_SIZE as u32,
                },
                MAX_CONTROL_FRAME_SIZE,
            )
            .await?;
            let _ = send.finish();
            return Ok(());
        }
    };

    send_message(
        send,
        &proto::BlobStreamHeader {
            status: Some(ok_status()),
            blob: Some(redeemed.reference),
            accepted_offset: offset as u64,
            chunk_size: crate::message::MAX_BLOB_CHUNK_SIZE as u32,
        },
        MAX_CONTROL_FRAME_SIZE,
    )
    .await?;

    if offset == redeemed.bytes.len() {
        send_message(
            send,
            &proto::BlobStreamChunk {
                transfer_id: request.transfer_id,
                offset: offset as u64,
                data: Bytes::new(),
                end_of_blob: true,
            },
            crate::message::MAX_BLOB_CHUNK_SIZE + 1024,
        )
        .await?;
    } else {
        let mut chunk_offset = offset;
        while chunk_offset < redeemed.bytes.len() {
            let chunk_end =
                (chunk_offset + crate::message::MAX_BLOB_CHUNK_SIZE).min(redeemed.bytes.len());
            send_message(
                send,
                &proto::BlobStreamChunk {
                    transfer_id: request.transfer_id,
                    offset: chunk_offset as u64,
                    data: redeemed.bytes.slice(chunk_offset..chunk_end),
                    end_of_blob: chunk_end == redeemed.bytes.len(),
                },
                crate::message::MAX_BLOB_CHUNK_SIZE + 1024,
            )
            .await?;
            chunk_offset = chunk_end;
        }
    }
    let _ = send.finish();
    Ok(())
}

include!("input/events.rs");
