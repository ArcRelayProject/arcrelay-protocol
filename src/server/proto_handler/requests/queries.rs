async fn handle_query(
    request_id: u64,
    query: proto::Query,
    coordinator: Arc<StateCoordinator>,
    action_provider: Option<Arc<dyn HostCapabilityProvider>>,
    blob_store: BlobStore,
    blob_owner: Vec<u8>,
    can_read_blobs: bool,
) -> proto::ServerControlFrame {
    let snapshot = match query.body {
        Some(proto::query::Body::GetSystem(_)) => {
            match coordinator.current_system_snapshot().await {
                Ok(value) => proto::Snapshot {
                    revision: protocol_u64(value.revision).max(1),
                    captured_at_ms: protocol_timestamp(value.captured_at_ms),
                    data: Some(proto::snapshot::Data::System(build_system_snapshot(
                        value.data.clone(),
                    ))),
                },
                Err(error) => {
                    return response_error(request_id, core_error_code(&error), error.to_string())
                }
            }
        }
        Some(proto::query::Body::ListProcesses(request)) => {
            let sort = process_sort_from_proto(
                proto::ProcessSortBy::try_from(request.sort_by)
                    .unwrap_or(proto::ProcessSortBy::Cpu),
            );
            match coordinator.query_process_list(sort).await {
                Ok(value) => {
                    let focused = coordinator
                        .current_focused_app_name()
                        .await
                        .map(|name| sanitize_text(name, 256));
                    proto::Snapshot {
                        revision: protocol_u64(value.revision).max(1),
                        captured_at_ms: protocol_timestamp(value.captured_at_ms),
                        data: Some(proto::snapshot::Data::Processes(build_process_snapshot(
                            value.data, focused,
                        ))),
                    }
                }
                Err(error) => {
                    return response_error(request_id, core_error_code(&error), error.to_string())
                }
            }
        }
        Some(proto::query::Body::GetMedia(_)) => {
            let value = coordinator.current_media_state().await;
            proto::Snapshot {
                revision: protocol_u64(value.revision).max(1),
                captured_at_ms: protocol_timestamp(value.captured_at_ms),
                data: Some(proto::snapshot::Data::Media(build_media_snapshot(
                    &value.data,
                ))),
            }
        }
        Some(proto::query::Body::GetClipboard(request)) => {
            let limit = if request.history_limit == 0 {
                20
            } else {
                request.history_limit.min(MAX_CLIPBOARD_ITEMS as u32)
            } as usize;
            let service = coordinator.service();
            let kinds = request
                .kinds
                .iter()
                .filter_map(|kind| proto::ClipboardContentKind::try_from(*kind).ok())
                .filter_map(|kind| match kind {
                    proto::ClipboardContentKind::Text => {
                        Some(arcrelay_core::domain::clipboard::ClipboardContentKind::Text)
                    }
                    proto::ClipboardContentKind::Html => {
                        Some(arcrelay_core::domain::clipboard::ClipboardContentKind::Html)
                    }
                    proto::ClipboardContentKind::Image => {
                        Some(arcrelay_core::domain::clipboard::ClipboardContentKind::Image)
                    }
                    proto::ClipboardContentKind::Files => {
                        Some(arcrelay_core::domain::clipboard::ClipboardContentKind::Files)
                    }
                    proto::ClipboardContentKind::Unspecified => None,
                })
                .collect();
            let cursor = request.cursor_timestamp_ms.zip(request.cursor_id).map(
                |(captured_at_ms, id)| arcrelay_core::domain::clipboard::ClipboardCursor {
                    sort_at_ms: captured_at_ms,
                    id,
                },
            );
            let page = match service
                .clipboard
                .history(arcrelay_core::domain::clipboard::ClipboardQuery {
                    include_total_count: false,
                    limit,
                    cursor,
                    search: (!request.search.trim().is_empty())
                        .then(|| request.search.trim().to_string()),
                    kinds,
                    favorite_only: request.favorite_only,
                    label_ids: request.label_ids.clone(),
                    sort_by: arcrelay_core::domain::clipboard::ClipboardSortBy::UpdatedAt,
                })
                .await
            {
                Ok(page) => page,
                Err(error) => {
                    return response_error(request_id, core_error_code(&error), error.to_string())
                }
            };
            let policy = match service.clipboard.policy().await {
                Ok(policy) => policy,
                Err(error) => {
                    return response_error(request_id, core_error_code(&error), error.to_string())
                }
            };
            let mut clipboard = match build_clipboard_snapshot(
                page.entries,
                policy,
                page.next_cursor,
                false,
            ) {
                Ok(snapshot) => snapshot,
                Err(message) => {
                    return response_error(request_id, proto::ErrorCode::ResourceExhausted, message)
                }
            };
            clipboard.labels = service
                .clipboard
                .labels()
                .await
                .unwrap_or_default()
                .into_iter()
                .map(crate::proto_msg::clipboard_label_to_proto)
                .collect();
            proto::Snapshot {
                revision: service.clipboard.revision().await.unwrap_or(1).max(1),
                captured_at_ms: now_ms(),
                data: Some(proto::snapshot::Data::Clipboard(clipboard)),
            }
        }
        Some(proto::query::Body::GetDevices(_)) => {
            let service = coordinator.service();
            let local_device = service.device.local_info().await.ok().map(|value| {
                let mut value = local_device_info_to_proto(value);
                value.name = sanitize_text(value.name, 128);
                value.ip = sanitize_text(value.ip, 128);
                value.app_version = sanitize_text(value.app_version, 64);
                value.port = value.port.min(u16::MAX as u32);
                value
            });
            let config = service.device.get_config().await.ok().map(|value| {
                let mut value = connection_config_to_proto(value);
                value.port = value.port.min(u16::MAX as u32);
                value.encrypted = true;
                value
            });
            proto::Snapshot {
                revision: 1,
                captured_at_ms: now_ms(),
                data: Some(proto::snapshot::Data::Devices(proto::DeviceSnapshot {
                    local_device,
                    config,
                    peers: vec![],
                })),
            }
        }
        Some(proto::query::Body::ListWindows(request)) => {
            let service = coordinator.service();
            let include_thumbnails = request.include_thumbnails && can_read_blobs;
            let mut thumbnail_window_ids = request
                .thumbnail_window_ids
                .into_iter()
                .filter(|window_id| *window_id != 0)
                .collect::<HashSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            thumbnail_window_ids.sort_unstable();
            if thumbnail_window_ids.len() > MAX_THUMBNAIL_WINDOW_IDS {
                return response_error(
                    request_id,
                    proto::ErrorCode::InvalidArgument,
                    format!("at most {MAX_THUMBNAIL_WINDOW_IDS} thumbnail window IDs are allowed"),
                );
            }
            let sample_result = if include_thumbnails && thumbnail_window_ids.is_empty() {
                // Compatibility path for older v3 clients. New clients use the
                // targeted path below so unrelated windows are never captured.
                match service.window_manager.list_windows().await {
                    Ok(windows) => Ok(arcrelay_core::domain::window_manager::WindowStateSample {
                        focused_app_name: windows
                            .iter()
                            .find(|window| window.is_focused)
                            .map(|window| window.app_name.clone()),
                        windows,
                        spaces: service
                            .window_manager
                            .list_spaces()
                            .await
                            .unwrap_or_default(),
                        on_screen_window_ids: Vec::new(),
                    }),
                    Err(error) => Err(error),
                }
            } else if include_thumbnails {
                service
                    .window_manager
                    .sample_window_state_with_thumbnails(&thumbnail_window_ids)
                    .await
            } else {
                service.window_manager.sample_window_state().await
            };
            match sample_result {
                Ok(sample) => proto::Snapshot {
                    revision: 1,
                    captured_at_ms: now_ms(),
                    data: Some(proto::snapshot::Data::Windows(proto::WindowListSnapshot {
                        windows: build_window_list(
                            sample.windows,
                            Some(&blob_store),
                            Some(&blob_owner),
                            include_thumbnails,
                        ),
                        spaces: build_space_list(sample.spaces),
                    })),
                },
                Err(error) => {
                    return response_error(request_id, core_error_code(&error), error.to_string())
                }
            }
        }
        Some(proto::query::Body::ListActions(_)) => proto::Snapshot {
            revision: 1,
            captured_at_ms: now_ms(),
            data: Some(proto::snapshot::Data::Actions(
                build_action_snapshot(action_provider.as_ref()).await,
            )),
        },
        Some(proto::query::Body::GetActionOutput(request)) => proto::Snapshot {
            revision: 1,
            captured_at_ms: now_ms(),
            data: Some(proto::snapshot::Data::ActionOutput(
                proto::ActionOutputSnapshot {
                    action_id: request.action_id.clone(),
                    lines: match action_provider.as_ref() {
                        Some(provider) => bounded_action_output(
                            provider.get_action_output(&request.action_id).await,
                        ),
                        None => Vec::new(),
                    },
                },
            )),
        },
        Some(proto::query::Body::GetNotifications(request)) => {
            let limit = if request.limit == 0 {
                50
            } else {
                request.limit.min(100)
            } as usize;
            let notifications = match action_provider.as_ref() {
                Some(provider) => match provider
                    .list_notifications(request.include_read, limit)
                    .await
                {
                    Ok(notifications) => notifications,
                    Err(error) => {
                        return response_error(
                            request_id,
                            error.code.protocol_code(),
                            error.message,
                        )
                    }
                },
                None => {
                    return response_error(
                        request_id,
                        proto::ErrorCode::Unsupported,
                        "notifications are unavailable",
                    )
                }
            };
            proto::Snapshot {
                revision: 1,
                captured_at_ms: now_ms(),
                data: Some(proto::snapshot::Data::Notifications(
                    proto::NotificationListSnapshot {
                        notifications: notifications
                            .into_iter()
                            .map(build_notification_info)
                            .collect(),
                    },
                )),
            }
        }
        Some(proto::query::Body::GetClipboardSync(request)) => {
            let service = coordinator.service();
            let page = match service
                .clipboard
                .sync_records(request.after_local_id, request.limit.clamp(1, 200) as usize)
                .await
            {
                Ok(page) => page,
                Err(error) => {
                    return response_error(request_id, core_error_code(&error), error.to_string())
                }
            };
            let mut records = Vec::with_capacity(page.records.len());
            for (_, record) in page.records {
                match sync_record_to_proto(record, &blob_store, &blob_owner) {
                    Ok(record) => records.push(record),
                    Err(message) => {
                        return response_error(
                            request_id,
                            proto::ErrorCode::ResourceExhausted,
                            message,
                        )
                    }
                }
            }
            proto::Snapshot {
                revision: service.clipboard.revision().await.unwrap_or(1).max(1),
                captured_at_ms: now_ms(),
                data: Some(proto::snapshot::Data::ClipboardSync(
                    proto::ClipboardSyncPage {
                        records,
                        next_cursor: page.next_cursor,
                    },
                )),
            }
        }
        None => {
            return response_error(request_id, proto::ErrorCode::InvalidArgument, "empty query")
        }
    };

    let frame = server_frame(proto::server_control_frame::Body::Response(
        proto::Response {
            request_id,
            status: Some(ok_status()),
            body: Some(proto::response::Body::Snapshot(snapshot)),
        },
    ));
    if frame.encoded_len() > MAX_CONTROL_FRAME_SIZE {
        response_error(
            request_id,
            proto::ErrorCode::ResourceExhausted,
            "query result exceeds the negotiated control-frame limit",
        )
    } else {
        frame
    }
}

