async fn handle_command(
    request_id: u64,
    command: proto::Command,
    coordinator: Arc<StateCoordinator>,
    action_provider: Option<Arc<dyn HostCapabilityProvider>>,
    clipboard_uploads: ClipboardUploadStore,
    device_id: String,
    device_name: String,
) -> proto::ServerControlFrame {
    let service = coordinator.service();
    let mut output = String::new();
    let mut refresh_media = false;
    let result: std::result::Result<(), CommandFailure> = match command.action {
        Some(proto::command::Action::PlaybackAction(command)) => {
            refresh_media = true;
            let action = playback_action_from_proto(
                proto::PlaybackAction::try_from(command.action)
                    .unwrap_or(proto::PlaybackAction::Play),
            );
            service
                .media_control
                .playback_action(action)
                .await
                .map_err(CommandFailure::from)
        }
        Some(proto::command::Action::SetSystemVolume(command)) => {
            refresh_media = true;
            service
                .media_control
                .set_system_volume(command.volume as u8)
                .await
                .map_err(CommandFailure::from)
        }
        Some(proto::command::Action::SetAppVolume(command)) => {
            refresh_media = true;
            service
                .media_control
                .set_app_volume(&command.app_name, command.volume as u8)
                .await
                .map_err(CommandFailure::from)
        }
        Some(proto::command::Action::SetMicrophone(command)) => {
            refresh_media = true;
            service
                .media_control
                .set_microphone_active(command.active)
                .await
                .map_err(CommandFailure::from)
        }
        Some(proto::command::Action::SetDnd(command)) => {
            refresh_media = true;
            service
                .media_control
                .set_dnd_active(command.active)
                .await
                .map_err(CommandFailure::from)
        }
        Some(proto::command::Action::KillProcess(command)) => service
            .process
            .kill(command.pid)
            .await
            .map_err(CommandFailure::from),
        Some(proto::command::Action::SetClipboard(command)) => service
            .clipboard
            .set_text(&command.content)
            .await
            .map_err(CommandFailure::from),
        Some(proto::command::Action::ClearClipboardHistory(_)) => service
            .clipboard
            .clear_history()
            .await
            .map_err(CommandFailure::from),
        Some(proto::command::Action::PasteClipboardRecord(command)) => service
            .clipboard
            .paste_record(command.record_id)
            .await
            .map_err(CommandFailure::from),
        Some(proto::command::Action::DeleteClipboardRecord(command)) => service
            .clipboard
            .delete(command.record_id)
            .await
            .map_err(CommandFailure::from),
        Some(proto::command::Action::SetClipboardFavorite(command)) => service
            .clipboard
            .set_favorite(command.record_id, command.favorite)
            .await
            .map_err(CommandFailure::from),
        Some(proto::command::Action::CreateClipboardLabel(command)) => service
            .clipboard
            .create_label(&command.name, &command.color)
            .await
            .map(|_| ())
            .map_err(CommandFailure::from),
        Some(proto::command::Action::UpdateClipboardLabel(command)) => service
            .clipboard
            .update_label(&command.label_id, &command.name, &command.color)
            .await
            .map_err(CommandFailure::from),
        Some(proto::command::Action::DeleteClipboardLabel(command)) => service
            .clipboard
            .delete_label(&command.label_id)
            .await
            .map_err(CommandFailure::from),
        Some(proto::command::Action::SetClipboardLabels(command)) => service
            .clipboard
            .set_labels(command.record_id, command.label_ids.clone())
            .await
            .map_err(CommandFailure::from),
        Some(proto::command::Action::UpdateClipboardPolicy(command)) => {
            let policy = command
                .policy
                .as_ref()
                .expect("validated clipboard policy command");
            service
                .clipboard
                .update_policy(arcrelay_core::domain::clipboard::ClipboardPolicy {
                    history_enabled: policy.history_enabled,
                    max_items: policy.max_items,
                    max_bytes: policy.max_bytes,
                    retention_days: policy.retention_days,
                    save_sensitive: policy.save_sensitive,
                })
                .await
                .map_err(CommandFailure::from)
        }
        Some(proto::command::Action::ApplyClipboardSyncRecord(command)) => {
            let record = command
                .record
                .as_ref()
                .expect("validated clipboard sync command");
            match sync_record_from_proto(record, &clipboard_uploads, &device_id, &device_name) {
                Ok(record) => service
                    .clipboard
                    .apply_sync_record(record, true)
                    .await
                    .map(|_| ())
                    .map_err(CommandFailure::from),
                Err(message) => Err(CommandFailure::invalid(message)),
            }
        }
        Some(proto::command::Action::FocusWindow(command)) => service
            .window_manager
            .focus_window(command.window_id)
            .await
            .map_err(CommandFailure::from),
        Some(proto::command::Action::SwitchSpace(command)) => {
            let result = service
                .window_manager
                .switch_space(command.space_id)
                .await
                .map_err(CommandFailure::from);
            if result.is_ok() {
                let coordinator = coordinator.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    coordinator.force_push_windows().await;
                });
            }
            result
        }
        Some(proto::command::Action::ExecuteQuickAction(command)) => match action_provider {
            Some(provider) => match provider.execute_action(&command.action_id).await {
                Ok(value) => {
                    output = value;
                    Ok(())
                }
                Err(error) => Err(CommandFailure::provider(error)),
            },
            None => Err(CommandFailure::unsupported("quick actions are unavailable")),
        },
        Some(proto::command::Action::RunAutomation(command)) => match action_provider {
            Some(provider) => match provider.run_automation(&command.automation_id).await { Ok(id) => {output=id;Ok(())}, Err(error)=>Err(CommandFailure::provider(error)) },
            None=>Err(CommandFailure::unsupported("automations unavailable")),
        },
        Some(proto::command::Action::SetAutomationEnabled(command)) => match action_provider {
            Some(provider) => provider.set_automation_enabled(&command.automation_id,command.enabled).await.map_err(CommandFailure::provider),
            None=>Err(CommandFailure::unsupported("automations unavailable")),
        },
        Some(proto::command::Action::CancelAutomation(command)) => match action_provider {
            Some(provider) => provider.cancel_automation(&command.activity_id).await.map_err(CommandFailure::provider),
            None=>Err(CommandFailure::unsupported("automations unavailable")),
        },
        Some(proto::command::Action::MarkNotificationRead(command)) => match action_provider {
            Some(provider) => match provider
                .mark_notification_read(&command.notification_id, &device_id, &device_name)
                .await
            {
                Ok(_) => Ok(()),
                Err(error) => Err(CommandFailure::provider(error)),
            },
            None => Err(CommandFailure::unsupported("notifications are unavailable")),
        },
        None => Err(CommandFailure::internal("empty command")),
    };

    if refresh_media && result.is_ok() {
        let coordinator = coordinator.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            coordinator.force_push_media().await;
        });
    }

    let frame = match result {
        Ok(()) => server_frame(proto::server_control_frame::Body::Response(
            proto::Response {
                request_id,
                status: Some(ok_status()),
                body: Some(proto::response::Body::CommandResult(proto::CommandResult {
                    output: truncate_utf8(output, MAX_COMMAND_OUTPUT_BYTES),
                })),
            },
        )),
        Err(error) => response_error(request_id, error.code, error.message),
    };
    if frame.encoded_len() > MAX_CONTROL_FRAME_SIZE {
        response_error(
            request_id,
            proto::ErrorCode::ResourceExhausted,
            "command result exceeds the negotiated control-frame limit",
        )
    } else {
        frame
    }
}

fn truncate_utf8(mut value: String, maximum: usize) -> String {
    if value.len() <= maximum {
        return value;
    }
    let mut end = maximum;
    while !value.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    value.truncate(end);
    value
}

fn bounded_action_output(lines: Vec<String>) -> Vec<String> {
    let mut selected = VecDeque::new();
    let mut total = 0_usize;
    for line in lines.into_iter().rev() {
        if selected.len() >= MAX_ACTION_OUTPUT_LINES {
            break;
        }
        let line = truncate_utf8(line, MAX_ACTION_OUTPUT_LINE_BYTES);
        if total.saturating_add(line.len()) > MAX_ACTION_OUTPUT_SNAPSHOT_BYTES {
            break;
        }
        total += line.len();
        selected.push_front(line);
    }
    selected.into_iter().collect()
}

struct CommandFailure {
    code: proto::ErrorCode,
    message: String,
}

impl CommandFailure {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            code: proto::ErrorCode::InvalidArgument,
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            code: proto::ErrorCode::Internal,
            message: message.into(),
        }
    }

    fn unsupported(message: impl Into<String>) -> Self {
        Self {
            code: proto::ErrorCode::Unsupported,
            message: message.into(),
        }
    }

    fn provider(error: HostCapabilityError) -> Self {
        Self {
            code: error.code.protocol_code(),
            message: error.message,
        }
    }
}

impl From<Error> for CommandFailure {
    fn from(error: Error) -> Self {
        let code = core_error_code(&error);
        Self {
            code,
            message: error.to_string(),
        }
    }
}

fn core_error_code(error: &Error) -> proto::ErrorCode {
    match error.kind() {
        arcrelay_core::ErrorKind::OperationFailed => proto::ErrorCode::FailedPrecondition,
        arcrelay_core::ErrorKind::NotFound => proto::ErrorCode::NotFound,
        arcrelay_core::ErrorKind::Unsupported => proto::ErrorCode::Unsupported,
        arcrelay_core::ErrorKind::Unavailable => proto::ErrorCode::Unavailable,
        arcrelay_core::ErrorKind::Internal => proto::ErrorCode::Internal,
    }
}
