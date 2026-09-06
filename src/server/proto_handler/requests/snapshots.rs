fn build_system_snapshot(
    snapshot: arcrelay_core::domain::system_monitor::SystemSnapshot,
) -> proto::SystemSnapshot {
    let mut snapshot = system_snapshot_to_proto(snapshot);
    if let Some(cpu) = snapshot.cpu.as_mut() {
        cpu.usage_percent = bounded_percent(cpu.usage_percent);
        cpu.core_count = cpu.core_count.min(4096);
        cpu.temperature_celsius = cpu.temperature_celsius.filter(|value| value.is_finite());
        cpu.model_name = sanitize_text(std::mem::take(&mut cpu.model_name), 256);
    }
    if let Some(memory) = snapshot.memory.as_mut() {
        memory.total_bytes = protocol_u64(memory.total_bytes);
        memory.used_bytes = protocol_u64(memory.used_bytes).min(memory.total_bytes);
        memory.usage_percent = bounded_percent(memory.usage_percent);
    }
    if let Some(network) = snapshot.network.as_mut() {
        network.download_bytes_per_sec = protocol_u64(network.download_bytes_per_sec);
        network.upload_bytes_per_sec = protocol_u64(network.upload_bytes_per_sec);
    }
    snapshot.gpus.truncate(MAX_GPU_ITEMS);
    for gpu in &mut snapshot.gpus {
        gpu.name = sanitize_text(std::mem::take(&mut gpu.name), 256);
        gpu.usage_percent = gpu.usage_percent.map(bounded_percent);
        gpu.temperature_celsius = gpu.temperature_celsius.filter(|value| value.is_finite());
    }
    snapshot.disks.truncate(MAX_DISK_ITEMS);
    for disk in &mut snapshot.disks {
        disk.name = sanitize_text(std::mem::take(&mut disk.name), 256);
        disk.total_bytes = protocol_u64(disk.total_bytes);
        disk.used_bytes = protocol_u64(disk.used_bytes).min(disk.total_bytes);
        disk.usage_percent = bounded_percent(disk.usage_percent);
    }
    snapshot
}

fn build_media_snapshot(
    state: &arcrelay_core::application::state_coordinator::MediaState,
) -> proto::MediaStateSnapshot {
    let mut seen_apps = HashSet::new();
    let mut app_volumes = Vec::new();
    for volume in &state.app_volumes {
        let app_name = sanitize_text(volume.app_name.clone(), 256);
        if app_name.is_empty() || !seen_apps.insert(app_name.clone()) {
            continue;
        }
        app_volumes.push(proto::AppVolume {
            app_name,
            volume: u32::from(volume.volume.min(100)),
        });
        if app_volumes.len() >= MAX_APP_VOLUME_ITEMS {
            break;
        }
    }

    proto::MediaStateSnapshot {
        playback: state.playback.as_ref().cloned().map(|playback| {
            let mut playback = playback_info_to_proto(playback);
            playback.title = sanitize_text(playback.title, 512);
            playback.artist = sanitize_text(playback.artist, 512);
            playback.source_app = sanitize_text(playback.source_app, 256);
            playback.position_secs = bounded_nonnegative(playback.position_secs);
            playback.duration_secs = bounded_nonnegative(playback.duration_secs);
            playback
        }),
        volume: Some(proto::VolumeInfo {
            system_volume: u32::from(state.volume.system_volume.min(100)),
            is_muted: state.volume.is_muted,
        }),
        app_volumes,
        microphone_active: state.microphone_active,
        dnd_active: state.dnd_active,
    }
}

fn build_process_snapshot(
    processes: Vec<arcrelay_core::domain::process::ProcessInfo>,
    focused_app_name: Option<String>,
) -> proto::ProcessListSnapshot {
    let mut seen_pids = HashSet::new();
    let mut bounded = Vec::new();
    for process in processes {
        let name = sanitize_text(process.name, 128);
        if process.pid == 0 || name.is_empty() || !seen_pids.insert(process.pid) {
            continue;
        }
        bounded.push(proto::ProcessInfo {
            pid: process.pid,
            name,
            cpu_percent: if process.cpu_percent.is_finite() {
                process.cpu_percent.max(0.0)
            } else {
                0.0
            },
            memory_bytes: protocol_u64(process.memory_bytes),
        });
        if bounded.len() >= MAX_PROCESS_ITEMS {
            break;
        }
    }
    proto::ProcessListSnapshot {
        processes: bounded,
        focused_app_name,
    }
}

fn build_window_list(
    windows: Vec<arcrelay_core::domain::window_manager::WindowInfo>,
    blob_store: Option<&BlobStore>,
    blob_owner: Option<&[u8]>,
    include_thumbnails: bool,
) -> Vec<proto::WindowInfo> {
    let mut seen_ids = HashSet::new();
    let mut bounded = Vec::new();
    for window in windows {
        if window.window_id == 0 || !seen_ids.insert(window.window_id) {
            continue;
        }
        let output = match (blob_store, blob_owner) {
            (Some(blob_store), Some(blob_owner)) => {
                window_with_blob(window, blob_store, blob_owner, include_thumbnails)
            }
            _ => bounded_window_meta(window),
        };
        bounded.push(output);
        if bounded.len() >= MAX_WINDOW_ITEMS {
            break;
        }
    }
    bounded
}

fn build_space_list(
    spaces: Vec<arcrelay_core::domain::window_manager::SpaceInfo>,
) -> Vec<proto::SpaceInfo> {
    let mut seen_ids = HashSet::new();
    let mut bounded = Vec::new();
    for space in spaces {
        if space.space_id == 0
            || space.space_id > i64::MAX as u64
            || !seen_ids.insert(space.space_id)
        {
            continue;
        }
        bounded.push(bounded_space(space));
        if bounded.len() >= MAX_SPACE_ITEMS {
            break;
        }
    }
    bounded
}

fn window_with_blob(
    mut window: arcrelay_core::domain::window_manager::WindowInfo,
    blob_store: &BlobStore,
    blob_owner: &[u8],
    include_thumbnail: bool,
) -> proto::WindowInfo {
    let thumbnail = include_thumbnail
        .then(|| {
            blob_store.insert(
                blob_owner,
                std::mem::take(&mut window.thumbnail_png),
                "image/png",
            )
        })
        .flatten();
    let mut output = bounded_window_meta(window);
    output.thumbnail = thumbnail;
    output
}

fn bounded_window_meta(
    window: arcrelay_core::domain::window_manager::WindowInfo,
) -> proto::WindowInfo {
    proto::WindowInfo {
        window_id: window.window_id,
        title: sanitize_text(window.title, 512),
        app_name: {
            let app_name = sanitize_text(window.app_name, 256);
            if app_name.is_empty() {
                "Unknown".to_string()
            } else {
                app_name
            }
        },
        is_focused: window.is_focused,
        thumbnail: None,
        space_id: protocol_u64(window.space_id),
    }
}

fn bounded_space(space: arcrelay_core::domain::window_manager::SpaceInfo) -> proto::SpaceInfo {
    proto::SpaceInfo {
        space_id: space.space_id,
        label: sanitize_text(space.label, 128),
        is_active: space.is_active,
    }
}

async fn build_action_snapshot(
    provider: Option<&Arc<dyn HostCapabilityProvider>>,
) -> proto::ActionListSnapshot {
    let Some(provider) = provider else {
        return proto::ActionListSnapshot {
            actions: vec![],
            automations: vec![],
        };
    };
    let (provided_actions, provided_automations) =
        tokio::join!(provider.list_actions(), provider.list_automations());
    let mut action_ids = HashSet::new();
    let actions = provided_actions
        .into_iter()
        .filter(|action| {
            valid_control_identifier(&action.id) && action_ids.insert(action.id.clone())
        })
        .take(MAX_ACTION_ITEMS)
        .map(|action| proto::QuickActionInfo {
            id: action.id,
            name: sanitize_text(action.name, 256),
            icon_id: sanitize_text(action.icon_id, 64),
            icon_svg: sanitize_text(action.icon_svg, 16 * 1024),
            color: sanitize_text(action.color, 64),
            group: sanitize_text(action.group, 256),
            action_type_label: sanitize_text(action.action_type_label, 128),
            sort_order: action.sort_order,
            is_toggle: action.is_toggle,
            is_running: action.is_running,
            requires_confirmation: action.requires_confirmation,
        })
        .collect();
    let mut automation_ids=HashSet::new();
    let automations=provided_automations.into_iter().filter(|a|valid_control_identifier(&a.id) && automation_ids.insert(a.id.clone())).take(MAX_AUTOMATION_ITEMS).map(|a|proto::AutomationInfo {
        id:a.id,name:sanitize_text(a.name,256),summary:sanitize_text(a.summary,2048),enabled:a.enabled,
        latest_activity_id:a.latest_activity_id.filter(|id|valid_control_identifier(id)),latest_status:a.latest_status.map(|s|sanitize_text(s,64)),reason:a.reason.map(|s|sanitize_text(s,2048)),next_run_at_ms:a.next_run_at_ms,completed_steps:a.completed_steps,total_steps:a.total_steps,
    }).collect();
    proto::ActionListSnapshot { actions, automations }
}

fn build_notification_info(notification: HostNotificationInfo) -> proto::NotificationInfo {
    proto::NotificationInfo {
        id: sanitize_text(notification.id, 256),
        title: sanitize_text(notification.title, 160),
        body: sanitize_text(notification.body, 8 * 1024),
        source: sanitize_text(notification.source, 120),
        kind: notification.kind,
        reference: notification
            .reference
            .map(|reference| sanitize_text(reference, 512)),
        created_at_ms: notification.created_at_ms.max(1),
        read_at_ms: notification.read_at_ms.map(|value| value.max(1)),
        read_by_device_name: notification
            .read_by_device_name
            .map(|name| sanitize_text(name, 128)),
    }
}

fn sync_record_to_proto(
    record: arcrelay_core::domain::clipboard::ClipboardSyncRecord,
    blob_store: &BlobStore,
    blob_owner: &[u8],
) -> std::result::Result<proto::ClipboardSyncRecord, &'static str> {
    use arcrelay_core::domain::clipboard::{ClipboardContentKind, ClipboardSyncChangeKind};
    use proto::clipboard_sync_record::Payload;
    let text_syntax_json = serde_json::to_string(&record.text_syntax)
        .map_err(|_| "clipboard text syntax is invalid")?;
    let payload = if record.deleted || record.kind == ClipboardContentKind::Files {
        None
    } else {
        match record.kind {
            ClipboardContentKind::Text => Some(Payload::Text(
                record.text.clone().ok_or("synchronized text payload is missing")?,
            )),
            ClipboardContentKind::Html => {
                let html = blob_store
                    .insert(
                        blob_owner,
                        record
                            .html
                            .as_ref()
                            .ok_or("synchronized HTML payload is missing")?
                            .as_bytes()
                            .to_vec(),
                        "text/html; charset=utf-8",
                    )
                    .ok_or("synchronized HTML cache is full")?;
                let rtf = record
                    .rtf
                    .as_ref()
                    .map(|rtf| {
                        blob_store
                            .insert(blob_owner, rtf.as_bytes().to_vec(), "text/rtf")
                            .ok_or("synchronized RTF cache is full")
                    })
                    .transpose()?;
                Some(Payload::RichText(proto::ClipboardRichTextPayload {
                    plain_text: record.text.clone().unwrap_or_default(),
                    html: Some(html),
                    rtf,
                }))
            }
            ClipboardContentKind::Image => Some(Payload::Image(
                blob_store
                    .insert(
                        blob_owner,
                        record
                            .image_png
                            .clone()
                            .ok_or("synchronized image payload is missing")?,
                        "image/png",
                    )
                    .ok_or("synchronized image cache is full")?,
            )),
            ClipboardContentKind::Files => None,
        }
    };
    Ok(proto::ClipboardSyncRecord {
        sync_id: record.sync_id,
        kind: clipboard_kind_to_proto(record.kind) as i32,
        width: record.width,
        height: record.height,
        preview: sanitize_text(record.preview, 2 * 1024),
        source_app: record.source_app.map(|value| sanitize_text(value, 256)),
        source_device_id: sanitize_text(record.source_device_id, 128),
        source_device_name: sanitize_text(record.source_device_name, 128),
        captured_at_ms: protocol_timestamp(record.captured_at_ms),
        revision: record.revision.max(1),
        updated_by_device_id: sanitize_text(record.updated_by_device_id, 128),
        favorite: record.favorite,
        favorite_revision: record.favorite_revision,
        favorite_updated_by_device_id: sanitize_text(
            record.favorite_updated_by_device_id,
            128,
        ),
        labels: record
            .labels
            .into_iter()
            .map(|label| proto::ClipboardLabel {
                id: sanitize_text(label.id, 128),
                name: sanitize_text(label.name, 128),
                color: sanitize_text(label.color, 16),
                revision: label.revision,
                updated_by_device_id: sanitize_text(label.updated_by_device_id, 128),
                deleted: label.deleted,
            })
            .collect(),
        label_memberships: record
            .label_memberships
            .into_iter()
            .map(|membership| proto::ClipboardLabelMembership {
                label_id: sanitize_text(membership.label_id, 128),
                attached: membership.attached,
                revision: membership.revision,
                updated_by_device_id: sanitize_text(membership.updated_by_device_id, 128),
            })
            .collect(),
        deleted: record.deleted,
        change_kind: match record.change_kind {
            ClipboardSyncChangeKind::Copy => proto::ClipboardSyncChangeKind::Copy,
            ClipboardSyncChangeKind::Edit => proto::ClipboardSyncChangeKind::Edit,
            ClipboardSyncChangeKind::Favorite => proto::ClipboardSyncChangeKind::Favorite,
            ClipboardSyncChangeKind::Label => proto::ClipboardSyncChangeKind::Label,
            ClipboardSyncChangeKind::Delete => proto::ClipboardSyncChangeKind::Delete,
            ClipboardSyncChangeKind::Snapshot => proto::ClipboardSyncChangeKind::Snapshot,
        } as i32,
        live: record.live,
        text_syntax_json,
        payload,
    })
}

fn sync_record_from_proto(
    record: &proto::ClipboardSyncRecord,
    uploads: &ClipboardUploadStore,
    authenticated_device_id: &str,
    authenticated_device_name: &str,
) -> std::result::Result<arcrelay_core::domain::clipboard::ClipboardSyncRecord, &'static str> {
    use arcrelay_core::domain::clipboard::{ClipboardContentKind, ClipboardSyncChangeKind};
    use arcrelay_core::domain::clipboard_text::detect_text_syntax;
    use proto::clipboard_sync_record::Payload;
    validate_clipboard_sync_record(record)?;
    let kind = match proto::ClipboardContentKind::try_from(record.kind)
        .unwrap_or(proto::ClipboardContentKind::Unspecified)
    {
        proto::ClipboardContentKind::Text => ClipboardContentKind::Text,
        proto::ClipboardContentKind::Html => ClipboardContentKind::Html,
        proto::ClipboardContentKind::Image => ClipboardContentKind::Image,
        proto::ClipboardContentKind::Files => ClipboardContentKind::Files,
        _ => return Err("invalid clipboard sync kind"),
    };
    let (text, html, rtf, image_png) = if record.deleted || kind == ClipboardContentKind::Files {
        (None, None, None, None)
    } else {
        match record.payload.as_ref().expect("validated clipboard payload") {
            Payload::Text(text) => (Some(text.clone()), None, None, None),
            Payload::RichText(rich_text) => {
                let html_ref = rich_text.html.as_ref().expect("validated HTML reference");
                let html = String::from_utf8(
                    uploads
                        .get(
                            &html_ref.blob_id,
                            &html_ref.sha256,
                            "text/html; charset=utf-8",
                        )
                        .ok_or("clipboard HTML upload is unavailable")?,
                )
                .map_err(|_| "clipboard HTML upload is not UTF-8")?;
                let rtf = rich_text
                    .rtf
                    .as_ref()
                    .map(|rtf_ref| {
                        String::from_utf8(
                            uploads
                                .get(&rtf_ref.blob_id, &rtf_ref.sha256, "text/rtf")
                                .ok_or("clipboard RTF upload is unavailable")?,
                        )
                        .map_err(|_| "clipboard RTF upload is not UTF-8")
                    })
                    .transpose()?;
                (Some(rich_text.plain_text.clone()), Some(html), rtf, None)
            }
            Payload::Image(image) => (
                None,
                None,
                None,
                Some(
                    uploads
                        .get(&image.blob_id, &image.sha256, "image/png")
                        .ok_or("clipboard image upload is unavailable")?,
                ),
            ),
        }
    };
    // The receiver is authoritative for syntax classification. Peers only
    // carry the field for display continuity; detection remains Rust-side.
    let text_syntax = detect_text_syntax(text.as_deref().unwrap_or_default());
    let canonical_sync_id = if record.deleted || kind == ClipboardContentKind::Files {
        record.sync_id.clone()
    } else {
        clipboard_sync_id(kind, &text, &html, &rtf, image_png.as_deref())
    };
    Ok(arcrelay_core::domain::clipboard::ClipboardSyncRecord {
        sync_id: canonical_sync_id,
        kind,
        text,
        html,
        rtf,
        image_png,
        width: record.width,
        height: record.height,
        preview: record.preview.clone(),
        source_app: record.source_app.clone(),
        source_device_id: authenticated_device_id.to_string(),
        source_device_name: authenticated_device_name.to_string(),
        captured_at_ms: record.captured_at_ms,
        revision: record.revision,
        updated_by_device_id: authenticated_device_id.to_string(),
        favorite: record.favorite,
        favorite_revision: record.favorite_revision,
        favorite_updated_by_device_id: authenticated_device_id.to_string(),
        // Labels are content-addressed against the canonical receiver-side
        // sync ID. Mobile-originated copy records do not author label state.
        labels: Vec::new(),
        label_memberships: Vec::new(),
        deleted: record.deleted,
        change_kind: match proto::ClipboardSyncChangeKind::try_from(record.change_kind)
            .unwrap_or(proto::ClipboardSyncChangeKind::Unspecified)
        {
            proto::ClipboardSyncChangeKind::Copy => ClipboardSyncChangeKind::Copy,
            proto::ClipboardSyncChangeKind::Edit => ClipboardSyncChangeKind::Edit,
            proto::ClipboardSyncChangeKind::Favorite => ClipboardSyncChangeKind::Favorite,
            proto::ClipboardSyncChangeKind::Label => ClipboardSyncChangeKind::Label,
            proto::ClipboardSyncChangeKind::Delete => ClipboardSyncChangeKind::Delete,
            proto::ClipboardSyncChangeKind::Snapshot => ClipboardSyncChangeKind::Snapshot,
            proto::ClipboardSyncChangeKind::Unspecified => {
                return Err("invalid clipboard sync change kind")
            }
        },
        live: record.live,
        text_syntax,
    })
}

fn clipboard_sync_id(
    kind: arcrelay_core::domain::clipboard::ClipboardContentKind,
    text: &Option<String>,
    html: &Option<String>,
    rtf: &Option<String>,
    image_png: Option<&[u8]>,
) -> String {
    use arcrelay_core::domain::clipboard::ClipboardContentKind;
    use sha2::{Digest, Sha256};
    use xxhash_rust::xxh3::Xxh3;

    let mut content_digest = Xxh3::new();
    match kind {
        ClipboardContentKind::Text => {
            content_digest.update(b"text\0");
            content_digest.update(text.as_deref().unwrap_or_default().as_bytes());
        }
        ClipboardContentKind::Html => {
            content_digest.update(b"rich-text\0");
            content_digest.update(html.as_deref().unwrap_or_default().as_bytes());
            content_digest.update(b"\0text\0");
            content_digest.update(text.as_deref().unwrap_or_default().as_bytes());
            if let Some(rtf) = rtf.as_deref() {
                content_digest.update(b"\0rtf\0");
                content_digest.update(rtf.as_bytes());
            }
        }
        ClipboardContentKind::Image => {
            content_digest.update(b"image\0");
            content_digest.update(image_png.unwrap_or_default());
        }
        ClipboardContentKind::Files => content_digest.update(b"files\0"),
    }
    let content_hash = format!("{:032x}", content_digest.digest128());
    let mut sync_digest = Sha256::new();
    sync_digest.update(b"arcrelay-clipboard-sync-v1\0");
    sync_digest.update(content_hash.as_bytes());
    format!("{:x}", sync_digest.finalize())
}

async fn notification_snapshot(
    provider: Option<&Arc<dyn HostCapabilityProvider>>,
    include_read: bool,
    limit: usize,
) -> HostCapabilityResult<proto::NotificationListSnapshot> {
    let provider = provider.ok_or_else(|| {
        HostCapabilityError::new(
            HostCapabilityErrorCode::Unsupported,
            "notifications are unavailable",
        )
    })?;
    let notifications = provider.list_notifications(include_read, limit).await?;
    Ok(proto::NotificationListSnapshot {
        notifications: notifications
            .into_iter()
            .map(build_notification_info)
            .collect(),
    })
}

fn sanitize_text(value: String, maximum: usize) -> String {
    let sanitized = value
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect();
    truncate_utf8(sanitized, maximum)
}

fn bounded_percent(value: f32) -> f32 {
    if value.is_finite() {
        value.clamp(0.0, 100.0)
    } else {
        0.0
    }
}

fn bounded_nonnegative(value: f64) -> f64 {
    if value.is_finite() {
        value.max(0.0)
    } else {
        0.0
    }
}

fn protocol_u64(value: u64) -> u64 {
    value.min(i64::MAX as u64)
}

fn protocol_timestamp(value: i64) -> i64 {
    if value > 0 {
        value
    } else {
        now_ms().max(1)
    }
}
