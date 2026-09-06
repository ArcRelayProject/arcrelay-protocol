#[allow(clippy::too_many_arguments)]
async fn handle_subscribe(
    request: proto::SubscribeRequest,
    coordinator: &Arc<StateCoordinator>,
    action_provider: &Option<Arc<dyn HostCapabilityProvider>>,
    features: &HashSet<i32>,
    capabilities: &HashSet<CapabilityId>,
    critical_tx: &mpsc::Sender<proto::ServerControlFrame>,
    system_sub: &mut Option<StateSubscription<SystemVersioned>>,
    media_sub: &mut Option<StateSubscription<MediaVersioned>>,
    window_sub: &mut Option<StateSubscription<WindowVersioned>>,
    clipboard_sub: &mut Option<StateSubscription<ClipboardVersioned>>,
    action_catalog_sub: &mut Option<PassiveSubscription>,
    action_output_sub: &mut Option<EventSubscription<OutputLine>>,
    notification_sub: &mut Option<EventSubscription<()>>,
    clipboard_sync_sub: &mut Option<EventSubscription<arcrelay_core::domain::clipboard::ClipboardSyncRecord>>,
) -> Result<()> {
    let topic = proto::SubscriptionTopic::try_from(request.topic)
        .unwrap_or(proto::SubscriptionTopic::Unspecified);
    let (feature, required) = match topic {
        proto::SubscriptionTopic::System => {
            (proto::Feature::System, CapabilityId::SystemRead)
        }
        proto::SubscriptionTopic::Media => {
            (proto::Feature::Media, CapabilityId::MediaRead)
        }
        proto::SubscriptionTopic::Windows => {
            (proto::Feature::Windows, CapabilityId::WindowRead)
        }
        proto::SubscriptionTopic::Clipboard => (
            proto::Feature::Clipboard,
            CapabilityId::ClipboardRead,
        ),
        proto::SubscriptionTopic::Actions => {
            (proto::Feature::Actions, CapabilityId::ActionRead)
        }
        proto::SubscriptionTopic::ActionOutput => {
            (proto::Feature::Actions, CapabilityId::ActionRead)
        }
        proto::SubscriptionTopic::Notifications => (
            proto::Feature::Notifications,
            CapabilityId::NotificationRead,
        ),
        proto::SubscriptionTopic::ClipboardSync => (
            proto::Feature::ClipboardSync,
            CapabilityId::ClipboardSync,
        ),
        proto::SubscriptionTopic::Unspecified => {
            send_subscription_result(
                critical_tx,
                request.request_id,
                0,
                topic,
                0,
                status(
                    proto::ErrorCode::InvalidArgument,
                    "invalid subscription topic",
                ),
            )
            .await?;
            return Ok(());
        }
    };
    if !features.contains(&(proto::Feature::Subscriptions as i32))
        || !features.contains(&(feature as i32))
    {
        send_subscription_result(
            critical_tx,
            request.request_id,
            0,
            topic,
            0,
            status(
                proto::ErrorCode::Unsupported,
                "subscription feature was not negotiated",
            ),
        )
        .await?;
        return Ok(());
    }
    if !capabilities.contains(&required) {
        send_subscription_result(
            critical_tx,
            request.request_id,
            0,
            topic,
            0,
            status(
                proto::ErrorCode::PermissionDenied,
                "subscription capability was not granted",
            ),
        )
        .await?;
        return Ok(());
    }

    let action_receiver = if topic == proto::SubscriptionTopic::ActionOutput {
        match action_provider
            .as_ref()
            .and_then(|provider| provider.subscribe_output())
        {
            Some(receiver) => Some(receiver),
            None => {
                send_subscription_result(
                    critical_tx,
                    request.request_id,
                    0,
                    topic,
                    0,
                    status(
                        proto::ErrorCode::Unavailable,
                        "action output subscription is unavailable",
                    ),
                )
                .await?;
                return Ok(());
            }
        }
    } else {
        None
    };
    let notification_receiver = if topic == proto::SubscriptionTopic::Notifications {
        match action_provider
            .as_ref()
            .and_then(|provider| provider.subscribe_notifications())
        {
            Some(receiver) => Some(receiver),
            None => {
                send_subscription_result(
                    critical_tx,
                    request.request_id,
                    0,
                    topic,
                    0,
                    status(
                        proto::ErrorCode::Unavailable,
                        "notification subscription is unavailable",
                    ),
                )
                .await?;
                return Ok(());
            }
        }
    } else {
        None
    };
    let clipboard_sync_receiver = if topic == proto::SubscriptionTopic::ClipboardSync {
        match coordinator.service().clipboard.subscribe_sync_changes() {
            Some(receiver) => Some(receiver),
            None => {
                send_subscription_result(
                    critical_tx,
                    request.request_id,
                    0,
                    topic,
                    0,
                    status(
                        proto::ErrorCode::Unavailable,
                        "clipboard sync subscription is unavailable",
                    ),
                )
                .await?;
                return Ok(());
            }
        }
    } else {
        None
    };

    let active_subscription_ids = [
        system_sub.as_ref().map(|sub| sub.id),
        media_sub.as_ref().map(|sub| sub.id),
        window_sub.as_ref().map(|sub| sub.id),
        clipboard_sub.as_ref().map(|sub| sub.id),
        action_catalog_sub.as_ref().map(|sub| sub.id),
        action_output_sub.as_ref().map(|sub| sub.id),
        notification_sub.as_ref().map(|sub| sub.id),
        clipboard_sync_sub.as_ref().map(|sub| sub.id),
    ];
    let subscription_id = loop {
        let candidate = random_nonzero_u64();
        if !active_subscription_ids.contains(&Some(candidate)) {
            break candidate;
        }
    };
    match topic {
        proto::SubscriptionTopic::System => {
            *system_sub = Some(StateSubscription {
                id: subscription_id,
                sequence: 0,
                receiver: coordinator.subscribe_system(),
            });
        }
        proto::SubscriptionTopic::Media => {
            *media_sub = Some(StateSubscription {
                id: subscription_id,
                sequence: 0,
                receiver: coordinator.subscribe_media(),
            });
        }
        proto::SubscriptionTopic::Windows => {
            *window_sub = Some(StateSubscription {
                id: subscription_id,
                sequence: 0,
                receiver: coordinator.subscribe_windows(),
            });
        }
        proto::SubscriptionTopic::Clipboard => {
            *clipboard_sub = Some(StateSubscription {
                id: subscription_id,
                sequence: 0,
                receiver: coordinator.subscribe_clipboard(),
            });
        }
        proto::SubscriptionTopic::Actions => {
            *action_catalog_sub = Some(PassiveSubscription {
                id: subscription_id,
                sequence: 0,
            });
        }
        proto::SubscriptionTopic::ActionOutput => {
            *action_output_sub = Some(EventSubscription {
                id: subscription_id,
                sequence: 0,
                receiver: action_receiver.expect("actions receiver was validated"),
                action_gap: ActionGap::default(),
            });
        }
        proto::SubscriptionTopic::Notifications => {
            *notification_sub = Some(EventSubscription {
                id: subscription_id,
                sequence: 0,
                receiver: notification_receiver.expect("notification receiver was validated"),
                action_gap: ActionGap::default(),
            });
        }
        proto::SubscriptionTopic::ClipboardSync => {
            *clipboard_sync_sub = Some(EventSubscription {
                id: subscription_id,
                sequence: 0,
                receiver: clipboard_sync_receiver.expect("clipboard sync receiver was validated"),
                action_gap: ActionGap::default(),
            });
        }
        proto::SubscriptionTopic::Unspecified => unreachable!(),
    }
    send_subscription_result(
        critical_tx,
        request.request_id,
        subscription_id,
        topic,
        1,
        ok_status(),
    )
    .await?;

    let initial = match topic {
        proto::SubscriptionTopic::System => {
            coordinator.latest_system().await.map(|value| {
                event_frame(
                    subscription_id,
                    1,
                    value.captured_at_ms,
                    proto::event::Data::System(build_system_snapshot(value.data.clone())),
                )
            })
        }
        proto::SubscriptionTopic::Media => {
            coordinator.latest_media().await.map(|value| {
                event_frame(
                    subscription_id,
                    1,
                    value.captured_at_ms,
                    proto::event::Data::Media(build_media_snapshot(&value.data)),
                )
            })
        }
        proto::SubscriptionTopic::Windows => {
            let spaces = current_spaces(coordinator).await;
            coordinator.latest_windows().await.map(|value| {
                event_frame(
                    subscription_id,
                    1,
                    value.captured_at_ms,
                    proto::event::Data::Windows(proto::WindowListSnapshot {
                        windows: build_window_list(value.data.clone(), None, None, false),
                        spaces,
                    }),
                )
            })
        }
        proto::SubscriptionTopic::Clipboard => coordinator.latest_clipboard().await.and_then(|value| {
                let snapshot = match build_clipboard_snapshot(
                    value.data.history.clone(),
                    value.data.policy.clone(),
                    None,
                    true,
                ) {
                    Ok(snapshot) => snapshot,
                    Err(message) => {
                        warn!(%message, "skipping initial clipboard event");
                        return None;
                    }
                };
                clipboard_event_frame(subscription_id, 1, value.captured_at_ms, snapshot)
            }),
        proto::SubscriptionTopic::Actions => Some(event_frame(
            subscription_id,
            1,
            now_ms(),
            proto::event::Data::Actions(build_action_snapshot(action_provider.as_ref()).await),
        )),
        proto::SubscriptionTopic::ActionOutput => None,
        proto::SubscriptionTopic::Notifications => {
            match notification_snapshot(action_provider.as_ref(), false, 100).await {
                Ok(snapshot) => Some(event_frame(
                    subscription_id,
                    1,
                    now_ms(),
                    proto::event::Data::Notifications(snapshot),
                )),
                Err(error) => {
                    warn!(%error, "skipping initial notification event");
                    None
                }
            }
        }
        proto::SubscriptionTopic::ClipboardSync => None,
        proto::SubscriptionTopic::Unspecified => None,
    };
    if let Some(frame) = initial {
        match topic {
            proto::SubscriptionTopic::System => system_sub.as_mut().map(|sub| sub.sequence = 1),
            proto::SubscriptionTopic::Media => media_sub.as_mut().map(|sub| sub.sequence = 1),
            proto::SubscriptionTopic::Windows => window_sub.as_mut().map(|sub| sub.sequence = 1),
            proto::SubscriptionTopic::Clipboard => {
                clipboard_sub.as_mut().map(|sub| sub.sequence = 1)
            }
            proto::SubscriptionTopic::Actions => {
                action_catalog_sub.as_mut().map(|sub| sub.sequence = 1)
            }
            proto::SubscriptionTopic::Notifications => {
                notification_sub.as_mut().map(|sub| sub.sequence = 1)
            }
            proto::SubscriptionTopic::ClipboardSync => None,
            _ => None,
        };
        // A successful subscription always delivers its baseline after the
        // result on the reliable critical queue. Later replaceable snapshots
        // may use the bounded state queue and recover from sequence gaps.
        send_critical(critical_tx, frame).await?;
    }
    Ok(())
}

async fn send_subscription_result(
    sender: &mpsc::Sender<proto::ServerControlFrame>,
    request_id: u64,
    subscription_id: u64,
    topic: proto::SubscriptionTopic,
    next_sequence: u64,
    result: proto::Status,
) -> Result<()> {
    send_critical(
        sender,
        server_frame(proto::server_control_frame::Body::SubscriptionResult(
            proto::SubscriptionResult {
                request_id,
                status: Some(result),
                subscription_id,
                topic: topic as i32,
                next_sequence,
            },
        )),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn handle_unsubscribe(
    request: proto::UnsubscribeRequest,
    critical_tx: &mpsc::Sender<proto::ServerControlFrame>,
    system_sub: &mut Option<StateSubscription<SystemVersioned>>,
    media_sub: &mut Option<StateSubscription<MediaVersioned>>,
    window_sub: &mut Option<StateSubscription<WindowVersioned>>,
    clipboard_sub: &mut Option<StateSubscription<ClipboardVersioned>>,
    action_catalog_sub: &mut Option<PassiveSubscription>,
    action_output_sub: &mut Option<EventSubscription<OutputLine>>,
    notification_sub: &mut Option<EventSubscription<()>>,
    clipboard_sync_sub: &mut Option<EventSubscription<arcrelay_core::domain::clipboard::ClipboardSyncRecord>>,
) -> Result<()> {
    if request.subscription_id == 0 {
        return send_subscription_result(
            critical_tx,
            request.request_id,
            0,
            proto::SubscriptionTopic::Unspecified,
            0,
            status(
                proto::ErrorCode::InvalidArgument,
                "subscription id must be non-zero",
            ),
        )
        .await;
    }
    let mut topic = proto::SubscriptionTopic::Unspecified;
    let mut found = false;
    if system_sub
        .as_ref()
        .is_some_and(|sub| sub.id == request.subscription_id)
    {
        *system_sub = None;
        topic = proto::SubscriptionTopic::System;
        found = true;
    }
    if media_sub
        .as_ref()
        .is_some_and(|sub| sub.id == request.subscription_id)
    {
        *media_sub = None;
        topic = proto::SubscriptionTopic::Media;
        found = true;
    }
    if window_sub
        .as_ref()
        .is_some_and(|sub| sub.id == request.subscription_id)
    {
        *window_sub = None;
        topic = proto::SubscriptionTopic::Windows;
        found = true;
    }
    if clipboard_sub
        .as_ref()
        .is_some_and(|sub| sub.id == request.subscription_id)
    {
        *clipboard_sub = None;
        topic = proto::SubscriptionTopic::Clipboard;
        found = true;
    }
    if action_catalog_sub
        .as_ref()
        .is_some_and(|sub| sub.id == request.subscription_id)
    {
        *action_catalog_sub = None;
        topic = proto::SubscriptionTopic::Actions;
        found = true;
    }
    if action_output_sub
        .as_ref()
        .is_some_and(|sub| sub.id == request.subscription_id)
    {
        *action_output_sub = None;
        topic = proto::SubscriptionTopic::ActionOutput;
        found = true;
    }
    if notification_sub
        .as_ref()
        .is_some_and(|sub| sub.id == request.subscription_id)
    {
        *notification_sub = None;
        topic = proto::SubscriptionTopic::Notifications;
        found = true;
    }
    if clipboard_sync_sub
        .as_ref()
        .is_some_and(|sub| sub.id == request.subscription_id)
    {
        *clipboard_sync_sub = None;
        topic = proto::SubscriptionTopic::ClipboardSync;
        found = true;
    }
    send_subscription_result(
        critical_tx,
        request.request_id,
        request.subscription_id,
        topic,
        0,
        if found {
            ok_status()
        } else {
            status(proto::ErrorCode::NotFound, "subscription was not found")
        },
    )
    .await
}

fn event_frame(
    subscription_id: u64,
    sequence: u64,
    captured_at_ms: i64,
    data: proto::event::Data,
) -> proto::ServerControlFrame {
    server_frame(proto::server_control_frame::Body::Event(proto::Event {
        subscription_id,
        sequence,
        captured_at_ms: protocol_timestamp(captured_at_ms),
        data: Some(data),
    }))
}

fn queue_event(
    sender: &mpsc::Sender<proto::ServerControlFrame>,
    frame: proto::ServerControlFrame,
    action_gap: &mut ActionGap,
) {
    if frame.encoded_len() > MAX_CONTROL_FRAME_SIZE {
        remember_action_gap(frame, Some(action_gap));
    } else if let Err(mpsc::error::TrySendError::Full(frame)) = sender.try_send(frame) {
        remember_action_gap(frame, Some(action_gap));
    }
}

fn remember_action_gap(frame: proto::ServerControlFrame, action_gap: Option<&mut ActionGap>) {
    let Some(action_gap) = action_gap else {
        return;
    };
    if let Some(proto::server_control_frame::Body::Event(event)) = frame.body {
        match event.data {
            Some(proto::event::Data::Gap(dropped_gap)) => {
                action_gap.remember_gap(dropped_gap);
            }
            Some(proto::event::Data::ActionOutput(output)) => {
                action_gap.remember_resource(event.sequence, output.action_id);
            }
            _ => {
                action_gap.remember_sequence(event.sequence);
                action_gap.resync_all = true;
            }
        }
    }
}

fn push_system_event(
    sender: &LatestStateQueue,
    sub: &mut StateSubscription<SystemVersioned>,
    value: Arc<SystemVersioned>,
) {
    let sequence = sub.next_sequence();
    let frame = event_frame(
        sub.id,
        sequence,
        value.captured_at_ms,
        proto::event::Data::System(build_system_snapshot(value.data.clone())),
    );
    sender.replace(frame);
}

fn push_media_event(
    sender: &LatestStateQueue,
    sub: &mut StateSubscription<MediaVersioned>,
    value: Arc<MediaVersioned>,
) {
    let sequence = sub.next_sequence();
    let frame = event_frame(
        sub.id,
        sequence,
        value.captured_at_ms,
        proto::event::Data::Media(build_media_snapshot(&value.data)),
    );
    sender.replace(frame);
}

fn push_window_event(
    sender: &LatestStateQueue,
    sub: &mut StateSubscription<WindowVersioned>,
    value: Arc<WindowVersioned>,
    spaces: Vec<proto::SpaceInfo>,
) {
    let sequence = sub.next_sequence();
    let frame = event_frame(
        sub.id,
        sequence,
        value.captured_at_ms,
        proto::event::Data::Windows(proto::WindowListSnapshot {
            windows: build_window_list(value.data.clone(), None, None, false),
            spaces,
        }),
    );
    sender.replace(frame);
}

fn push_clipboard_event(
    sender: &LatestStateQueue,
    sub: &mut StateSubscription<ClipboardVersioned>,
    value: Arc<ClipboardVersioned>,
) {
    let snapshot = match build_clipboard_snapshot(
        value.data.history.clone(),
        value.data.policy.clone(),
        None,
        true,
    ) {
        Ok(snapshot) => snapshot,
        Err(message) => {
            warn!(%message, "skipping clipboard event");
            return;
        }
    };
    let sequence = sub.sequence.saturating_add(1);
    let Some(frame) = clipboard_event_frame(sub.id, sequence, value.captured_at_ms, snapshot)
    else {
        return;
    };
    sub.sequence = sequence;
    sender.replace(frame);
}

fn build_clipboard_snapshot(
    history: Vec<arcrelay_core::domain::clipboard::ClipboardSummary>,
    policy: arcrelay_core::domain::clipboard::ClipboardPolicy,
    next_cursor: Option<arcrelay_core::domain::clipboard::ClipboardCursor>,
    skip_oversized_history: bool,
) -> std::result::Result<proto::ClipboardSnapshot, &'static str> {
    let mut seen_ids = HashSet::new();
    let mut bounded_history = Vec::with_capacity(history.len().min(MAX_CLIPBOARD_ITEMS));
    for entry in history.into_iter().take(MAX_CLIPBOARD_ITEMS) {
        if entry.preview.len() > MAX_CLIPBOARD_PREVIEW_BYTES {
            if skip_oversized_history {
                warn!(
                    clipboard_entry_id = entry.id,
                    "omitting clipboard summary larger than 2 KiB"
                );
                continue;
            }
            return Err("clipboard summary exceeds the 2 KiB protocol limit");
        }
        let timestamp_ms = entry.captured_at.timestamp_millis();
        if entry.id == 0
            || entry.id > i64::MAX as u64
            || timestamp_ms <= 0
            || !seen_ids.insert(entry.id)
        {
            warn!(
                clipboard_entry_id = entry.id,
                "omitting clipboard history entry with invalid metadata"
            );
            continue;
        }
        let mut converted = clipboard_summary_to_proto(entry);
        converted.source_app = converted.source_app.take().and_then(|source_app| {
            let source_app = truncate_utf8(source_app, 256);
            (!source_app.chars().any(char::is_control)).then_some(source_app)
        });
        converted.timestamp_ms = timestamp_ms;
        bounded_history.push(converted);
    }

    Ok(proto::ClipboardSnapshot {
        history: bounded_history,
        next_cursor_timestamp_ms: next_cursor.map(|cursor| cursor.sort_at_ms),
        next_cursor_id: next_cursor.map(|cursor| cursor.id),
        policy: Some(clipboard_policy_to_proto(policy)),
        labels: Vec::new(),
    })
}

fn clipboard_event_frame(
    subscription_id: u64,
    sequence: u64,
    captured_at_ms: i64,
    mut snapshot: proto::ClipboardSnapshot,
) -> Option<proto::ServerControlFrame> {
    loop {
        let frame = event_frame(
            subscription_id,
            sequence,
            captured_at_ms,
            proto::event::Data::Clipboard(snapshot.clone()),
        );
        if frame.encoded_len() <= MAX_CONTROL_FRAME_SIZE {
            return Some(frame);
        }
        if snapshot.history.pop().is_none() {
            warn!("skipping clipboard event that exceeds the control-frame limit");
            return None;
        }
    }
}

fn push_action_output_event(
    sender: &mpsc::Sender<proto::ServerControlFrame>,
    sub: &mut EventSubscription<OutputLine>,
    output: OutputLine,
) {
    if !valid_control_identifier(&output.action_id) {
        warn!("discarding action output with an invalid action id");
        return;
    }
    let sequence = sub.next_sequence();
    let data = if sub.action_gap.first_missing_sequence.is_some() {
        // This output is replaced by the gap marker, so it must be included in
        // the resources the client refreshes along with any previously dropped
        // action output.
        sub.action_gap.remember_resource(sequence, output.action_id);
        let first_missing = sub
            .action_gap
            .first_missing_sequence
            .take()
            .expect("action gap was checked");
        let resource_ids = sub.action_gap.take_resource_ids();
        proto::event::Data::Gap(proto::EventGap {
            first_missing_sequence: first_missing,
            next_available_sequence: sequence.saturating_add(1),
            resync_required: true,
            resource_ids,
        })
    } else {
        proto::event::Data::ActionOutput(proto::ActionOutputChunk {
            action_id: output.action_id,
            lines: vec![truncate_utf8(output.text, MAX_ACTION_OUTPUT_LINE_BYTES)],
        })
    };
    queue_event(
        sender,
        event_frame(sub.id, sequence, now_ms(), data),
        &mut sub.action_gap,
    );
}

fn push_clipboard_sync_event(
    sender: &mpsc::Sender<proto::ServerControlFrame>,
    sub: &mut EventSubscription<arcrelay_core::domain::clipboard::ClipboardSyncRecord>,
    record: arcrelay_core::domain::clipboard::ClipboardSyncRecord,
    blob_store: &BlobStore,
    blob_owner: &[u8],
) {
    let record = match sync_record_to_proto(record, blob_store, blob_owner) {
        Ok(record) => record,
        Err(message) => {
            warn!(%message, "skipping clipboard sync event");
            return;
        }
    };
    let sequence = sub.next_sequence();
    queue_event(
        sender,
        event_frame(
            sub.id,
            sequence,
            now_ms(),
            proto::event::Data::ClipboardSync(record),
        ),
        &mut sub.action_gap,
    );
}

async fn push_notification_event(
    sender: &LatestStateQueue,
    sub: &mut EventSubscription<()>,
    provider: Option<&Arc<dyn HostCapabilityProvider>>,
) {
    let snapshot = match notification_snapshot(provider, false, 100).await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            warn!(%error, "skipping notification event");
            return;
        }
    };
    let sequence = sub.next_sequence();
    sender.replace(event_frame(
        sub.id,
        sequence,
        now_ms(),
        proto::event::Data::Notifications(snapshot),
    ));
}

async fn current_spaces(coordinator: &StateCoordinator) -> Vec<proto::SpaceInfo> {
    build_space_list(coordinator.spaces().await)
}

async fn recv_system(
    sub: &mut Option<StateSubscription<SystemVersioned>>,
) -> Option<Arc<SystemVersioned>> {
    match sub.as_mut() {
        Some(sub) => recv_latest(&mut sub.receiver).await,
        None => std::future::pending().await,
    }
}

async fn recv_media(
    sub: &mut Option<StateSubscription<MediaVersioned>>,
) -> Option<Arc<MediaVersioned>> {
    match sub.as_mut() {
        Some(sub) => recv_latest(&mut sub.receiver).await,
        None => std::future::pending().await,
    }
}

async fn recv_windows(
    sub: &mut Option<StateSubscription<WindowVersioned>>,
) -> Option<Arc<WindowVersioned>> {
    match sub.as_mut() {
        Some(sub) => recv_latest(&mut sub.receiver).await,
        None => std::future::pending().await,
    }
}

async fn recv_clipboard(
    sub: &mut Option<StateSubscription<ClipboardVersioned>>,
) -> Option<Arc<ClipboardVersioned>> {
    match sub.as_mut() {
        Some(sub) => recv_latest(&mut sub.receiver).await,
        None => std::future::pending().await,
    }
}

async fn recv_latest<T: Clone>(receiver: &mut watch::Receiver<Option<Arc<T>>>) -> Option<Arc<T>> {
    receiver.changed().await.ok()?;
    receiver.borrow_and_update().clone()
}

async fn recv_action_output(sub: &mut Option<EventSubscription<OutputLine>>) -> Option<OutputLine> {
    match sub.as_mut() {
        Some(sub) => match sub.receiver.recv().await {
            Ok(value) => Some(value),
            Err(broadcast::error::RecvError::Lagged(count)) => {
                sub.action_gap
                    .remember_sequence(sub.sequence.saturating_add(1));
                sub.action_gap.resync_all = true;
                warn!(count, "action output subscription lagged");
                None
            }
            Err(_) => None,
        },
        None => std::future::pending().await,
    }
}

async fn recv_notification_change(sub: &mut Option<EventSubscription<()>>) -> Option<()> {
    match sub.as_mut() {
        Some(sub) => match sub.receiver.recv().await {
            Ok(()) => Some(()),
            Err(broadcast::error::RecvError::Lagged(count)) => {
                warn!(count, "notification subscription lagged; sending latest snapshot");
                Some(())
            }
            Err(_) => None,
        },
        None => std::future::pending().await,
    }
}

async fn recv_clipboard_sync(
    sub: &mut Option<
        EventSubscription<arcrelay_core::domain::clipboard::ClipboardSyncRecord>,
    >,
) -> Option<arcrelay_core::domain::clipboard::ClipboardSyncRecord> {
    match sub.as_mut() {
        Some(sub) => match sub.receiver.recv().await {
            Ok(record) => Some(record),
            Err(broadcast::error::RecvError::Lagged(count)) => {
                warn!(count, "clipboard sync subscription lagged; merge is required");
                None
            }
            Err(_) => None,
        },
        None => std::future::pending().await,
    }
}
