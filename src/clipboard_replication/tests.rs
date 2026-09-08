use super::*;
use arcrelay_core::{
    domain::clipboard::{
        ClipboardContentKind, ClipboardLabelMembership, ClipboardQuery, ClipboardSyncChangeKind,
        ClipboardTextSyntax,
    },
    infrastructure::clipboard_test_support::TestClipboardRepository,
};
use arcrelay_network::{NetworkRuntime, NetworkRuntimeConfig, PeerAdvertisement, SessionKind};
use std::{
    path::Path,
    sync::atomic::{AtomicUsize, Ordering},
};
use tokio::io::AsyncReadExt;

fn record(index: usize, device: &str) -> ClipboardReplicaRecord {
    ClipboardReplicaRecord {
        first_captured_at_ms: 1_700_000_000_000 + index as i64,
        copy_count: 1,
        record: ClipboardSyncRecord {
            sync_id: format!("{index:064x}"),
            kind: ClipboardContentKind::Text,
            text: Some(format!("record {index}")),
            html: None,
            rtf: None,
            image_png: None,
            width: None,
            height: None,
            preview: format!("record {index}"),
            source_app: Some("Test".into()),
            source_device_id: device.into(),
            source_device_name: device.into(),
            captured_at_ms: 1_700_000_000_000 + index as i64,
            revision: 1,
            updated_by_device_id: device.into(),
            favorite: false,
            favorite_revision: 1,
            favorite_updated_by_device_id: device.into(),
            labels: vec![],
            label_memberships: vec![],
            deleted: false,
            change_kind: ClipboardSyncChangeKind::Snapshot,
            live: false,
            text_syntax: ClipboardTextSyntax::Plain,
        },
    }
}
fn label(id: &str) -> ClipboardLabel {
    ClipboardLabel {
        id: id.into(),
        name: format!("项目 {id}"),
        color: "#3388ff".into(),
        revision: 1,
        updated_by_device_id: "left".into(),
        deleted: false,
    }
}
async fn database(path: Option<&Path>) -> Arc<ClipboardApplicationService> {
    let service = TestClipboardRepository::open(path).await.unwrap().service();
    let mut policy = service.policy().await.unwrap();
    policy.retention_days = 0;
    policy.max_items = 10_000;
    service.update_policy(policy).await.unwrap();
    service
}
async fn network(directory: &Path, name: &str) -> Arc<NetworkRuntime> {
    let mut config = NetworkRuntimeConfig::new(
        directory.to_owned(),
        arcrelay_network::DeviceMetadata {
            name: name.into(),
            platform: "test".into(),
            model: "test".into(),
        },
        Arc::new(arcrelay_peer::InMemoryPeerRepository::default()),
    );
    config.listen_address = "127.0.0.1".parse().unwrap();
    NetworkRuntime::bind(config).await.unwrap()
}
fn advertisement(runtime: &NetworkRuntime) -> PeerAdvertisement {
    let address =
        std::net::SocketAddr::new("127.0.0.1".parse().unwrap(), runtime.local_port().unwrap());
    PeerAdvertisement {
        device_id: runtime.device_id(),
        public_key: runtime.public_key(),
        metadata: runtime.metadata(),
        addresses: vec![address.ip()],
        connection_addresses: vec![address],
        port: address.port(),
        certificate_sha256: runtime.certificate_sha256(),
        last_seen_at_ms: 1,
    }
}
type ScanHook =
    Arc<tokio::sync::Mutex<Option<(Arc<ClipboardApplicationService>, ClipboardReplicaRecord)>>>;
struct Link {
    left: PeerClient,
    right: PeerClient,
    fetches: Arc<AtomicUsize>,
    after_scan: ScanHook,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    _networks: Vec<Arc<NetworkRuntime>>,
    _directory: tempfile::TempDir,
}
impl Drop for Link {
    fn drop(&mut self) {
        self.left.connection.close(0u32.into(), b"test complete");
        for task in &self.tasks {
            task.abort();
        }
    }
}
async fn link(
    left: Arc<ClipboardApplicationService>,
    right: Arc<ClipboardApplicationService>,
) -> Link {
    let directory = tempfile::tempdir().unwrap();
    let a = network(&directory.path().join("a"), "A").await;
    let b = network(&directory.path().join("b"), "B").await;
    let mut incoming = b.subscribe();
    let pairing = a
        .connect(&advertisement(&b), SessionKind::Pairing)
        .await
        .unwrap();
    let responder = incoming.recv().await.unwrap();
    a.confirm_pairing(&pairing).await.unwrap();
    b.confirm_pairing(&responder).await.unwrap();
    pairing.close("paired");
    let initiator = a
        .connect(&advertisement(&b), SessionKind::Control)
        .await
        .unwrap();
    let responder = incoming.recv().await.unwrap();
    let left_connection = initiator.transport_handle();
    let right_connection = responder.transport_handle();
    let fetches = Arc::new(AtomicUsize::new(0));
    let after_scan = Arc::new(tokio::sync::Mutex::new(
        None::<(Arc<ClipboardApplicationService>, ClipboardReplicaRecord)>,
    ));
    let tasks = [
        (left_connection.clone(), left),
        (right_connection.clone(), right),
    ]
    .into_iter()
    .map(|(connection, service)| {
        let fetches = fetches.clone();
        let after_scan = after_scan.clone();
        tokio::spawn(async move {
            let mut requests = tokio::task::JoinSet::new();
            while let Ok((mut send, mut recv)) = connection.accept_bi().await {
                let service = service.clone();
                let fetches = fetches.clone();
                let after_scan = after_scan.clone();
                requests.spawn(async move {
                    assert_eq!(recv.read_u8().await.unwrap(), STREAM_KIND_CLIPBOARD_REPLICA);
                    let request: proto::ClipboardReplicaRequest = read(&mut recv).await.unwrap();
                    let is_scan = matches!(
                        request.body,
                        Some(proto::clipboard_replica_request::Body::Scan(_))
                    );
                    if matches!(
                        request.body,
                        Some(proto::clipboard_replica_request::Body::Fetch(_))
                    ) {
                        fetches.fetch_add(1, Ordering::SeqCst);
                    }
                    if let Err(error) = serve_request(&mut send, &mut recv, &service, request).await
                    {
                        write(&mut send, &failure(&error)).await.unwrap();
                    }
                    if is_scan {
                        if let Some((service, record)) = after_scan.lock().await.take() {
                            service.apply_replica_record(record).await.unwrap();
                        }
                    }
                    let _ = send.finish();
                });
                while requests.try_join_next().is_some() {}
            }
        })
    })
    .collect();
    Link {
        left: PeerClient::new(left_connection),
        right: PeerClient::new(right_connection),
        fetches,
        after_scan,
        tasks,
        _networks: vec![a, b],
        _directory: directory,
    }
}
async fn manifest(service: &ClipboardApplicationService) -> Vec<ClipboardReplicaRecord> {
    let mut records = local_manifest(service)
        .await
        .unwrap()
        .into_values()
        .collect::<Vec<_>>();
    records.sort_by(|a, b| a.record.sync_id.cmp(&b.record.sync_id));
    records
}
async fn assert_same(left: &ClipboardApplicationService, right: &ClipboardApplicationService) {
    let left = manifest(left)
        .await
        .into_iter()
        .map(metadata_to_wire)
        .collect::<Vec<_>>();
    let right = manifest(right)
        .await
        .into_iter()
        .map(metadata_to_wire)
        .collect::<Vec<_>>();
    assert_eq!(left, right);
}

#[tokio::test]
async fn newest_first_history_exceeds_old_cache_and_repeated_sync_is_idempotent() {
    let left = database(None).await;
    let right = database(None).await;
    // More than three pages plus >32 MiB of image data. Recent records include
    // old local IDs that were recopied, favorites and a label membership.
    for index in 1..=305 {
        left.apply_replica_record(record(index, "left"))
            .await
            .unwrap();
    }
    for index in 306..=311 {
        let mut image = record(index, "left");
        image.record.kind = ClipboardContentKind::Image;
        image.record.text = None;
        image.record.width = Some(16);
        image.record.height = Some(16);
        image.record.image_png = Some(vec![index as u8; 6 * 1024 * 1024]);
        left.apply_replica_record(image).await.unwrap();
    }
    let mut recent = record(1, "left");
    recent.record.captured_at_ms += 10_000;
    recent.record.revision = 9;
    recent.copy_count = 9;
    recent.record.favorite = true;
    recent.record.favorite_revision = 3;
    recent.record.label_memberships = vec![ClipboardLabelMembership {
        label_id: "tagged".into(),
        attached: true,
        revision: 2,
        updated_by_device_id: "left".into(),
    }];
    left.apply_replica_labels(vec![label("tagged"), label("unused")])
        .await
        .unwrap();
    left.apply_replica_record(recent.clone()).await.unwrap();
    let mut long = record(312, "left");
    long.record.text = Some("x".repeat(900 * 1024));
    left.apply_replica_record(long).await.unwrap();
    assert_eq!(
        left.replica_page(None, 10).await.unwrap().records[0]
            .record
            .sync_id,
        recent.record.sync_id
    );
    let connection = link(left.clone(), right.clone()).await;
    // Reconcile from the responder, exercising the same connection in reverse.
    let result = reconcile(right.clone(), connection.right.clone())
        .await
        .unwrap();
    assert!(result.converged, "{result:?}");
    assert_eq!(result.received, 312);
    assert_eq!(result.failed, 0);
    assert_eq!(result.labels_received, 2);
    assert_eq!(result.total_records, 312);
    assert_same(&left, &right).await;
    let item = right
        .replica_record(&recent.record.sync_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item.first_captured_at_ms, recent.first_captured_at_ms);
    assert_eq!(item.copy_count, 9);
    let fetches = connection.fetches.load(Ordering::SeqCst);
    for _ in 0..2 {
        let result = reconcile(left.clone(), connection.left.clone())
            .await
            .unwrap();
        assert!(result.converged);
        assert_eq!(
            result.received + result.sent + result.labels_received + result.labels_sent,
            0
        );
        assert_eq!(result.total_records, 312);
    }
    assert_eq!(
        connection.fetches.load(Ordering::SeqCst),
        fetches,
        "unchanged payloads must not be downloaded again"
    );
}

#[tokio::test]
async fn labels_detaches_deletes_and_metadata_updates_need_no_payload_and_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("right.sqlite");
    let left = database(None).await;
    let right = database(Some(&path)).await;
    let initial = record(1, "left");
    left.apply_replica_record(initial.clone()).await.unwrap();
    let connection = link(left.clone(), right.clone()).await;
    assert!(
        reconcile(right.clone(), connection.right.clone())
            .await
            .unwrap()
            .converged
    );
    let fetches = connection.fetches.load(Ordering::SeqCst);
    left.apply_replica_labels(vec![label("unused"), label("tagged")])
        .await
        .unwrap();
    let mut tagged = initial.clone();
    tagged.record.text = None;
    tagged.record.favorite = true;
    tagged.record.favorite_revision = 2;
    tagged.record.label_memberships = vec![ClipboardLabelMembership {
        label_id: "tagged".into(),
        attached: true,
        revision: 1,
        updated_by_device_id: "left".into(),
    }];
    left.apply_replica_record(tagged.clone()).await.unwrap();
    let result = reconcile(left.clone(), connection.left.clone())
        .await
        .unwrap();
    assert!(result.converged, "{result:?}");
    assert_eq!(result.sent, 1);
    assert_eq!(result.labels_sent, 2);
    let mut detached = tagged;
    detached.record.label_memberships[0].revision = 2;
    detached.record.label_memberships[0].attached = false;
    right.apply_replica_record(detached).await.unwrap();
    let mut deleted_label = label("unused");
    deleted_label.revision = 2;
    deleted_label.deleted = true;
    right
        .apply_replica_labels(vec![deleted_label])
        .await
        .unwrap();
    assert!(
        reconcile(left.clone(), connection.left.clone())
            .await
            .unwrap()
            .converged
    );
    assert_same(&left, &right).await;
    assert_eq!(connection.fetches.load(Ordering::SeqCst), fetches);
    drop(connection);
    drop(right);
    let right = database(Some(&path)).await;
    assert_same(&left, &right).await;
    assert!(right
        .replica_labels()
        .await
        .unwrap()
        .iter()
        .any(|l| l.id == "unused" && l.deleted));
    let mut deleted = initial;
    deleted.record.deleted = true;
    deleted.record.revision = 3;
    deleted.record.text = None;
    left.apply_replica_record(deleted).await.unwrap();
    let connection = link(left.clone(), right.clone()).await;
    let result = reconcile(right.clone(), connection.right.clone())
        .await
        .unwrap();
    assert!(result.converged, "{result:?}");
    assert_eq!(result.total_records, 0);
    assert_same(&left, &right).await;
}

#[tokio::test]
async fn three_device_union_converges_with_stable_order_regardless_of_insertion_order() {
    let a = database(None).await;
    let b = database(None).await;
    let c = database(None).await;
    for (service, indexes) in [(&a, vec![3, 1]), (&b, vec![2, 3]), (&c, vec![4, 2, 1])] {
        for index in indexes {
            let mut r = record(index, "origin");
            r.record.captured_at_ms = 1_700_000_000_100;
            r.first_captured_at_ms = r.record.captured_at_ms;
            service.apply_replica_record(r).await.unwrap();
        }
    }
    let ab = link(a.clone(), b.clone()).await;
    let ac = link(a.clone(), c.clone()).await;
    for _ in 0..2 {
        assert!(
            reconcile(a.clone(), ab.left.clone())
                .await
                .unwrap()
                .converged
        );
        assert!(
            reconcile(a.clone(), ac.left.clone())
                .await
                .unwrap()
                .converged
        );
    }
    assert_same(&a, &b).await;
    assert_same(&b, &c).await;
    let mut orders = vec![];
    for service in [&a, &b, &c] {
        let mut query = ClipboardQuery::recent(2);
        let mut ids = vec![];
        loop {
            let page = service.history(query.clone()).await.unwrap();
            assert_eq!(page.total_count, Some(4));
            ids.extend(page.entries.iter().map(|r| r.sync_id.clone()));
            let Some(cursor) = page.next_cursor else {
                break;
            };
            query.cursor = Some(cursor);
        }
        orders.push(ids);
    }
    assert_eq!(orders[0], orders[1]);
    assert_eq!(orders[1], orders[2]);
    assert_eq!(
        orders[0],
        (1..=4)
            .rev()
            .map(|i| format!("{i:064x}"))
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn one_oversized_record_is_reported_without_blocking_other_records() {
    let a = database(None).await;
    let b = database(None).await;
    let mut huge = record(3, "a");
    huge.record.text = Some("x".repeat(1024 * 1024 + 1));
    a.apply_replica_record(huge).await.unwrap();
    a.apply_replica_record(record(2, "a")).await.unwrap();
    a.apply_replica_record(record(4, "a")).await.unwrap();
    let connection = link(a.clone(), b.clone()).await;
    let result = reconcile(b.clone(), connection.right.clone())
        .await
        .unwrap();
    assert!(!result.converged);
    assert_eq!(result.failed, 1);
    assert_eq!(result.received, 2);
    assert_eq!(result.total_records, 2);
    assert_eq!(b.replica_page(None, 10).await.unwrap().records.len(), 2);
}

#[tokio::test]
async fn wire_checks_integrity_and_metadata_only_does_not_require_image_bytes() {
    let (header, parts) = encode_record(record(1, "a")).unwrap();
    let (mut send, mut recv) = tokio::io::duplex(4096);
    send_parts(&mut send, vec![vec![b'!'; parts[0].len()]])
        .await
        .unwrap();
    assert!(receive_record(&mut recv, header)
        .await
        .unwrap_err()
        .contains("integrity"));
    let mut metadata = record(2, "a");
    metadata.record.kind = ClipboardContentKind::Image;
    metadata.record.text = None;
    let header = metadata_to_wire(metadata);
    let mut empty = tokio::io::empty();
    assert!(receive_record(&mut empty, header.clone()).await.is_ok());
    let mut live = header;
    live.metadata.as_mut().unwrap().live = true;
    live.metadata.as_mut().unwrap().change_kind = 1;
    assert!(receive_record(&mut empty, live).await.is_err());
}

#[tokio::test]
async fn disabled_edits_are_not_bypassed_by_snapshot_or_reported_as_converged() {
    let a = database(None).await;
    let b = database(None).await;
    let original = record(1, "a");
    a.apply_replica_record(original.clone()).await.unwrap();
    b.apply_replica_record(original).await.unwrap();
    let mut settings = b.sync_preferences();
    settings.sync_edits_and_deletes = false;
    b.update_sync_preferences(settings);
    let mut edited = record(1, "a");
    edited.record.revision = 2;
    edited.record.text = Some("edited".into());
    a.apply_replica_record(edited).await.unwrap();
    let connection = link(a.clone(), b.clone()).await;
    let result = reconcile(a.clone(), connection.left.clone()).await.unwrap();
    assert!(!result.converged);
    assert_eq!(result.failed, 1);
    assert_eq!(
        b.replica_record(&format!("{:064x}", 1))
            .await
            .unwrap()
            .unwrap()
            .record
            .text
            .as_deref(),
        Some("record 1")
    );
}

#[tokio::test]
async fn revision_fence_recovers_old_id_recopied_behind_a_pagination_cursor() {
    let a = database(None).await;
    let b = database(None).await;
    for index in 1..=205 {
        a.apply_replica_record(record(index, "a")).await.unwrap();
    }
    let connection = link(a.clone(), b.clone()).await;
    let mut newest = record(1, "a");
    newest.record.captured_at_ms += 10_000;
    newest.record.revision = 2;
    newest.copy_count = 2;
    *connection.after_scan.lock().await = Some((a.clone(), newest.clone()));
    let result = reconcile(b.clone(), connection.right.clone())
        .await
        .unwrap();
    assert!(result.converged, "{result:?}");
    assert_eq!(result.total_records, 205);
    assert_eq!(result.local_revision, b.revision().await.unwrap());
    assert_eq!(result.remote_revision, a.revision().await.unwrap());
    assert_same(&a, &b).await;
    assert_eq!(
        b.replica_page(None, 1).await.unwrap().records[0]
            .record
            .sync_id,
        newest.record.sync_id
    );
}

#[tokio::test]
async fn recency_converges_independently_of_conflicting_content_revisions() {
    let a = database(None).await;
    let b = database(None).await;
    let mut edited = record(1, "a");
    edited.record.revision = 5;
    edited.record.text = Some("newer edit".into());
    let mut recopied = record(1, "b");
    recopied.record.captured_at_ms += 10_000;
    recopied.copy_count = 2;
    a.apply_replica_record(edited).await.unwrap();
    b.apply_replica_record(recopied.clone()).await.unwrap();
    let connection = link(a.clone(), b.clone()).await;
    let result = reconcile(a.clone(), connection.left.clone()).await.unwrap();
    assert!(result.converged, "{result:?}");
    assert_same(&a, &b).await;
    let item = a
        .replica_record(&recopied.record.sync_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item.record.text.as_deref(), Some("newer edit"));
    assert_eq!(item.record.captured_at_ms, recopied.record.captured_at_ms);
}

#[tokio::test]
async fn disabled_service_rejects_replica_requests() {
    let service = database(None).await;
    let mut settings = service.sync_preferences();
    settings.enabled = false;
    service.update_sync_preferences(settings);
    let (mut client, mut server) = tokio::io::duplex(4096);
    let task = tokio::spawn(async move {
        let (mut recv, mut send) = tokio::io::split(&mut server);
        serve_stream(&mut send, &mut recv, &service).await.unwrap();
    });
    let response: proto::ClipboardReplicaResponse = read(&mut client).await.unwrap();
    assert_eq!(
        response.status.unwrap().code,
        proto::ErrorCode::FailedPrecondition as i32
    );
    assert!(response.body.is_none());
    task.await.unwrap();
}

#[tokio::test]
async fn live_selection_survives_restart_and_snapshot_never_selects_the_system_clipboard() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("clipboard.sqlite");
    let service = database(Some(&path)).await;
    let historical = record(99, "history");
    service.apply_replica_record(historical).await.unwrap();
    assert!(service.replica_selection().await.unwrap().is_none());
    let mut live = record(1, "live");
    live.record.change_kind = ClipboardSyncChangeKind::Copy;
    live.record.live = true;
    service.apply_replica_record(live.clone()).await.unwrap();
    drop(service);
    let service = database(Some(&path)).await;
    let selection = service.replica_selection().await.unwrap().unwrap();
    assert_eq!(selection.sync_id, live.record.sync_id);
    assert!(selection.live);
    assert!(selection.text.is_none());
    let mut stale = record(2, "late");
    stale.record.captured_at_ms = live.record.captured_at_ms - 1;
    stale.first_captured_at_ms = stale.record.captured_at_ms;
    stale.record.live = true;
    stale.record.change_kind = ClipboardSyncChangeKind::Copy;
    service.apply_replica_record(stale).await.unwrap();
    assert_eq!(
        service.replica_selection().await.unwrap().unwrap().sync_id,
        live.record.sync_id
    );
    let remote = database(None).await;
    let connection = link(service.clone(), remote.clone()).await;
    let selected = service
        .replica_record(&selection.sync_id)
        .await
        .unwrap()
        .unwrap();
    let mut selected = selected;
    selected.record.live = true;
    selected.record.change_kind = ClipboardSyncChangeKind::Copy;
    assert!(connection
        .left
        .send_record(selected.clone(), true)
        .await
        .unwrap());
    assert!(!connection.left.send_record(selected, true).await.unwrap());
    assert_eq!(
        remote.replica_selection().await.unwrap().unwrap().sync_id,
        selection.sync_id
    );
    live.record.revision += 1;
    live.record.live = false;
    live.record.change_kind = ClipboardSyncChangeKind::Edit;
    live.record.text = Some("subsequent edit".into());
    service.apply_replica_record(live).await.unwrap();
    assert!(
        service.replica_selection().await.unwrap().is_none(),
        "a later edit must never be replayed as a copy"
    );
}

#[tokio::test]
async fn retention_keeps_recent_and_tagged_data_without_prune_redownload_cycles() {
    let a = database(None).await;
    let b = database(None).await;
    for index in 1..=5 {
        a.apply_replica_record(record(index, "a")).await.unwrap();
    }
    let mut tagged = record(1, "a");
    tagged.record.label_memberships = vec![ClipboardLabelMembership {
        label_id: "keep".into(),
        attached: true,
        revision: 1,
        updated_by_device_id: "a".into(),
    }];
    a.apply_replica_labels(vec![label("keep")]).await.unwrap();
    a.apply_replica_record(tagged).await.unwrap();
    let mut policy = b.policy().await.unwrap();
    policy.max_items = 3;
    b.update_policy(policy).await.unwrap();
    let connection = link(a.clone(), b.clone()).await;
    let first = reconcile(b.clone(), connection.right.clone())
        .await
        .unwrap();
    assert!(
        !first.converged,
        "different retention scopes must be reported"
    );
    let ids = b
        .replica_page(None, 10)
        .await
        .unwrap()
        .records
        .into_iter()
        .map(|r| r.record.sync_id)
        .collect::<Vec<_>>();
    assert_eq!(
        ids,
        vec![
            format!("{:064x}", 5),
            format!("{:064x}", 4),
            format!("{:064x}", 1)
        ]
    );
    let revision = b.revision().await.unwrap();
    let fetches = connection.fetches.load(Ordering::SeqCst);
    let second = reconcile(b.clone(), connection.right.clone())
        .await
        .unwrap();
    assert!(!second.converged);
    assert_eq!(second.received + second.sent, 0);
    assert_eq!(b.revision().await.unwrap(), revision);
    assert_eq!(connection.fetches.load(Ordering::SeqCst), fetches);
    assert_eq!(second.total_records, 3);
}

#[tokio::test]
async fn matching_retention_policies_converge_despite_device_local_file_history() {
    use arcrelay_core::domain::clipboard::{ClipboardPayload, ClipboardRepository};
    let a = database(None).await;
    let mut policy = a.policy().await.unwrap();
    policy.max_items = 3;
    a.update_policy(policy.clone()).await.unwrap();
    for index in 1..=3 {
        a.apply_replica_record(record(index, "a")).await.unwrap();
    }
    let repository = TestClipboardRepository::open(None).await.unwrap();
    repository.update_policy(policy).await.unwrap();
    let prototype = a.history(ClipboardQuery::recent(10)).await.unwrap().entries[0].clone();
    for index in 0..2 {
        let mut summary = prototype.clone();
        summary.kind = ClipboardContentKind::Files;
        summary.character_count = None;
        repository
            .store_local(
                ClipboardPayload::Files(vec![format!("/device-local/{index}")]),
                format!("local-file-{index}"),
                summary,
            )
            .await
            .unwrap();
    }
    let b = repository.service();
    let connection = link(a.clone(), b.clone()).await;
    let first = reconcile(b.clone(), connection.right.clone())
        .await
        .unwrap();
    assert!(first.converged, "{first:?}");
    assert_eq!(first.total_records, 3);
    assert_eq!(
        a.replica_page(None, 10).await.unwrap().records,
        b.replica_page(None, 10).await.unwrap().records
    );
    assert_eq!(
        b.history(ClipboardQuery::recent(10))
            .await
            .unwrap()
            .entries
            .len(),
        5
    );
    let fetches = connection.fetches.load(Ordering::SeqCst);
    let second = reconcile(b, connection.right.clone()).await.unwrap();
    assert!(second.converged);
    assert_eq!(second.received + second.sent, 0);
    assert_eq!(connection.fetches.load(Ordering::SeqCst), fetches);
}
