//! Symmetric, authenticated desktop replication. SQLite records are the durable
//! source of truth; notifications accelerate delivery and periodic manifests
//! repair missed notifications. No clipboard history payload enters BlobStore.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use arcrelay_core::application::clipboard_service::ClipboardApplicationService;
use arcrelay_core::domain::clipboard::{
    ClipboardLabel, ClipboardReplicaCursor, ClipboardReplicaRecord, ClipboardSyncRecord,
};
use arcrelay_wire::{proto, MAX_CONTROL_FRAME_SIZE, STREAM_KIND_CLIPBOARD_REPLICA};
use prost::Message;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

mod wire;
use wire::*;
#[cfg(test)]
mod tests;

type Result<T> = std::result::Result<T, String>;
pub const VERSION: u32 = 2;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_MANIFEST_RECORDS: usize = 100_000;

#[derive(Debug, Clone, Default)]
pub struct ReconciliationResult {
    pub received: usize,
    pub sent: usize,
    pub labels_received: usize,
    pub labels_sent: usize,
    pub failed: usize,
    pub total_records: usize,
    pub converged: bool,
    /// Revision fences verified by the zero-change pass, never sampled later.
    pub local_revision: u64,
    pub remote_revision: u64,
    pub errors: Vec<String>,
    differences: usize,
}

impl ReconciliationResult {
    fn failure(&mut self, sync_id: &str, error: String) {
        self.failed += 1;
        let digest = Sha256::digest(sync_id.as_bytes());
        let record_id = digest[..6]
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        tracing::warn!(event = "clipboard.replica.record_failed", %record_id, %error, "clipboard record remains pending reconciliation");
        if self.errors.len() < 8 {
            self.errors.push(format!("{record_id}: {error}"));
        }
    }
}

fn ok(body: proto::clipboard_replica_response::Body) -> proto::ClipboardReplicaResponse {
    proto::ClipboardReplicaResponse {
        status: Some(proto::Status {
            code: proto::ErrorCode::Ok as i32,
            ..Default::default()
        }),
        body: Some(body),
    }
}
fn failure(message: &str) -> proto::ClipboardReplicaResponse {
    proto::ClipboardReplicaResponse {
        status: Some(proto::Status {
            code: proto::ErrorCode::FailedPrecondition as i32,
            message: message.to_owned(),
            ..Default::default()
        }),
        body: None,
    }
}
async fn write<W: AsyncWrite + Unpin, M: Message>(send: &mut W, value: &M) -> Result<()> {
    arcrelay_transport::write_frame(send, &value.encode_to_vec(), MAX_CONTROL_FRAME_SIZE)
        .await
        .map_err(|e| e.to_string())
}
async fn read<R: AsyncRead + Unpin, M: Message + Default>(recv: &mut R) -> Result<M> {
    let bytes = arcrelay_transport::read_frame(recv, MAX_CONTROL_FRAME_SIZE)
        .await
        .map_err(|e| e.to_string())?;
    M::decode(bytes.as_slice()).map_err(|_| "invalid clipboard replication frame".to_owned())
}
fn cursor_to_wire(cursor: ClipboardReplicaCursor) -> proto::ClipboardReplicaCursor {
    proto::ClipboardReplicaCursor {
        captured_at_ms: cursor.captured_at_ms,
        sync_id: cursor.sync_id,
    }
}
fn cursor_from_wire(cursor: proto::ClipboardReplicaCursor) -> ClipboardReplicaCursor {
    ClipboardReplicaCursor {
        captured_at_ms: cursor.captured_at_ms,
        sync_id: cursor.sync_id,
    }
}

/// The caller must check negotiated ClipboardSync v2 and the authenticated
/// peer's ClipboardSync grant, and bound concurrent streams before invoking.
pub async fn serve_stream<W: AsyncWrite + Unpin, R: AsyncRead + Unpin>(
    send: &mut W,
    recv: &mut R,
    clipboard: &ClipboardApplicationService,
) -> Result<()> {
    tokio::time::timeout(REQUEST_TIMEOUT, async {
        if !clipboard.sync_preferences().enabled {
            write(send, &failure("clipboard synchronization is disabled")).await?;
            return Ok(());
        }
        let request: proto::ClipboardReplicaRequest = read(recv).await?;
        match serve_request(send, recv, clipboard, request).await {
            Ok(()) => Ok(()),
            Err(error) => {
                // Errors are static validation text or already typed application errors;
                // never include a clipboard preview or payload in this response.
                write(send, &failure(&error)).await
            }
        }
    })
    .await
    .map_err(|_| "clipboard replication request timed out".to_owned())?
}

async fn serve_request<W: AsyncWrite + Unpin, R: AsyncRead + Unpin>(
    send: &mut W,
    recv: &mut R,
    clipboard: &ClipboardApplicationService,
    request: proto::ClipboardReplicaRequest,
) -> Result<()> {
    use proto::clipboard_replica_request::Body as Request;
    use proto::clipboard_replica_response::Body as Response;
    match request.body.ok_or("missing clipboard request")? {
        Request::Revision(_) => {
            write(
                send,
                &ok(Response::Revision(
                    clipboard.revision().await.map_err(|e| e.to_string())?,
                )),
            )
            .await
        }
        Request::Scan(scan) => {
            let page = clipboard
                .replica_page(
                    scan.cursor.map(cursor_from_wire),
                    scan.limit.clamp(1, 100) as usize,
                )
                .await
                .map_err(|e| e.to_string())?;
            let mut result = proto::ClipboardReplicaPage {
                records: Vec::new(),
                next_cursor: page.next_cursor.map(cursor_to_wire),
            };
            let mut bytes = 0;
            for record in page.records {
                let wire = metadata_to_wire(record);
                if !result.records.is_empty()
                    && bytes + wire.encoded_len() > MAX_CONTROL_FRAME_SIZE / 2
                {
                    result.next_cursor = result
                        .records
                        .last()
                        .and_then(|r| r.metadata.as_ref())
                        .map(|m| proto::ClipboardReplicaCursor {
                            captured_at_ms: m.captured_at_ms,
                            sync_id: m.sync_id.clone(),
                        });
                    break;
                }
                bytes += wire.encoded_len();
                result.records.push(wire);
            }
            write(send, &ok(Response::Page(result))).await
        }
        Request::Fetch(sync_id) => {
            let replica = clipboard
                .replica_record(&sync_id)
                .await
                .map_err(|e| e.to_string())?
                .ok_or("clipboard record no longer exists")?;
            if !clipboard.should_send_sync_record(&replica.record) {
                return Err("clipboard synchronization is disabled for this record".into());
            }
            let (header, parts) = encode_record(replica)?;
            write(send, &ok(Response::Record(header))).await?;
            send_parts(send, parts).await
        }
        Request::Check(header) => {
            let replica = metadata_from_wire(header)?;
            clipboard
                .check_replica_storage(&replica)
                .await
                .map_err(|e| e.to_string())?;
            write(send, &ok(Response::Changed(1))).await
        }
        Request::Store(header) => {
            let replica = receive_record(recv, header).await?;
            let changed = clipboard
                .apply_replica_record(replica)
                .await
                .map_err(|e| e.to_string())?;
            write(send, &ok(Response::Changed(u32::from(changed)))).await
        }
        Request::ScanLabels(cursor) => {
            let mut labels = clipboard
                .replica_labels()
                .await
                .map_err(|e| e.to_string())?;
            labels.sort_by(|a, b| a.id.cmp(&b.id));
            labels.retain(|l| l.id > cursor);
            let more = labels.len() > 100;
            labels.truncate(100);
            let next_cursor = if more {
                labels.last().map(|l| l.id.clone())
            } else {
                None
            };
            write(
                send,
                &ok(Response::Labels(proto::ClipboardReplicaLabels {
                    labels: labels.into_iter().map(label_to_wire).collect(),
                    next_cursor,
                })),
            )
            .await
        }
        Request::StoreLabels(labels) => {
            if labels.labels.len() > 100 {
                return Err("clipboard label page exceeds limit".into());
            }
            let mut ids = HashSet::new();
            let labels = labels
                .labels
                .into_iter()
                .map(|l| {
                    if !ids.insert(l.id.clone()) {
                        return Err("duplicate clipboard label".into());
                    }
                    label_from_wire(l)
                })
                .collect::<Result<Vec<_>>>()?;
            let changed = clipboard
                .apply_replica_labels(labels)
                .await
                .map_err(|e| e.to_string())?;
            write(send, &ok(Response::Changed(changed as u32))).await
        }
    }
}

#[derive(Clone)]
pub struct PeerClient {
    connection: quinn::Connection,
}
impl PeerClient {
    pub fn new(connection: quinn::Connection) -> Self {
        Self { connection }
    }

    async fn request(
        &self,
        body: proto::clipboard_replica_request::Body,
        parts: Vec<Vec<u8>>,
    ) -> Result<(proto::clipboard_replica_response::Body, quinn::RecvStream)> {
        tokio::time::timeout(REQUEST_TIMEOUT, async {
            let (mut send, mut recv) =
                self.connection.open_bi().await.map_err(|e| e.to_string())?;
            send.write_u8(STREAM_KIND_CLIPBOARD_REPLICA)
                .await
                .map_err(|e| e.to_string())?;
            write(
                &mut send,
                &proto::ClipboardReplicaRequest { body: Some(body) },
            )
            .await?;
            send_parts(&mut send, parts).await?;
            send.finish().map_err(|e| e.to_string())?;
            let response: proto::ClipboardReplicaResponse = read(&mut recv).await?;
            let status = response.status.ok_or("clipboard response has no status")?;
            if status.code != proto::ErrorCode::Ok as i32 {
                return Err(status.message);
            }
            Ok((response.body.ok_or("clipboard response has no body")?, recv))
        })
        .await
        .map_err(|_| "clipboard request timed out".to_owned())?
    }

    pub async fn revision(&self) -> Result<u64> {
        match self
            .request(
                proto::clipboard_replica_request::Body::Revision(true),
                vec![],
            )
            .await?
            .0
        {
            proto::clipboard_replica_response::Body::Revision(revision) => Ok(revision),
            _ => Err("invalid clipboard revision response".into()),
        }
    }
    async fn fetch(&self, sync_id: &str) -> Result<ClipboardReplicaRecord> {
        let (body, mut recv) = self
            .request(
                proto::clipboard_replica_request::Body::Fetch(sync_id.to_owned()),
                vec![],
            )
            .await?;
        let proto::clipboard_replica_response::Body::Record(header) = body else {
            return Err("invalid clipboard record response".into());
        };
        if header
            .metadata
            .as_ref()
            .is_none_or(|r| r.sync_id != sync_id)
        {
            return Err("clipboard response record mismatch".into());
        }
        tokio::time::timeout(REQUEST_TIMEOUT, receive_record(&mut recv, header))
            .await
            .map_err(|_| "clipboard download timed out".to_owned())?
    }
    pub async fn send_record(
        &self,
        replica: ClipboardReplicaRecord,
        include_payload: bool,
    ) -> Result<bool> {
        if include_payload && !replica.record.live {
            self.request(
                proto::clipboard_replica_request::Body::Check(metadata_to_wire(replica.clone())),
                vec![],
            )
            .await?;
        }
        // Deliver only the definitions referenced by this record. Unused labels
        // are reconciled separately, without attaching the entire dictionary.
        let definitions = replica
            .record
            .labels
            .iter()
            .filter(|l| {
                replica
                    .record
                    .label_memberships
                    .iter()
                    .any(|m| m.label_id == l.id)
            })
            .cloned()
            .collect::<Vec<_>>();
        for labels in definitions.chunks(100) {
            self.request(
                proto::clipboard_replica_request::Body::StoreLabels(
                    proto::ClipboardReplicaLabels {
                        labels: labels.iter().cloned().map(label_to_wire).collect(),
                        next_cursor: None,
                    },
                ),
                vec![],
            )
            .await?;
        }
        let (header, parts) = if include_payload {
            encode_record(replica)?
        } else {
            (metadata_to_wire(replica), vec![])
        };
        match self
            .request(proto::clipboard_replica_request::Body::Store(header), parts)
            .await?
            .0
        {
            proto::clipboard_replica_response::Body::Changed(changed) => Ok(changed > 0),
            _ => Err("invalid clipboard receipt".into()),
        }
    }
}

pub fn covers(current: &ClipboardReplicaRecord, incoming: &ClipboardReplicaRecord) -> bool {
    let (a, b) = (&current.record, &incoming.record);
    (a.revision, &a.updated_by_device_id) >= (b.revision, &b.updated_by_device_id)
        && (a.favorite_revision, &a.favorite_updated_by_device_id)
            >= (b.favorite_revision, &b.favorite_updated_by_device_id)
        && a.captured_at_ms >= b.captured_at_ms
        && current.first_captured_at_ms <= incoming.first_captured_at_ms
        && current.copy_count >= incoming.copy_count
        && b.label_memberships.iter().all(|incoming| {
            a.label_memberships.iter().any(|current| {
                current.label_id == incoming.label_id
                    && (current.revision, &current.updated_by_device_id)
                        >= (incoming.revision, &incoming.updated_by_device_id)
            })
        })
}
fn needs_payload(
    current: Option<&ClipboardReplicaRecord>,
    incoming: &ClipboardReplicaRecord,
) -> bool {
    !incoming.record.deleted
        && current.is_none_or(|c| {
            (c.record.revision, &c.record.updated_by_device_id)
                < (
                    incoming.record.revision,
                    &incoming.record.updated_by_device_id,
                )
        })
}
async fn local_manifest(
    clipboard: &ClipboardApplicationService,
) -> Result<HashMap<String, ClipboardReplicaRecord>> {
    let mut records = HashMap::new();
    let mut cursor = None;
    loop {
        let page = clipboard
            .replica_page(cursor, 100)
            .await
            .map_err(|e| e.to_string())?;
        for r in page.records {
            records.insert(r.record.sync_id.clone(), r);
        }
        if records.len() > MAX_MANIFEST_RECORDS {
            return Err("clipboard manifest exceeds safety limit".into());
        }
        cursor = page.next_cursor;
        if cursor.is_none() {
            return Ok(records);
        }
    }
}

/// Bidirectional anti-entropy, usable from either role of the control session.
/// A zero-change pass with stable revision fences is required to report success.
pub async fn reconcile(
    clipboard: Arc<ClipboardApplicationService>,
    client: PeerClient,
) -> Result<ReconciliationResult> {
    let mut result = ReconciliationResult::default();
    for _ in 0..3 {
        let pass = async {
            let local_before = clipboard.revision().await.map_err(|e| e.to_string())?;
            let remote_before = client.revision().await?;
            let before = result.differences;
            reconcile_once(&clipboard, &client, &mut result).await?;
            if result.failed > 0 {
                return Ok(false);
            }
            let local_after = clipboard.revision().await.map_err(|e| e.to_string())?;
            let remote_after = client.revision().await?;
            if before == result.differences
                && local_before == local_after
                && remote_before == remote_after
            {
                result.converged = true;
                result.local_revision = local_after;
                result.remote_revision = remote_after;
            }
            Ok::<_, String>(result.converged)
        }
        .await;
        match pass {
            Ok(true) => break,
            Ok(false) if result.failed > 0 => break,
            Ok(false) => {}
            Err(error) => {
                result.failure("connection", error);
                break;
            }
        }
    }
    Ok(result)
}

async fn reconcile_once(
    clipboard: &ClipboardApplicationService,
    client: &PeerClient,
    result: &mut ReconciliationResult,
) -> Result<()> {
    use proto::clipboard_replica_request::Body as Request;
    use proto::clipboard_replica_response::Body as Response;
    // Synchronize the dictionary even when no record currently uses a label.
    let mut remote_labels = HashMap::new();
    let mut label_cursor = String::new();
    loop {
        let Response::Labels(page) = client
            .request(Request::ScanLabels(label_cursor.clone()), vec![])
            .await?
            .0
        else {
            return Err("invalid clipboard labels response".into());
        };
        let labels = page
            .labels
            .into_iter()
            .map(label_from_wire)
            .collect::<Result<Vec<_>>>()?;
        for label in &labels {
            remote_labels.insert(label.id.clone(), label.clone());
        }
        let changed = clipboard
            .apply_replica_labels(labels)
            .await
            .map_err(|e| e.to_string())?;
        result.labels_received += changed;
        result.differences += changed;
        let Some(next) = page.next_cursor else { break };
        if next <= label_cursor || remote_labels.len() > 10_000 {
            return Err("invalid clipboard labels cursor".into());
        }
        label_cursor = next;
    }
    let labels = clipboard
        .replica_labels()
        .await
        .map_err(|e| e.to_string())?
        .into_iter()
        .filter(|label| {
            remote_labels.get(&label.id).is_none_or(|r| {
                (r.revision, &r.updated_by_device_id)
                    < (label.revision, &label.updated_by_device_id)
            })
        })
        .collect::<Vec<_>>();
    for labels in labels.chunks(100) {
        let Response::Changed(changed) = client
            .request(
                Request::StoreLabels(proto::ClipboardReplicaLabels {
                    labels: labels.iter().cloned().map(label_to_wire).collect(),
                    next_cursor: None,
                }),
                vec![],
            )
            .await?
            .0
        else {
            return Err("invalid clipboard label receipt".into());
        };
        result.labels_sent += changed as usize;
        result.differences += labels.len();
    }
    let local = local_manifest(clipboard).await?;
    let mut remote = HashMap::new();
    let mut cursor: Option<proto::ClipboardReplicaCursor> = None;
    loop {
        let Response::Page(page) = client
            .request(
                Request::Scan(proto::ClipboardReplicaScan {
                    cursor: cursor.clone(),
                    limit: 100,
                }),
                vec![],
            )
            .await?
            .0
        else {
            return Err("invalid clipboard manifest response".into());
        };
        for header in page.records {
            let id = header
                .metadata
                .as_ref()
                .map(|m| m.sync_id.clone())
                .unwrap_or_default();
            let record = match metadata_from_wire(header) {
                Ok(record) => record,
                Err(error) => {
                    result.failure(&id, error);
                    continue;
                }
            };
            let id = record.record.sync_id.clone();
            if !local.get(&id).is_some_and(|l| covers(l, &record)) {
                result.differences += 1;
                let apply = async {
                    clipboard
                        .check_replica_storage(&record)
                        .await
                        .map_err(|e| e.to_string())?;
                    let record = if needs_payload(local.get(&id), &record) {
                        client.fetch(&id).await?
                    } else {
                        record.clone()
                    };
                    clipboard
                        .apply_replica_record(record)
                        .await
                        .map_err(|e| e.to_string())
                }
                .await;
                match apply {
                    Ok(true) => result.received += 1,
                    Ok(false) => {}
                    Err(error) => result.failure(&id, error),
                }
            }
            remote.insert(id, record);
            if remote.len() > MAX_MANIFEST_RECORDS {
                return Err("clipboard manifest exceeds safety limit".into());
            }
        }
        let Some(next) = page.next_cursor else { break };
        if cursor.as_ref().is_some_and(|old| {
            (next.captured_at_ms, &next.sync_id) >= (old.captured_at_ms, &old.sync_id)
        }) {
            return Err("invalid clipboard manifest cursor".into());
        }
        cursor = Some(next);
    }
    let mut local = local_manifest(clipboard)
        .await?
        .into_values()
        .collect::<Vec<_>>();
    local.sort_by(|a, b| {
        (b.record.captured_at_ms, &b.record.sync_id)
            .cmp(&(a.record.captured_at_ms, &a.record.sync_id))
    });
    result.total_records = local.iter().filter(|r| !r.record.deleted).count();
    for metadata in local {
        let id = metadata.record.sync_id.clone();
        if remote.get(&id).is_some_and(|r| covers(r, &metadata)) {
            continue;
        }
        result.differences += 1;
        let send = async {
            let include_payload = needs_payload(remote.get(&id), &metadata);
            let replica = if include_payload {
                clipboard
                    .replica_record(&id)
                    .await
                    .map_err(|e| e.to_string())?
                    .ok_or("clipboard record was removed during synchronization")?
            } else {
                metadata
            };
            client.send_record(replica, include_payload).await
        }
        .await;
        match send {
            Ok(true) => result.sent += 1,
            Ok(false) => {}
            Err(error) => result.failure(&id, error),
        }
    }
    Ok(())
}
