use super::*;
use arcrelay_core::domain::collections::{ClipboardCollection, MAX_COLLECTION_BYTES};

pub fn fingerprint(value: &ClipboardCollection) -> String {
    let encoded = serde_json::to_vec(value).expect("collection serialization is infallible");
    format!("{:x}", Sha256::digest(encoded))
}

pub fn decode(record: proto::ClipboardCollectionRecord) -> Result<ClipboardCollection> {
    if record.format_version != 1 || record.document_json.len() > MAX_COLLECTION_BYTES {
        return Err("unsupported or oversized clipboard collection".into());
    }
    let value: ClipboardCollection = serde_json::from_str(&record.document_json)
        .map_err(|_| "invalid clipboard collection document")?;
    value.validate()?;
    Ok(value)
}
pub fn encode(value: &ClipboardCollection) -> Result<proto::ClipboardCollectionRecord> {
    value.validate()?;
    Ok(proto::ClipboardCollectionRecord {
        format_version: 1,
        document_json: serde_json::to_string(value).map_err(|_| "cannot encode collection")?,
    })
}
pub async fn page(
    clipboard: &ClipboardApplicationService,
    after: String,
) -> Result<proto::ClipboardCollectionPage> {
    let p = clipboard.sync_preferences();
    if !p.enabled {
        return Err("clipboard synchronization is disabled".into());
    }
    if after.len() > 64 {
        return Err("invalid collection cursor".into());
    }
    let values = clipboard
        .collections(after)
        .await
        .map_err(|e| e.to_string())?;
    Ok(proto::ClipboardCollectionPage {
        next_cursor: values.last().map(|v| v.id.clone()).unwrap_or_default(),
        records: values.iter().map(encode).collect::<Result<_>>()?,
        sync_edits_and_deletes: p.sync_edits_and_deletes,
        sync_favorites: p.sync_favorites,
    })
}

pub(super) async fn reconcile(
    clipboard: &ClipboardApplicationService,
    client: &PeerClient,
    result: &mut ReconciliationResult,
) -> Result<()> {
    use proto::clipboard_replica_request::Body as Request;
    use proto::clipboard_replica_response::Body as Response;
    let mut cursor = String::new();
    let mut remote = HashMap::new();
    let mut remote_edits;
    let mut remote_favorites;
    loop {
        let Response::Collections(page) = client
            .request(
                Request::ScanCollections(proto::ClipboardCollectionScan {
                    after_id: cursor.clone(),
                }),
                vec![],
            )
            .await?
            .0
        else {
            return Err("invalid collection page".into());
        };
        // Never interpret preferences from a previous page after the user changes them.
        remote_edits = page.sync_edits_and_deletes;
        remote_favorites = page.sync_favorites;
        for wire in page.records {
            let value = decode(wire)?;
            if value.id <= cursor {
                return Err("invalid collection order".into());
            }
            let changed = clipboard
                .receive_collection(value.clone(), remote_edits, remote_favorites)
                .await
                .map_err(|e| e.to_string())?;
            result.received += usize::from(changed);
            result.differences += usize::from(changed);
            remote.insert(value.id.clone(), value);
        }
        if remote.len() > 10_000 {
            return Err("too many collections".into());
        }
        if page.next_cursor.is_empty() {
            break;
        }
        if page.next_cursor <= cursor {
            return Err("invalid collection cursor".into());
        }
        cursor = page.next_cursor;
    }
    result.collection_receipts = remote
        .iter()
        .map(|(id, value)| (id.clone(), fingerprint(value)))
        .collect();
    let p = clipboard.sync_preferences();
    cursor.clear();
    loop {
        let page = clipboard
            .collections(cursor.clone())
            .await
            .map_err(|e| e.to_string())?;
        if page.is_empty() {
            break;
        }
        for value in page {
            cursor = value.id.clone();
            let old = remote.get(&value.id);
            let send_content = old.is_none()
                || (p.sync_edits_and_deletes
                    && remote_edits
                    && old.is_some_and(|r| {
                        (value.revision, &value.updated_by_device_id)
                            > (r.revision, &r.updated_by_device_id)
                    }));
            let send_favorite = p.sync_favorites
                && remote_favorites
                && old.is_some_and(|r| {
                    (
                        value.favorite_revision,
                        &value.favorite_updated_by_device_id,
                    ) > (r.favorite_revision, &r.favorite_updated_by_device_id)
                });
            if !send_content && !send_favorite {
                continue;
            }
            if value.deleted && (!p.sync_edits_and_deletes || !remote_edits) {
                continue;
            }
            // A favorite-only send must not bypass the content synchronization switch.
            let mut outgoing = if send_content {
                value.clone()
            } else {
                old.cloned().ok_or("collection version missing")?
            };
            if p.sync_favorites && remote_favorites {
                outgoing.favorite = value.favorite;
                outgoing.favorite_revision = value.favorite_revision;
                outgoing.favorite_updated_by_device_id =
                    value.favorite_updated_by_device_id.clone();
            } else if let Some(old) = old {
                outgoing.favorite = old.favorite;
                outgoing.favorite_revision = old.favorite_revision;
                outgoing.favorite_updated_by_device_id = old.favorite_updated_by_device_id.clone();
            } else {
                outgoing.favorite = false;
                outgoing.favorite_revision = 0;
                outgoing.favorite_updated_by_device_id.clear();
            }
            let Response::Changed(changed) = client
                .request(Request::StoreCollection(encode(&outgoing)?), vec![])
                .await?
                .0
            else {
                return Err("invalid collection receipt".into());
            };
            result.sent += changed as usize;
            result.differences += changed as usize;
        }
    }
    Ok(())
}
