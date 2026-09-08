use super::*;
use arcrelay_core::domain::clipboard::{
    ClipboardContentKind as Kind, ClipboardLabelMembership, ClipboardSyncChangeKind as Change,
};

const TEXT_LIMIT: usize = 1024 * 1024;
const RICH_LIMIT: usize = 16 * 1024 * 1024;
const IMAGE_LIMIT: usize = 20 * 1024 * 1024;

pub(super) fn label_to_wire(label: ClipboardLabel) -> proto::ClipboardLabel {
    proto::ClipboardLabel {
        id: label.id,
        name: label.name,
        color: label.color,
        revision: label.revision,
        updated_by_device_id: label.updated_by_device_id,
        deleted: label.deleted,
    }
}

pub(super) fn label_from_wire(label: proto::ClipboardLabel) -> Result<ClipboardLabel> {
    if !identifier(&label.id)
        || label.name.trim().is_empty()
        || label.name.len() > 128
        || !valid_color(&label.color)
        || !version(label.revision, &label.updated_by_device_id)
    {
        return Err("invalid clipboard label metadata".into());
    }
    Ok(ClipboardLabel {
        id: label.id,
        name: label.name,
        color: label.color,
        revision: label.revision,
        updated_by_device_id: label.updated_by_device_id,
        deleted: label.deleted,
    })
}

fn identifier(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 128 && !value.chars().any(char::is_control)
}
fn version(revision: u64, device: &str) -> bool {
    revision > 0 && revision <= i64::MAX as u64 && identifier(device)
}
fn valid_color(color: &str) -> bool {
    matches!(color.len(), 4 | 7 | 9)
        && color.starts_with('#')
        && color[1..].bytes().all(|b| b.is_ascii_hexdigit())
}

pub(super) fn metadata_to_wire(replica: ClipboardReplicaRecord) -> proto::ClipboardReplicaRecord {
    let r = replica.record;
    proto::ClipboardReplicaRecord {
        first_captured_at_ms: replica.first_captured_at_ms,
        copy_count: replica.copy_count,
        parts: Vec::new(),
        metadata_only: true,
        metadata: Some(proto::ClipboardSyncRecord {
            sync_id: r.sync_id,
            kind: match r.kind {
                Kind::Text => 1,
                Kind::Html => 2,
                Kind::Image => 3,
                Kind::Files => 4,
            },
            width: r.width,
            height: r.height,
            preview: r.preview,
            source_app: r.source_app,
            source_device_id: r.source_device_id,
            source_device_name: r.source_device_name,
            captured_at_ms: r.captured_at_ms,
            revision: r.revision,
            updated_by_device_id: r.updated_by_device_id,
            favorite: r.favorite,
            favorite_revision: r.favorite_revision,
            favorite_updated_by_device_id: r.favorite_updated_by_device_id,
            labels: Vec::new(),
            label_memberships: r
                .label_memberships
                .into_iter()
                .map(|m| proto::ClipboardLabelMembership {
                    label_id: m.label_id,
                    attached: m.attached,
                    revision: m.revision,
                    updated_by_device_id: m.updated_by_device_id,
                })
                .collect(),
            deleted: r.deleted,
            change_kind: match r.change_kind {
                Change::Copy => 1,
                Change::Edit => 2,
                Change::Favorite => 3,
                Change::Delete => 4,
                Change::Snapshot => 5,
                Change::Label => 6,
            },
            live: r.live,
            text_syntax_json: String::new(),
            payload: None,
        }),
    }
}

pub(super) fn metadata_from_wire(
    wire: proto::ClipboardReplicaRecord,
) -> Result<ClipboardReplicaRecord> {
    let r = wire.metadata.ok_or("missing clipboard replica metadata")?;
    if r.sync_id.len() != 64
        || !r.sync_id.bytes().all(|b| b.is_ascii_hexdigit())
        || !version(r.revision, &r.updated_by_device_id)
        || !version(r.favorite_revision, &r.favorite_updated_by_device_id)
        || !identifier(&r.source_device_id)
        || r.source_device_name.len() > 128
        || r.preview.len() > 2048
        || r.source_app.as_ref().is_some_and(|v| v.len() > 256)
        || wire.first_captured_at_ms <= 0
        || wire.first_captured_at_ms > r.captured_at_ms
        || r.captured_at_ms > chrono_now_ms().saturating_add(300_000)
        || wire.copy_count == 0
        || wire.copy_count > i32::MAX as u32
        || !r.labels.is_empty()
        || r.payload.is_some()
        || r.label_memberships.len() > 4096
    {
        return Err("invalid clipboard replica metadata".into());
    }
    let mut ids = HashSet::new();
    let memberships = r
        .label_memberships
        .into_iter()
        .map(|m| {
            if !identifier(&m.label_id)
                || !version(m.revision, &m.updated_by_device_id)
                || !ids.insert(m.label_id.clone())
            {
                return Err("invalid clipboard label membership".to_owned());
            }
            Ok(ClipboardLabelMembership {
                label_id: m.label_id,
                attached: m.attached,
                revision: m.revision,
                updated_by_device_id: m.updated_by_device_id,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let kind = match r.kind {
        1 => Kind::Text,
        2 => Kind::Html,
        3 => Kind::Image,
        _ => return Err("unsupported clipboard replica kind".into()),
    };
    let change_kind = match r.change_kind {
        1 => Change::Copy,
        2 => Change::Edit,
        3 => Change::Favorite,
        4 => Change::Delete,
        5 => Change::Snapshot,
        6 => Change::Label,
        _ => return Err("invalid clipboard replica change".into()),
    };
    if r.live && (change_kind != Change::Copy || r.deleted) {
        return Err("invalid live clipboard replica".into());
    }
    Ok(ClipboardReplicaRecord {
        first_captured_at_ms: wire.first_captured_at_ms,
        copy_count: wire.copy_count,
        record: ClipboardSyncRecord {
            sync_id: r.sync_id,
            kind,
            text: None,
            html: None,
            rtf: None,
            image_png: None,
            width: r.width,
            height: r.height,
            preview: r.preview,
            source_app: r.source_app,
            source_device_id: r.source_device_id,
            source_device_name: r.source_device_name,
            captured_at_ms: r.captured_at_ms,
            revision: r.revision,
            updated_by_device_id: r.updated_by_device_id,
            favorite: r.favorite,
            favorite_revision: r.favorite_revision,
            favorite_updated_by_device_id: r.favorite_updated_by_device_id,
            labels: Vec::new(),
            label_memberships: memberships,
            deleted: r.deleted,
            change_kind,
            live: r.live,
            text_syntax: Default::default(),
        },
    })
}

pub(super) fn encode_record(
    mut replica: ClipboardReplicaRecord,
) -> Result<(proto::ClipboardReplicaRecord, Vec<Vec<u8>>)> {
    let r = &mut replica.record;
    let mut parts = Vec::new();
    let mut add = |value: Option<Vec<u8>>, media: &str, limit: usize| -> Result<()> {
        let bytes = value.ok_or("clipboard payload is unavailable")?;
        if bytes.len() > limit {
            return Err("clipboard payload exceeds transfer limit".into());
        }
        parts.push((bytes, media.to_owned()));
        Ok(())
    };
    if !r.deleted {
        match r.kind {
            Kind::Text => add(
                r.text.take().map(String::into_bytes),
                "text/plain",
                TEXT_LIMIT,
            )?,
            Kind::Html => {
                add(
                    Some(r.text.take().unwrap_or_default().into_bytes()),
                    "text/plain",
                    TEXT_LIMIT,
                )?;
                add(
                    r.html.take().map(String::into_bytes),
                    "text/html",
                    RICH_LIMIT,
                )?;
                if let Some(rtf) = r.rtf.take() {
                    add(Some(rtf.into_bytes()), "text/rtf", RICH_LIMIT)?;
                }
            }
            Kind::Image => add(r.image_png.take(), "image/png", IMAGE_LIMIT)?,
            Kind::Files => return Err("file paths cannot be replicated".into()),
        }
    }
    let mut wire = metadata_to_wire(replica);
    wire.metadata_only = false;
    wire.parts = parts
        .iter()
        .map(|(bytes, media)| {
            let digest = Sha256::digest(bytes).to_vec();
            proto::BlobRef {
                blob_id: digest.clone(),
                size: bytes.len() as u64,
                sha256: digest,
                media_type: media.clone(),
            }
        })
        .collect();
    Ok((wire, parts.into_iter().map(|p| p.0).collect()))
}

pub(super) async fn receive_record<R: AsyncRead + Unpin>(
    recv: &mut R,
    wire: proto::ClipboardReplicaRecord,
) -> Result<ClipboardReplicaRecord> {
    let metadata_only = wire.metadata_only;
    let references = wire.parts.clone();
    let mut replica = metadata_from_wire(wire)?;
    let r = &mut replica.record;
    if metadata_only {
        if !references.is_empty() || r.live {
            return Err("invalid metadata-only clipboard replica".into());
        }
        return Ok(replica);
    }
    let expected: Vec<(&str, usize)> = if r.deleted {
        vec![]
    } else {
        match r.kind {
            Kind::Text => vec![("text/plain", TEXT_LIMIT)],
            Kind::Html => {
                let mut v = vec![("text/plain", TEXT_LIMIT), ("text/html", RICH_LIMIT)];
                if references.len() == 3 {
                    v.push(("text/rtf", RICH_LIMIT));
                }
                v
            }
            Kind::Image => vec![("image/png", IMAGE_LIMIT)],
            Kind::Files => unreachable!(),
        }
    };
    if references.len() != expected.len() {
        return Err("invalid clipboard payload parts".into());
    }
    for (reference, (media, limit)) in references.into_iter().zip(expected) {
        if reference.media_type != media
            || reference.size > limit as u64
            || reference.sha256.len() != 32
            || reference.blob_id != reference.sha256
        {
            return Err("invalid clipboard payload descriptor".into());
        }
        let mut bytes = Vec::with_capacity(reference.size as usize);
        while bytes.len() < reference.size as usize {
            let chunk = arcrelay_transport::read_frame(recv, arcrelay_wire::MAX_BLOB_CHUNK_SIZE)
                .await
                .map_err(|e| e.to_string())?;
            if chunk.is_empty() || chunk.len() > reference.size as usize - bytes.len() {
                return Err("invalid clipboard payload chunk".into());
            }
            bytes.extend_from_slice(&chunk);
        }
        if Sha256::digest(&bytes).as_slice() != reference.sha256 {
            return Err("clipboard payload integrity check failed".into());
        }
        if media == "image/png" {
            r.image_png = Some(bytes);
        } else {
            let text = String::from_utf8(bytes).map_err(|_| "clipboard payload is not UTF-8")?;
            match media {
                "text/plain" => r.text = Some(text),
                "text/html" => r.html = Some(text),
                "text/rtf" => r.rtf = Some(text),
                _ => unreachable!(),
            }
        }
    }
    if r.kind == Kind::Text && r.text.as_ref().is_none_or(String::is_empty) && !r.deleted {
        return Err("clipboard text is empty".into());
    }
    r.text_syntax = arcrelay_core::domain::clipboard_text::detect_text_syntax(
        r.text.as_deref().unwrap_or_default(),
    );
    Ok(replica)
}

pub(super) async fn send_parts<W: AsyncWrite + Unpin>(
    send: &mut W,
    parts: Vec<Vec<u8>>,
) -> Result<()> {
    for part in parts {
        for chunk in part.chunks(arcrelay_wire::MAX_BLOB_CHUNK_SIZE) {
            arcrelay_transport::write_frame(send, chunk, arcrelay_wire::MAX_BLOB_CHUNK_SIZE)
                .await
                .map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}

fn chrono_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
