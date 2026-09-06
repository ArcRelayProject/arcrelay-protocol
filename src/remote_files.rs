use std::collections::HashSet;
use std::path::PathBuf;

use arcrelay_peer::{CapabilityId, Grant, GrantConstraints, GrantDirection};
use arcrelay_wire::proto;
pub use arcrelay_wire::{
    MAX_REMOTE_FILE_CONTENT_SIZE, MAX_REMOTE_FILE_MESSAGE_SIZE, MAX_REMOTE_FILE_THUMBNAIL_SIZE,
};

use async_trait::async_trait;
use prost::Message;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};

pub const DEFAULT_REMOTE_DIRECTORY_PAGE_SIZE: u32 = 100;
pub const MAX_REMOTE_DIRECTORY_PAGE_SIZE: u32 = 200;
/// Protocol v2 requires every failed response to carry a stable error code.
pub const REMOTE_FILE_PROTOCOL_VERSION: u32 = 2;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub enum RemoteFileErrorCode {
    InvalidArgument,
    PermissionDenied,
    NotFound,
    Conflict,
    ResourceExhausted,
    FailedPrecondition,
    Unavailable,
    Cancelled,
    Internal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct RemoteFileError {
    pub code: RemoteFileErrorCode,
    pub message: String,
}

impl RemoteFileError {
    pub fn new(code: RemoteFileErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for RemoteFileError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.message.fmt(formatter)
    }
}

impl std::error::Error for RemoteFileError {}

pub type RemoteFileResult<T> = std::result::Result<T, RemoteFileError>;

#[derive(Debug, thiserror::Error)]
pub enum RemoteFileCodecError {
    #[error("invalid remote file message: {0}")]
    Validation(String),
    #[error("failed to decode remote file protobuf: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("remote file frame error: {0}")]
    Frame(#[from] arcrelay_transport::FrameError),
}

impl From<String> for RemoteFileCodecError {
    fn from(message: String) -> Self {
        Self::Validation(message)
    }
}

impl From<&str> for RemoteFileCodecError {
    fn from(message: &str) -> Self {
        Self::Validation(message.to_owned())
    }
}

pub type RemoteFileCodecResult<T> = std::result::Result<T, RemoteFileCodecError>;

#[derive(Debug, Clone, Default)]
pub struct RemoteFileAccess {
    read_all: bool,
    write_all: bool,
    read_shares: HashSet<String>,
    write_shares: HashSet<String>,
}

impl RemoteFileAccess {
    pub fn from_grants(grants: &[Grant], direction: GrantDirection) -> Self {
        let mut access = Self::default();
        for grant in grants.iter().filter(|grant| grant.direction == direction) {
            let (all, shares, writable_constraint) = match &grant.constraints {
                GrantConstraints::None => (true, &[][..], true),
                GrantConstraints::RemoteFileShares {
                    share_ids,
                    writable,
                } => (false, share_ids.as_slice(), *writable),
            };
            match grant.capability {
                CapabilityId::RemoteFilesRead => {
                    access.read_all |= all;
                    access.read_shares.extend(shares.iter().cloned());
                }
                CapabilityId::RemoteFilesWrite if writable_constraint => {
                    access.write_all |= all;
                    access.write_shares.extend(shares.iter().cloned());
                }
                _ => {}
            }
        }
        access
    }

    pub fn can_list_shares(&self) -> bool {
        self.read_all
            || self.write_all
            || !self.read_shares.is_empty()
            || !self.write_shares.is_empty()
    }

    pub fn can_read(&self, share_id: &str) -> bool {
        self.read_all || self.read_shares.contains(share_id)
    }

    pub fn can_write(&self, share_id: &str) -> bool {
        self.write_all || self.write_shares.contains(share_id)
    }

    #[cfg(test)]
    pub(crate) fn allow_all() -> Self {
        Self {
            read_all: true,
            write_all: true,
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct RemoteFileShare {
    pub id: String,
    pub name: String,
    pub writable: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub enum RemoteFileKind {
    File,
    Folder,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub enum RemoteFileSortKey {
    #[default]
    Name,
    Modified,
    Type,
    Size,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub enum RemoteFileSortDirection {
    #[default]
    Ascending,
    Descending,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct RemoteFileEntry {
    pub name: String,
    pub relative_path: String,
    pub kind: RemoteFileKind,
    pub size: u64,
    pub modified_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct RemoteFileDirectoryPage {
    #[serde(default)]
    pub entries: Vec<RemoteFileEntry>,
    #[serde(default)]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RemoteFileThumbnail {
    pub bytes: Vec<u8>,
    pub media_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "operation", rename_all = "camelCase")]
pub enum RemoteFileRequest {
    ListShares,
    ListDirectory {
        share_id: String,
        relative_path: String,
        #[serde(default)]
        cursor: Option<String>,
        /// Zero selects the bounded default page size. Callers must follow
        /// `next_cursor`; unpaginated directory listing is not supported.
        #[serde(default)]
        limit: u32,
        #[serde(default)]
        search: Option<String>,
        #[serde(default)]
        sort_key: RemoteFileSortKey,
        #[serde(default)]
        sort_direction: RemoteFileSortDirection,
    },
    CreateDirectory {
        share_id: String,
        relative_path: String,
        name: String,
    },
    Rename {
        share_id: String,
        relative_path: String,
        new_name: String,
    },
    Delete {
        share_id: String,
        relative_path: String,
    },
    Download {
        share_id: String,
        relative_path: String,
    },
    Thumbnail {
        share_id: String,
        relative_path: String,
        max_dimension: u32,
    },
    Upload {
        share_id: String,
        relative_path: String,
        name: String,
        size: u64,
        overwrite: bool,
        #[serde(default)]
        expected_modified_at_ms: Option<i64>,
    },
}

impl RemoteFileRequest {
    /// Stable, non-sensitive operation name suitable for diagnostics.
    pub const fn operation_name(&self) -> &'static str {
        match self {
            Self::ListShares => "list_shares",
            Self::ListDirectory { .. } => "list_directory",
            Self::CreateDirectory { .. } => "create_directory",
            Self::Rename { .. } => "rename",
            Self::Delete { .. } => "delete",
            Self::Download { .. } => "download",
            Self::Thumbnail { .. } => "thumbnail",
            Self::Upload { .. } => "upload",
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::ListShares => Ok(()),
            Self::ListDirectory {
                share_id,
                relative_path,
                cursor,
                limit,
                search,
                ..
            } => {
                validate_share_id(share_id)?;
                validate_relative_path(relative_path)?;
                if *limit > MAX_REMOTE_DIRECTORY_PAGE_SIZE
                    || cursor.as_ref().is_some_and(|value| {
                        value.len() > 1024 || value.chars().any(char::is_control)
                    })
                    || search.as_ref().is_some_and(|value| {
                        value.len() > 512 || value.chars().any(char::is_control)
                    })
                {
                    return Err("invalid remote directory query".into());
                }
                Ok(())
            }
            Self::CreateDirectory {
                share_id,
                relative_path,
                name,
            }
            | Self::Upload {
                share_id,
                relative_path,
                name,
                ..
            } => {
                validate_share_id(share_id)?;
                validate_relative_path(relative_path)?;
                validate_file_name(name)
            }
            Self::Rename {
                share_id,
                relative_path,
                new_name,
            } => {
                validate_share_id(share_id)?;
                validate_relative_path(relative_path)?;
                validate_file_name(new_name)
            }
            Self::Delete {
                share_id,
                relative_path,
            }
            | Self::Download {
                share_id,
                relative_path,
            } => {
                validate_share_id(share_id)?;
                validate_relative_path(relative_path)
            }
            Self::Thumbnail {
                share_id,
                relative_path,
                max_dimension,
            } => {
                validate_share_id(share_id)?;
                validate_relative_path(relative_path)?;
                if !(16..=4096).contains(max_dimension) {
                    return Err("invalid thumbnail dimensions".into());
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RemoteFileResponse {
    pub ok: bool,
    pub error: Option<RemoteFileError>,
    #[serde(default)]
    pub shares: Vec<RemoteFileShare>,
    #[serde(default)]
    pub entries: Vec<RemoteFileEntry>,
    #[serde(default)]
    pub next_cursor: Option<String>,
    pub entry: Option<RemoteFileEntry>,
    #[serde(default)]
    pub thumbnail_size: u64,
    #[serde(default)]
    pub thumbnail_media_type: Option<String>,
}

impl RemoteFileResponse {
    pub fn success() -> Self {
        Self {
            ok: true,
            error: None,
            shares: Vec::new(),
            entries: Vec::new(),
            next_cursor: None,
            entry: None,
            thumbnail_size: 0,
            thumbnail_media_type: None,
        }
    }

    pub fn failure(code: RemoteFileErrorCode, message: impl Into<String>) -> Self {
        let mut message = message.into();
        if message.len() > 8 * 1024 {
            let mut boundary = 8 * 1024;
            while !message.is_char_boundary(boundary) {
                boundary -= 1;
            }
            message.truncate(boundary);
        }
        Self {
            ok: false,
            error: Some(RemoteFileError::new(code, message)),
            shares: Vec::new(),
            entries: Vec::new(),
            next_cursor: None,
            entry: None,
            thumbnail_size: 0,
            thumbnail_media_type: None,
        }
    }

    pub fn from_error(error: RemoteFileError) -> Self {
        Self::failure(error.code, error.message)
    }

    fn validate(&self) -> Result<(), String> {
        if self.ok == self.error.is_some()
            || self
                .error
                .as_ref()
                .is_some_and(|error| error.message.len() > 8 * 1024)
            || self.shares.len() > 128
            || self.entries.len() > MAX_REMOTE_DIRECTORY_PAGE_SIZE as usize
            || self
                .next_cursor
                .as_ref()
                .is_some_and(|cursor| cursor.len() > 1024 || cursor.chars().any(char::is_control))
            || self.thumbnail_size > MAX_REMOTE_FILE_THUMBNAIL_SIZE
            || (self.thumbnail_size == 0) != self.thumbnail_media_type.is_none()
            || self
                .thumbnail_media_type
                .as_ref()
                .is_some_and(|media_type| media_type.len() > 256)
        {
            return Err("invalid remote file response".into());
        }
        for share in &self.shares {
            validate_share_id(&share.id)?;
            if share.name.is_empty() || share.name.len() > 255 {
                return Err("invalid remote file share".into());
            }
        }
        for entry in self.entries.iter().chain(self.entry.iter()) {
            validate_file_name(&entry.name)?;
            validate_relative_path(&entry.relative_path)?;
            if entry.size > MAX_REMOTE_FILE_CONTENT_SIZE {
                return Err("remote file entry exceeds the content limit".into());
            }
        }
        Ok(())
    }
}

fn validate_share_id(value: &str) -> Result<(), String> {
    if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
        return Err("invalid remote file share id".into());
    }
    Ok(())
}

fn validate_relative_path(value: &str) -> Result<(), String> {
    if value.len() > 4096
        || value.contains('\\')
        || value
            .chars()
            .any(|character| character == '\0' || character.is_control())
        || (!value.is_empty()
            && value.split('/').any(|component| {
                component.is_empty()
                    || component == "."
                    || component == ".."
                    || component.len() > 255
            }))
    {
        return Err("invalid remote file path".into());
    }
    Ok(())
}

fn validate_file_name(value: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > 255
        || matches!(value, "." | "..")
        || value.contains(['/', '\\', '\0'])
        || value.chars().any(char::is_control)
    {
        return Err("invalid remote file name".into());
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct RemoteFileDownload {
    pub path: PathBuf,
    pub entry: RemoteFileEntry,
}

#[derive(Debug, Clone)]
pub struct RemoteFileUpload {
    pub destination: PathBuf,
    pub entry: RemoteFileEntry,
    pub overwrite: bool,
    pub expected_modified_at_ms: Option<i64>,
}

#[async_trait]
pub trait RemoteFileProvider: Send + Sync {
    async fn list_shares(&self) -> RemoteFileResult<Vec<RemoteFileShare>>;

    async fn list_directory(
        &self,
        share_id: &str,
        relative_path: &str,
        cursor: Option<&str>,
        limit: u32,
        search: Option<&str>,
        sort_key: RemoteFileSortKey,
        sort_direction: RemoteFileSortDirection,
    ) -> RemoteFileResult<RemoteFileDirectoryPage>;

    async fn create_directory(
        &self,
        share_id: &str,
        relative_path: &str,
        name: &str,
    ) -> RemoteFileResult<RemoteFileEntry>;

    async fn rename(
        &self,
        share_id: &str,
        relative_path: &str,
        new_name: &str,
    ) -> RemoteFileResult<RemoteFileEntry>;

    async fn delete(&self, share_id: &str, relative_path: &str) -> RemoteFileResult<()>;

    async fn prepare_download(
        &self,
        share_id: &str,
        relative_path: &str,
    ) -> RemoteFileResult<RemoteFileDownload>;

    async fn prepare_thumbnail(
        &self,
        _share_id: &str,
        _relative_path: &str,
        _max_dimension: u32,
    ) -> RemoteFileResult<Option<RemoteFileThumbnail>> {
        Ok(None)
    }

    async fn prepare_upload(
        &self,
        share_id: &str,
        relative_path: &str,
        name: &str,
        size: u64,
        overwrite: bool,
        expected_modified_at_ms: Option<i64>,
    ) -> RemoteFileResult<RemoteFileUpload>;
}

pub trait RemoteFileWireMessage: Sized {
    fn encode_wire(&self) -> RemoteFileCodecResult<Vec<u8>>;
    fn decode_wire(bytes: &[u8]) -> RemoteFileCodecResult<Self>;
}

impl RemoteFileWireMessage for RemoteFileRequest {
    fn encode_wire(&self) -> RemoteFileCodecResult<Vec<u8>> {
        use proto::remote_file_request_frame::Body;

        self.validate()?;
        let body = match self {
            Self::ListShares => Body::ListShares(proto::RemoteFileListSharesRequest {}),
            Self::ListDirectory {
                share_id,
                relative_path,
                cursor,
                limit,
                search,
                sort_key,
                sort_direction,
            } => Body::ListDirectory(proto::RemoteFileListDirectoryRequest {
                share_id: share_id.clone(),
                relative_path: relative_path.clone(),
                cursor: cursor.clone(),
                limit: *limit,
                search: search.clone(),
                sort_key: encode_sort_key(*sort_key),
                sort_direction: encode_sort_direction(*sort_direction),
            }),
            Self::CreateDirectory {
                share_id,
                relative_path,
                name,
            } => Body::CreateDirectory(proto::RemoteFileCreateDirectoryRequest {
                share_id: share_id.clone(),
                relative_path: relative_path.clone(),
                name: name.clone(),
            }),
            Self::Rename {
                share_id,
                relative_path,
                new_name,
            } => Body::Rename(proto::RemoteFileRenameRequest {
                share_id: share_id.clone(),
                relative_path: relative_path.clone(),
                new_name: new_name.clone(),
            }),
            Self::Delete {
                share_id,
                relative_path,
            } => Body::Delete(proto::RemoteFileDeleteRequest {
                share_id: share_id.clone(),
                relative_path: relative_path.clone(),
            }),
            Self::Download {
                share_id,
                relative_path,
            } => Body::Download(proto::RemoteFileDownloadRequest {
                share_id: share_id.clone(),
                relative_path: relative_path.clone(),
            }),
            Self::Thumbnail {
                share_id,
                relative_path,
                max_dimension,
            } => Body::Thumbnail(proto::RemoteFileThumbnailRequest {
                share_id: share_id.clone(),
                relative_path: relative_path.clone(),
                max_dimension: *max_dimension,
            }),
            Self::Upload {
                share_id,
                relative_path,
                name,
                size,
                overwrite,
                expected_modified_at_ms,
            } => Body::Upload(proto::RemoteFileUploadRequest {
                share_id: share_id.clone(),
                relative_path: relative_path.clone(),
                name: name.clone(),
                size: *size,
                overwrite: *overwrite,
                expected_modified_at_ms: *expected_modified_at_ms,
            }),
        };
        Ok(proto::RemoteFileRequestFrame { body: Some(body) }.encode_to_vec())
    }

    fn decode_wire(bytes: &[u8]) -> RemoteFileCodecResult<Self> {
        use proto::remote_file_request_frame::Body;

        let frame = proto::RemoteFileRequestFrame::decode(bytes)?;
        let request = match frame
            .body
            .ok_or_else(|| "remote file request body is missing".to_owned())?
        {
            Body::ListShares(_) => Self::ListShares,
            Body::ListDirectory(request) => Self::ListDirectory {
                share_id: request.share_id,
                relative_path: request.relative_path,
                cursor: request.cursor,
                limit: request.limit,
                search: request.search,
                sort_key: decode_sort_key(request.sort_key)?,
                sort_direction: decode_sort_direction(request.sort_direction)?,
            },
            Body::CreateDirectory(request) => Self::CreateDirectory {
                share_id: request.share_id,
                relative_path: request.relative_path,
                name: request.name,
            },
            Body::Rename(request) => Self::Rename {
                share_id: request.share_id,
                relative_path: request.relative_path,
                new_name: request.new_name,
            },
            Body::Delete(request) => Self::Delete {
                share_id: request.share_id,
                relative_path: request.relative_path,
            },
            Body::Download(request) => Self::Download {
                share_id: request.share_id,
                relative_path: request.relative_path,
            },
            Body::Thumbnail(request) => Self::Thumbnail {
                share_id: request.share_id,
                relative_path: request.relative_path,
                max_dimension: request.max_dimension,
            },
            Body::Upload(request) => Self::Upload {
                share_id: request.share_id,
                relative_path: request.relative_path,
                name: request.name,
                size: request.size,
                overwrite: request.overwrite,
                expected_modified_at_ms: request.expected_modified_at_ms,
            },
        };
        request.validate()?;
        Ok(request)
    }
}

impl RemoteFileWireMessage for RemoteFileResponse {
    fn encode_wire(&self) -> RemoteFileCodecResult<Vec<u8>> {
        self.validate()?;
        Ok(proto::RemoteFileResponseFrame {
            ok: self.ok,
            error: self.error.as_ref().map(|error| error.message.clone()),
            shares: self.shares.iter().map(encode_share).collect(),
            entries: self.entries.iter().map(encode_entry).collect(),
            next_cursor: self.next_cursor.clone(),
            entry: self.entry.as_ref().map(encode_entry),
            thumbnail_size: self.thumbnail_size,
            thumbnail_media_type: self.thumbnail_media_type.clone(),
            error_code: self
                .error
                .as_ref()
                .map(|error| encode_error_code(error.code)),
        }
        .encode_to_vec())
    }

    fn decode_wire(bytes: &[u8]) -> RemoteFileCodecResult<Self> {
        let frame = proto::RemoteFileResponseFrame::decode(bytes)?;
        let error = match (frame.error_code, frame.error) {
            (Some(code), Some(message)) => {
                Some(RemoteFileError::new(decode_error_code(code)?, message))
            }
            (None, None) => None,
            _ => {
                return Err(
                    "remote file response error code and message must appear together".into(),
                )
            }
        };
        let response = Self {
            ok: frame.ok,
            error,
            shares: frame.shares.into_iter().map(decode_share).collect(),
            entries: frame
                .entries
                .into_iter()
                .map(decode_entry)
                .collect::<Result<_, _>>()?,
            next_cursor: frame.next_cursor,
            entry: frame.entry.map(decode_entry).transpose()?,
            thumbnail_size: frame.thumbnail_size,
            thumbnail_media_type: frame.thumbnail_media_type,
        };
        response.validate()?;
        Ok(response)
    }
}

fn encode_error_code(code: RemoteFileErrorCode) -> i32 {
    match code {
        RemoteFileErrorCode::InvalidArgument => proto::RemoteFileErrorCode::InvalidArgument as i32,
        RemoteFileErrorCode::PermissionDenied => {
            proto::RemoteFileErrorCode::PermissionDenied as i32
        }
        RemoteFileErrorCode::NotFound => proto::RemoteFileErrorCode::NotFound as i32,
        RemoteFileErrorCode::Conflict => proto::RemoteFileErrorCode::Conflict as i32,
        RemoteFileErrorCode::ResourceExhausted => {
            proto::RemoteFileErrorCode::ResourceExhausted as i32
        }
        RemoteFileErrorCode::FailedPrecondition => {
            proto::RemoteFileErrorCode::FailedPrecondition as i32
        }
        RemoteFileErrorCode::Unavailable => proto::RemoteFileErrorCode::Unavailable as i32,
        RemoteFileErrorCode::Cancelled => proto::RemoteFileErrorCode::Cancelled as i32,
        RemoteFileErrorCode::Internal => proto::RemoteFileErrorCode::Internal as i32,
    }
}

fn decode_error_code(code: i32) -> RemoteFileCodecResult<RemoteFileErrorCode> {
    match proto::RemoteFileErrorCode::try_from(code) {
        Ok(proto::RemoteFileErrorCode::InvalidArgument) => Ok(RemoteFileErrorCode::InvalidArgument),
        Ok(proto::RemoteFileErrorCode::PermissionDenied) => {
            Ok(RemoteFileErrorCode::PermissionDenied)
        }
        Ok(proto::RemoteFileErrorCode::NotFound) => Ok(RemoteFileErrorCode::NotFound),
        Ok(proto::RemoteFileErrorCode::Conflict) => Ok(RemoteFileErrorCode::Conflict),
        Ok(proto::RemoteFileErrorCode::ResourceExhausted) => {
            Ok(RemoteFileErrorCode::ResourceExhausted)
        }
        Ok(proto::RemoteFileErrorCode::FailedPrecondition) => {
            Ok(RemoteFileErrorCode::FailedPrecondition)
        }
        Ok(proto::RemoteFileErrorCode::Unavailable) => Ok(RemoteFileErrorCode::Unavailable),
        Ok(proto::RemoteFileErrorCode::Cancelled) => Ok(RemoteFileErrorCode::Cancelled),
        Ok(proto::RemoteFileErrorCode::Internal) => Ok(RemoteFileErrorCode::Internal),
        Ok(proto::RemoteFileErrorCode::Unspecified) | Err(_) => {
            Err("invalid remote file error code".into())
        }
    }
}

fn encode_share(share: &RemoteFileShare) -> proto::RemoteFileShareInfo {
    proto::RemoteFileShareInfo {
        id: share.id.clone(),
        name: share.name.clone(),
        writable: share.writable,
    }
}

fn decode_share(share: proto::RemoteFileShareInfo) -> RemoteFileShare {
    RemoteFileShare {
        id: share.id,
        name: share.name,
        writable: share.writable,
    }
}

fn encode_entry(entry: &RemoteFileEntry) -> proto::RemoteFileEntryInfo {
    proto::RemoteFileEntryInfo {
        name: entry.name.clone(),
        relative_path: entry.relative_path.clone(),
        kind: match entry.kind {
            RemoteFileKind::File => proto::RemoteFileEntryKind::File as i32,
            RemoteFileKind::Folder => proto::RemoteFileEntryKind::Folder as i32,
        },
        size: entry.size,
        modified_at_ms: entry.modified_at_ms,
    }
}

fn decode_entry(entry: proto::RemoteFileEntryInfo) -> RemoteFileCodecResult<RemoteFileEntry> {
    let kind = match proto::RemoteFileEntryKind::try_from(entry.kind) {
        Ok(proto::RemoteFileEntryKind::File) => RemoteFileKind::File,
        Ok(proto::RemoteFileEntryKind::Folder) => RemoteFileKind::Folder,
        _ => return Err("invalid remote file entry kind".into()),
    };
    Ok(RemoteFileEntry {
        name: entry.name,
        relative_path: entry.relative_path,
        kind,
        size: entry.size,
        modified_at_ms: entry.modified_at_ms,
    })
}

fn encode_sort_key(key: RemoteFileSortKey) -> i32 {
    match key {
        RemoteFileSortKey::Name => proto::RemoteFileSortKey::Name as i32,
        RemoteFileSortKey::Modified => proto::RemoteFileSortKey::Modified as i32,
        RemoteFileSortKey::Type => proto::RemoteFileSortKey::Type as i32,
        RemoteFileSortKey::Size => proto::RemoteFileSortKey::Size as i32,
    }
}

fn decode_sort_key(key: i32) -> RemoteFileCodecResult<RemoteFileSortKey> {
    match proto::RemoteFileSortKey::try_from(key) {
        Ok(proto::RemoteFileSortKey::Unspecified | proto::RemoteFileSortKey::Name) => {
            Ok(RemoteFileSortKey::Name)
        }
        Ok(proto::RemoteFileSortKey::Modified) => Ok(RemoteFileSortKey::Modified),
        Ok(proto::RemoteFileSortKey::Type) => Ok(RemoteFileSortKey::Type),
        Ok(proto::RemoteFileSortKey::Size) => Ok(RemoteFileSortKey::Size),
        Err(_) => Err("invalid remote file sort key".into()),
    }
}

fn encode_sort_direction(direction: RemoteFileSortDirection) -> i32 {
    match direction {
        RemoteFileSortDirection::Ascending => proto::RemoteFileSortDirection::Ascending as i32,
        RemoteFileSortDirection::Descending => proto::RemoteFileSortDirection::Descending as i32,
    }
}

fn decode_sort_direction(direction: i32) -> RemoteFileCodecResult<RemoteFileSortDirection> {
    match proto::RemoteFileSortDirection::try_from(direction) {
        Ok(
            proto::RemoteFileSortDirection::Unspecified | proto::RemoteFileSortDirection::Ascending,
        ) => Ok(RemoteFileSortDirection::Ascending),
        Ok(proto::RemoteFileSortDirection::Descending) => Ok(RemoteFileSortDirection::Descending),
        Err(_) => Err("invalid remote file sort direction".into()),
    }
}

pub async fn write_remote_message<W, T>(writer: &mut W, message: &T) -> RemoteFileCodecResult<()>
where
    W: AsyncWrite + Unpin,
    T: RemoteFileWireMessage,
{
    let bytes = message.encode_wire()?;
    arcrelay_transport::write_frame(writer, &bytes, MAX_REMOTE_FILE_MESSAGE_SIZE).await?;
    Ok(())
}

pub async fn read_remote_message<R, T>(reader: &mut R) -> RemoteFileCodecResult<T>
where
    R: AsyncRead + Unpin,
    T: RemoteFileWireMessage,
{
    let bytes = arcrelay_transport::read_frame(reader, MAX_REMOTE_FILE_MESSAGE_SIZE).await?;
    T::decode_wire(&bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as _;

    #[test]
    fn protobuf_decode_errors_keep_their_source() {
        let error = RemoteFileRequest::decode_wire(&[0xff]).unwrap_err();
        assert!(matches!(error, RemoteFileCodecError::Decode(_)));
        assert!(error.source().is_some());
    }

    #[tokio::test]
    async fn request_and_response_round_trip() {
        let (mut client, mut server) = tokio::io::duplex(4096);
        let request = RemoteFileRequest::ListDirectory {
            share_id: "home".into(),
            relative_path: "Projects".into(),
            cursor: Some("100".into()),
            limit: 100,
            search: Some("report".into()),
            sort_key: RemoteFileSortKey::Modified,
            sort_direction: RemoteFileSortDirection::Descending,
        };
        write_remote_message(&mut client, &request).await.unwrap();
        let decoded: RemoteFileRequest = read_remote_message(&mut server).await.unwrap();
        assert_eq!(decoded, request);
    }

    #[tokio::test]
    async fn error_response_round_trip_preserves_machine_code() {
        let (mut client, mut server) = tokio::io::duplex(4096);
        let response = RemoteFileResponse::failure(
            RemoteFileErrorCode::PermissionDenied,
            "write access is not permitted",
        );
        write_remote_message(&mut client, &response).await.unwrap();
        let decoded: RemoteFileResponse = read_remote_message(&mut server).await.unwrap();
        assert_eq!(decoded, response);
        assert_eq!(
            decoded.error.unwrap().code,
            RemoteFileErrorCode::PermissionDenied
        );
    }

    #[test]
    fn upload_request_without_revision_uses_optional_field_default() {
        let request = RemoteFileRequest::decode_wire(
            &proto::RemoteFileRequestFrame {
                body: Some(proto::remote_file_request_frame::Body::Upload(
                    proto::RemoteFileUploadRequest {
                        share_id: "home".into(),
                        relative_path: "".into(),
                        name: "notes.txt".into(),
                        size: 4,
                        overwrite: true,
                        expected_modified_at_ms: None,
                    },
                )),
            }
            .encode_to_vec(),
        )
        .unwrap();
        assert_eq!(
            request,
            RemoteFileRequest::Upload {
                share_id: "home".into(),
                relative_path: "".into(),
                name: "notes.txt".into(),
                size: 4,
                overwrite: true,
                expected_modified_at_ms: None,
            }
        );
    }

    #[test]
    fn unspecified_directory_options_use_bounded_defaults() {
        let request = RemoteFileRequest::decode_wire(
            &proto::RemoteFileRequestFrame {
                body: Some(proto::remote_file_request_frame::Body::ListDirectory(
                    proto::RemoteFileListDirectoryRequest {
                        share_id: "home".into(),
                        relative_path: "".into(),
                        ..Default::default()
                    },
                )),
            }
            .encode_to_vec(),
        )
        .unwrap();
        assert_eq!(
            request,
            RemoteFileRequest::ListDirectory {
                share_id: "home".into(),
                relative_path: "".into(),
                cursor: None,
                limit: 0,
                search: None,
                sort_key: RemoteFileSortKey::Name,
                sort_direction: RemoteFileSortDirection::Ascending,
            }
        );
    }
}
