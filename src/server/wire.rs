use prost::Message;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::error::{ProtocolError, Result};
use crate::message::{MAX_CONTROL_FRAME_SIZE, MAX_RELIABLE_INPUT_FRAME_SIZE};
use crate::proto_msg::proto::{ClientControlFrame, ReliableInputFrame, ServerControlFrame};

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

pub async fn read_stream_kind<R: AsyncRead + Unpin>(reader: &mut R) -> Result<u8> {
    arcrelay_transport::read_stream_kind(reader)
        .await
        .map_err(map_frame_error)
}

pub async fn recv_message<R, M>(reader: &mut R, max_size: usize) -> Result<M>
where
    R: AsyncRead + Unpin,
    M: Message + Default,
{
    let bytes = arcrelay_transport::read_frame(reader, max_size)
        .await
        .map_err(map_frame_error)?;
    M::decode(bytes.as_slice())
        .map_err(|error| ProtocolError::Other(format!("protobuf decode failed: {error}")))
}

pub async fn send_message<W, M>(writer: &mut W, message: &M, max_size: usize) -> Result<()>
where
    W: AsyncWrite + Unpin,
    M: Message,
{
    let bytes = message.encode_to_vec();
    arcrelay_transport::write_frame(writer, &bytes, max_size)
        .await
        .map_err(map_frame_error)
}

pub async fn recv_control<R: AsyncRead + Unpin>(reader: &mut R) -> Result<ClientControlFrame> {
    recv_message(reader, MAX_CONTROL_FRAME_SIZE).await
}

pub async fn send_control<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &ServerControlFrame,
) -> Result<()> {
    send_message(writer, frame, MAX_CONTROL_FRAME_SIZE).await
}

pub async fn recv_reliable_input<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<ReliableInputFrame> {
    recv_message(reader, MAX_RELIABLE_INPUT_FRAME_SIZE).await
}

fn map_frame_error(error: arcrelay_transport::FrameError) -> ProtocolError {
    match error {
        arcrelay_transport::FrameError::Io(error) => ProtocolError::Io(error),
        arcrelay_transport::FrameError::TooLarge { actual, .. }
        | arcrelay_transport::FrameError::LengthOverflow(actual) => {
            ProtocolError::MessageTooLarge(actual)
        }
        arcrelay_transport::FrameError::Empty => {
            ProtocolError::Other("empty protobuf frame".into())
        }
    }
}
