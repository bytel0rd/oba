use oba_network::commons::{Message, OperationType};
use oba_network::request::Request;

use crate::helpers::ArcSocket;
use crate::streams::{AtomicStreamDB};


pub async fn process(stream_db: AtomicStreamDB, message: &Message<Request>, socket: ArcSocket) -> anyhow::Result<()> {
    let ok_message = Message::new(OperationType::RESPONSE, "Successful")?;

    match message.operation {
        OperationType::StreamAppend => {
            if let Request::AppendStreamReq { key, value, expire } = &message.data {
                let db = stream_db.read().await;
                db.append_stream(key.as_str(), value, expire.clone()).await?;
                socket.write_to_atomic_socket(ok_message.to_encoded()?.as_ref()).await?;
            }
        }
        OperationType::StreamDelete => {}
        OperationType::StreamListen => {
            if let Request::ListenToStreamReq { key, start_from } = &message.data {
                let db = stream_db.read().await;
                db.listen_to_stream(key.as_str(), start_from.clone(), socket.clone()).await?;
            }
        }
        OperationType::StreamGet => {
            if let Request::GetStreamReq { key, end_at, start_from } = &message.data {
                let db = stream_db.read().await;
                let result = db.get_stream(key.as_str(), start_from.clone(), end_at.clone()).await?;
                let message = Message::new(OperationType::RESPONSE, result)?;
                socket.write_to_atomic_socket(message.to_encoded()?.as_ref()).await?;
            }
        }
        _ => {}
    }

    Ok(())
}