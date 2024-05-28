use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::Debug;
use std::string::FromUtf8Error;
use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync;
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, error, info, trace};
use uuid::Uuid;

use oba_network::commons::{Message, OperationType, StoreMeta};
use oba_network::encoding::{Encoding, EncodingState};
use oba_network::request::Request;

pub struct ObaClient {
    host_url: String,
}

#[derive(Debug, Clone)]
pub enum ObaClientError {
    SocketError(String),
    ServerError(String),
    ClientError(String),
    InvalidResponseType,
    ThirdPartyError(String),
}

impl From<anyhow::Error> for  ObaClientError {
    fn from(value: anyhow::Error) -> Self {
        ObaClientError::ClientError(value.to_string())
    }
}

impl ObaClient {
    pub fn new(host_url: String) -> Self {
        ObaClient {
            host_url
        }
    }

    pub async fn append_to_stream(&self, key: &str, value: &[u8], timeout_in_mills_secs: Option<i64>) -> Result<(), ObaClientError> {
        let stream_message = Message::new(OperationType::StreamAppend, Request::AppendStreamReq {
            key: key.to_string(),
            value: value.to_vec(),
            // expire: timeout_in_mills_secs,
            expire: None,
        })?;
        ObaClient::send_and_respond(self.host_url.as_str(), stream_message).await?;
        Ok(())
    }

    pub async fn listen_to_stream(&self, key: &str, checkpoint_time: Option<i64>, sender: tokio::sync::mpsc::Sender<Result<Vec<StoreMeta<Vec<u8>>>, ObaClientError>>) -> Result<(), ObaClientError> {

        let mut socket = ObaClient::open_tcp_stream(self.host_url.as_str()).await?;
        let (mut socket_read, mut socket_write) = tokio::io::split(socket);

        let stream_message = Message::new(OperationType::StreamListen, Request::ListenToStreamReq {
            key: key.to_string(),
            start_from: checkpoint_time,
        })?;
        socket_write.write_all(stream_message.to_stream()?.as_ref()).await
            .map_err(|e| ObaClientError::SocketError(e.to_string()))?;

        let mut packet_encoding = Encoding::default();
        let mut cached_buffer = BytesMut::new();

        loop {
            let mut read_buffer = BytesMut::with_capacity(1024);

            let bytes_read_length = socket_read.read_buf(&mut read_buffer).await
                .map_err(|e| ObaClientError::SocketError(e.to_string()))?;
            if bytes_read_length > 0 {
                trace!("read_bytes= {:?}", &read_buffer);

                let (left, right) = read_buffer.split_at(bytes_read_length);

                // read previous incomplete bytes here
                cached_buffer.put(left.as_ref());
                debug!("cached_buffer= {:?}", &cached_buffer);

                match packet_encoding.read_from_stream(cached_buffer.clone().as_mut()) {
                    Ok((state, incomplete_read)) => {
                        // - Incomplete: All data read in
                        // - Incomplete: data not enough to be read
                        // - Complete: data fully read -in
                        // - Complete: data remaining
                        match state {
                            EncodingState::Complete => {
                                cached_buffer.clear();
                                if let Some(remaining_bytes) = incomplete_read {
                                    cached_buffer.put_slice(remaining_bytes);
                                }

                                // process encoded message here
                                let message = bincode::deserialize::<Message<Vec<StoreMeta<Vec<u8>>>>>(packet_encoding.get_bytes())
                                    .map_err(|e| ObaClientError::ClientError("Unable to deserialize streamed response".to_string()))?;
                                let _ = sender.send(Ok(message.data)).await;
                                packet_encoding = Encoding::default();

                            }
                            EncodingState::Incomplete => {
                                cached_buffer.clear();
                                if let Some(remaining_bytes) = incomplete_read {
                                    cached_buffer.put_slice(remaining_bytes);
                                }
                            }
                        }
                        continue;
                    }
                    Err(err) => {
                        debug!("encoding error= {:?}", &err);
                        let _ = sender.send(Err(ObaClientError::ThirdPartyError(err.to_string()))).await;
                        break;
                    }
                }
            }
        }

        Ok(())
    }


    async fn open_tcp_stream(host_url: &str) -> Result<TcpStream, ObaClientError> {
        TcpStream::connect(host_url).await
            .map_err(|e| { ObaClientError::ThirdPartyError(e.to_string()) })
    }

    async fn send_and_respond(host_url: &str, message: Message<Request>) -> Result<Vec<u8>, ObaClientError> {
        let mut socket = ObaClient::open_tcp_stream(host_url).await?;
        let (mut socket_read, mut socket_write) = tokio::io::split(socket);

        socket_write.write_all(message.to_stream()?.as_ref()).await
            .map_err(|e| ObaClientError::SocketError(e.to_string()));

        let mut packet_encoding = Encoding::default();
        let mut cached_buffer = BytesMut::new();

        loop {
            let mut read_buffer = BytesMut::new();

            let bytes_read_length = socket_read.read_buf(&mut read_buffer).await
                .map_err(|e| ObaClientError::SocketError(e.to_string()))?;
            if bytes_read_length > 0 {
                let (left, right) = read_buffer.split_at(bytes_read_length);
                trace!("read_bytes= {:?}", &read_buffer);

                // read previous incomplete bytes here
                cached_buffer.put(left.as_ref());
                match packet_encoding.read_from_stream(cached_buffer.clone().as_mut()) {
                    Ok((state, incomplete_read)) => {
                        // - Incomplete: All data read in
                        // - Incomplete: data not enough to be read
                        // - Complete: data fully read -in
                        // - Complete: data remaining
                        match state {
                            EncodingState::Complete => {
                                cached_buffer.clear();
                                if let Some(remaining_bytes) = incomplete_read {
                                    cached_buffer.put_slice(remaining_bytes);
                                }

                                // process encoded message here
                                let message = bincode::deserialize::<Message<Vec<u8>>>(packet_encoding.get_bytes())
                                    .map_err(|e| ObaClientError::ClientError("Unable to deserialize server response".to_string()))?;
                                debug!("response={:?}", message.data);
                                return match message.operation {
                                    OperationType::RESPONSE => {
                                        Ok(message.data)
                                    }
                                    OperationType::ERROR => {
                                        match String::from_utf8(message.data) {
                                            Ok(message) => {
                                                Err(ObaClientError::ServerError(message))
                                            }
                                            Err(err) => {
                                                debug!("failed to read error response message. err= {:?}", &err);
                                                Err(ObaClientError::ServerError("UnknownError".to_string()))
                                            }
                                        }
                                    }
                                    _ => {
                                        Err(ObaClientError::InvalidResponseType)
                                    }
                                }
                            }
                            EncodingState::Incomplete => {
                                cached_buffer.clear();
                                if let Some(remaining_bytes) = incomplete_read {
                                    cached_buffer.put_slice(remaining_bytes);
                                }
                            }
                        }
                    }
                    Err(err) => {
                        return Err(ObaClientError::ThirdPartyError(err.to_string()));
                    }
                }
            }
        }
    }
}

