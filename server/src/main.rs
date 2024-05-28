use std::sync::{Arc};
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, Interest, Ready};
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tracing::{debug, error, info, trace};

use oba_network::commons::{Message, OperationType};
use oba_network::encoding::{self, Encoding, EncodingState};
use oba_network::request::Request;

use crate::executor::process;
use crate::helpers::{ArcSocket, AtomicSocketWriter};
use crate::streams::{AtomicStreamDB, StreamDB};

mod streams;
mod executor;
mod helpers;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    info!("Initializing application");

    let stream_db: AtomicStreamDB = Arc::new(RwLock::new(StreamDB::new()));

    let stream_db_1 = stream_db.clone();
    let clean_keys_interval_millisecond = 200;
    tokio::spawn(async move {
        loop {
            stream_db_1.read()
                .await
                .clean_expired_keys()
                .await;

            tokio::time::sleep(Duration::from_millis(clean_keys_interval_millisecond)).await;
        }
    });


    let listening_address = "localhost:9050";
    let listener = tokio::net::TcpListener::bind(listening_address).await.unwrap();

    info!("listening on {}", listening_address);
    loop {
        let (socket, socket_address) = listener.accept().await.unwrap();
        let stream_db_1 = stream_db.clone();
        tokio::spawn(async move {
            if let Err(err) = read_socket(stream_db_1, socket).await {
                error!("Error reading from socket. err={:?}", err);
            }
        });
    }
}

async fn read_socket(stream_db: AtomicStreamDB, mut socket: TcpStream) -> anyhow::Result<()> {
    let deserialize_error = Message::new(OperationType::ERROR, "Error deserializing request")?;
    let (mut socket_read, mut socket_write) = tokio::io::split(socket);
    let writer: ArcSocket = Arc::new(AtomicSocketWriter::new(socket_write));

    let mut packet_encoding = Encoding::default();
    let mut cached_buffer = BytesMut::new();


    loop {
        let mut read_buffer = BytesMut::new();

        let bytes_read_length = socket_read.read_buf(&mut read_buffer).await?;
        if bytes_read_length > 0 {
            let (left, right) = read_buffer.split_at(bytes_read_length);
            debug!("read_bytes= {:?}", &read_buffer);

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
                            let serialized_result = bincode::deserialize::<Message<Request>>(packet_encoding.get_bytes());
                            if serialized_result.is_err() {
                                writer.write_to_atomic_socket(deserialize_error.to_encoded()?.as_ref()).await?;
                                return Ok(());
                            }
                            packet_encoding = Encoding::default();
                            let message = serialized_result.unwrap();
                            let stream_db = stream_db.clone();
                            let writer = writer.clone();


                            tokio::spawn(async move {
                                trace!("processing message id={:?}", &message.id);
                                if let Err(err) = process(stream_db, &message, writer).await {
                                    error!("processing message request req={:?} err={:?}", &message.id, err);
                                }
                            }).await;
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
                    error!("encoding error= {:?}", err);
                    packet_encoding = Encoding::default();
                    cached_buffer.clear();
                    continue;
                }
            }
        }
    }
}