use std::sync::Arc;
use std::time::Duration;

use bytes::BufMut;
use rand::random;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tracing::{error, info};

use crate::client::{ObaClient, ObaClientError};

mod client;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let node_id: i64 = random();

    info!("node id: {}", node_id);
    let host_url = "localhost:9050";
    let client = Arc::new(ObaClient::new(host_url.to_string()));

    // spawn a writer
    let writer = client.clone();
    tokio::spawn(async move {
        loop {
            let x: i64 = random();
            let value = format!("server: {} generated: {}", node_id, x);
            writer.append_to_stream("stream_1", value.as_bytes(), None).await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });

    let (tx, mut rx) = tokio::sync::mpsc::channel(10);


    // spawn a streamer
    let streamer = client.clone();
    let tx_1 = tx.clone();
    tokio::spawn(async move {
        streamer.listen_to_stream("stream_1", None, tx_1).await.unwrap();
    });

    while let Some(received)  = rx.recv().await {
        match received {
            Ok(streams) => {
                for s in streams {
                    let value = String::from_utf8_lossy(s.value.as_slice());
                    info!("Node: {} Received {}", node_id, value);
                }

            }
            Err(err) => {
                error!("stream error={:?}", err);
            }
        }
    }


    Ok(())
}
