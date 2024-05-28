use std::collections::{BTreeMap, HashSet};
use std::error::Error;
use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use serde::{Deserialize, Serialize};
use tokio::io::WriteHalf;
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tracing::{error, trace};

use oba_network::commons::{Message, OperationType, StoreMeta};

use crate::helpers::{ArcSocket, hash_key_to_unsigned_int};

#[derive(Debug, Clone)]
pub enum Expire {
    NEVER,
    At(chrono::DateTime<chrono::Utc>),
}

impl From<Option<i64>> for Expire {
    fn from(value: Option<i64>) -> Self {
        let value = value
            .map(|v| chrono::DateTime::from_timestamp_millis(v))
            .flatten();

        match value {
            None => { Expire::NEVER }
            Some(timestamp) => { Expire::At(timestamp) }
        }
    }
}

impl Default for Expire {
    fn default() -> Self {
        Expire::NEVER
    }
}

#[derive(Debug, thiserror::Error, strum::Display)]
pub enum StreamError {
    UnableToAcquireWriteLock,
    MissingWriter,
    FailedToWriteToSocket,
    UnableToCleanSocket,
}


pub struct StreamDB {
    db: RwLock<BTreeMap<u64, Vec<StoreMeta<Vec<u8>>>>>,
    expires_at: RwLock<BTreeMap<u64, Expire>>,
    listeners: Arc<RwLock<BTreeMap<u64, HashSet<ArcSocket>>>>,
}

impl StreamDB {
    pub fn new() -> Self {
        StreamDB {
            db: RwLock::new(BTreeMap::new()),
            expires_at: RwLock::new(BTreeMap::new()),
            listeners: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

pub type AtomicStreamDB = Arc<RwLock<StreamDB>>;

impl StreamDB {
    /// Append item to stream or create a new stream
    pub async fn append_stream(&self, key: &str, value: &Vec<u8>, expire: Option<i64>) -> Result<(), StreamError> {
        let key = hash_key_to_unsigned_int(key.as_bytes());
        let item_to_append = StoreMeta {
            timestamp: Default::default(),
            value: value.clone(),
        };

        let item_to_append_dup = item_to_append.clone();

        {
            let mut db = self.db.write().await;
            let mut expires_at = self.expires_at.write().await;

            if db.contains_key(&key) {
                if let Some(stream_values) = db.get_mut(&key) {
                    stream_values.push(item_to_append);
                }
            } else {
                db.insert(key, vec![item_to_append]);
            }

            expires_at.insert(key, expire.into());
        }

        let notification_message = vec![item_to_append_dup];
        let mut sockets_to_remove = HashSet::new();
        {
            if let Some(listeners) = self.listeners.read().await.get(&key) {
                // optimize for concurrent writing of sockets
                for l in listeners {
                    let stream_message = Message::new(OperationType::STREAM, notification_message.clone())
                        .map_err(|e| {
                            trace!("failed to serialize stream message error={:?}", e.to_string());
                            StreamError::FailedToWriteToSocket
                        })?;
                    if let Ok(encoded) = stream_message.to_encoded() {
                        // remove failed sockets
                        if let Err(err) = l.write_to_atomic_socket(encoded.as_ref()).await {
                            error!("failed to write message into stream error={:?}", err.to_string());
                            sockets_to_remove.insert(l.clone());
                        }
                    }
                }
            }
        }

        {
            if !sockets_to_remove.is_empty() {
                if let Some(listeners) = self.listeners.write().await.get_mut(&key) {
                    for socket in sockets_to_remove {
                        listeners.remove(&socket);
                    }
                }
            }
        }

        Ok(())
    }

    /// Read from streams and continuously listen for new streams
    pub async fn listen_to_stream(&self, key: &str, start_from: Option<i64>, socket: ArcSocket) -> anyhow::Result<()> {
        let hash_key = hash_key_to_unsigned_int(key.as_bytes());

        {
            let mut listners = self.listeners.write().await;
            if listners.contains_key(&hash_key) {
                if let Some(existing_listeners) = listners.get_mut(&hash_key) {
                    existing_listeners.insert(socket.clone());
                }
            } else {
                let mut new_socket_set = HashSet::new();
                new_socket_set.insert(socket.clone());
                listners.insert(hash_key, new_socket_set);
            }
        }

        let store_items = self.get_stream(key, start_from, None).await?;
        let stream_message = Message::new(OperationType::STREAM, store_items.unwrap_or(vec![]))?;

        let write_result = socket.write_to_atomic_socket(stream_message.to_stream()?.as_ref()).await;
        if write_result.is_err() {
            self.clean_listener(key, socket.clone()).await?;
            error!("write previous stream error= {}", write_result.unwrap_err().to_string());
            return Err(StreamError::FailedToWriteToSocket.into());
        }

        Ok(())
    }

    /// Get entire stream snapshot at the moment
    pub async fn get_stream(&self, key: &str,
                            start_from: Option<i64>,
                            end_at: Option<i64>,
    ) -> Result<Option<Vec<StoreMeta<Vec<u8>>>>, StreamError> {
        let key = hash_key_to_unsigned_int(key.as_bytes());
        let stream_db = self.db.read().await;

        let streams = stream_db.get(&key);
        if streams.is_none() {
            return Ok(None);
        }

        let start_time_stamp = start_from.unwrap_or(0);
        let end_time_stamp = end_at.unwrap_or(chrono::Utc::now().timestamp());

        let filtered_streams = streams.unwrap()
            .iter().filter_map(|v| {
            let entry_timestamp = v.timestamp.timestamp();
            if entry_timestamp >= start_time_stamp && entry_timestamp <= end_time_stamp {
                return Some(v.clone());
            }
            return None;
        }).collect::<Vec<StoreMeta<Vec<u8>>>>();

        Ok(Some(filtered_streams))
    }

    /// Delete the entire stream
    pub async fn delete_stream(&mut self, key: &str) -> Result<(), StreamError> {
        let key = hash_key_to_unsigned_int(key.as_bytes());

        let mut db = self.db.write().await;
        let mut expires_at = self.expires_at.write().await;

        let mut listeners = self.listeners.write().await;

        db.remove(&key);
        expires_at.remove(&key);
        listeners.remove(&key);
        Ok(())
    }

    pub async fn has_stream(&self, key: &str) -> Result<bool, StreamError> {
        let key = hash_key_to_unsigned_int(key.as_bytes());
        let metadata_store = self.expires_at.read().await;
        let contains_key = metadata_store.contains_key(&key);
        Ok(contains_key)
    }

    pub async fn clean_listener(&self, key: &str, socket: ArcSocket) -> Result<(), StreamError> {
        let key = hash_key_to_unsigned_int(key.as_bytes());

        let mut listeners = self.listeners.write().await;
        if let Some(listeners) = listeners.get_mut(&key) {
            listeners.remove(&socket);
        }
        Ok(())
    }

    pub async fn clean_expired_keys(&self)  {
        let mut keys_to_delete = HashSet::new();
        trace!("Cleaning expired keys");

        {
            let expires_at_store = self.expires_at.read().await;
            let current_time = chrono::Utc::now();
            expires_at_store.iter()
                .for_each(|(&key, value)| {
                    match value {
                        Expire::At(time) => {
                            if current_time > time.to_utc() {
                                keys_to_delete.insert(key.clone());
                            }
                        }
                        Expire::NEVER => {}
                    }
                });
        }

        if !keys_to_delete.is_empty() {
            let mut db = self.db.write().await;
            let mut expires_at_store = self.expires_at.write().await;
            let mut listeners = self.listeners.write().await;


            keys_to_delete.iter().for_each(|key| {
                db.remove(key);
                expires_at_store.remove(key);
                listeners.remove(key);
            });
        }

    }
}