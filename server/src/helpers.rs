use std::error::Error;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::{Arc};

use tokio::io::{AsyncWriteExt, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use xxhash_rust::const_xxh3::xxh3_64 as const_xxh3;

pub fn hash_key_to_unsigned_int(key: &[u8]) -> u64 {
    const_xxh3(key)
}

pub type ArcSocket = Arc<AtomicSocketWriter>;

pub struct AtomicSocketWriter {
    socket: Mutex<WriteHalf<TcpStream>>,
    id: uuid::Uuid
}

impl Debug for AtomicSocketWriter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "socket: {}", self.id)
    }
}

impl PartialEq for AtomicSocketWriter {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Hash for AtomicSocketWriter {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(self.id.as_bytes())
    }
}

impl std::cmp::Eq for AtomicSocketWriter {}

#[derive(Debug)]
pub enum SocketError {
    UnableToAcquireWriteLock(Option<Box<dyn Error>>),
    MissingWriter,
    FailedToWriteToSocket(Option<Box<dyn Error>>),
    UnableToCleanSocket(Option<Box<dyn Error>>),
}

impl AtomicSocketWriter {
    pub fn new(writer: WriteHalf<TcpStream>) -> Self {
        AtomicSocketWriter {
            socket: Mutex::new(writer),
            id: uuid::Uuid::new_v4(),
        }
    }

    pub async fn write_to_atomic_socket(&self, bytes: &[u8]) -> anyhow::Result<()> {
        let mut socket = self.socket.lock().await;
        socket.write_all(bytes).await
            .map_err(|e| Box::new(e))?;

        Ok(())
    }
}
