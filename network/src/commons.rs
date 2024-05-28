use std::error::Error;
use bytes::BytesMut;
use serde::{Deserialize, Serialize};
use crate::encoding::{create_v1_encoding, create_v1_stream_encoding, DataEncodingType};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum OperationType {
    StreamAppend,
    StreamDelete,
    StreamListen,
    StreamGet,
    RESPONSE,
    STREAM,
    ERROR,
}

impl Default for OperationType {
    fn default() -> Self { OperationType::ERROR }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreMeta<T> {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub value: T,
}


#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct Message<T> {
    pub operation: OperationType,
    pub data: T,
    pub timestamp: Option<i64>,
    pub id: Option<String>
}

impl <'a, T: serde::ser::Serialize + serde::Deserialize<'a>> Message< T> {
    pub fn new(operation: OperationType, data: T) ->  anyhow::Result<Self> {
        Ok(Message {
            operation,
            data,
            timestamp: Some(chrono::Utc::now().timestamp()),
            id: Some(uuid::Uuid::new_v4().to_string()),
        })
    }

    pub fn to_encoded(&self) -> anyhow::Result<BytesMut> {
        Ok(create_v1_encoding(DataEncodingType::BINARY, bincode::serialize(self)?.as_slice()))
    }

    pub fn to_stream(&self) -> anyhow::Result<BytesMut> {
        Ok(create_v1_stream_encoding(DataEncodingType::BINARY, bincode::serialize(self)?.as_slice()))
    }
}