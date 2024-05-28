use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Request {
    AppendStreamReq {
        key: String,
        value: Vec<u8>,
        expire: Option<i64>,
    },
    ListenToStreamReq {
        key: String,
        start_from: Option<i64>,
    },
    GetStreamReq {
        key: String,
        start_from: Option<i64>,
        end_at: Option<i64>,
    }
}
