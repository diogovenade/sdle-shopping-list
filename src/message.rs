use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
pub enum Request {
    CreateList { name: String },
    AddItem { list_id: Uuid, item_name: String, amount: i64 },
    RemoveItem { list_id: Uuid, item_name: String },
    GetList { list_id: Uuid },
    SyncState { list_id: Uuid, state: Vec<u8> },
}

#[derive(Serialize, Deserialize)]
pub enum Response {
    Success { list_id: Uuid },
    ListState { items: Vec<String> },
    Error { message: String },
}