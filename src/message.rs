use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;
use crate::crdt::ShoppingList;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MembershipTable(pub HashMap<String, String>);

impl MembershipTable {
    pub fn insert(&mut self, uuid: String, addr: String) {
        self.0.insert(uuid, addr);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Msg {
    HELLO { uuid: String, addr: String },
    GOSSIP { table: MembershipTable },
    PING,
    ACK,
    // Shopping list operations
    GET_LIST { list_id: Uuid },
    PUT_LIST { list: ShoppingList },
    MERGE_LIST { list: ShoppingList },
    LIST_RESPONSE { list: Option<ShoppingList> },
}

impl Msg {
    pub fn name(&self) -> &'static str {
        match self {
            Msg::HELLO { .. } => "HELLO",
            Msg::GOSSIP { .. } => "GOSSIP",
            Msg::PING => "PING",
            Msg::ACK => "ACK",
            Msg::GET_LIST { .. } => "GET_LIST",
            Msg::PUT_LIST { .. } => "PUT_LIST",
            Msg::MERGE_LIST { .. } => "MERGE_LIST",
            Msg::LIST_RESPONSE { .. } => "LIST_RESPONSE",
        }
    }
}