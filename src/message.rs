use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;
use crate::{crdt::ShoppingList, server::MembershipTable};

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
pub enum Msg {
    Hello { uuid: Uuid, addr: String },
    Gossip { table: MembershipTable },  
    Ping,
    Ack { request_id: String },
    Nack {request_id: String },
    AckList {request_id: String, list: ShoppingList},
    // Shopping list operations
    GetList { list: ShoppingList },
    PutList { list: ShoppingList },
    ReplicateList {id: String, list: ShoppingList, write: bool},
    MergeList { list: ShoppingList },
    ListResponse { list: Option<ShoppingList> },
    Handoff {request_id: String, list: ShoppingList, original_node: Uuid}
}

impl Msg {
    pub fn name(&self) -> &'static str {
        match self {
            Msg::Hello { .. } => "HELLO",
            Msg::Gossip { .. } => "GOSSIP",
            Msg::Ack { .. } => "ACK",
            Msg::Nack { .. } => "NACK",
            Msg::GetList { .. } => "GET_LIST",
            Msg::PutList { .. } => "PUT_LIST",
            Msg::MergeList { .. } => "MERGE_LIST",
            Msg::ListResponse { .. } => "LIST_RESPONSE",
            Msg::ReplicateList {..} => "REPLICATE",
            Msg::AckList { .. } => "AckList",
            Msg::Handoff { .. } => "HANDOFF",
            Msg::Ping => "PING",

        }
    }
}