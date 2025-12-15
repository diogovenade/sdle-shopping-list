use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;
use crate::{crdt::ShoppingList, crdt::AWORMap, crdt::Item, server::MembershipTable};

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
    Handoff {request_id: String, list: ShoppingList, original_node: Uuid},
    Leave
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
            Msg::Leave => "LEAVE",
        }
    }
}


/// tests

#[test]
fn test_hello_message() {
    let uuid = Uuid::new_v4();
    let addr = "127.0.0.1:8080".to_string();
    let msg = Msg::Hello { uuid, addr: addr.clone() };
    
    assert_eq!(msg.name(), "HELLO");
    
    match msg {
        Msg::Hello { uuid: id, addr: a } => {
            assert_eq!(id, uuid);
            assert_eq!(a, addr);
        }
        _ => panic!("Expected HELLO message"),
    }
}

#[test]
fn test_gossip_message() {
    let mut table = HashMap::new();
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();
    table.insert(uuid1, "192.168.1.1:5000".to_string());
    table.insert(uuid2, "192.168.1.2:5001".to_string());
    
    let membership_table = MembershipTable(table.clone());
    let msg = Msg::Gossip { table: membership_table };
    
    assert_eq!(msg.name(), "GOSSIP");
    
    match msg {
        Msg::Gossip { table: t } => {
            assert_eq!(t.0.len(), 2);
            assert!(t.0.contains_key(&uuid1));
            assert!(t.0.contains_key(&uuid2));
        }
        _ => panic!("Expected GOSSIP message"),
    }
}

#[test]
fn test_ack_message() {
    let request_id = "req-12345".to_string();
    let msg = Msg::Ack { request_id: request_id.clone() };
    
    assert_eq!(msg.name(), "ACK");
    
    match msg {
        Msg::Ack { request_id: id } => assert_eq!(id, request_id),
        _ => panic!("Expected ACK message"),
    }
}

#[test]
fn test_nack_message() {
    let request_id = "req-67890".to_string();
    let msg = Msg::Nack { request_id: request_id.clone() };
    
    assert_eq!(msg.name(), "NACK");
    
    match msg {
        Msg::Nack { request_id: id } => assert_eq!(id, request_id),
        _ => panic!("Expected NACK message"),
    }
}

#[test]
fn test_ack_list_message() {
    let request_id = "req-list-123".to_string();
    let list_id = Uuid::new_v4();
    let list = ShoppingList {
        id: list_id,
        list: AWORMap::new(),
    };
    
    let msg = Msg::AckList { 
        request_id: request_id.clone(), 
        list: list.clone() 
    };
    
    assert_eq!(msg.name(), "AckList");
    
    match msg {
        Msg::AckList { request_id: id, list: l } => {
            assert_eq!(id, request_id);
            assert_eq!(l.id, list_id);
        }
        _ => panic!("Expected AckList message"),
    }
}

#[test]
fn test_get_list_message() {
    let list_id = Uuid::new_v4();
    let list = ShoppingList {
        id: list_id,
        list: AWORMap::new(),
    };
    let msg = Msg::GetList { list: list.clone() };
    
    assert_eq!(msg.name(), "GET_LIST");
    
    match msg {
        Msg::GetList { list: l } => assert_eq!(l.id, list_id),
        _ => panic!("Expected GET_LIST message"),
    }
}

#[test]
fn test_put_list_message() {
    let list_id = Uuid::new_v4();
    let list = ShoppingList {
        id: list_id,
        list: AWORMap::new(),
    };
    
    let msg = Msg::PutList { list: list.clone() };
    
    assert_eq!(msg.name(), "PUT_LIST");
    
    match msg {
        Msg::PutList { list: l } => {
            assert_eq!(l.id, list_id);
            assert!(l.list.is_empty());
        }
        _ => panic!("Expected PUT_LIST message"),
    }
}

#[test]
fn test_replicate_list_message() {
    let list_id = Uuid::new_v4();
    let client_id = Uuid::new_v4();
    
    let mut map = AWORMap::new();
    map.insert("milk".to_string(), Item::new(1, false, client_id));
    
    let list = ShoppingList {
        id: list_id,
        list: map,
    };
    
    let msg = Msg::ReplicateList { 
        id: "repl-001".to_string(), 
        list: list.clone(), 
        write: true 
    };
    
    assert_eq!(msg.name(), "REPLICATE");
    
    match msg {
        Msg::ReplicateList { id, list: l, write } => {
            assert_eq!(id, "repl-001");
            assert_eq!(l.id, list_id);
            assert!(write);
            assert!(!l.list.is_empty());
        }
        _ => panic!("Expected REPLICATE message"),
    }
}

#[test]
fn test_merge_list_message() {
    let list_id = Uuid::new_v4();
    let client_id = Uuid::new_v4();
    
    let mut map = AWORMap::new();
    map.insert("bread".to_string(), Item::new(2, false, client_id));
    
    let list = ShoppingList {
        id: list_id,
        list: map,
    };
    
    let msg = Msg::MergeList { list: list.clone() };
    
    assert_eq!(msg.name(), "MERGE_LIST");
    
    match msg {
        Msg::MergeList { list: l } => {
            assert_eq!(l.id, list_id);
            assert!(!l.list.is_empty());
            assert!(l.list.items.contains_key("bread"));
        }
        _ => panic!("Expected MERGE_LIST message"),
    }
}

#[test]
fn test_list_response_message_some() {
    let list_id = Uuid::new_v4();
    let list = ShoppingList {
        id: list_id,
        list: AWORMap::new(),
    };
    
    let msg = Msg::ListResponse { list: Some(list.clone()) };
    
    assert_eq!(msg.name(), "LIST_RESPONSE");
    
    match msg {
        Msg::ListResponse { list: Some(l) } => {
            assert_eq!(l.id, list_id);
        }
        _ => panic!("Expected LIST_RESPONSE with Some"),
    }
}

#[test]
fn test_list_response_message_none() {
    let msg = Msg::ListResponse { list: None };
    
    assert_eq!(msg.name(), "LIST_RESPONSE");
    
    match msg {
        Msg::ListResponse { list: None } => {}
        _ => panic!("Expected LIST_RESPONSE with None"),
    }
}

#[test]
fn test_message_serialization_hello() {
    let uuid = Uuid::new_v4();
    let addr = "127.0.0.1:9000".to_string();
    let msg = Msg::Hello { uuid, addr: addr.clone() };
    
    let serialized = serde_json::to_string(&msg).expect("Serialization failed");
    let deserialized: Msg = serde_json::from_str(&serialized).expect("Deserialization failed");
    
    match deserialized {
        Msg::Hello { uuid: id, addr: a } => {
            assert_eq!(id, uuid);
            assert_eq!(a, addr);
        }
        _ => panic!("Deserialized wrong message type"),
    }
}

#[test]
fn test_message_serialization_ack() {
    let request_id = "req-abc123".to_string();
    let msg = Msg::Ack { request_id: request_id.clone() };
    
    let serialized = serde_json::to_string(&msg).expect("Serialization failed");
    let deserialized: Msg = serde_json::from_str(&serialized).expect("Deserialization failed");
    
    match deserialized {
        Msg::Ack { request_id: id } => assert_eq!(id, request_id),
        _ => panic!("Deserialized wrong message type"),
    }
}

#[test]
fn test_message_serialization_list() {
    let list_id = Uuid::new_v4();
    let client_id = Uuid::new_v4();
    
    let mut map = AWORMap::new();
    map.insert("eggs".to_string(), Item::new(12, true, client_id));
    
    let list = ShoppingList {
        id: list_id,
        list: map,
    };
    
    let msg = Msg::PutList { list: list.clone() };
    
    let serialized = serde_json::to_string(&msg).expect("Serialization failed");
    let deserialized: Msg = serde_json::from_str(&serialized).expect("Deserialization failed");
    
    match deserialized {
        Msg::PutList { list: l } => {
            assert_eq!(l.id, list_id);
            assert!(l.list.items.contains_key("eggs"));
        }
        _ => panic!("Deserialized wrong message type"),
    }
}

#[test]
fn test_message_serialization_gossip() {
    let mut table = HashMap::new();
    let uuid = Uuid::new_v4();
    table.insert(uuid, "10.0.0.1:3000".to_string());
    
    let membership_table = MembershipTable(table.clone());
    let msg = Msg::Gossip { table: membership_table };
    
    let serialized = serde_json::to_string(&msg).expect("Serialization failed");
    let deserialized: Msg = serde_json::from_str(&serialized).expect("Deserialization failed");
    
    match deserialized {
        Msg::Gossip { table: t } => {
            assert_eq!(t.0.len(), 1);
            assert!(t.0.contains_key(&uuid));
        }
        _ => panic!("Deserialized wrong message type"),
    }
}

#[test]
fn test_message_with_multiple_items() {
    let list_id = Uuid::new_v4();
    let client_id = Uuid::new_v4();
    
    let mut map = AWORMap::new();
    map.insert("apples".to_string(), Item::new(5, false, client_id));
    map.insert("bananas".to_string(), Item::new(3, true, client_id));
    map.insert("oranges".to_string(), Item::new(7, false, client_id));
    
    let list = ShoppingList {
        id: list_id,
        list: map,
    };
    
    let msg = Msg::MergeList { list: list.clone() };
    
    match msg {
        Msg::MergeList { list: l } => {
            assert_eq!(l.list.items.len(), 3);
            assert!(l.list.items.contains_key("apples"));
            assert!(l.list.items.contains_key("bananas"));
            assert!(l.list.items.contains_key("oranges"));
        }
        _ => panic!("Expected MERGE_LIST message"),
    }
}

#[test]
fn test_replicate_list_write_false() {
    let list_id = Uuid::new_v4();
    let list = ShoppingList {
        id: list_id,
        list: AWORMap::new(),
    };
    
    let msg = Msg::ReplicateList { 
        id: "repl-read-001".to_string(), 
        list: list.clone(), 
        write: false 
    };
    
    match msg {
        Msg::ReplicateList { id, list: l, write } => {
            assert_eq!(id, "repl-read-001");
            assert_eq!(l.id, list_id);
            assert!(!write);
        }
        _ => panic!("Expected REPLICATE message"),
    }
}
