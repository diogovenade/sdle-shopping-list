use sdle::crdt::{ShoppingList, AWORMap, Item};
use sdle::message::Msg;
use uuid::Uuid;

#[test]
fn test_get_list_message() {
    let list_id = Uuid::new_v4();
    let msg = Msg::GET_LIST { list_id };
    
    assert_eq!(msg.name(), "GET_LIST");
    
    match msg {
        Msg::GET_LIST { list_id: id } => assert_eq!(id, list_id),
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
    
    let msg = Msg::PUT_LIST { list: list.clone() };
    
    assert_eq!(msg.name(), "PUT_LIST");
    
    match msg {
        Msg::PUT_LIST { list: l } => {
            assert_eq!(l.id, list_id);
            assert!(l.list.is_empty());
        }
        _ => panic!("Expected PUT_LIST message"),
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
    
    let msg = Msg::MERGE_LIST { list: list.clone() };
    
    assert_eq!(msg.name(), "MERGE_LIST");
    
    match msg {
        Msg::MERGE_LIST { list: l } => {
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
    
    let msg = Msg::LIST_RESPONSE { list: Some(list.clone()) };
    
    assert_eq!(msg.name(), "LIST_RESPONSE");
    
    match msg {
        Msg::LIST_RESPONSE { list: Some(l) } => {
            assert_eq!(l.id, list_id);
        }
        _ => panic!("Expected LIST_RESPONSE with Some"),
    }
}

#[test]
fn test_list_response_message_none() {
    let msg = Msg::LIST_RESPONSE { list: None };
    
    assert_eq!(msg.name(), "LIST_RESPONSE");
    
    match msg {
        Msg::LIST_RESPONSE { list: None } => {}
        _ => panic!("Expected LIST_RESPONSE with None"),
    }
}

#[test]
fn test_message_serialization() {
    let list_id = Uuid::new_v4();
    let msg = Msg::GET_LIST { list_id };
    
    let serialized = serde_json::to_string(&msg).expect("Serialization failed");
    let deserialized: Msg = serde_json::from_str(&serialized).expect("Deserialization failed");
    
    match deserialized {
        Msg::GET_LIST { list_id: id } => assert_eq!(id, list_id),
        _ => panic!("Deserialized wrong message type"),
    }
}
