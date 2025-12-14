use uuid::Uuid;
use zmq::{Context, Error as zmqErr, SocketType, Socket};
use crate::storage::{ClientStorage};
use crate::crdt::{Mergeable, ShoppingList};
use crate::message::Msg;
use serde_json;
use anyhow::Result;
use std::collections::HashMap;

pub struct ShoppingListInterface {
    pub list_id: Uuid,
    pub items: HashMap<String, (u64, bool)> // <name, (amount, acquired)>
}

impl ShoppingListInterface {
    fn from_crdt(sl: &ShoppingList) -> Self {
        let mut items = HashMap::new();
        let map = &sl.list;
        for (k, v) in map.items.iter() {
            let name = k.clone();
            let amount = v.amount.value_total() as u64;
            let acquired: bool = v.acquired.val != 0; 
            items.insert(name, (amount, acquired));
        }
        Self {
            list_id: sl.id,
            items: items,
        }
    }
}

#[derive(Clone)]
pub struct ItemInterface {
    pub name: String,
    pub amount: u64,
    pub acquired: bool,
}

pub struct Client {
    pub id: Uuid,
    storage_handler: ClientStorage,
    context: Context,
    socket: Socket,
}

impl Client {
    pub fn new() -> Result<Self> {
        let storage_handler = ClientStorage::new()?;
        let context = Context::new();
        let socket = context.socket(SocketType::REQ)?;
        socket.connect("tcp://127.0.0.1:5555")?;
        Ok(Self {
            id: storage_handler.client_id,
            storage_handler,
            context,
            socket,
        })
    }

    pub fn retrieve_available_lists(&self) -> Result<Option<Vec<ShoppingListInterface>>> {
        let lists = self.storage_handler.get_user_lists()?;

        match lists {
            Some(vec) => {
                let mut list_interface_vec: Vec<ShoppingListInterface> = Vec::new();
                for list in vec {
                    list_interface_vec.push(ShoppingListInterface::from_crdt(&list));
                }
                Ok(Some(list_interface_vec))
            }
            None => {
                Ok(None)
            }
        }
    }

    fn fetch_list(&self, list_id: Uuid) -> Result<Option<ShoppingList>> {
        let msg = Msg::GET_LIST { list_id };
        let payload = serde_json::to_vec(&msg).expect("Failed to serialize Msg");

        self.socket.send(payload, 0)?;

        let reply = self.socket.recv_msg(0)?;
        let response: Msg = serde_json::from_slice(&reply)?;

        match response {
            Msg::LIST_RESPONSE { list } => Ok(list),
            _ => anyhow::bail!("Unexpected response from server"),
        }
    }

    fn send_list(&self, list: &ShoppingList) -> Result<()> {
        let msg = Msg::PUT_LIST { list: list.clone() };
        let payload = serde_json::to_vec(&msg)?;

        self.socket.send(payload, 0)?;

        let reply = self.socket.recv_msg(0)?;
        println!("Server response: {:?}", String::from_utf8_lossy(&reply));

        Ok(())
    }

    pub fn retrieve_list(&self, list_id: Uuid) -> Result<ShoppingListInterface> {
        let local_list = self.storage_handler.read_shopping_list(list_id)?;
        let remote_list = self.fetch_list(list_id)?;

        let merged_list = if let Some(remote) = remote_list {
            let mut merged = local_list;
            merged.list.merge(&remote.list);
            merged
        } else {
            local_list
        };

        let list_interface = ShoppingListInterface::from_crdt(&merged_list);

        Ok(list_interface)
    }

    pub fn send_item_storage_request(&mut self, item_name: String, quantity: u64, acquired: bool, shoppinglist_id: Uuid) -> Result<()> {
        self.storage_handler.handle_item_storage_request(item_name, quantity, acquired, shoppinglist_id)?;

        let updated_list = self.storage_handler.read_shopping_list(shoppinglist_id)?;
        self.send_list(&updated_list)?;
        Ok(())
    }

    pub fn connect(&self) -> Result<(), zmqErr> {
        println!("Connecting to proxy frontend...");
        let context = Context::new();
        let requester = context.socket(SocketType::REQ)?;
        requester.connect("tcp://127.0.0.1:5555")?;

        let list_id = Uuid::new_v4();
        let shopping_list = crate::crdt::ShoppingList {
            id: list_id,
            list: crate::crdt::AWORMap::new(),
        };

        // Send a PutList message with the new shopping list
        let msg = Msg::GetList { list: shopping_list };
        let payload = serde_json::to_vec(&msg).expect("Failed to serialize Msg");

        requester.send(payload, 0)?;

        let reply = requester.recv_multipart(0)?;
        println!("Received reply: {:?}", reply);

        Ok(())
    }
}
