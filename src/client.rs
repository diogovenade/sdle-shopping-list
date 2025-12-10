use uuid::Uuid;
use zmq::{Context, Error as zmqErr, SocketType};
use crate::storage::{ClientStorage};
use crate::crdt::{ShoppingList};
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

pub struct Client {
    id: Uuid,
    storage_handler: ClientStorage,
}

impl Client {
    pub fn new() -> Result<Self> {
        let storage_handler = ClientStorage::new()?;
        Ok(Self {
            id: storage_handler.client_id,
            storage_handler,
        })
    }

    pub fn show_available_lists(&self) -> Result<Option<Vec<ShoppingListInterface>>> {
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

    pub fn send_item_storage_request(&mut self, item_name: String, quantity: u64, acquired: bool, shoppinglist_id: Uuid) -> Result<()> {
        self.storage_handler.handle_item_storage_request(item_name, quantity, acquired, shoppinglist_id)?; 
        Ok(())
    }

    pub fn connect(&self) -> Result<(), zmqErr> {
        println!("Connecting to server...");
        let context = Context::new();
        let requester = context.socket(SocketType::REQ)?;
        let _ = requester.connect("tcp://localhost:5555");

        for request in 1..11 {
            println!("Sending hello... {}", request);
            let message = "Hello server!";
            requester.send(message, 0)?;
            let message = requester.recv_msg(0)?;
            println!("Received: {}", message.as_str().unwrap_or("Invalid UTF-8"));
        }

        Ok(())
    }
}
