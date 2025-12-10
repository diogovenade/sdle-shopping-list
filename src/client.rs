use uuid::Uuid;
use zmq::{Context, Error as zmqErr, SocketType};
use crate::storage::{ClientStorage};
use anyhow::Result;

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
