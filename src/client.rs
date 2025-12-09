use uuid::Uuid;
use zmq::{Context, Error as zmqErr, SocketType};
use crate::storage::{ClientStorage};

pub struct Client {
    id: Uuid,
    storage_handler: ClientStorage,
}

impl Client {
    pub fn connect() -> Result<(), zmqErr> {
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
