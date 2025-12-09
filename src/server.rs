use zmq::{Context, Error as zmqErr, SocketType};

pub struct Server {}

impl Server {
    pub fn setup_server() -> Result<(), zmqErr> {
        print!("Beginning server...");
        let context = Context::new();
        let responder = context.socket(SocketType::REP)?;
        let _ = responder.bind("tcp://*:5555")?;

        loop {
            let message = responder.recv_msg(0)?;
            println!(
                "Received something: {}",
                message.as_str().unwrap_or("Invalid.")
            );
            let message = "Back at you.";
            let _ = responder.send(message, 0);
        }
    }
}
