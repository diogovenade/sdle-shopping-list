use uuid::Uuid;
use zmq::{Context, Error, SocketType};

struct Client {
    id: Uuid,
}
pub fn client_connect() -> Result<(), Error> {
    println!("Connecting to server...");
    let context = Context::new();
    let requester = context.socket(SocketType::REQ)?;
    let _ = requester.connect("tcp://localhost:5555");

    for request in 1..11 {
        println!("Sending hello... {}", request);
        let message = "Hello Server!";
        requester.send(message, 0)?;
        let message = requester.recv_msg(0)?;
        println!("Received: {}", message.as_str().unwrap_or("Invalid UTF-8"));
    }

    // drop(requester); // rust handles this automatically
    // Context::destroy(&mut context); // this too

    Ok(())
}
