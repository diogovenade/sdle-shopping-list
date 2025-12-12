use anyhow::Result;
use zmq::{Context, Socket, SocketType, Message, POLLIN};
use std::collections::HashMap;

pub struct Proxy {
    context: Context,
    frontend: Socket, // ROUTER for clients
    backend: Socket,  // ROUTER for servers
}

impl Proxy {
    pub fn new(frontend_addr: &str, backend_addr: &str) -> Result<Self> {
        let context = Context::new();

        let frontend = context.socket(SocketType::ROUTER)?;
        frontend.bind(frontend_addr)?;

        let backend = context.socket(SocketType::ROUTER)?;
        backend.bind(backend_addr)?;

        Ok(Proxy {
            context,
            frontend,
            backend,
        })
    }

    pub fn start(&self) -> Result<()> {
        let mut items = [
            self.frontend.as_poll_item(POLLIN),
            self.backend.as_poll_item(POLLIN),
        ];

        loop {
            zmq::poll(&mut items, -1)?;

            // Client -> Server
            if items[0].is_readable() {
                let msg = self.frontend.recv_multipart(0)?;
                // TODO: Use consistent hashing to pick server identity
                // let server_id = hash_ring.get_coordinator(&request_key);
                // Prepend server identity to msg and send to backend
                self.backend.send_multipart(msg, 0)?;
            }

            // Server -> Client
            if items[1].is_readable() {
                let msg = self.backend.recv_multipart(0)?;
                // Forward to client
                self.frontend.send_multipart(msg, 0)?;
            }
        }
    }
}
