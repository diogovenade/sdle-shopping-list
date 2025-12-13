use anyhow::Result;
use uuid::Uuid;
use zmq::{Context, Socket, SocketType, POLLIN};
use crate::hash_ring::{HashRing, REPLICAS, VNODES};
use crate::message::Msg;

pub struct Proxy {
    context: Context,
    frontend: Socket, // ROUTER for clients
    backend: Socket,  // ROUTER for servers
    ring: HashRing,
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
            ring: HashRing::new(VNODES, REPLICAS),
        })
    }

    pub fn start(&mut self) -> Result<()> {
        let target_server_id = "peer1".to_string(); // apenas para testar, depois mudar com lógica do hash ring

        let mut items = [
            self.frontend.as_poll_item(POLLIN),
            self.backend.as_poll_item(POLLIN),
        ];

        loop {
            zmq::poll(&mut items, -1)?;

            // Frontend
            if items[0].is_readable() {
                let mut msg = self.frontend.recv_multipart(0)?;
                // TODO: Use consistent hashing to pick server identity
                // algo como -> let server_id = hash_ring.get_coordinator(&request_key);
                // Prepend server identity to msg and send to backend

                // msg is typically: [client_id][empty][payload]
                // Backend ROUTER requires: [server_id][client_id][empty][payload]
                msg.insert(0, target_server_id.as_bytes().to_vec());
                self.backend.send_multipart(msg, 0)?;
            }

            // Backend
            if items[1].is_readable() {
                let mut msg = self.backend.recv_multipart(0)?;

                // msg is typically: [server_id][client_id][empty][payload]
                // Frontend ROUTER expects: [client_id][empty][payload]

                // registration from server
                if msg.len() == 2 && msg[1].as_slice() == b"READY" {
                    if let Ok(id_str) = str::from_utf8(&msg[0]) {
                        if let Ok(uuid) = Uuid::parse_str(id_str) {
                            self.ring.add_node(uuid);
                            println!("Added server {} to hash ring", uuid);
                            continue;
                        }
                    }
                    continue;
                }

                if !msg.is_empty() {
                    msg.remove(0); // drop server_id routing frame
                }
                self.frontend.send_multipart(msg, 0)?;
            }
        }
    }
}
