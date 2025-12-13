use anyhow::Result;
use tokio::time::sleep;
use uuid::Uuid;
use zmq::{Context, Error, PollItem, SNDMORE, Socket, SocketType};

use rand::seq::{IndexedRandom, SliceRandom};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::crdt::{Mergeable, ShoppingList};
use crate::hash_ring::{HashRing, REPLICAS, VNODES};
use crate::message::Msg;
use crate::storage::ServerStorage;

const GOSSIP_INTERVAL: u64 = 500;
const JOIN_TIMEOUT: u64 = 1500;
const FAILURE_TIMEOUT: u64 = 1000;


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MembershipTable(pub HashMap<Uuid, String>);

impl MembershipTable {
    pub fn insert(&mut self, uuid: Uuid, addr: String) {
        self.0.insert(uuid, addr);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureTable(pub HashMap<Uuid, (String , Msg)>);

impl FailureTable {
    pub fn insert(&mut self, uuid: Uuid, addr: String, msg: Msg) {
        self.0.insert(uuid, (addr, msg));
    }
}


pub struct Peer {
    pub uuid: Uuid,
    storage: Mutex<ServerStorage>,

    // network related
    addr: String,
    ctx: Context,
    router: Mutex<Socket>,              // for incoming messages
    dealer: Mutex<Socket>,
    membership: Mutex<MembershipTable>, // stores known addresses
    hashring: Mutex<HashRing>,          // stores hashring
    failure: Mutex<FailureTable>        // stores nodes which are failing
}

pub type SharedPeer = Arc<Peer>;

impl Peer {
    pub fn new(uuid: Uuid, bind_addr: &str, proxy_addr: &str) -> Result<Self> {
        let ctx = Context::new();

        let router = ctx.socket(SocketType::ROUTER)?;
        // router.set_identity(uuid.as_bytes())?; // ROUTER socket identity -> not important, router is the one who needs to know the requests identity
        router.bind(bind_addr)?;

        let dealer = ctx.socket(SocketType::DEALER)?;
        dealer.set_identity(uuid.as_bytes())?;
        dealer.connect(proxy_addr)?;

        // this table will be changed if joining an active cluster
        let mut membership = MembershipTable(HashMap::new());
        membership.insert(uuid.clone(), bind_addr.to_string());

        let storage = ServerStorage::new(&uuid.to_string())?;

        let mut hashring = HashRing::new(VNODES, REPLICAS);
        hashring.add_node(uuid.clone());

        let failure = FailureTable(HashMap::new());

        anyhow::Ok(Self {
            ctx,
            uuid,
            router: Mutex::new(router),
            dealer: Mutex::new(dealer),
            membership: Mutex::new(membership),
            addr: bind_addr.to_string(),
            storage: Mutex::new(storage),
            hashring: Mutex::new(hashring),
            failure: Mutex::new(failure),
        })
    }

    // ---- NETWORK ----

    // open socket connection to peer
    fn connect_to_peer(&self, uuid: &Uuid) -> Result<Socket> {
        let peer_addr = {
            let table = self.membership.lock().unwrap();
            match table.0.get(uuid) {
                Some(e) => e.clone(),
                None => {
                    anyhow::bail!("Cannot connect to {uuid}: peer not found in membership table")
                }
            }
        };

        let dealer = self.ctx.socket(SocketType::DEALER)?;
        let _ = dealer.set_identity(self.uuid.to_string().as_bytes());
        let _ = dealer.connect(&peer_addr);

        // self.dealers.insert(uuid.to_string(), dealer);

        anyhow::Ok(dealer)
    }

    fn close_conn(&self, uuid: &Uuid, socket: Socket) -> Result<()> {
        let peer_addr = {
            let table = self.membership.lock().unwrap();
            match table.0.get(uuid) {
                Some(e) => e.clone(),
                None => {
                    anyhow::bail!(
                        "Cannot close connection to {uuid}: peer not found in membership table"
                    );
                }
            }
        };

        socket.disconnect(&peer_addr)?;

        anyhow::Ok(())
    }

    // wrapper to send messages, open/closes conn and serializes Msg to JSON
    fn send_to(&self, uuid: &Uuid, msg: &Msg) -> Result<()> {
        // open socket
        let socket = self.connect_to_peer(uuid)?;

        // send empty frame first
        let _ = socket.send("", zmq::SNDMORE);

        // send msg
        let data = serde_json::to_vec(msg)?;
        println!("[{}] Sending {:?} to {}", self.uuid, msg.name(), uuid);
        socket.send(data, 0)?;

        // close socket
        self.close_conn(uuid, socket)?;

        anyhow::Ok(())
    }

    fn send_gossip(&self) -> Result<()> {
        let table = {
            let table = self.membership.lock().unwrap();
            table.clone()
        };

        let msg = Msg::GOSSIP { table };

        let peers: Vec<Uuid> = {
            let table = self.membership.lock().unwrap();
            table.0.keys().cloned().collect()
        };

        let p = match peers.choose(&mut rand::rng()) {
            Some(p) => p.clone(),
            None => {
                anyhow::bail!("no peers");
            }
        };

        if p == self.uuid {
            // only other peers
            return anyhow::Ok(());
        }

        self.send_to(&p, &msg)?;

        anyhow::Ok(())
    }

    fn send_gossip_to(&self, uuid: &Uuid) -> Result<()> {
        // ensure the peer exists
        {
            let table = self.membership.lock().unwrap();
            if !table.0.contains_key(uuid) {
                anyhow::bail!("{} not found", uuid);
            }
        }

        let table_snapshot = {
            let table = self.membership.lock().unwrap();
            table.clone()
        };

        let msg = Msg::GOSSIP {
            table: table_snapshot,
        };

        self.send_to(uuid, &msg)?;

        anyhow::Ok(())
    }

    fn send_to_proxy(&self, msg: &Msg) -> Result<()> {
        // TODO: isto e capaz de nao funcionar mt bem -> listen_proxy tem que drop lock


        anyhow::Ok(())
    }

    // por agora threads implementadas
    //   1) gossip p membership
    //   2) listener

    async fn gossip(peer: SharedPeer) {
        let interval = Duration::from_millis(GOSSIP_INTERVAL);

        loop {
            sleep(interval).await;
            let peer_clone = Arc::clone(&peer);
            let result = tokio::task::spawn_blocking(move || peer_clone.send_gossip()).await;

            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => eprintln!("Gossip failed: {:?}", e),
                Err(e) => eprintln!("Blocking task failed to execute: {:?}", e),
            }
        }
    }

    async fn listen(peer: SharedPeer) {
        tokio::task::spawn_blocking(move || {
            let router = &peer.router;

            loop {
                let router_guard = router.lock().unwrap();
                let mut items = [router_guard.as_poll_item(zmq::POLLIN)];

                match zmq::poll(&mut items, -1) {
                    Ok(ready_count) if ready_count > 0 => {
                        if items[0].is_readable() {
                            // receive identity frame
                            let identity_msg = match router_guard.recv_msg(zmq::DONTWAIT) {
                                Ok(msg) => msg,
                                Err(e) => {
                                    eprintln!("Failed to receive identity: {:?}", e);
                                    continue;
                                }
                            };

                            // receive empty frame
                            let _ = match router_guard.recv_msg(0) {
                                Ok(msg) => msg,
                                Err(e) => {
                                    eprintln!("Failed to receive empty frame: {:?}", e);
                                    continue;
                                }
                            };

                            // receive data frame
                            let data_msg = match router_guard.recv_msg(0) {
                                Ok(msg) => msg,
                                Err(e) => {
                                    eprintln!("Failed to receive data frame: {:?}", e);
                                    continue;
                                }
                            };

                            let msg: Msg = match serde_json::from_slice(&data_msg) {
                                Ok(msg) => msg,
                                Err(e) => {
                                    eprintln!("Failed to parse message: {:?}", e);
                                    continue;
                                }
                            };

                            // spawn new thread for concurrent message handling
                            let peer_clone = Arc::clone(&peer);
                            tokio::spawn(async move {
                                let sender_uuid_str = match identity_msg.as_str() {
                                    Some(id) => id,
                                    None => {
                                        eprintln!("Identity frame invalid: {:?}", identity_msg);
                                        return;
                                    }
                                };

                                let sender_uuid = match Uuid::from_str(sender_uuid_str) {
                                    Ok(s) => s,
                                    Err(e) => {
                                        eprintln!("error parsing uuid: {e}");
                                        return;
                                    }
                                };

                                if let Err(e) = peer_clone.handle_incoming(&sender_uuid, msg) {
                                    eprintln!(
                                        "Error handling message from {}: {:?}",
                                        sender_uuid, e
                                    );
                                }
                            });
                        }
                    }
                    Ok(_) => continue, // no events
                    Err(e) => {
                        eprintln!("ZMQ poll error: {:?}", e);
                        continue;
                    }
                }
            }
        });
    }

    async fn listen_proxy(peer: SharedPeer) {
        tokio::task::spawn_blocking(move || {
            let dealer = &peer.dealer;
            loop {
                let dealer_guard = dealer.lock().unwrap();
                let mut items = [dealer_guard.as_poll_item(zmq::POLLIN)];

                match zmq::poll(&mut items, -1) {
                    Ok(ready_count) if ready_count > 0 => {
                        if items[0].is_readable() {
                            // receive client_id frame
                            let client_id_msg = match dealer_guard.recv_msg(zmq::DONTWAIT) {
                                Ok(msg) => msg,
                                Err(e) => {
                                    eprintln!(
                                        "[{}] Failed to receive client_id: {:?}",
                                        peer.uuid, e
                                    );
                                    continue;
                                }
                            };

                            // receive empty frame
                            let _ = match dealer_guard.recv_msg(0) {
                                Ok(msg) => msg,
                                Err(e) => {
                                    eprintln!(
                                        "[{}] Failed to receive empty frame: {:?}",
                                        peer.uuid, e
                                    );
                                    continue;
                                }
                            };

                            // receive data frame
                            let data_msg = match dealer_guard.recv_msg(0) {
                                Ok(msg) => msg,
                                Err(e) => {
                                    eprintln!(
                                        "[{}] Failed to receive data frame: {:?}",
                                        peer.uuid, e
                                    );
                                    continue;
                                }
                            };

                            let payload_str = std::str::from_utf8(&data_msg).ok();

                            println!(
                                "[{}] proxy: received request from client {} payload={:?}",
                                peer.uuid,
                                client_id_msg.as_str().unwrap_or("<binary>"),
                                payload_str.unwrap_or("<binary>")
                            );

                            // TODO: parse/dispatch real client requests
                            let reply = if let Some(s) = payload_str {
                                format!("{}: ok ({})", peer.uuid, s).into_bytes()
                            } else {
                                format!("{}: ok ({} bytes)", peer.uuid, data_msg.len()).into_bytes()
                            };

                            let out_frames = vec![client_id_msg.to_vec(), Vec::new(), reply];

                            if let Err(e) = dealer_guard.send_multipart(out_frames, 0) {
                                eprintln!("[{}] send to proxy failed: {:?}", peer.uuid, e);
                            }
                        }
                    }
                    Ok(_) => continue, // no events
                    Err(e) => {
                        eprintln!("[{}] proxy poll error: {:?}", peer.uuid, e);
                        continue;
                    }
                }
            }
        });
    }

    pub async fn start(peer: SharedPeer) {
        let listen_peer = Arc::clone(&peer);
        tokio::spawn(async move {
            Peer::listen(listen_peer).await;
        });

        let gossip_peer = Arc::clone(&peer);
        tokio::spawn(async move {
            Peer::gossip(gossip_peer).await;
        });

        let proxy = Arc::clone(&peer);
        tokio::spawn(async move {
            Peer::listen_proxy(proxy).await;
        });

        println!("[{}] Started successfully!", peer.uuid);
    }

    // dont have peer in membership table -> normal send_to/connect_peer dont work
    pub fn join(&self, seed_addr: &str) -> Result<()> {
        let socket = self.ctx.socket(SocketType::DEALER)?;
        socket.set_identity(self.uuid.to_string().as_bytes())?;
        socket.connect(seed_addr)?;

        let hello = Msg::HELLO {
            uuid: (self.uuid.clone()),
            addr: (self.addr.clone()),
        };

        // empty frame first
        socket.send("", zmq::SNDMORE)?;

        // follow with data
        let data = serde_json::to_vec(&hello)?;
        socket.send(data, 0)?;

        println!("[{}] Sent HELLO to {}", self.uuid, seed_addr);

        let start = Instant::now();

        // block until getting message
        let mut buf: Option<Msg> = None;
        while start.elapsed() < Duration::from_millis(JOIN_TIMEOUT) {
            // TODO: remover unwrap
            let router = self.router.lock().unwrap();

            let identity = match router.recv_msg(zmq::DONTWAIT) {
                Ok(msg) => msg,
                Err(e) if e == zmq::Error::EAGAIN => {
                    // DEALER-ROUTER specific error to allow async
                    // eprintln!("No message received");
                    continue;
                }
                Err(e) => {
                    eprintln!("Failed to receive identity {}", e);
                    continue;
                }
            };

            let _ = match router.recv_msg(0) {
                Ok(msg) => msg,
                Err(e) => {
                    eprintln!("Failed to receive empty frame {}", e);
                    continue;
                }
            };

            let data = match router.recv_msg(0) {
                Ok(msg) => msg,
                Err(e) => {
                    eprintln!("Failed to receive data frame {}", e);
                    continue;
                }
            };

            buf = match serde_json::from_slice(&data) {
                Ok(msg) => Some(msg),
                Err(e) => {
                    eprintln!("Failed to parse message {} : {:?}", e, data);
                    continue;
                }
            };

            let sender_uuid = match identity.as_str() {
                Some(m) => m,
                None => {
                    eprint!("Listen failed: no identity specified in request");
                    continue;
                }
            };
        }

        let msg: Msg = match buf {
            Some(msg) => msg,
            None => {
                anyhow::bail!("No message received");
            }
        };

        match msg {
            Msg::GOSSIP { table } => {
                let mut membership = self.membership.lock().unwrap();
                *membership = table;
            }
            other => anyhow::bail!("expected GOSSIP table, got {:?}", other),
        }

        // println!("Received membership table from GOSSIP");
        println!("[{}] Joined cluster successfully!", self.uuid);

        socket.disconnect(seed_addr)?;

        anyhow::Ok(())
    }

    // ---- HANDLE MESSAGES ----

    fn handle_incoming(&self, identity: &Uuid, msg: Msg) -> Result<()> {
        match msg {
            Msg::GOSSIP { table } => {
                println!("[{}] Received GOSSIP from {}", self.uuid, identity);
                // update our table if it changed
                let mut membership_guard = self.membership.lock().unwrap();

                for (u, addr) in table.0 {
                    membership_guard.insert(u, addr);
                }

                // update hashring
                let peer_uuids = membership_guard.0.keys().cloned().collect();
                self.add_nodes(peer_uuids);
            }

            Msg::PING {} => {
                println!("[{}] Received PING from {}", self.uuid, identity);
                self.send_to(identity, &Msg::ACK)?;
            }

            Msg::ACK => {
                println!("[{}] Received ACK from {}", self.uuid, identity);
            }

            Msg::HELLO { uuid, addr } => {
                println!("[{}] Received HELLO from {}", self.uuid, identity);

                {
                    // fazer operaçoes com locks dentro de brackets para ter a certeza q lock é solto

                    let mut membership_guard = self.membership.lock().unwrap();

                    membership_guard.insert(uuid.clone(), addr);
                }

                println!(
                    "[{}] Updated membership table: added {}",
                    self.uuid, identity
                );

                // send gossip to new node
                match self.send_gossip_to(&uuid) {
                    Ok(_) => {}
                    Err(e) => eprintln!("[{}] Error sending gossip: {}", self.uuid, e),
                }

                // send random gossip immediatelly
                let _ = self.send_gossip();
            }

            Msg::GET_LIST { list_id } => {
                println!(
                    "[{}] Received GET_LIST for {} from {}",
                    self.uuid, list_id, identity
                );

                // TODO: tem que haver forma de ver se nodes mandam ACK para este especifico request
                if let Ok(_) = self.is_coordinator(&list_id) {
                    let replicas = self.get_replicas(&list_id);

                    for uuid in replicas {
                        self.send_to(&uuid, &msg);
                    }

                }

                let list = {
                    let storage = self.storage.lock().unwrap();
                    storage.get_shopping_list(&list_id)?
                };

                let response = Msg::LIST_RESPONSE { list };

                self.send_to(identity, &response)?;
            }

            Msg::PUT_LIST { list } => {
                println!(
                    "[{}] Received PUT_LIST for {} from {}",
                    self.uuid, list.id, identity
                );
                let mut storage = self.storage.lock().unwrap();
                storage.write_shopping_list(&list)?;
                println!("[{}] Stored shopping list {}", self.uuid, list.id);
            }

            Msg::MERGE_LIST { list } => {
                println!(
                    "[{}] Received MERGE_LIST for {} from {}",
                    self.uuid, list.id, identity
                );
                let mut storage = self.storage.lock().unwrap();

                match storage.get_shopping_list(&list.id)? {
                    Some(mut existing) => {
                        existing.list.merge(&list.list);
                        storage.write_shopping_list(&existing)?;
                        println!("[{}] Merged shopping list {}", self.uuid, list.id);
                    }
                    None => {
                        // no existing list, just store it
                        storage.write_shopping_list(&list)?;
                        println!(
                            "[{}] Stored new shopping list {} (no existing to merge)",
                            self.uuid, list.id
                        );
                    }
                }
            }

            Msg::LIST_RESPONSE { .. } => {
                println!("[{}] Received LIST_RESPONSE from {}", self.uuid, identity);
                // responses are typically handled by the requester?
            }
        }
        anyhow::Ok(())
    }

    // ---- UTILITIES ----

    fn add_nodes(&self, nodes: Vec<Uuid>) {
        let mut hashring_guard = self.hashring.lock().expect("poisoned");

        for n in nodes {
            hashring_guard.add_node(n);
        }
    }

    fn is_coordinator(&self, list_id: &Uuid) -> Result<bool> {
        let hashring = self.hashring.lock().expect("poisoned");

        let node_id = hashring.get_coordinator(list_id).expect("must have coordinator");

        Ok(node_id == self.uuid.clone())
    }

    fn get_replicas(&self, list_id: &Uuid) -> Vec<Uuid> {
        // NOTE: fn assumes we're the coordinator 
        let hashring = self.hashring.lock().expect("poisoned");

        // skip 'ourselves' 
        hashring.get_preference_list(list_id).iter().skip(1).cloned().collect()
    }

    fn get_addresses(&self, ids: Vec<Uuid>) -> Vec<String> {
        let membership = self.membership.lock().expect("poisoned");

        ids.iter()
            .filter_map(|id| membership.0.get(id).cloned())
            .collect()
    }
}
