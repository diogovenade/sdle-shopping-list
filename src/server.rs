use anyhow::Result;
use tokio::time::error::Elapsed;
use tokio::time::{sleep, timeout};
use uuid::Uuid;
use zmq::{Context, Error, PollItem, SNDMORE, Socket, SocketType};

use rand::seq::{IndexedRandom, SliceRandom};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::crdt::{Mergeable, ShoppingList};
use crate::hash_ring::{HashRing, READ_NODES, REPLICAS, VNODES, WRITE_NODES};
use crate::message::Msg;
use crate::storage::ServerStorage;

const GOSSIP_INTERVAL: u64 = 500; // ms
const JOIN_TIMEOUT: u64 = 1500;
const FAILURE_TIMEOUT: u64 = 1000;
const REPLICATE_TIMEOUT: u64 = 3000; 


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MembershipTable(pub HashMap<Uuid, String>);

impl MembershipTable {
    pub fn insert(&mut self, server_id: Uuid, addr: String) {
        self.0.insert(server_id, addr);
    }
}

// for maintaining long-lived connections.

pub struct DealerMap(pub Mutex<HashMap<Uuid, Arc<Mutex<Socket>>>>);

impl DealerMap {
    //TODO
}

#[derive(Debug, Clone)]
pub struct PeerFailureInfo {
    pub last_failure: Instant,
    pub consecutive_failures: u32,
}

#[derive(Debug, Clone)]
pub struct FailureTable(pub HashMap<Uuid, PeerFailureInfo>);

impl FailureTable {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn mark_failure(&mut self, server_id: Uuid) {
        let now = Instant::now();

        self.0.entry(server_id).and_modify(|e| {
            e.last_failure = now;
            e.consecutive_failures +=1;
        }).or_insert(
            PeerFailureInfo {
                last_failure: now,
                consecutive_failures: 1
            }
        );
    }

    pub fn clear_failure(&mut self, server_id: &Uuid) {
        self.0.remove(server_id);
    }

    pub fn is_available(&self, server_id: &Uuid) -> bool {
        match self.0.get(server_id) {
            None => true, 
            Some(info) => {
                // consider unavailable if more than 3 consecutive failures, OR failed within last FAILURE_TIMEOUT ms
                if info.consecutive_failures > 3 {
                    return false;
                }
                info.last_failure.elapsed() > Duration::from_millis(FAILURE_TIMEOUT)
            }
        }
    }
}

// for storing requests information
struct QuoromState {
    needed: usize, 
    received: usize, 
    pub responder: Option<oneshot::Sender<()>>,
    lists: Vec<ShoppingList>
}

impl QuoromState {
    pub fn is_quorom_reached(&self) -> bool{
        self.received >= self.needed
    }

    pub fn merge_lists(&self) -> Result<ShoppingList> {
        let mut merged = match self.lists.first() {
            Some(list) => list.clone(),
            None => return Err(anyhow::anyhow!("No shopping lists to merge")),
        };

        for list in self.lists.iter().skip(1) {
            merged.list.merge(&list.list);
        }

        Ok(merged)
    }

    pub fn add_list(&mut self, list: ShoppingList) {
        self.received += 1;
        self.lists.push(list);
    }

    pub fn increment_received(&mut self) {
        self.received += 1;
    }
}


pub struct Peer {
    pub uuid: Uuid, // NOTE: apparently the server id is the dealer id of the one connected to the proxy
    storage: Mutex<ServerStorage>, 

    // network related
    addr: String,
    ctx: Context,
    router: Mutex<Socket>,              // for incoming messages
    proxy_dealer: Mutex<Socket>,
    membership: Mutex<MembershipTable>, // stores known addresses
    hashring: Mutex<HashRing>,          // stores hashring
    failure: Mutex<FailureTable>,        // stores nodes which are failing
    in_flight: Mutex<HashMap<String, QuoromState>> // unresolved requests
}

pub type SharedPeer = Arc<Peer>;

impl Peer {
    pub fn new(context: &Context, uuid: Uuid, bind_addr: &str, proxy_addr: &str) -> Result<Self> {
        let ctx = context.clone();

        let router = ctx.socket(SocketType::ROUTER)?;
        // router.set_identity(uuid.as_bytes())?; // ROUTER socket identity -> not important, router is the one who needs to know the requests identity
        router.bind(bind_addr)?;

        let dealer = ctx.socket(SocketType::DEALER)?;
        dealer.set_identity(uuid.to_string().as_bytes())?;
        dealer.connect(proxy_addr)?;
        dealer.send("READY", 0)?;

        // this table will be changed if joining an active cluster
        let mut membership = MembershipTable(HashMap::new());
        membership.insert(uuid, bind_addr.to_string());

        let storage = ServerStorage::new(uuid)?;

        let mut hashring = HashRing::new(VNODES, REPLICAS);
        hashring.add_node(uuid);

        let failure = FailureTable::new();

        let in_flight = HashMap::<String, QuoromState>::new();

        anyhow::Ok(Self {
            ctx,
            uuid,
            router: Mutex::new(router),
            proxy_dealer: Mutex::new(dealer),
            membership: Mutex::new(membership),
            addr: bind_addr.to_string(),
            storage: Mutex::new(storage),
            hashring: Mutex::new(hashring),
            failure: Mutex::new(failure),
            in_flight: Mutex::new(in_flight)
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
    // NOTE: it is a TERRIBLE design to open ephemeral sockets and connections
    // whenever we want to send a message. sockets and connections NEED to be long lived
    // and reused. also, we CANNOT reuse the identity of the server (dealer connected to 
    // proxy) for each of its dealers connected to other peers!!
    fn send_to(&self, uuid: &Uuid, msg: &Msg) -> Result<()> {
        // open socket
        let socket = match self.connect_to_peer(uuid) {
            Ok(s) => s,
            Err(e) => {
                self.mark_peer_failed(uuid);
                return Err(e);
            }
        };

        // send empty frame first
        if let Err(e) = socket.send("", zmq::SNDMORE) {
            self.mark_peer_failed(uuid);
            return Err(e.into());
        }

        // send msg
        let data = serde_json::to_vec(msg)?;
        println!("[{}] Sending {:?} to {}", self.uuid, msg.name(), uuid);
        
        match socket.send(data, 0) {
            Ok(_) => {
                // success! clear any previous failures
                self.clear_peer_failure(uuid);
            }
            Err(e) => {
                self.mark_peer_failed(uuid);
                self.close_conn(uuid, socket).ok(); // Try to close, ignore errors
                return Err(e.into());
            }
        }

        // close socket
        self.close_conn(uuid, socket)?;

        Ok(())
    }

    fn send_gossip(&self) -> Result<()> {
        let table = {
            let table = self.membership.lock().unwrap();
            table.clone()
        };

        let msg = Msg::Gossip { table };

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

        let msg = Msg::Gossip {
            table: table_snapshot,
        };

        self.send_to(uuid, &msg)?;

        anyhow::Ok(())
    }

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

                                if let Err(e) = peer_clone.handle_incoming(&sender_uuid, msg).await {
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
            let dealer = &peer.proxy_dealer;
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

                            // Parse the message and handle it
                            match serde_json::from_slice::<Msg>(&data_msg) {
                                Ok(msg) => {
                                    let client_id = client_id_msg.to_vec();
                                    drop(dealer_guard);
                                    
                                    if let Err(e) = peer.handle_incoming_proxy(client_id, msg) {
                                        eprintln!("[{}] Error handling proxy request: {:?}", peer.uuid, e);
                                    }
                                }
                                Err(e) => {
                                    eprintln!("[{}] Failed to parse message: {:?}", peer.uuid, e);
                                    let error_reply = format!("Error: Invalid message format").into_bytes();
                                    let out_frames = vec![client_id_msg.to_vec(), Vec::new(), error_reply];
                                    if let Err(e) = dealer_guard.send_multipart(out_frames, 0) {
                                        eprintln!("[{}] send error reply failed: {:?}", peer.uuid, e);
                                    }
                                }
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

        let hello = Msg::Hello {
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
            Msg::Gossip { table } => {
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

    fn handle_incoming_proxy(&self, client_id: Vec<u8>, msg: Msg) -> Result<()> {
        match msg {
            Msg::GetList { list_id } => {
                println!(
                    "[{}] Received GET_LIST for {} from proxy client",
                    self.uuid, list_id
                );

                let list = {
                    let storage = self.storage.lock().unwrap();
                    storage.get_shopping_list(&list_id)?
                };

                let response = Msg::ListResponse { list } { list };
                let reply = serde_json::to_vec(&response)?;

                let dealer_guard = self.proxy_dealer.lock().unwrap();
                let out_frames = vec![client_id, Vec::new(), reply];
                dealer_guard.send_multipart(out_frames, 0)?;
            }

            Msg::PutList { list } => {
                println!(
                    "[{}] Received PUT_LIST for {} from proxy client",
                    self.uuid, list.id
                );
                let mut storage = self.storage.lock().unwrap();
                storage.write_shopping_list(&list)?;
                
                let response = format!("Stored shopping list {}", list.id).into_bytes();
                let dealer_guard = self.proxy_dealer.lock().unwrap();
                let out_frames = vec![client_id, Vec::new(), response];
                dealer_guard.send_multipart(out_frames, 0)?;
            }

            _ => {
                let error_reply = format!("Error: Unsupported message type for client requests").into_bytes();
                let dealer_guard = self.proxy_dealer.lock().unwrap();
                let out_frames = vec![client_id, Vec::new(), error_reply];
                dealer_guard.send_multipart(out_frames, 0)?;
            }
        }
        anyhow::Ok(())
    }

    async fn handle_incoming(&self, identity: &Uuid, msg: Msg) -> Result<()> {
        match msg {
            Msg::Gossip { table } => {
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

            Msg::Hello { uuid, addr } => {
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

            Msg::GetList { list } => {
                println!(
                    "[{}] Received GET_LIST for {} from {}",
                    self.uuid, list.id, identity
                );

                let stored = {
                    let storage = self.storage.lock().expect("poisoned");
                    storage.get_shopping_list(&list.id)?
                };

                match stored {
                    None => {
                        eprintln!("[{}] Error reading list {}: list not stored", self.uuid, list.id);
                    },
                    Some(mut l) => {
                        match self.send_read_replicate(&list, self.get_replicas(&l.id)).await {
                            Ok(s) => {
                                l.list.merge(&s.list);

                                {
                                    let mut storage = self.storage.lock().expect("poisoned");
                                    storage.write_shopping_list(&l)?;
                                }

                                // TODO send ACK to proxy
                                
                                println!("[{}] Read shopping list {}", self.uuid, list.id);

                            },
                            Err(e) =>  {
                                // TODO send NACK to proxy
                                eprintln!("[{}] Error reading list {}: {}", self.uuid, list.id, e);
                            }
                        }
                    }
                }                
            }

            Msg::PutList { list } => {
                // proxy -> coordinator
                println!(
                    "[{}] Received PUT for {} from {}",
                    self.uuid, list.id, identity
                );

                // TODO: send Ack/Nack to proxy
                match self.send_write_replicate(&list, self.get_replicas(&list.id)).await {
                    Ok(_) => {
                        // only store if we get replicas to store
                        let mut storage = self.storage.lock().expect("poisoned");
                        storage.write_shopping_list(&list)?;
                        
                        println!("[{}] Stored shopping list {}", self.uuid, list.id);

                    },
                    Err(e) =>  {
                        eprintln!("[{}] Error storing list {}: {}", self.uuid, list.id, e);
                    }
                }
                
            }

            Msg::ReplicateList { id, list, write } => {
                if write {
                    // WRITE
                    let storing = {
                        let mut storage = self.storage.lock().expect("poisoned");
                        storage.write_shopping_list(&list)
                    };

                    let msg = Msg::Ack { request_id: id };

                    match storing {
                        Ok(()) => {
                            self.send_to(identity, &msg);
                        },
                        Err(e) => {
                            eprintln!("[{}] Error storing list {}: {}", self.uuid, list.id, e)
                            // do nothing -> if coordinator doesnt receive enough ACKs in X ms it fails
                        }
                    }
                } else {
                    // READ 
                    let stored = {
                        let mut storage = self.storage.lock().expect("poisoned");   
                        storage.get_shopping_list(&list.id)
                    };

                    match stored {
                        Ok(Some(l)) => {
                            let msg = Msg::AckList { request_id: id, list: l };
                            self.send_to(identity, &msg);
                        }
                        Ok(None) => {
                            eprintln!("[{}] Error reading list {}: list not stored", self.uuid, list.id)
                        }
                        Err(e) => {
                            eprintln!("[{}] Error reading list {}: {}", self.uuid, list.id, e)
                            // do nothing -> if coordinator doesnt receive enough ACKs in X ms it fails
                        }
                    }
                }
            }

            Msg::Ack {request_id} => {
                let mut g = self.in_flight.lock().expect("poisoned");
                let quorom_state = g.get_mut(&request_id);

                match quorom_state {
                    Some(q) => {
                        q.increment_received();

                        if q.is_quorom_reached() {
                            if let Some(tx) = q.responder.take() {
                                let _ = tx.send(());
                            }
                        }
                    }
                    None => eprintln!("[{}] Error Ack: request with id {} not found", self.uuid, request_id)
                }
            }


            Msg::Nack { request_id } => {
                // TODO 
            }

            Msg::AckList { request_id, list } => {
                let mut g = self.in_flight.lock().expect("poisoned");
                let quorom_state = g.get_mut(&request_id);

                match quorom_state {
                    Some(q) => {
                        // adds list to state and +1 received
                        q.add_list(list);

                        if q.is_quorom_reached() {
                            if let Some(tx) = q.responder.take() {
                                let _ = tx.send(());
                            }
                        }
                        
                    },
                    None => eprintln!("[{}] Error AckList {}: request with id {} not found", self.uuid, list.id, request_id)
                }
            }

            Msg::MergeList { list } => {
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

            Msg::ListResponse { .. } => {
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

    async fn send_replicate(&self, shopping_list: &ShoppingList, replicas: Vec<Uuid>, write: bool) -> Result<()> {
        let request_id = Uuid::new_v4().to_string();
        let msg = Msg::ReplicateList { id: request_id.clone(), list: shopping_list.clone(), write };

        let (tx, rx) = oneshot::channel();

        let state = QuoromState {
            needed: if write {WRITE_NODES} else {READ_NODES},
            received: 0,
            responder: Some(tx),
            lists: vec![shopping_list.clone()]
        };

        {
            // scope helps -> drops lock auto
            self.in_flight.lock().expect("poisoned").insert(request_id.clone(), state);
        }

        for replica in replicas {
            self.send_to(&replica, &msg);
        }
        
        let result = timeout(Duration::from_millis(REPLICATE_TIMEOUT), rx).await;

        self.in_flight.lock().expect("poisoned").remove(&request_id);

        match result {
            Ok(Ok(())) => {
                Ok(())
            }
            Ok(Err(_)) => {
                Err(anyhow::anyhow!("quorum aborted"))
            }
            Err(_) => {
                Err(anyhow::anyhow!("quorum timed out"))
            }

        }
    }

    async fn send_write_replicate(&self, shopping_list: &ShoppingList, replicas: Vec<Uuid>) -> Result<()> {
        self.send_replicate(shopping_list, replicas, true).await
    }

    async fn send_read_replicate(&self, shopping_list: &ShoppingList, replicas: Vec<Uuid>) -> Result<ShoppingList> {
        let request_id = Uuid::new_v4().to_string();
        let msg = Msg::ReplicateList { id: request_id.clone(), list: shopping_list.clone(), write: false };

        let (tx, rx) = oneshot::channel();

        let state = QuoromState {
            needed: READ_NODES,
            received: 0,
            responder: Some(tx),
            lists: vec![shopping_list.clone()],
        };

        self.in_flight.lock().expect("poisoned").insert(request_id.clone(), state);

        for replica in replicas {
            self.send_to(&replica, &msg);
        }

        let result = timeout(Duration::from_millis(REPLICATE_TIMEOUT), rx).await;

        let quorom_state = self.in_flight.lock().expect("poisoned").remove(&request_id);

        match result {
            Ok(Ok(())) => {
                if let Some(q) = quorom_state {
                    q.merge_lists()
                } else {
                    Err(anyhow::anyhow!("quorum state missing"))
                }
            }
            Ok(Err(_)) => Err(anyhow::anyhow!("quorum aborted")),
            Err(_) => Err(anyhow::anyhow!("quorum timed out")),
        }
    }

    // ---- FAILURE DETECTION ----

    fn mark_peer_failed(&self, uuid: &Uuid) {
        let mut failure_table = self.failure.lock().expect("poisoned");
        failure_table.mark_failure(*uuid);
        eprintln!(
            "[{}] Marked peer {} as failed (consecutive failures: {})",
            self.uuid,
            uuid,
            failure_table.0.get(uuid).map(|f| f.consecutive_failures).unwrap_or(0)
        );
    }

    fn clear_peer_failure(&self, uuid: &Uuid) {
        let mut failure_table = self.failure.lock().expect("poisoned");
        if failure_table.0.contains_key(uuid) {
            println!("[{}] Cleared failure status for peer {}", self.uuid, uuid);
            failure_table.clear_failure(uuid);
        }
    }

    pub fn is_peer_available(&self, uuid: &Uuid) -> bool {
        // Check if peer is in membership table
        let in_membership = {
            let membership = self.membership.lock().expect("poisoned");
            membership.0.contains_key(uuid)
        };

        if !in_membership {
            return false;
        }

        // Check if peer has recent failures
        let failure_table = self.failure.lock().expect("poisoned");
        failure_table.is_available(uuid)
    }

    #[allow(dead_code)]
    fn get_failure_count(&self) -> usize {
        let failure_table = self.failure.lock().expect("poisoned");
        failure_table.0.len()
    }

    #[allow(dead_code)]
    fn get_failed_peers(&self) -> Vec<Uuid> {
        let failure_table = self.failure.lock().expect("poisoned");
        failure_table.0.keys().copied().collect()
    }
}
