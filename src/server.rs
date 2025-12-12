use anyhow::Result;
use tokio::time::sleep;
use uuid::Uuid;
use zmq::{Context, Error, PollItem, SNDMORE, Socket, SocketType};

use rand::seq::{IndexedRandom, SliceRandom};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::storage::ServerStorage;

use crate::crdt::{ShoppingList, Mergeable};

/*
what i think server needs:
  - *Sockets*: one DEALER for output and one ROUTER for input for each peer, this should ensure comms
    with every peer.

  - *Discovery*: there must be a _seed_ node which address is known to allow for new nodes to join, but
    to starts comms its is a bit harder -> must create sockets for each known address (known by the seed
    node!)

  - *Membership*: there must be some kind of table to keep track of membership (maybe crdt)

  - *Gossip*: a simple gossip protocol must be used to ensure membership is known across server nodes ->
    each T seconds choose a random known node and send them own membership table, node then updates its
    own table and repeats. On node JOIN, seed node sends to new node its membership table and start gossip
    immeadiately.

  - *DBs*: following Amazon Dynamo paper, two DBs used. One to store server data, this being CRDTs with
    shopping lists info. The second used to store data from a _hinted handoff_. (maybe good idea to save
    membership, maybe not if it comes from failure detection)

  - *Hinted Handoff*: happens when a node can't save a replica's data, then coordinator sends to another
    node (node_i + N) and in its metadata includes a reference to the node which failed. this is stored in
    the DB to later be sent back to the node.

  - *Permanent failure* / *Replica sync*: ainda nao vi mas tem algo a ver com merkle trees ainda nao percebi
    se precisamos pq estamos a usar CRDTs, mas provavelemnte sim (tp dar schedule a um merge entre replicas
    caso uma morra)

  - *Failure Detection*: local failure detection, if node A ---send m---> node B and node B doesn't answer
    in T seconds, A may consider B failed and reroute. A should then periodically retry node B to check for
    recovery (maybe mandar membership??)

  - *Coordinator*: a coordinator node is responsible for the hash key space between itself and last node on
    the ring. it must execute write/read operations on nodes in this space. This involves collecting and
    storing on its own DB as well as in the replica nodes (N nodes). This may cause uneven load. So coordinator
    can be any of the top N nodes in the preference list -> the who replied faster to the last read request
    (store this somewehere in metadata, maybe proxy can be aware of this and identify/keep track for each
    hash key space the fastest node)
 */

const GOSSIP_INTERVAL: u64 = 500;
const JOIN_TIMEOUT: u64 = 1500;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MembershipTable(pub HashMap<String, String>);

// so p nao ter q usar self.0 :p
impl MembershipTable {
    pub fn insert(&mut self, uuid: String, addr: String) {
        self.0.insert(uuid, addr);
    }
}

// TODO: mudar isto para message.rs, pensar em mais mensagens
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Msg {
    HELLO { uuid: String, addr: String },
    GOSSIP { table: MembershipTable },
    PING,
    ACK,
    // Shopping list operations
    GET_LIST { list_id: Uuid },
    PUT_LIST { list: ShoppingList },
    MERGE_LIST { list: ShoppingList },
    LIST_RESPONSE { list: Option<ShoppingList> },
}

impl Msg {
    pub fn name(&self) -> &'static str {
        match self {
            Msg::HELLO { .. } => "HELLO",
            Msg::GOSSIP { .. } => "GOSSIP",
            Msg::PING => "PING",
            Msg::ACK => "ACK",
            Msg::GET_LIST { .. } => "GET_LIST",
            Msg::PUT_LIST { .. } => "PUT_LIST",
            Msg::MERGE_LIST { .. } => "MERGE_LIST",
            Msg::LIST_RESPONSE { .. } => "LIST_RESPONSE",
        }
    }
}

pub struct Peer {
    pub uuid: String,
    storage: Mutex<ServerStorage>,

    // network related
    addr: String,
    ctx: Context,
    router: Mutex<Socket>,              // for incoming messages
    membership: Mutex<MembershipTable>, // stores known addresses
}

pub type SharedPeer = Arc<Peer>;

impl Peer {
    pub fn new(uuid: &str, bind_addr: &str) -> Result<Self> {
        let ctx = Context::new();

        let router = ctx.socket(SocketType::ROUTER)?;
        // router.set_identity(uuid.as_bytes())?; // ROUTER socket identity -> not important, router is the one who needs to know the requests identity
        router.bind(bind_addr)?;

        // this table will be changed if joining an active cluster
        let mut membership = MembershipTable(HashMap::new());
        membership.0.insert(uuid.to_string(), bind_addr.to_string());

        let storage = ServerStorage::new(uuid)?;

        anyhow::Ok(Self {
            ctx,
            uuid: uuid.to_string(),
            router: Mutex::new(router),
            membership: Mutex::new(membership),
            addr: bind_addr.to_string(),
            storage: Mutex::new(storage),
        })
    }

    // open socket connection to peer
    fn connect_to_peer(&self, uuid: &str) -> Result<Socket> {
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

        dealer.set_identity(self.uuid.as_bytes());
        dealer.connect(&peer_addr);

        // self.dealers.insert(uuid.to_string(), dealer);

        anyhow::Ok(dealer)
    }

    fn close_conn(&self, uuid: &str, socket: Socket) -> Result<()> {
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
    fn send_to(&self, uuid: &str, msg: &Msg) -> Result<()> {
        // open socket
        let socket = self.connect_to_peer(&uuid)?;

        // send empty frame first
        let _ = socket.send("", zmq::SNDMORE);

        // send msg
        let data = serde_json::to_vec(msg)?;
        println!("[{}] Sending {:?} to {}", self.uuid, msg.name(), uuid);
        socket.send(data, 0)?;

        // close socket
        self.close_conn(&uuid, socket)?;

        anyhow::Ok(())
    }

    fn send_gossip(&self) -> Result<()> {
        let table = {
            let table = self.membership.lock().unwrap();
            table.clone()
        };

        let msg = Msg::GOSSIP { table };

        let peers: Vec<String> = {
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

    fn send_gossip_to(&self, uuid: &str) -> Result<()> {
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
                                let sender_uuid = match identity_msg.as_str() {
                                    Some(id) => id,
                                    None => {
                                        eprintln!("Identity frame invalid");
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

    pub async fn start(peer: SharedPeer) {
        let listen_peer = Arc::clone(&peer);
        tokio::spawn(async move {
            Peer::listen(listen_peer).await;
        });

        let gossip_peer = Arc::clone(&peer);
        tokio::spawn(async move {
            Peer::gossip(gossip_peer).await;
        });

        println!("[{}] Started successfully!", peer.uuid);
    }

    pub fn ping(&mut self, uuid: &str) -> Result<()> {
        let msg: Msg = Msg::PING;

        self.send_to(uuid, &msg)
    }

    // dont have peer in membership table -> normal send_to/connect_peer dont work
    pub fn join(&self, seed_addr: &str) -> Result<()> {
        let socket = self.ctx.socket(SocketType::DEALER)?;
        socket.set_identity(self.uuid.as_bytes())?;
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

    fn handle_incoming(&self, identity: &str, msg: Msg) -> Result<()> {
        match msg {
            Msg::GOSSIP { table } => {
                println!("[{}] Received GOSSIP from {}", self.uuid, identity);
                // update our table if it changed
                let mut membership_guard = self.membership.lock().unwrap();

                for (u, addr) in table.0 {
                    membership_guard.insert(u, addr);
                }
            }

            Msg::PING {} => {
                println!("[{}] Received PING from {}", self.uuid, identity);
                self.send_to(&identity, &Msg::ACK)?;
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
                println!("[{}] Received GET_LIST for {} from {}", self.uuid, list_id, identity);
                let list = {
                    let storage = self.storage.lock().unwrap();
                    storage.get_shopping_list(&list_id)?
                };

                let response = Msg::LIST_RESPONSE { list };
                self.send_to(identity, &response)?;
            }

            Msg::PUT_LIST { list } => {
                println!("[{}] Received PUT_LIST for {} from {}", self.uuid, list.id, identity);
                let mut storage = self.storage.lock().unwrap();
                storage.write_shopping_list(&list)?;
                println!("[{}] Stored shopping list {}", self.uuid, list.id);
            }

            Msg::MERGE_LIST { list } => {
                println!("[{}] Received MERGE_LIST for {} from {}", self.uuid, list.id, identity);
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
                        println!("[{}] Stored new shopping list {} (no existing to merge)", self.uuid, list.id);
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
}
