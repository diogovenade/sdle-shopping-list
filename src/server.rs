use anyhow::Result;
use uuid::Uuid;
use zmq::{Context, Error, SNDMORE, Socket, SocketType};

use rand::seq::{IndexedRandom, SliceRandom};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::storage::ServerStorage;

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
}

impl Msg {
    pub fn name(&self) -> &'static str {
        match self {
            Msg::HELLO { .. } => "HELLO",
            Msg::GOSSIP { .. } => "GOSSIP",
            Msg::PING => "PING",
            Msg::ACK => "ACK",
        }
    }
}

pub struct Peer {
    uuid: String,
    storage: ServerStorage,
    
    // network related
    addr: String,
    ctx: Context,
    router: Socket, // for incoming messages
    membership: MembershipTable, // stores known addresses
    
    // TODO: missing hashring 
}

pub type SharedPeer = Arc<Mutex<Peer>>;

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
            router,
            membership,
            addr: bind_addr.to_string(),
            storage
        })
    }

    // open socket connection to peer
    fn connect_to_peer(&self, uuid: &str) -> Result<Socket> {
        let peer_addr = match self.membership.0.get(uuid) {
            Some(e) => e,
            None => {
                anyhow::bail!("Cannot connect to {uuid}: peer not found in membership table");
            }
        };

        let dealer = self.ctx.socket(SocketType::DEALER)?;
        
        dealer.set_identity(self.uuid.as_bytes());
        dealer.connect(&peer_addr);

        // self.dealers.insert(uuid.to_string(), dealer);

        anyhow::Ok(dealer)
    }

    fn close_conn(&self, uuid: &str, socket: Socket) -> Result<()> {
        let peer_addr = match self.membership.0.get(uuid) {
            Some(e) => e,
            None => {
                anyhow::bail!(
                    "Cannot close connection to {uuid}: peer not found in membership table"
                );
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
        let table = self.membership.clone();
        let msg = Msg::GOSSIP { table };
        let peers: Vec<String> = self.membership.0.keys().cloned().collect();

        let p = match peers.choose(&mut rand::rng()) {
            Some(p) => p,
            None => {
                anyhow::bail!("error sending gossip: no peers");
            }
        };

        if p == &self.uuid {
            // only other peers
            return anyhow::Ok(());
        }

        self.send_to(p, &msg)?;

        anyhow::Ok(())
    }

    fn send_gossip_to(&self, uuid: &str) -> Result<()> {
        let table = self.membership.clone();
        let msg = Msg::GOSSIP { table };
        let p = match self.membership.0.get(uuid) {
            Some(p) => p,
            None => anyhow::bail!("Error sending gossip: {} not found", uuid),
        };

        self.send_to(p, &msg);

        anyhow::Ok(())
    }

    // por agora threads implementadas
    //   1) gossip p membership
    //   2) listener

    fn gossip(peer: SharedPeer) {
        thread::spawn(move || {
            loop {
                {
                    let peer = peer.lock().unwrap_or_else(|poisoned| {
                        eprintln!("Mutex poisoned");
                        poisoned.into_inner()
                    });

                    let _ = peer.send_gossip();
                }

                thread::sleep(Duration::from_millis(GOSSIP_INTERVAL));
            }
        });
    }

    fn listen(peer: SharedPeer) {
        thread::spawn(move || {
            loop {
                let mut peer = peer.lock().unwrap_or_else(|poisoned| {
                    eprintln!("Mutex poisoned");
                    poisoned.into_inner()
                });

                let identity = match peer.router.recv_msg(zmq::DONTWAIT) {
                    Ok(msg) => msg,
                    Err(e) if e == zmq::Error::EAGAIN => {
                        // DEALER-ROUTER specific error to allow async

                        drop(peer);

                        // maybe adicionar isto no caso de nao haver mensagens mas prov e ma ideia
                        //   -> larga escala temos que estar a espera q servers estejam smp a
                        //      receber mensagens
                        // thread::sleep(Duration::from_millis(1));

                        continue;
                    }
                    Err(e) => {
                        eprintln!("Failed to receive identity {}", e);
                        continue;
                    }
                };

                let _ = match peer.router.recv_msg(0) {
                    Ok(msg) => msg,
                    Err(e) => {
                        eprintln!("Failed to receive empty frame {}", e);
                        continue;
                    }
                };

                let data = match peer.router.recv_msg(0) {
                    Ok(msg) => msg,
                    Err(e) => {
                        eprintln!("Failed to receive data frame {}", e);
                        continue;
                    }
                };

                let msg: Msg = match serde_json::from_slice(&data) {
                    Ok(msg) => msg,
                    Err(e) => {
                        eprintln!("Failed to parse message {}", e);
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

                let _ = peer.handle_incoming(sender_uuid, msg);
            }
        });
    }

    pub fn start(peer: SharedPeer) {
        let listen_peer = Arc::clone(&peer);
        let gossip_peer = Arc::clone(&peer);

        Self::listen(listen_peer);
        Self::gossip(gossip_peer);
    }

    pub fn ping(&mut self, uuid: &str) -> Result<()> {
        let msg: Msg = Msg::PING;

        self.send_to(uuid, &msg)
    }

    // dont have peer in membership table -> normal send_to/connect_peer dont work
    pub fn join(&mut self, seed_addr: &str) -> Result<()> {
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

        println!("sent HELLO to {}", seed_addr);

        let start = Instant::now();

        // block until getting message
        let mut buf: Option<Msg> = None;
        while start.elapsed() < Duration::from_millis(JOIN_TIMEOUT) {
            let identity = match self.router.recv_msg(zmq::DONTWAIT) {
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

            let _ = match self.router.recv_msg(0) {
                Ok(msg) => msg,
                Err(e) => {
                    eprintln!("Failed to receive empty frame {}", e);
                    continue;
                }
            };

            let data = match self.router.recv_msg(0) {
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
                self.membership = table.clone();
            }
            other => anyhow::bail!("expected GOSSIP table, got {:?}", other),
        }

        // println!("Received membership table from GOSSIP");
        println!("Joined cluster successfully!");

        socket.disconnect(seed_addr)?;

        anyhow::Ok(())
    }

    fn handle_incoming(&mut self, identity: &str, msg: Msg) -> Result<()> {
        match msg {
            Msg::GOSSIP { table } => {
                println!("[{}] Received GOSSIP from {}", self.uuid, identity);
                // update our table if it changed
                for (u, addr) in table.0 {
                    self.membership.insert(u, addr);
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
                // update our table
                self.membership.insert(uuid.clone(), addr);

                // send gossip to new node
                let _ = self.send_gossip_to(&uuid);

                // send random gossip immediatelly
                let _ = self.send_gossip();
            }
        }
        anyhow::Ok(())
    }
}
