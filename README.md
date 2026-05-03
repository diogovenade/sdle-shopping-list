# Local-First Shopping List

Local-first distributed shopping list developed for the [Large Scale Distributed Systems](https://sigarra.up.pt/feup/en/ucurr_geral.ficha_uc_view?pv_ocorrencia_id=560268) course by group T06G04.

## Group Members

1. Bernardo Costa (up202207579@up.pt)
2. Diogo Venade (up202207805@up.pt)
3. Ismael Moniz (up202206871@up.pt)
4. Vasco Costa (up202109923@up.pt)

## Overview

This project implements a terminal-based shopping list application with local
persistence and best-effort synchronization through a replicated server cluster.
Clients store their data locally first, so list edits can be made even when the
server side is unavailable. When connectivity exists, clients exchange complete
shopping list CRDT states with the server cluster and merge the returned state.

The system is composed of:

- A CLI client for creating lists and editing items.
- A ZeroMQ proxy that routes client requests to server peers.
- A server cluster with peer membership, consistent hashing, replication, and
  quorum-based reads/writes.
- SQLite databases for both client-side and server-side persistence.

## Technologies

- Rust 2024 edition
- Tokio for asynchronous server tasks
- ZeroMQ for client, proxy, and peer messaging
- SQLite through `rusqlite` for persistent storage
- `serde`/`serde_json` for message serialization
- UUIDs for clients, peers, and shopping lists

## Architecture

Clients communicate with the proxy frontend at `tcp://127.0.0.1:5555`. Server
peers connect to the proxy backend at `tcp://127.0.0.1:5556` and also communicate
directly with each other through peer addresses starting at `tcp://127.0.0.1:6000`.

The default server command starts a five-peer local cluster:

- Seed peer: `tcp://127.0.0.1:6000`
- Additional peers: `tcp://127.0.0.1:6001` through `tcp://127.0.0.1:6004`

Shopping lists are distributed using a consistent hash ring. Each list is mapped
to a preference list of server peers. The current constants are:

- `REPLICAS = 3`
- `WRITE_NODES = 2`
- `READ_NODES = 2`
- `VNODES = 3`

The server also includes support for gossip-style membership propagation, failure
detection, hinted handoff, and peer leave logic, although dynamic cluster changes
are not fully exposed through the CLI.

## Data Model

A shopping list is identified by a UUID and contains an add-wins observed-remove
map-like structure keyed by item name. Each item stores:

- Quantity as a PN-Counter.
- Acquired status as a last-writer-wins register.

Both clients and servers persist CRDT state in SQLite. Client databases are stored
under:

```text
data/clientstorage/<username>/client.db
```

Server storage is created under the project `data/` directory during local runs.

## Requirements

- Rust toolchain with Cargo.
- A working ZeroMQ environment. The Rust `zmq` crate links against ZeroMQ, so the
  system library may be required depending on the local platform.

On Debian/Ubuntu-like systems, install ZeroMQ with:

```bash
sudo apt install libzmq3-dev
```

## Build

```bash
cargo build
```

## Running Locally

Run the proxy, server cluster, and one or more clients in separate terminals.

Terminal 1:

```bash
cargo run proxy
```

Terminal 2:

```bash
cargo run server
```

Terminal 3:

```bash
cargo run client
```

When the client starts, enter an alphabetic username. The username determines the
local SQLite database path, so using the same username reopens the same local
client state.

## Client Commands

Main menu:

- `c` creates a new shopping list.
- `a` opens a list by UUID.
- `<number>` opens one of the locally known lists.
- `q` quits.

List view:

- `a` adds a new item.
- `<number>` edits an item.
- `r` refreshes the list by fetching and merging server state.
- `b` returns to the main menu.
- `q` quits.

Item edit view:

- `a<number>` changes the item quantity, for example `a5`.
- `d` toggles the acquired status.
- `b` returns to the list.
- `q` quits.

Item names and usernames currently accept alphabetic ASCII characters only.

## Other Commands

```bash
cargo run add-peer
cargo run remove-peer <port>
```

`add-peer` is currently not implemented. `remove-peer` sends a leave request to a
peer address, but peer removal is not fully integrated with the proxy and still
has known issues.

## Tests

Run the test suite with:

```bash
cargo test
```

## Repository Layout

```text
src/
  cli.rs        Terminal UI and input handling
  client.rs     Client-side list operations and proxy communication
  crdt.rs       Shopping list, item, and CRDT implementations
  hash_ring.rs  Consistent hashing and replication placement
  message.rs    Network message definitions
  proxy.rs      ZeroMQ frontend/backend proxy
  server.rs     Peer implementation, replication, quorum handling, membership
  storage.rs    SQLite persistence for clients and servers

doc/
  ArchitectureSDLE  Draw.io architecture source
  demo.mp4          Demo video
```
