use anyhow::Result;
use sdle::cli::{ClientInterfaceManager, InputRule};
use sdle::client::Client;
use sdle::message::Msg;
use sdle::proxy::Proxy;
use sdle::server::{Peer, SharedPeer};
use std::fmt::format;
use std::io::{self, Write};
use std::path::Path;
use std::{env, fs};
use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};
use uuid::Uuid;
use zmq::Context;

fn print_usage() {
    eprintln!("Usage: cargo run <client|server|proxy|add-peer|remove-peer>");
}

fn write_port(port: u16) -> std::io::Result<()> {
    let path = Path::new("data");
    fs::create_dir_all(path)?;

    let mut file = fs::File::create(path.join("port.txt"))?;
    writeln!(file, "{}", port)?;
    Ok(())
}

fn read_port() -> std::io::Result<u16> {
    let path = format!("data/port.txt");
    let contents = fs::read_to_string(path)?;

    Ok(contents.trim().parse::<u16>().expect("error on port parse"))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    let Some(run_opt) = args.get(1) else {
        print_usage();
        drop(args);
        std::process::exit(1);
    };

    let mut ctx = Context::new(); //NOTE: only one context should be created. lol ja criamos tantos contextos

    let seed_addr = "tcp://127.0.0.1:6000";
    let proxy_backend = "tcp://127.0.0.1:5556";
    let mut port = 6001;

    match run_opt.as_str() {
        "client" => {
            let mut username = String::new();
            print!("\x1B[2J\x1B[1;1H");
            println!("Enter username");
            print!("> ");
            io::stdout().flush().unwrap();
            ClientInterfaceManager::read_validated_input(
                &mut username,
                &[InputRule::Custom(Box::new(|s| {
                    s[..].chars().all(|c| c.is_ascii_alphabetic())
                }))],
            );
            let client = Client::new(username)?;
            let mut client_interface = ClientInterfaceManager::new(client);
            while !client_interface.is_done() {
                client_interface.render();
            }
        }
        "server" => {
            let p1 = Peer::new(&ctx, Uuid::new_v4(), &seed_addr, &proxy_backend)?;

            let p2 = Peer::new(
                &ctx,
                Uuid::new_v4(),
                &format!("tcp://127.0.0.1:{}", port),
                &proxy_backend,
            )?;
            port += 1;

            let p3 = Peer::new(
                &ctx,
                Uuid::new_v4(),
                &format!("tcp://127.0.0.1:{}", port),
                &proxy_backend,
            )?;
            port += 1;

            let p4 = Peer::new(
                &ctx,
                Uuid::new_v4(),
                &format!("tcp://127.0.0.1:{}", port),
                &proxy_backend,
            )?;
            port += 1;

            let p5 = Peer::new(
                &ctx,
                Uuid::new_v4(),
                &format!("tcp://127.0.0.1:{}", port),
                &proxy_backend,
            )?;
            port += 1;

            let _ = write_port(port);

            let sp1: SharedPeer = Arc::new(p1);
            let sp2: SharedPeer = Arc::new(p2);
            let sp3: SharedPeer = Arc::new(p3);
            let sp4: SharedPeer = Arc::new(p4);
            let sp5: SharedPeer = Arc::new(p5);

            // start seed node first
            Peer::start(Arc::clone(&sp1)).await;

            thread::sleep(Duration::from_millis(100));

            if let Err(e) = sp2.join(&seed_addr) {
                eprintln!("{} failed to join: {}", sp2.uuid, e);
            }

            if let Err(e) = sp3.join(&seed_addr) {
                eprintln!("{} failed to join: {}", sp3.uuid, e);
            }

            if let Err(e) = sp4.join(&seed_addr) {
                eprintln!("{} failed to join: {}", sp2.uuid, e);
            }

            if let Err(e) = sp5.join(&seed_addr) {
                eprintln!("{} failed to join: {}", sp3.uuid, e);
            }

            Peer::start(Arc::clone(&sp2)).await;
            Peer::start(Arc::clone(&sp3)).await;
            Peer::start(Arc::clone(&sp4)).await;
            Peer::start(Arc::clone(&sp5)).await;

            loop {
                thread::sleep(Duration::from_millis(1));
            }
        }
        "proxy" => {
            let frontend_addr = "tcp://127.0.0.1:5555";
            let backend_addr = "tcp://127.0.0.1:5556";
            let mut proxy = Proxy::new(frontend_addr, backend_addr)?;
            println!(
                "Proxy running: frontend at {}, backend at {}",
                frontend_addr, backend_addr
            );
            proxy.start()?;
        }

        "add-peer" => {
            println!("Unfortunately, not implemented (probably due to a deadlock) :(");
            // let port = read_port()?;
            // let peer = Peer::new(
            //     &ctx,
            //     Uuid::new_v4(),
            //     &format!("tcp://127.0.0.1:{}", port),
            //     &proxy_backend,
            // )?;

            // let _ = write_port(port + 1);

            // let sp: SharedPeer = Arc::new(peer);

            // if let Err(e) = sp.join(&seed_addr) {
            //     eprintln!("{} failed to join: {}", sp.uuid, e);
            // }

            // Peer::start(Arc::clone(&sp)).await;

            // loop {
            //     thread::sleep(Duration::from_millis(1));
            // }
        }

        "remove-peer" => {
            println!("Unfortunately, not fully implemented (but we got close, check peer.leave(), missing proxy leave and some bugfixin) :(");

            let port = args.get(2);

            if let Some(port) = port {
                let addr = format!("tcp://127.0.0.1:{}", port);
                let ctx = Context::new();
                let socket = ctx.socket(zmq::REQ)?;
                socket.set_identity(Uuid::new_v4().to_string().as_bytes()); // acho q e preciso
                socket.connect(&addr)?;

                let msg = Msg::Leave;

                socket.send(serde_json::to_vec(&msg)?, 0)?;

                println!("Sent leave request to {}", addr);
            } else {
                println!("Usage: cargo run remove-peer <port>")
            }
        }

        _ => {
            print_usage();
            drop(args);
            ctx.destroy().expect("Failed to destroy zmq context.");
            std::process::exit(1);
        }
    }

    Ok(())
}
