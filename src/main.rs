use anyhow::Result;
use sdle::cli::ClientInterfaceManager;
use sdle::client::Client;
use sdle::proxy::Proxy;
use sdle::server::{Peer, SharedPeer};
use uuid::Uuid;
use std::env;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
    thread
};

fn print_usage() {
    eprintln!("Usage: cargo run <client|server|proxy|client-test>");
}

#[tokio::main(flavor="multi_thread")]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    let Some(run_opt) = args.get(1) else {
        print_usage();
        std::process::exit(1);
    };

    match run_opt.as_str() {
        "client" => {
            let client = Client::new()?;
            let mut client_interface = ClientInterfaceManager::new(client);
            while !client_interface.is_done() {
                client_interface.render();
            }
        }
        "server" => {
            let seed_addr = "tcp://127.0.0.1:6000";
            let proxy_backend = "tcp://127.0.0.1:5556";

            let p1 = Peer::new(Uuid::new_v4(), &seed_addr, &proxy_backend)?;
            let p2 = Peer::new(Uuid::new_v4(), "tcp://127.0.0.1:6001", &proxy_backend)?;
            let p3 = Peer::new(Uuid::new_v4(), "tcp://127.0.0.1:6002", &proxy_backend)?;

            let sp1: SharedPeer = Arc::new(p1);
            let sp2: SharedPeer = Arc::new(p2);
            let sp3: SharedPeer = Arc::new(p3);

            // start seed node first
            Peer::start(Arc::clone(&sp1)).await;

            thread::sleep(Duration::from_millis(100));

            if let Err(e) = sp2.join(&seed_addr) {
                eprintln!("{} failed to join: {}", sp2.uuid, e);
            }

            if let Err(e) = sp3.join(&seed_addr) {
                eprintln!("{} failed to join: {}", sp3.uuid, e);
            }


            Peer::start(Arc::clone(&sp2)).await;
            Peer::start(Arc::clone(&sp3)).await;

            // to test -> peer 2 pings peer 3 every 2 seconds
            // {
            //     let sp2_clone = Arc::clone(&sp2);
            //     thread::spawn(move || {
            //         loop {
            //             {
            //                 let mut p2_guard = sp2_clone.lock().unwrap();
            //                 if let Err(e) = p2_guard.ping("peer3") {
            //                     eprintln!("[p2 -> p3] Ping failed: {}", e);
            //                 } else {
            //                     println!("[p2 -> p3] Ping sent");
            //                 }
            //             }
            //             thread::sleep(Duration::from_secs(2));
            //         }
            //     });
            // }

            loop {
                thread::sleep(Duration::from_secs(1));
            }
        },
        "proxy" => {
            let frontend_addr = "tcp://127.0.0.1:5555";
            let backend_addr = "tcp://127.0.0.1:5556";
            let proxy = Proxy::new(frontend_addr, backend_addr)?;
            println!("Proxy running: frontend at {}, backend at {}", frontend_addr, backend_addr);
            proxy.start()?;
        }
        "client-test" => {
            let client = Client::new()?;
            client.connect()?;
        }
        _ => {
            print_usage();
            std::process::exit(1);
        }
    }

    Ok(())
}
