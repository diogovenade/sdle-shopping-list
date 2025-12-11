use anyhow::Result;
use sdle::cli::ClientInterfaceManager;
use sdle::client::Client;
use sdle::server::{Peer, SharedPeer};
use std::env;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
    thread
};

fn print_usage() {
    eprintln!("Usage: cargo run <client|server>");
}

fn main() -> Result<()> {
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

            let p1 = Peer::new("peer1", &seed_addr)?;
            let p2 = Peer::new("peer2", "tcp://127.0.0.1:6001")?;
            let p3 = Peer::new("peer3", "tcp://127.0.0.1:6002")?;

            let sp1: SharedPeer = Arc::new(Mutex::new(p1));
            let sp2: SharedPeer = Arc::new(Mutex::new(p2));
            let sp3: SharedPeer = Arc::new(Mutex::new(p3));

            // start seed node first
            Peer::start(Arc::clone(&sp1));

            thread::sleep(Duration::from_millis(100));

            {
                let mut g = sp2.lock().unwrap();
                match g.join(seed_addr) {
                    Ok(_) => {},
                    Err(e) => eprintln!("Peer 2 failed to join: {}", e),
                }
            }

            {
                let mut g = sp3.lock().unwrap();
                match g.join(seed_addr) {
                    Ok(_) => {},
                    Err(e) => eprintln!("Peer 3 failed to join: {}", e),
                }
            }

            Peer::start(Arc::clone(&sp2));
            Peer::start(Arc::clone(&sp3));

            // to test -> peer 2 pings peer 3 every 2 seconds
            {
                let sp2_clone = Arc::clone(&sp2);
                thread::spawn(move || {
                    loop {
                        {
                            let mut p2_guard = sp2_clone.lock().unwrap();
                            if let Err(e) = p2_guard.ping("peer3") {
                                eprintln!("[p2 -> p3] Ping failed: {}", e);
                            } else {
                                println!("[p2 -> p3] Ping sent");
                            }
                        }
                        thread::sleep(Duration::from_secs(2));
                    }
                });
            }

            loop {
                thread::sleep(Duration::from_secs(1));
            }
        }
        _ => {
            print_usage();
            std::process::exit(1);
        }
    }

    Ok(())
}
