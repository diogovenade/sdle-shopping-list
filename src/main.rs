use std::env;

use uuid::Uuid;

use crate::server::Peer;

mod client;
mod server;

fn print_usage() {
    eprintln!("Usage: cargo run <client|server>");
}

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();

    let Some(run_opt) = args.get(1) else {
        print_usage();
        std::process::exit(1);
    };

    match run_opt.as_str() {
        "client" => {
            if let Err(e) = client::client_connect() {
                eprintln!("Client error: {}", e);
                std::process::exit(1);
            }

            Ok(())
        }
        "server" => {
            let seed_addr = "tcp://127.0.0.1:6000";
            let seed_uuid = &Uuid::new_v4().to_string();
            let mut seed = Peer::new(&seed_uuid, &seed_addr)?;

            let seed_thread = std::thread::spawn(move || {
                seed.start().unwrap();
            });


            let mut p = Peer::new(&Uuid::new_v4().to_string(), "tcp://127.0.0.1:6001")?;
            p.join_cluster(&seed_addr)?;

            let p_thread = std::thread::spawn(move || {
                p.start().unwrap();
            });

            seed_thread.join().unwrap();
            p_thread.join().unwrap();

            Ok(())
        }
        _ => {
            print_usage();
            std::process::exit(1);
        }
    }
}
