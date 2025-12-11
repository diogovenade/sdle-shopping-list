use anyhow::Result;
use sdle::cli::ClientInterfaceManager;
use sdle::client::Client;
use sdle::server::Server;
use std::env;
use uuid::Uuid;

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
            if let Err(e) = Server::setup_server() {
                eprintln!("Server error: {}", e);
                std::process::exit(1);
            }
        }
        _ => {
            print_usage();
            std::process::exit(1);
        }
    }

    Ok(())
}
