use std::env;
use sdle::client::Client;
use sdle::server::Server;

fn print_usage() {
    eprintln!("Usage: cargo run <client|server>");
}

fn main() {
    let args: Vec<String> = env::args().collect();

    let Some(run_opt) = args.get(1) else {
        print_usage();
        std::process::exit(1);
    };

    match run_opt.as_str() {
        "client" => {
            if let Err(e) = Client::connect() {
                eprintln!("Client error: {}", e);
                std::process::exit(1);
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
}
