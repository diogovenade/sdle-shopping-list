use std::env;
use sdle::client::Client;
use sdle::server::Server;
use anyhow::{Result};
use sdle::cli::ClientInterfaceManager;
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

            /* client.send_item_storage_request("apples".to_string(), 10, false, Uuid::new_v4())?;
            let lists_opt= client.show_available_lists()?;

            match lists_opt {
                Some(lists) => {
                    for list in lists {
                        let list_id= list.list_id;
                        println!("List with id {list_id}:\n");
                        for (k, v) in &list.items {
                            let name = k;
                            let (amount, acquired) = v;

                            println!("{name}: {amount} {{{acquired}}}");
                        }
                    }
                }
                None => {
                    println!("No lists available");
                }
            } */
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
