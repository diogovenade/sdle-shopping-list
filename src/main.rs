use std::env;

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
            // run client
        }
        "server" => {
            // run server
        }
        _ => {
            print_usage();
            std::process::exit(1);
        }
    }
}
