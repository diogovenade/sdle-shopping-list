use uuid::Uuid;
use crate::client::Client;
use std::io::{self, Write};

const CLEAR_SEQUENCE: &'static str = "\x1B[2J\x1B[1;1H";

enum State {
    ListShow(Uuid),
    MainMenu,
    Exit,
}

pub struct ClientInterfaceManager {
    screen: State,
    client: Client,
}

impl ClientInterfaceManager {
    pub fn new(client: Client) -> Self {
        Self {
            screen: State::MainMenu,
            client,
        }
    }

    pub fn render(&mut self) {
        match std::mem::replace(&mut self.screen, State::MainMenu) {
            State::MainMenu => {
                let client_id = self.client.id;
                print!("{}", CLEAR_SEQUENCE);
                println!("Logged in as client: {}", client_id.to_string());
                println!("Press q to quit.");
                let mut input = String::new();
                Self::loop_input(&mut input, &["q"]);
                self.handle_main_menu(input);
            }
            State::ListShow(list_id) => {
                todo!(); 
            }
            State::Exit => {
                // byebye
            }
        }
    }

    pub fn is_done(&self) -> bool {
        matches!(self.screen, State::Exit)
    }

    fn loop_input(dest: &mut String, acceptable: &[&str]) {
        loop {
            dest.clear();
            io::stdout().flush().unwrap();

            match io::stdin().read_line(dest) {
                Ok(_) => {
                    let trimmed = dest.trim();
                    if acceptable.contains(&trimmed) {
                        *dest = trimmed.to_string();
                        break;
                    } else {
                        println!("Invalid input. Acceptable values are: {:?}", acceptable);
                        print!("Try again: ");
                    }
                }
                Err(e) => {
                    println!("Problematic input! {}", e);
                    print!("Try again: ");
                }
            }
        }
    }

    fn handle_main_menu(&mut self, input: String) {
        match input.as_str() {
            "q" => {
                self.screen = State::Exit;
            }
            _ => {

            }
        }
    }
}
