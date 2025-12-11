use crate::client::{Client, ShoppingListInterface, ItemInterface };
use std::{
    io::{self, Write},
    ops::RangeBounds,
};
use uuid::Uuid;
use std::collections::HashMap;

const CLEAR_SEQUENCE: &'static str = "\x1B[2J\x1B[1;1H";
const UPPER_ITEM_COUNT_LIMIT: usize = 1000000;

enum InputRule<'a> {
    AcceptStrings(&'a [&'a str]),
    AcceptNumberRange { low: usize, high: usize },
    Custom(Box<dyn Fn(&str) -> bool + 'a>),
}

enum State {
    ListCreate(Uuid),
    ListEdit(Uuid),
    MainMenu,
    Exit,
    ItemEdit(Uuid, ItemInterface),
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
                let list_vec = self.print_lists();
                println!("Commands: q - quit; <nr> - open list number <nr>; a - add new list;");
                let mut input = String::new();
                Self::read_validated_input(
                    &mut input,
                    &[
                        InputRule::AcceptStrings(&["a", "q"]),
                        InputRule::AcceptNumberRange {
                            low: 1,
                            high: list_vec.len(),
                        },
                    ],
                );
                self.handle_main_menu(input, &list_vec);
            }
            State::ListCreate(list_id) => {
                println!("You are editing a new list! Get started adding some items.");
            }
            State::ListEdit(list_id) => {
                println!("Loading...");
                let mut input = String::new();
                if let Ok(list) = self.client.retrieve_list(list_id) {
                    print!("{}", CLEAR_SEQUENCE);
                    let items = Self::print_and_process_list_items(list);

                    println!("\nCommands: q - quit; b- back to menu; <nr> - edit item number <nr>; a - add new item;");
                    Self::read_validated_input(&mut input, &[
                        InputRule::AcceptStrings(&["q", "b"]),
                        InputRule::AcceptNumberRange { low: 1, high: items.len() },
                    ]);
                    self.handle_list_edit(input, &items, list_id);
                } else {
                    println!("Error retrieving list items.");
                    self.screen = State::MainMenu;
                    io::stdout().flush().unwrap();
                    let _ = io::stdin().read_line(&mut input);
                } 
            }
            State::ItemEdit(list_id, item) => {
                println!("Editing item {}. a<nr> - change amount to <nr>; d - toggle acquired status; b - go back, doing nothing;", &item.name);
                print!(">");
                let mut input = String::new();
                Self::read_validated_input(&mut input, &[
                    InputRule::AcceptStrings(&["q", "d", "b"]),
                    InputRule::Custom(Box::new(|s: &str | {
                        s.starts_with('a') && s.len() > 1 && s[1..].chars().all(|c| c.is_ascii_digit())
                    }))
                ]);
                self.handle_item_edit(input, item, list_id);
            }
            State::Exit => {
                // byebye
            }
        }
    }

    fn handle_item_edit(&mut self, input: String, item: ItemInterface, list_id: Uuid) {
        match input.as_str(){
            "q" => {
                self.screen = State::Exit;
            }
            "b" => {
                self.screen = State::ListEdit(list_id);
            }
            "d" => {
                let acquired = !item.acquired;

                match self.client.send_item_storage_request(item.name, item.amount, acquired, list_id) {
                    Ok(_) => {
                        self.screen = State::ListEdit(list_id);
                    }
                    Err(e) => {
                        println!("Error on item storage: {e}");
                        io::stdout().flush().unwrap();
                        let _ = io::stdin().read_line(&mut String::new());
                        self.screen = State::ListEdit(list_id);
                    }
                }
            }
            other => {
                if other.starts_with('a') {
                    if let Ok(amount) = other[1..].parse::<usize>() {
                        if amount < 1 {
                            println!("Invalid amount! Must be bigger than 0");
                            io::stdout().flush().unwrap();
                            let _ = io::stdin().read_line(&mut String::new());
                            self.screen = State::ListEdit(list_id);
                            return;
                        }
                        match self.client.send_item_storage_request(item.name, amount as u64, item.acquired, list_id) {
                            Ok(_) => {
                                self.screen = State::ListEdit(list_id);
                                return;
                            }
                            Err(e) => {
                                println!("Error on item storage: {e}");
                                io::stdout().flush().unwrap();
                                let _ = io::stdin().read_line(&mut String::new());
                                self.screen = State::ListEdit(list_id);
                                return;
                            }
                        } 
                    }
                    println!("Invalid input: {}", other);
                }
                println!("Invalid input: {}", other);
            }
        }
    }

    fn handle_list_edit(&mut self, input: String, items: &Vec<ItemInterface>, list_id: Uuid) {
        match input.as_str() {
            "q" => {
                self.screen = State::Exit;
            }
            "b" => {
                self.screen = State::MainMenu;
            }
            "a" => {
                let item = Self::build_item();

                match self.client.send_item_storage_request(item.name, item.amount, item.acquired, list_id) {
                    Ok(_) => {

                    }
                    Err(e) => {
                        println!("Error on item storage! {e}");
                        io::stdout().flush().unwrap();
                        let _ = io::stdin().read_line(&mut String::new());
                        self.screen = State::ListEdit(list_id);
                        return;
                    }
                }
                self.screen = State::ListEdit(list_id);
                return;
            }
            "d" => {
                todo!(); //TODO: implement item deletion
            }
            other => {
                if let Ok(selection) = other.parse::<usize>() {
                    let index = selection.checked_sub(1);
                     
                    if let Some(idx) = index { 
                        if idx < items.len() {
                            let chosen_item = &items[idx];
                            self.screen = State::ItemEdit(list_id, chosen_item.clone());
                            return;
                        }
                    }

                    println!("Invalid selection: {}", other);
                    return;
                }
                println!("Unknown command: {}", other); // safeguard
            }
        }
    }

    fn handle_main_menu(&mut self, input: String, lists: &Vec<Uuid>) {
        match input.as_str() {
            "q" => {
                self.screen = State::Exit;
            }
            "a" => {
                self.screen = State::ListCreate(Uuid::new_v4());
            }
            other => {
                if let Ok(selection) = other.parse::<usize>() {
                    let index = selection.checked_sub(1);

                    if let Some(idx) = index {
                        if idx < lists.len() {
                            let chosen_list_id = lists[idx];
                            self.screen = State::ListEdit(chosen_list_id);
                            return;
                        }
                    }

                    println!("Invalid selection: {}", other);
                    return;
                }
                println!("Unknown command: {}", other); // probably won't reach this point but meh
            }
        }
    }

    fn print_and_process_list_items(list: ShoppingListInterface) -> Vec<ItemInterface> {
        let mut items: Vec<ItemInterface> = Vec::new();
        println!("You are viewing list {}", list.list_id);
        let mut i = 1;
        for (item_name, item) in list.items {
            print!("{i}: {item_name} - qty: {}", item.0);
            if item.1 {
                println!("☒");
            } else {
                println!("☐");
            }
            i+=1;
            items.push(ItemInterface {name: item_name, amount: item.0, acquired: item.1});
        }

        items
    }

    fn print_lists(&self) -> Vec<Uuid> {
        let mut shopping_list_vec = Vec::new();
        match self.client.retrieve_available_lists() {
            Ok(Some(lists)) => {
                println!("Available lists:");
                for i in 1..=lists.len() {
                    let list_id = lists
                        .get(i - 1)
                        .expect("Indexing error on lists...")
                        .list_id;
                    println!("{i}: {list_id}");
                    shopping_list_vec.push(list_id);
                }
            }
            Ok(None) => {
                println!("No lists available locally.");
            }
            Err(e) => {
                println!("Error when retrieving lists: {e}");
            }
        }

        shopping_list_vec
    }

    pub fn is_done(&self) -> bool {
        matches!(self.screen, State::Exit)
    }

    fn read_validated_input(dest: &mut String, rules: &[InputRule<'_>]) {
        loop {
            dest.clear();
            io::stdout().flush().unwrap();

            match io::stdin().read_line(dest) {
                Ok(_) => {
                    let trimmed = dest.trim();

                    if Self::is_valid(trimmed, rules) {
                        *dest = trimmed.to_string();
                        return;
                    }

                    println!("Invalid input. Try again:");
                    print!("> ");
                }

                Err(e) => {
                    println!("Error reading input: {}", e);
                    print!("> ");
                }
            }
        }
    }

    fn is_valid(input: &str, rules: &[InputRule<'_>]) -> bool {
        for rule in rules {
            match rule {
                InputRule::AcceptStrings(list) => {
                    if list.contains(&input) {
                        return true;
                    }
                }
                InputRule::AcceptNumberRange { low, high } => {
                    if let Ok(n) = input.parse::<usize>() {
                        if (*low..=*high).contains(&n) {
                            return true;
                        }
                    }
                }
                InputRule::Custom(func) => {
                    if func(input) {
                        return true;
                    }
                }
            }
        }

        false
    }

    fn build_item() -> ItemInterface {
        println!("Enter item name: ");
        print!("> ");
        let mut name = String::new();
        Self::read_validated_input(&mut name, &[
            InputRule::Custom(Box::new(|s: &str | {
                s.chars().all(|c| c.is_ascii_alphabetic())
            }))
        ]);
        let mut amount_string = String::new();
        Self::read_validated_input(&mut amount_string, &[
            InputRule::AcceptNumberRange { low: 1, high: UPPER_ITEM_COUNT_LIMIT }
        ]);

        //WARNING: this should be fine because of the input sanizitation and the limit, but be careful.
        let amount = amount_string.parse::<u64>().expect("Int parsing error.");

        ItemInterface { name, amount, acquired: false} 
    }
}
