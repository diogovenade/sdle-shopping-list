use crate::crdt::{Item, ShoppingList};
use anyhow::Result;
use rusqlite::{params, Connection, Result as sqlResult, Row, ToSql};
use std::path::PathBuf;
use thiserror::Error;
use uuid::Uuid;

pub trait Stored {
    fn from_schema() -> Self;
    fn insert(&self, conn: &Connection, client_id: Uuid) -> Result<()>;
}

impl Stored for ShoppingList {
    fn from_schema() -> Self {
        todo!();
    }

    fn insert(&self, conn: &Connection, client_id: Uuid) -> Result<()> {
        let shopping_list_id = self.id.to_string();
        let map = &self.list; 

        let tx = conn.unchecked_transaction()?;

        for (name, item) in &map.items {
            // Insert positive gcounter 
            let p_id = {
                tx.execute(
                    "INSERT INTO gcounter (owner_actor) VALUES (?1)",
                    (&client_id.to_string(),),
                )?;

                tx.last_insert_rowid()
            };
            
            // Insert negative gcounter
            let n_id = {
                tx.execute(
                    "INSERT INTO gcounter (owner_actor) VALUES (?1)",
                    (&client_id.to_string(),),
                )?;

                tx.last_insert_rowid()
            };

            // Insert positive counter actor counts 
            for (actor, count) in &item.amount.p.counter {
                tx.execute(
                    "INSERT INTO gcounter_actor_values (gcounter_id, actor_id, value) VALUES (?1, ?2 ,?3)",
                    (&p_id, &actor.to_string(), count),
                )?;
            }

            // Insert negative counter actor counts 
            for (actor, count) in &item.amount.n.counter {
                tx.execute(
                    "INSERT INTO gcounter_actor_values (gcounter_id, actor_id, value) VALUES (?1, ?2 ,?3)", 
                    (&n_id, &actor.to_string(), count),
                )?;
            }

            // Insert item 
            tx.execute(
                "INSERT INTO awormap_items (
                    shopping_list_id,
                    item_name,
                    acquired_val,
                    acquired_clock,
                    acquired_actor,
                    p_gcounter_id,
                    n_gcounter_id
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                (
                    &shopping_list_id,
                    name,
                    &item.acquired.val,
                    &item.acquired.clock,
                    item.acquired.actor.to_string(),
                    &p_id,
                    &n_id,
                ),
            )?;
        }

        tx.commit()?;

        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum DbError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
}

struct ClientStorage {
    client_id: Uuid,
    db_path: PathBuf,
    db_conn: Connection,
    cached_lists: Vec<ShoppingList>,
}

impl ClientStorage {
    pub fn new(client_id: Uuid) -> Result<Self, DbError> {
        let db_path = Self::compute_db_path()?;
        println!("Computed db_path successfully.");
        let db_conn = Connection::open(&db_path)?;
        println!("Database connection established successfully.");
        
        db_conn.execute_batch("PRAGMA foreign_keys = ON;")?;
        db_conn.execute_batch("PRAGMA journal_mode = WAL;")?;
        println!("Pragmas executed successfully.");

        Self::initialize_schema(&db_conn)?;
        println!("Schema initialized successfully.");

        Ok(ClientStorage {
            client_id,
            db_path,
            db_conn,
            cached_lists: Vec::new(),
        })
    }

    fn compute_db_path() -> std::io::Result<PathBuf> {
        let mut root = std::env::current_dir()?; // appropriate directory for dev,
                                                               // i.e. cargo run, cargo test, etc.
        root.push("data");
        root.push("clientstorage");
        std::fs::create_dir_all(&root)?;
        root.push("client.db");
        Ok(root)
    }

    fn initialize_schema(db_conn: &Connection) -> Result<(), DbError> {
        db_conn.execute_batch(
            r#"
                CREATE TABLE IF NOT EXISTS gcounter (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner_actor TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS gcounter_actor_values (
                    gcounter_id INTEGER NOT NULL,
                    actor_id TEXT NOT NULL,
                    value INTEGER NOT NULL,
                    PRIMARY KEY (gcounter_id, actor_id),
                    FOREIGN KEY (gcounter_id) REFERENCES gcounter(id)
                );

                CREATE TABLE IF NOT EXISTS awormap_items (
                    item_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    shopping_list_id TEXT NOT NULL,
                    item_name TEXT NOT NULL,
                    acquired_val BOOLEAN NOT NULL,
                    acquired_clock INTEGER NOT NULL,
                    acquired_actor TEXT NOT NULL,
                    p_gcounter_id INTEGER NOT NULL,
                    n_gcounter_id INTEGER NOT NULL,
                    FOREIGN KEY(p_gcounter_id) REFERENCES gcounter(id),
                    FOREIGN KEY(n_gcounter_id) REFERENCES gcounter(id),
                    UNIQUE (shopping_list_id, item_name)
                );
            "#,
        )?;
        Ok(())
    }

    pub fn teardown(self) -> Result<(), DbError> {
        // cleanup happens automatically on consumption of self
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn test_init_teardown_client_database() {
        let client_id = Uuid::new_v4();
        let cs = ClientStorage::new(client_id);
        assert_eq!(cs.is_err(), false);
        let teardown_result = cs.unwrap().teardown();
        assert_eq!(teardown_result.is_err(), false);
    }
}
