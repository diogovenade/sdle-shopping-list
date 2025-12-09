use crate::crdt::{ShoppingList, GCounter, LWWReg, PNCounter, Item, AWORMap};
use anyhow::Result;
use rusqlite::{params, Connection, Result as sqlResult, Row, ToSql};
use std::path::PathBuf;
use thiserror::Error;
use uuid::Uuid;
use std::collections::HashMap;

struct StoredItemRow {
    item_id: i64,
    name: String,
    acquired_val: u32,
    acquired_clock: u32,
    acquired_actor: String,
    p_id: i64,
    n_id: i64
}

#[derive(Error, Debug)]
pub enum DbError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
}

pub struct ClientStorage {
    pub client_id: Uuid,
    db_path: PathBuf,
    db_conn: Connection,
    pub cached_lists: Vec<ShoppingList>,
}

impl ClientStorage {
    pub fn new() -> Result<Self> {
        let db_path = Self::compute_db_path()?;
        let db_conn = Connection::open(&db_path)?;
        db_conn.execute_batch("PRAGMA foreign_keys = ON;")?;
        db_conn.execute_batch("PRAGMA journal_mode = WAL;")?;
        Self::initialize_schema(&db_conn)?;
        let client_id = Self::request_client_id(&db_conn)?;
        Ok(ClientStorage {
            client_id,
            db_path,
            db_conn,
            cached_lists: Vec::new(),
        })
    }

    fn request_client_id(conn: &Connection) -> Result<Uuid> {
        let mut stmt = conn.prepare(
            "SELECT value 
                  FROM client_metadata
                  WHERE key = 'client_id'"
        )?;
        
        let mut rows = stmt.query([])?;

        if let Some(row) = rows.next()? {
            let s: String = row.get(0)?;
            return Ok(Uuid::try_parse(s.as_str())?);
        }

        let id = Uuid::new_v4();
        conn.execute(
            "INSERT INTO client_metadata (key, value) VALUES ('client_id', ?1)",
            [&id.to_string()],
        )?;
        Ok(id)
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
                CREATE TABLE IF NOT EXISTS client_metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                
                CREATE TABLE IF NOT EXISTS gcounter (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
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

    pub fn handle_item_storage_request(&self, item_name: String, quantity: u64, acquired: bool, shopping_list_id: Uuid) -> Result<()> {
        /*let read_result = self.read_item(item_name, shopping_list_id);
        
        let item_opt = read_result?;

        match item_opt {
            Some(i) => {
                
            }
            None => {
                let new_item = 
            }
        }

        Ok(())*/
        todo!();
    }
    
    fn read_gcounter(&self, id: i64) -> Result<GCounter> {
        let mut stmt = self.db_conn.prepare(
            "SELECT actor_id, value 
                  FROM gcounter_actor_values
                  WHERE gcounter_id = ?1"
        )?;

        let mut counter = HashMap::new();

        let rows = stmt.query_map([id], |row| {
            let actor: String = row.get(0)?;
            let value: u64 = row.get(1)?;
            Ok((Uuid::try_parse(&actor), value))
        })?;

        for entry in rows {
            let (actor, value) = entry?;
            match actor {
                Ok(actor_uuid) => {
                    counter.insert(actor_uuid, value);
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }

        Ok(GCounter {
            counter,
            actor_id: self.client_id
        })
    }

    fn read_item(&self, item_name: String, shopping_list_id: Uuid) -> Result<Option<Item>> {
        let mut stmt = self.db_conn.prepare(
        "SELECT 
                item_id,
                item_name,
                acquired_val,
                acquired_clock,
                acquired_actor,
                p_gcounter_id,
                n_gcounter_id
             FROM awormap_items
             WHERE shopping_list_id = ?1 AND item_name = ?2"
        )?;

        let mut rows = stmt.query([shopping_list_id.to_string(), item_name])?;

        let row = match rows.next()? {
            Some(row) => row,
            None => return Ok(None),
        };

        let _item_id: i64 = row.get(0)?; // useless id
        let _name: String = row.get(1)?; // you already know the name because you passed it in
        let acquired_val: u32 = row.get(2)?;
        let acquired_clock: u32 = row.get(3)?;
        let acquired_actor_string: String = row.get(4)?;
        let p_id: i64 = row.get(5)?;
        let n_id: i64 = row.get(6)?;

        let acquired_actor = Uuid::parse_str(&acquired_actor_string)?;

        let p = self.read_gcounter(p_id)?;
        let n = self.read_gcounter(n_id)?;
        let amount = PNCounter { p, n };

        let acquired = LWWReg {
            val: acquired_val,
            clock: acquired_clock,
            actor: acquired_actor,
        };

        Ok(Some(Item { amount, acquired }))
    }

    fn read_shopping_list(&self, id: Uuid) -> Result<ShoppingList> {
        let mut stmt = self.db_conn.prepare(
        "SELECT 
                item_id,
                item_name,
                acquired_val,
                acquired_clock,
                acquired_actor,
                p_gcounter_id,
                n_gcounter_id
             FROM awormap_items
             WHERE shopping_list_id = ?1"
        )?;
        
        let rows = stmt.query_map([id.to_string()], |row| {
            Ok(StoredItemRow {
                item_id: row.get(0)?,
                name: row.get(1)?,
                acquired_val: row.get(2)?,
                acquired_clock: row.get(3)?,
                acquired_actor: row.get(4)?,
                p_id: row.get(5)?,
                n_id: row.get(6)?,
            })
        })?;

        let mut awormap = AWORMap::new();

        for row_res in rows {
            let row = row_res?;

            let p = self.read_gcounter(row.p_id)?;
            let n = self.read_gcounter(row.n_id)?;
            let amount = PNCounter { p, n };

            let actor_uuid = Uuid::try_parse(row.acquired_actor.as_str())?; 
            let acquired = LWWReg::new(
                row.acquired_val as u32,
                row.acquired_clock as u32,
                actor_uuid
            );

            awormap.insert(row.name, Item { amount, acquired });
        }
        Ok(ShoppingList { id, list: awormap })
    }

    fn write_shopping_list(&mut self, shopping_list: &ShoppingList) -> Result<()> {
        let shopping_list_id = shopping_list.id.to_string();
        let map = &shopping_list.list; 

        let tx = self.db_conn.transaction()?;

        for (name, item) in &map.items {
            // Insert positive gcounter 
            let p_id = {
                tx.execute(
                    "INSERT INTO gcounter DEFAULT VALUES",
                    (),
                )?;

                tx.last_insert_rowid()
            };
            
            // Insert negative gcounter
            let n_id = {
                tx.execute(
                    "INSERT INTO gcounter DEFAULT VALUES",
                    (),
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
                    &item.acquired.actor.to_string(),
                    &p_id,
                    &n_id,
                ),
            )?;
        }

        tx.commit()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_teardown_client_database() {
        let cs = ClientStorage::new();
        assert_eq!(cs.is_err(), false);
        let teardown_result = cs.unwrap().teardown();
        assert_eq!(teardown_result.is_err(), false);
    }
}
