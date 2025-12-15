use crate::crdt::{AWORMap, GCounter, Item, LWWReg, Mergeable, PNCounter, ShoppingList};
use crate::hash_ring::HashRing;
use anyhow::{Context, Result};
use rusqlite::{Connection, Result as sqlResult, Row, ToSql, params, OptionalExtension};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use uuid::Uuid;

struct StoredItemRow {
    item_id: i64,
    name: String,
    acquired_val: u32,
    acquired_clock: u32,
    acquired_actor: String,
    p_id: i64,
    n_id: i64,
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
    pub fn new(username: String) -> Result<Self> {
        let db_path = Self::compute_db_path(username)?;
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
                  WHERE key = 'client_id'",
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

    fn compute_db_path(username: String) -> std::io::Result<PathBuf> {
        let mut root = std::env::current_dir()?; // appropriate directory for dev,
        // i.e. cargo run, cargo test, etc.
        root.push("data");
        root.push("clientstorage");
        root.push(username);
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

                CREATE TABLE IF NOT EXISTS local_lists (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    list_uuid TEXT UNIQUE NOT NULL
                );
                
                CREATE TABLE IF NOT EXISTS gcounter (
                    id INTEGER PRIMARY KEY AUTOINCREMENT
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

    pub fn handle_item_storage_request(
        &mut self,
        item_name: String,
        quantity: u64,
        acquired: bool,
        shopping_list_id: Uuid,
    ) -> Result<()> {
        self.ensure_list(shopping_list_id)?;
        let read_result = self.read_item(&item_name, shopping_list_id);

        let item_opt = read_result?;

        match item_opt {
            Some(stored_item_row) => {
                let mut stored_item = self.build_item(&stored_item_row)?;

                let current_quantity = stored_item.amount.value_total();
                let delta = quantity as i64 - current_quantity;

                let mut local_updated_item = stored_item.clone();

                // apply pncounter mutations
                if delta > 0 {
                    local_updated_item.amount.inc_by(delta as u64); // TODO: does actor id match?
                } else if delta < 0 {
                    local_updated_item.amount.dec_by((-delta) as u64);
                }

                // apply lwwreg mutation
                let new_acquired_val = acquired as u32;
                if new_acquired_val != stored_item.acquired.val {
                    local_updated_item.acquired = LWWReg {
                        val: new_acquired_val,
                        clock: stored_item.acquired.clock + 1,
                        actor: self.client_id,
                    };
                }

                stored_item.merge(&local_updated_item);

                self.overwrite_item(
                    &stored_item,
                    stored_item_row.item_id,
                    stored_item_row.p_id,
                    stored_item_row.n_id,
                )?;
            }
            None => {
                let new_item = Item::new(quantity, acquired, self.client_id);
                self.write_item(&shopping_list_id.to_string(), &new_item, &item_name)?;
            }
        }

        Ok(())
    }

    fn ensure_list(&mut self, shopping_list_id: Uuid) -> Result<()> {
        let tx = self.db_conn.transaction()?;

        tx.execute(
            "INSERT OR IGNORE INTO local_lists (list_uuid) VALUES (?1)",
            params![shopping_list_id.to_string()],
        )?;

        tx.commit()?;
        Ok(())
    }

    pub fn get_user_lists(&self) -> Result<Option<Vec<ShoppingList>>> {
        let mut list_vec: Vec<ShoppingList> = Vec::new();
        let mut stmt = self.db_conn.prepare(
            "SELECT list_uuid 
                  FROM local_lists",
        )?;

        let lists = stmt.query_map([], |row| {
            let uuid_str: String = row.get(0)?;
            Ok(Uuid::try_parse(&uuid_str))
        })?;

        for list_res in lists {
            let list_uuid = list_res??;
            let list = self.read_shopping_list(list_uuid)?;
            list_vec.push(list);
        }

        if list_vec.is_empty() {
            Ok(None)
        } else {
            Ok(Some(list_vec))
        }
    }

    fn read_gcounter(&self, id: i64) -> Result<GCounter> {
        let mut stmt = self.db_conn.prepare(
            "SELECT actor_id, value 
                  FROM gcounter_actor_values
                  WHERE gcounter_id = ?1",
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
            actor_id: self.client_id,
        })
    }

    fn read_item(
        &self,
        item_name: &String,
        shopping_list_id: Uuid,
    ) -> Result<Option<StoredItemRow>> {
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
             WHERE shopping_list_id = ?1 AND item_name = ?2",
        )?;

        let mut rows = stmt.query([shopping_list_id.to_string(), item_name.clone()])?;

        let row = match rows.next()? {
            Some(row) => row,
            None => return Ok(None),
        };

        Ok(Some(StoredItemRow {
            item_id: row.get(0)?,
            name: row.get(1)?,
            acquired_val: row.get(2)?,
            acquired_clock: row.get(3)?,
            acquired_actor: row.get(4)?,
            p_id: row.get(5)?,
            n_id: row.get(6)?,
        }))
    }

    pub fn read_shopping_list(&self, id: Uuid) -> Result<ShoppingList> {
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
             WHERE shopping_list_id = ?1",
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
            let item = self.build_item(&row)?;

            awormap.insert(row.name, item);
        }
        Ok(ShoppingList { id, list: awormap })
    }

    fn build_item(&self, intermediate: &StoredItemRow) -> Result<Item> {
        let p = self.read_gcounter(intermediate.p_id)?;
        let n = self.read_gcounter(intermediate.n_id)?;
        let amount = PNCounter { p, n };

        let actor_uuid = Uuid::try_parse(&intermediate.acquired_actor.as_str())?;
        let acquired = LWWReg::new(
            intermediate.acquired_val as u32,
            intermediate.acquired_clock as u32,
            actor_uuid,
        );

        Ok(Item { amount, acquired })
    }

    fn overwrite_item(
        &mut self,
        item: &Item,
        item_id: i64,
        p_gcounter_id: i64,
        n_gcounter_id: i64,
    ) -> Result<()> {
        let tx = self.db_conn.transaction()?;

        tx.execute(
            "UPDATE awormap_items
                    SET acquired_val = ?1,
                        acquired_clock = ?2,
                        acquired_actor = ?3
                    WHERE item_id = ?4",
            params![
                item.acquired.val as i64,
                item.acquired.clock as i64,
                item.acquired.actor.to_string(),
                item_id,
            ],
        )?;

        {
            let mut stmt_upsert = tx.prepare(
                "INSERT INTO gcounter_actor_values (gcounter_id, actor_id, value)
                     VALUES (?1, ?2, ?3)
                 ON CONFLICT(gcounter_id, actor_id) DO UPDATE SET value = excluded.value",
            )?;

            for (actor_uuid, &val_u64) in &item.amount.p.counter {
                let actor_str = actor_uuid.to_string();
                let val_i64 = val_u64 as i64;
                stmt_upsert.execute(params![p_gcounter_id, actor_str, val_i64])?;
            }
        }

        {
            let mut stmt_upsert = tx.prepare(
                "INSERT INTO gcounter_actor_values (gcounter_id, actor_id, value)
                     VALUES (?1, ?2, ?3)
                 ON CONFLICT(gcounter_id, actor_id) DO UPDATE SET value = excluded.value",
            )?;

            for (actor_uuid, &val_u64) in &item.amount.n.counter {
                let actor_str = actor_uuid.to_string();
                let val_i64 = val_u64 as i64;
                stmt_upsert.execute(params![n_gcounter_id, actor_str, val_i64])?;
            }
        }

        tx.commit()?;

        Ok(())
    }

    fn write_item(
        &mut self,
        shopping_list_id: &String,
        item: &Item,
        item_name: &String,
    ) -> Result<()> {
        let tx = self.db_conn.transaction()?;

        // Insert positive gcounter
        let p_id = {
            tx.execute("INSERT INTO gcounter DEFAULT VALUES", ())?;

            tx.last_insert_rowid()
        };

        // Insert negative gcounter
        let n_id = {
            tx.execute("INSERT INTO gcounter DEFAULT VALUES", ())?;

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
                shopping_list_id,
                item_name,
                &item.acquired.val,
                &item.acquired.clock,
                &item.acquired.actor.to_string(),
                &p_id,
                &n_id,
            ),
        )?;

        tx.commit()?;

        Ok(())
    }

    pub fn write_shopping_list(&mut self, shopping_list: &ShoppingList) -> Result<()> {
        let shopping_list_id = shopping_list.id.to_string();
        let map = &shopping_list.list;

        let tx = self.db_conn.transaction()?;

        for (name, item) in &map.items {
            // Check if item exists
            let row: Option<(i64, i64)> = tx.query_row(
                "SELECT p_gcounter_id, n_gcounter_id FROM awormap_items WHERE shopping_list_id = ?1 AND item_name = ?2",
                (&shopping_list_id, name),
                |r| Ok((r.get(0)?, r.get(1)?)),
            ).optional()?;

            let (p_id, n_id) = if let Some((p, n)) = row {
                // reuse existing gcounters
                (p, n)
            } else {
                // Insert new positive gcounter
                let p_id = {
                    tx.execute("INSERT INTO gcounter DEFAULT VALUES", ())?;
                    tx.last_insert_rowid()
                };

                // Insert new negative gcounter
                let n_id = {
                    tx.execute("INSERT INTO gcounter DEFAULT VALUES", ())?;
                    tx.last_insert_rowid()
                };

                (p_id, n_id)
            };

            // Update positive counter actor values
            for (actor, count) in &item.amount.p.counter {
                tx.execute(
                    "INSERT OR REPLACE INTO gcounter_actor_values (gcounter_id, actor_id, value)
                     VALUES (?1, ?2, ?3)",
                    (&p_id, &actor.to_string(), count),
                )?;
            }

            // Update negative counter actor values
            for (actor, count) in &item.amount.n.counter {
                tx.execute(
                    "INSERT OR REPLACE INTO gcounter_actor_values (gcounter_id, actor_id, value)
                     VALUES (?1, ?2, ?3)",
                    (&n_id, &actor.to_string(), count),
                )?;
            }

            // Insert or update item
            tx.execute(
                "INSERT INTO awormap_items (
                    shopping_list_id,
                    item_name,
                    acquired_val,
                    acquired_clock,
                    acquired_actor,
                    p_gcounter_id,
                    n_gcounter_id
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                ON CONFLICT(shopping_list_id, item_name)
                DO UPDATE SET
                    acquired_val = excluded.acquired_val,
                    acquired_clock = excluded.acquired_clock,
                    acquired_actor = excluded.acquired_actor,
                    p_gcounter_id = excluded.p_gcounter_id,
                    n_gcounter_id = excluded.n_gcounter_id;",
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

pub struct ServerStorage {
    pub server_id: Uuid,
    db_path: PathBuf,
    db_conn: Connection,
}

impl ServerStorage {
    pub fn new(server_id: Uuid) -> Result<Self> {
        let id_str = server_id.to_string();
        let db_path = Self::compute_db_path(id_str.as_str())?;
        let db_conn = Connection::open(&db_path)?;

        db_conn.execute_batch("PRAGMA foreign_keys = ON;")?;
        db_conn.execute_batch("PRAGMA journal_mode = WAL;")?;

        Self::initialize_schema(&db_conn)?;

        Ok(Self {
            server_id,
            db_path,
            db_conn,
        })
    }

    fn compute_db_path(uuid: &str) -> std::io::Result<PathBuf> {
        let mut root = std::env::current_dir()?;

        root.push("data");
        root.push("serverstorage");
        std::fs::create_dir_all(&root)?;

        let file = format!("server-{}.db", uuid);
        root.push(file);
        Ok(root)
    }

    fn initialize_schema(conn: &Connection) -> Result<()> {
        // TODO: acho que isto chega mas continuar a verificar

        // created_at secalhar ajuda -> podemos dar query por hinted_handoff != null e sort por mais antigos
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS shopping_lists (
                id              BLOB PRIMARY KEY UNIQUE NOT NULL,
                crdt_data       BLOB NOT NULL,
                hinted_handoff  TEXT
            )",
        )?;

        Ok(())
    }

    pub fn write_shopping_list(&mut self, shopping_list: &ShoppingList) -> Result<()> {
        let data: Vec<u8> = serde_json::to_vec(&shopping_list)?;

        let stored = match self.get_shopping_list(&shopping_list.id) {
            Ok(Some(s)) => Some(s),
            Ok(None) => None,
            Err(_) => None,
        };

        let new_shopping_list = match stored {
            Some(mut s) => {
                s.list.merge(&shopping_list.list);
                s
            }
            None => shopping_list.clone(),
        };

        let hash = HashRing::hash(&new_shopping_list.id.to_string()).to_be_bytes();

        let tx = self.db_conn.transaction()?;

        tx.execute(
            "INSERT OR REPLACE INTO shopping_lists (id, crdt_data, hinted_handoff)
         VALUES (?1, ?2, ?3)",
            params![hash.as_slice(), data, None::<String>],
        )?;

        tx.commit()?;

        Ok(())
    }

    pub fn write_shopping_list_handoff(
        &mut self,
        shopping_list: &ShoppingList,
        server_id: &Uuid,
    ) -> Result<()> {
        let data: Vec<u8> = serde_json::to_vec(&shopping_list)?;

        let stored = match self.get_shopping_list(&shopping_list.id) {
            Ok(Some(s)) => Some(s),
            Ok(None) => None,
            Err(_) => None,
        };

        let new_shopping_list = match stored {
            Some(mut s) => {
                s.list.merge(&shopping_list.list);
                s
            }
            None => shopping_list.clone(),
        };

        let hash = HashRing::hash(&new_shopping_list.id.to_string()).to_be_bytes();

        let tx = self.db_conn.transaction()?;

        tx.execute(
            "INSERT OR REPLACE INTO shopping_lists (id, crdt_data, hinted_handoff)
         VALUES (?1, ?2, ?3)",
            params![hash.as_slice(), data, server_id.to_string()],
        )?;

        tx.commit()?;

        Ok(())
    }

    pub fn get_shopping_list(&self, shopping_list_id: &Uuid) -> Result<Option<ShoppingList>> {
        let hash = HashRing::hash(&shopping_list_id.to_string()).to_be_bytes();

        let mut stmt = self
            .db_conn
            .prepare("SELECT crdt_data FROM shopping_lists WHERE id = ?1")?;

        let mut rows = stmt.query(params![hash.as_slice()])?;

        if let Some(row) = rows.next()? {
            let data: Vec<u8> = row.get(0)?;
            let list: ShoppingList = serde_json::from_slice(&data)?;

            return Ok(Some(list));
        }

        Ok(None)
    }

    // return dest + shopping list
    pub fn get_hinted_handoffs(&self) -> Result<Vec<(Uuid, ShoppingList)>> {
        let mut stmt = self
            .db_conn
            .prepare(
                "SELECT crdt_data, hinted_handoff FROM shopping_lists \
                WHERE hinted_handoff IS NOT NULL",
            )
            .context("failed to prepare hinted handoff query")?;

        let rows = stmt
            .query_map([], |row| {
                let data: Vec<u8> = row.get(0)?;
                let uuid_raw: String = row.get(1)?;
                let shopping_list = serde_json::from_slice(&data).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        data.len(),
                        rusqlite::types::Type::Blob,
                        Box::new(e),
                    )
                })?;
                let uuid = Uuid::parse_str(&uuid_raw).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        uuid_raw.len(),
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })?;
                Ok((uuid, shopping_list))
            })
            .context("failed to execute hinted handoff query")?
            .collect::<std::result::Result<Vec<_>, rusqlite::Error>>()
            .context("failed to map hinted handoff rows")?;

        Ok(rows)
    }

    // acho que isto funciona para quando node entra no hash ring
    // isma: this is a start, but does not cover all cases.
    // a server may even need to take in data whose hash is bigger than its own,
    // if no other server is in between. ownership of data is circular and there is wraparound
    // to be taken into consideration.
    // NOTE: solved probably
    pub fn get_old_data(&self, node_id: &Uuid, prev_node_hash: u128) -> Result<Vec<ShoppingList>> {
        let hash = HashRing::hash(&node_id.to_string());
        let prev_bytes = prev_node_hash.to_be_bytes();

        let query = if prev_node_hash < hash {
            "SELECT crdt_data FROM shopping_lists WHERE id > ?1 AND id <= ?2 AND hinted_handoff IS NULL"
        } else {
            // wraparound case
            "SELECT crdt_data FROM shopping_lists WHERE (id > ?1 OR id <= ?2) AND hinted_handoff IS NULL"
        };

        let mut stmt = self.db_conn.prepare(query)?;

        let hash_bytes = hash.to_be_bytes();

        let rows = stmt
            .query_map(params![hash_bytes.as_slice(), prev_bytes.as_slice(),], |row| {
                let data: Vec<u8> = row.get(0)?;

                let shopping_list: ShoppingList = serde_json::from_slice(&data).unwrap();

                Ok(shopping_list)
            })?
            .collect::<Result<Vec<ShoppingList>, _>>()?;

        Ok(rows)
    }

    pub fn get_all_rows(&self) -> Result<Vec<(u128, ShoppingList)>> {
        let mut stmt = self
            .db_conn
            .prepare("SELECT id, crdt_data FROM shopping_lists")?;

        let rows = stmt
            .query_map([], |row| {
                // id is stored as BLOB, read as Vec<u8> and convert to u128
                let id_blob: Vec<u8> = row.get(0)?;
                let mut id_bytes = [0u8; 16];
                id_bytes.copy_from_slice(&id_blob);
                let id_hash = u128::from_be_bytes(id_bytes);

                let crdt_data: Vec<u8> = row.get(1)?;
                let shopping_list: ShoppingList =
                    serde_json::from_slice(&crdt_data).expect("json parsing error");

                Ok((id_hash, shopping_list))
            })?
            .collect::<Result<Vec<_>, rusqlite::Error>>()?;

        Ok(rows)
    }

    pub fn delete_shopping_list(&mut self, shopping_list_id: &Uuid) -> Result<()> {
        let tx = self.db_conn.transaction()?;
        let hash = HashRing::hash(&shopping_list_id.to_string()).to_be_bytes();

        tx.execute(
            "DELETE FROM shopping_lists WHERE id = ?1",
            params![hash.as_slice()],
        )?;

        tx.commit()?;
        Ok(())
    }
}
