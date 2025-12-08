use crate::crdt::{Item, ShoppingList};
use anyhow::Result;
use rusqlite::{params, Connection, Result as sqlResult, Row, ToSql};
use std::path::PathBuf;
use thiserror::Error;
use uuid::Uuid;

pub trait Stored {
    fn from_schema() -> Self;
    fn insert(&self, conn: &Connection) -> Result<()>;
}

impl Stored for Item {
    fn from_schema() -> Self {
        todo!();
    }

    fn insert(&self, conn: &Connection) -> Result<()> {
        todo!();
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
    lists: Vec<ShoppingList>,
}

impl ClientStorage {
    //TODO: change storage directory
    fn get_db_path(&self) -> std::io::Result<PathBuf> {
        let mut path = dirs::config_dir().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "No user config directory found.",
            )
        })?;
        path.push("sdlestorage");

        std::fs::create_dir_all(&path)?;

        path.push("client.db");
        Ok(path)
    }

    fn setup_db(&self) -> sqlResult<Connection, DbError> {
        let db_path = self.get_db_path()?;

        let conn = Connection::open(db_path)?;

        self.initialize_schema(&conn)?;

        Ok(conn)
    }

    fn initialize_schema(&self, conn: &Connection) -> Result<(), DbError> {
        conn.execute_batch(
            r#"
                CREATE TABLE gcounter (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner_actor TEXT NOT NULL
                );
            "#,
        )?;
        conn.execute_batch(
            r#"
                CREATE TABLE gcounter_actor_values (
                    gcounter_id INTEGER NOT NULL,
                    actor_id TEXT NOT NULL,
                    value INTEGER NOT NULL,
                    PRIMARY KEY (gcounter_id, actor_id),
                    FOREIGN KEY (gcounter_id) REFERENCES gcounter(id)
                );
            "#,
        )?;
        conn.execute_batch(
            r#"
                CREATE TABLE awormap_items (
                    item_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    shopping_list_id TEXT NOT NULL,
                    item_name TEXT NOT NULL,
                    acquired_val INTEGER NOT NULL,
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
}
