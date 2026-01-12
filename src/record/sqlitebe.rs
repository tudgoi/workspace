use rusqlite::{Connection, OptionalExtension};
use thiserror::Error;

use crate::repo::{Hash, RepoError, ToRepoError};
use crate::repo::backend::{KeyType, Backend};

#[derive(Debug, Error)]
pub enum SqliteBackendError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("hash parsing error: {0}")]
    HashParse(String),
}

impl ToRepoError for SqliteBackendError {
    fn to_repo_error(self) -> RepoError {
        RepoError::Backend(Box::new(self))
    }
}

pub struct SqliteBackend<'a> {
    pub conn: &'a Connection,
}

impl<'a> SqliteBackend<'a> {
    pub fn new(conn: &'a Connection) -> Self {
        Self { conn }
    }
}

impl<'a> Backend for SqliteBackend<'a> {
    type Error = SqliteBackendError;

    fn get(&self, key_type: KeyType, key: &str) -> Result<Option<Vec<u8>>, Self::Error> {
        match key_type {
            KeyType::Node => {
                let hash = Hash::from_hex(key).map_err(SqliteBackendError::HashParse)?;
                self.conn
                    .query_row("SELECT blob FROM repo WHERE hash = ?1", [hash.0], |row| {
                        row.get(0)
                    })
                    .optional()
                    .map_err(SqliteBackendError::from)
            }
            KeyType::Ref => {
                self.conn
                    .query_row("SELECT hash FROM refs WHERE name = ?1", [key], |row| {
                        row.get(0)
                    })
                    .optional()
                    .map_err(SqliteBackendError::from)
            }
            KeyType::Secret => {
                self.conn
                    .query_row("SELECT value FROM secrets WHERE name = ?1", [key], |row| {
                        row.get(0)
                    })
                    .optional()
                    .map_err(SqliteBackendError::from)
            }
        }
    }

    fn set(&self, key_type: KeyType, key: &str, value: &[u8]) -> Result<(), Self::Error> {
        match key_type {
            KeyType::Node => {
                let hash = Hash::from_hex(key).map_err(SqliteBackendError::HashParse)?;
                self.conn.execute(
                    "INSERT OR IGNORE INTO repo (hash, blob) VALUES (?1, ?2)",
                    (hash.0, value),
                )?;
                Ok(())
            }
            KeyType::Ref => {
                self.conn.execute(
                    "INSERT OR REPLACE INTO refs (name, hash) VALUES (?1, ?2)",
                    (key, value),
                )?;
                Ok(())
            }
            KeyType::Secret => {
                self.conn.execute(
                    "INSERT OR REPLACE INTO secrets (name, value) VALUES (?1, ?2)",
                    (key, value),
                )?;
                Ok(())
            }
        }
    }

    fn list(&self, key_type: KeyType) -> Result<Vec<String>, Self::Error> {
        match key_type {
            KeyType::Node => {
                let mut stmt = self.conn.prepare("SELECT hash FROM repo")?;
                let rows = stmt.query_map([], |row| {
                    let hash_bytes: Vec<u8> = row.get(0)?;
                    let bytes: [u8; 32] = hash_bytes
                        .try_into()
                        .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(0, 32))?;
                    Ok(Hash(bytes).to_string())
                })?;

                let mut hashes = Vec::new();
                for h in rows {
                    hashes.push(h?);
                }
                Ok(hashes)
            }
            KeyType::Ref => {
                let mut stmt = self.conn.prepare("SELECT name FROM refs")?;
                let rows = stmt.query_map([], |row| row.get(0))?;

                let mut refs = Vec::new();
                for r in rows {
                    refs.push(r?);
                }
                Ok(refs)
            }
            KeyType::Secret => {
                let mut stmt = self.conn.prepare("SELECT name FROM secrets")?;
                let rows = stmt.query_map([], |row| row.get(0))?;

                let mut secrets = Vec::new();
                for s in rows {
                    secrets.push(s?);
                }
                Ok(secrets)
            }
        }
    }

    fn delete(&self, key_type: KeyType, keys: &[&str]) -> Result<usize, Self::Error> {
        match key_type {
            KeyType::Node => {
                if keys.is_empty() {
                    return Ok(0);
                }
                let tx = self.conn.unchecked_transaction()?;
                let mut deleted = 0;
                {
                    let mut stmt = tx.prepare("DELETE FROM repo WHERE hash = ?1")?;
                    for key in keys {
                        let hash = Hash::from_hex(key).map_err(SqliteBackendError::HashParse)?;
                        deleted += stmt.execute([hash.0])?;
                    }
                }
                tx.commit()?;
                Ok(deleted)
            }
            KeyType::Ref => {
                if keys.is_empty() {
                    return Ok(0);
                }
                let tx = self.conn.unchecked_transaction()?;
                let mut deleted = 0;
                {
                    let mut stmt = tx.prepare("DELETE FROM refs WHERE name = ?1")?;
                    for key in keys {
                        deleted += stmt.execute([key])?;
                    }
                }
                tx.commit()?;
                Ok(deleted)
            }
            KeyType::Secret => {
                if keys.is_empty() {
                    return Ok(0);
                }
                let tx = self.conn.unchecked_transaction()?;
                let mut deleted = 0;
                {
                    let mut stmt = tx.prepare("DELETE FROM secrets WHERE name = ?1")?;
                    for key in keys {
                        deleted += stmt.execute([key])?;
                    }
                }
                tx.commit()?;
                Ok(deleted)
            }
        }
    }

    fn vacuum(&self) -> Result<(), Self::Error> {
        self.conn.execute("VACUUM", [])?;
        Ok(())
    }

    fn stats(
        &self,
        key_type: KeyType,
    ) -> Result<(usize, std::collections::BTreeMap<usize, usize>), Self::Error> {
        if key_type == KeyType::Node {
            let mut stmt = self.conn.prepare("SELECT length(blob) as size FROM repo")?;
            let rows = stmt.query_map([], |row| row.get::<_, usize>(0))?;

            let mut count = 0;
            let mut distribution = std::collections::BTreeMap::new();

            for size in rows {
                let size = size?;
                count += 1;
                *distribution.entry(size).or_insert(0) += 1;
            }

            Ok((count, distribution))
        } else {
            Ok((0, std::collections::BTreeMap::new()))
        }
    }
}
