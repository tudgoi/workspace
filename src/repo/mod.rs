use rusqlite::Connection;
use rusqlite::OptionalExtension;
use std::marker::PhantomData;
use thiserror::Error;
use serde::{Deserialize, Serialize};

mod mst;

#[derive(Error, Debug)]
pub enum RepoError {
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("backend error: {0}")]
    Backend(String),
}

pub trait Backend {
    fn read(&self, hash: &[u8; 32]) -> Result<Vec<u8>, RepoError>;
    fn write(&self, hash: &[u8; 32], blob: &Vec<u8>) -> Result<(), RepoError>;
}

pub struct SqliteBackend<'a> {
    conn: &'a Connection,
}

impl<'a> SqliteBackend<'a> {
    pub fn new(conn: &'a Connection) -> Self {
        Self { conn }
    }
}

impl<'a> Backend for SqliteBackend<'a> {
    fn read(&self, hash: &[u8; 32]) -> Result<Vec<u8>, RepoError> {
        self.conn
            .query_row("SELECT blob FROM repo WHERE hash = ?1", [hash], |row| {
                row.get(0)
            })
            .map_err(RepoError::from)
    }

    fn write(&self, hash: &[u8; 32], blob: &Vec<u8>) -> Result<(), RepoError> {
        self.conn.execute(
            "INSERT OR IGNORE INTO repo (hash, blob) VALUES (?1, ?2)",
            (hash, blob),
        )?;
        Ok(())
    }
}

pub struct Repo<K, V, B> {
    backend: B,
    _k: PhantomData<K>,
    _v: PhantomData<V>,
}

impl<K, V, B: Backend> Repo<K, V, B> {
    pub fn new(backend: B) -> Self {
        Self {
            backend,
            _k: PhantomData,
            _v: PhantomData,
        }
    }
}

impl<'a, K, V> Repo<K, V, SqliteBackend<'a>> {
    pub fn get_root(&self, name: &str) -> Result<Option<Vec<u8>>, RepoError> {
        self.backend.conn
            .query_row("SELECT hash FROM refs WHERE name = ?1", [name], |row| {
                row.get(0)
            })
            .optional()
            .map_err(RepoError::from)
    }

    pub fn set_root(&self, name: &str, hash: &[u8]) -> Result<(), RepoError> {
        self.backend.conn.execute(
            "INSERT OR REPLACE INTO refs (name, hash) VALUES (?1, ?2)",
            (name, hash),
        )?;
        Ok(())
    }
}

impl<K, V, B> Repo<K, V, B>
where
    B: Backend,
    K: mst::Key + Serialize + for<'de> Deserialize<'de> + Clone,
    V: Serialize + for<'de> Deserialize<'de> + Clone,
{
    pub fn write_node(&mut self, node: &mst::MstNode<K, V>) -> Result<[u8; 32], RepoError> {
        let json = serde_json::to_vec(node)?;
        let hash = blake3::hash(&json);
        let hash_bytes = *hash.as_bytes();

        self.backend.write(&hash_bytes, &json)?;

        Ok(hash_bytes)
    }

    pub fn read_node(&self, hash: &[u8; 32]) -> Result<mst::MstNode<K, V>, RepoError> {
        let json = self.backend.read(hash)?;
        let node = serde_json::from_slice(&json)?;
        Ok(node)
    }
}