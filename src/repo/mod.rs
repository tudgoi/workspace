use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::repo::mst::MstNode;

mod mst;
pub mod sqlitebe;

#[cfg(test)]
pub mod test_backend;

#[cfg(test)]
mod tests;

const ROOT_REF: &str = "root";

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct Hash(pub [u8; 32]);

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
    fn read(&self, hash: &Hash) -> Result<Vec<u8>, RepoError>;
    fn write(&self, hash: &Hash, blob: &[u8]) -> Result<(), RepoError>;
    fn set_ref(&self, name: &str, hash: &Hash) -> Result<(), RepoError>;
    fn get_ref(&self, name: &str) -> Result<Option<Hash>, RepoError>;
}

pub struct Repo<B: Backend> {
    backend: B,
}

impl<B: Backend> Repo<B> {
    pub fn new(backend: B) -> Self {
        Self {
            backend,
        }
    }
    
    pub fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, RepoError> {
        let root_hash = self.backend.get_ref(ROOT_REF)?;
        match root_hash {
            Some(h) => {
                let root_node = self.read_node(&h)?;
                root_node.get(self, key)
            }
            None => Ok(None),
        }
    }
    
    pub fn write(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), RepoError> {
        let root_hash = self.backend.get_ref(ROOT_REF)?;
        let mut root_node = match root_hash {
            Some(h) => self.read_node(&h)?,
            None => MstNode::empty(),
        };

        let new_root_hash = root_node.upsert(self, key, value)?;
        self.backend.set_ref(ROOT_REF, &new_root_hash)?;
        Ok(())
    }
}

pub trait Store {
    fn write_node(&mut self, node: &MstNode) -> Result<Hash, RepoError>;
    fn read_node(&self, hash: &Hash) -> Result<MstNode, RepoError>;
}

impl<B: Backend> Store for Repo<B> {
    fn write_node(&mut self, node: &MstNode) -> Result<Hash, RepoError> {
        let json = serde_json::to_vec(node)?;
        let hasher = blake3::hash(&json);
        let hash = Hash(*hasher.as_bytes());

        self.backend.write(&hash, &json)?;

        Ok(hash)
    }

    fn read_node(&self, hash: &Hash) -> Result<MstNode, RepoError> {
        let json = self.backend.read(hash)?;
        let node = serde_json::from_slice(&json)?;
        Ok(node)
    }
}
