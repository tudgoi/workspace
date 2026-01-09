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

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Hash, Debug)]
pub struct Hash(pub [u8; 32]);

#[derive(Error, Debug)]
pub enum RepoError {
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("postcard error: {0}")]
    Postcard(#[from] postcard::Error),
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("lz4 error: {0}")]
    Lz4(#[from] lz4_flex::block::DecompressError),
    #[error("backend error: {0}")]
    Backend(String),
}

pub struct RepoStats {
    pub key_value_count: usize,
    pub total_value_size: usize,
    pub value_size_distribution: std::collections::BTreeMap<usize, usize>,
    pub node_count: usize,
    pub total_node_size: usize,
    pub node_size_distribution: std::collections::BTreeMap<usize, usize>,
}

pub trait Backend {
    fn read(&self, hash: &Hash) -> Result<Vec<u8>, RepoError>;
    fn write(&self, hash: &Hash, blob: &[u8]) -> Result<(), RepoError>;
    fn set_ref(&self, name: &str, hash: &Hash) -> Result<(), RepoError>;
    fn get_ref(&self, name: &str) -> Result<Option<Hash>, RepoError>;
    fn list_refs(&self) -> Result<Vec<(String, Hash)>, RepoError>;
    fn delete_nodes(&self, hashes: &[Hash]) -> Result<usize, RepoError>;
    fn list_all_node_hashes(&self) -> Result<Vec<Hash>, RepoError>;
    fn vacuum(&self) -> Result<(), RepoError>;
    fn stats(&self) -> Result<(usize, std::collections::BTreeMap<usize, usize>), RepoError>;
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

    pub fn stats(&self) -> Result<RepoStats, RepoError> {
        let root_hash = self.backend.get_ref(ROOT_REF)?;
        let mut kv_count = 0;
        let mut total_value_size = 0;
        let mut value_sizes = std::collections::BTreeMap::new();

        if let Some(h) = root_hash {
            self.traverse_stats(&h, &mut kv_count, &mut total_value_size, &mut value_sizes)?;
        }

        let (node_count, node_sizes) = self.backend.stats()?;
        let total_node_size = node_sizes.iter().map(|(s, c)| s * c).sum();

        Ok(RepoStats {
            key_value_count: kv_count,
            total_value_size,
            value_size_distribution: value_sizes,
            node_count,
            total_node_size,
            node_size_distribution: node_sizes,
        })
    }

    pub fn gc(&self) -> Result<usize, RepoError> {
        let mut reachable = std::collections::HashSet::new();
        let refs = self.backend.list_refs()?;

        for (_, hash) in refs {
            self.traverse_reachable(&hash, &mut reachable)?;
        }

        let all_hashes = self.backend.list_all_node_hashes()?;
        let to_delete: Vec<Hash> = all_hashes
            .into_iter()
            .filter(|h| !reachable.contains(h))
            .collect();

        let deleted = if to_delete.is_empty() {
            0
        } else {
            self.backend.delete_nodes(&to_delete)?
        };

        self.backend.vacuum()?;

        Ok(deleted)
    }

    fn traverse_reachable(
        &self,
        hash: &Hash,
        reachable: &mut std::collections::HashSet<Hash>,
    ) -> Result<(), RepoError> {
        if reachable.contains(hash) {
            return Ok(());
        }

        reachable.insert(hash.clone());
        let node = self.read_node(hash)?;

        if let Some(ref h) = node.left {
            self.traverse_reachable(h, reachable)?;
        }

        for item in node.items {
            if let Some(ref h) = item.right {
                self.traverse_reachable(h, reachable)?;
            }
        }

        Ok(())
    }

    fn traverse_stats(
        &self,
        hash: &Hash,
        kv_count: &mut usize,
        total_value_size: &mut usize,
        value_sizes: &mut std::collections::BTreeMap<usize, usize>,
    ) -> Result<(), RepoError> {
        let node = self.read_node(hash)?;
        if let Some(ref h) = node.left {
            self.traverse_stats(h, kv_count, total_value_size, value_sizes)?;
        }

        for item in node.items {
            *kv_count += 1;
            let size = item.value.len();
            *total_value_size += size;
            *value_sizes.entry(size).or_insert(0) += 1;

            if let Some(ref h) = item.right {
                self.traverse_stats(h, kv_count, total_value_size, value_sizes)?;
            }
        }

        Ok(())
    }
}

pub trait Store {
    fn write_node(&mut self, node: &MstNode) -> Result<Hash, RepoError>;
    fn read_node(&self, hash: &Hash) -> Result<MstNode, RepoError>;
}

impl<B: Backend> Store for Repo<B> {
    fn write_node(&mut self, node: &MstNode) -> Result<Hash, RepoError> {
        let bytes = postcard::to_stdvec(node)?;
        let compressed = lz4_flex::compress_prepend_size(&bytes);
        let hasher = blake3::hash(&compressed);
        let hash = Hash(*hasher.as_bytes());

        self.backend.write(&hash, &compressed)?;

        Ok(hash)
    }

    fn read_node(&self, hash: &Hash) -> Result<MstNode, RepoError> {
        let compressed = self.backend.read(hash)?;
        let decompressed = lz4_flex::decompress_size_prepended(&compressed)?;
        let node = postcard::from_bytes(&decompressed)?;
        Ok(node)
    }
}
