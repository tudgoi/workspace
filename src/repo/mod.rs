use std::error::Error;
use std::fmt::Display;

pub use crate::repo::mst::PrefixIterator;
use crate::repo::{
    backend::{Backend, KeyType},
    mst::MstNode,
};
use iroh::SecretKey;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub mod backend;
mod mst;
pub mod sync;

#[cfg(test)]
pub mod test_backend;

#[cfg(test)]
mod tests;

const IROH_SECRET: &str = "iroh";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepoRefType {
    Working,
    Committed,
}

impl RepoRefType {
    fn as_str(&self) -> &'static str {
        match self {
            RepoRefType::Working => "working",
            RepoRefType::Committed => "committed",
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Diff {
    Added(Vec<u8>, Vec<u8>),
    Changed(Vec<u8>, Vec<u8>, Vec<u8>), // key, old_value, new_value
    Removed(Vec<u8>, Vec<u8>),
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Hash, Debug, PartialOrd, Ord)]
pub struct Hash(pub [u8; 32]);

impl Hash {
    pub fn from_hex(hex: &str) -> Result<Self, String> {
        if hex.len() != 64 {
            return Err("Invalid hex length".to_string());
        }
        let mut bytes = [0u8; 32];
        for i in 0..32 {
            let byte_str = &hex[i * 2..i * 2 + 2];
            bytes[i] = u8::from_str_radix(byte_str, 16)
                .map_err(|e| format!("Invalid hex character: {}", e))?;
        }
        Ok(Hash(bytes))
    }

    pub fn to_hex(&self) -> String {
        let mut hex = String::with_capacity(64);
        for byte in self.0.iter() {
            use std::fmt::Write;
            write!(hex, "{:02x}", byte).unwrap();
        }
        hex
    }
}

impl Display for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0.iter().take(4) {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}

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
    Backend(#[from] Box<dyn Error + Send + Sync>),
    #[error("`{0}` ref not found")]
    RefNotFound(String),
    #[error("hash parsing error: {0}")]
    HashParse(String),
}

impl RepoError {
    pub fn backend<E: Error + Send + Sync + 'static>(e: E) -> Self {
        RepoError::Backend(Box::new(e))
    }
}

pub trait ToRepoError {
    fn to_repo_error(self) -> RepoError;
}

pub struct RepoStats {
    pub key_value_count: usize,
    pub total_value_size: usize,
    pub value_size_distribution: std::collections::BTreeMap<usize, usize>,
    pub node_count: usize,
    pub total_node_size: usize,
    pub node_size_distribution: std::collections::BTreeMap<usize, usize>,
}

pub struct Repo<B: Backend> {
    pub backend: B,
}

impl<B: Backend> Repo<B>
where
    B::Error: ToRepoError,
{
    pub fn new(backend: B) -> Self {
        Self { backend }
    }

    pub fn init(&self) -> Result<(), RepoError> {
        let empty_node = MstNode::empty();
        let hash = self.write_node(&empty_node)?;
        self.backend
            .set(
                KeyType::Ref,
                RepoRefType::Working.as_str().as_bytes(),
                &hash.0,
            )
            .map_err(|e| e.to_repo_error())?;
        self.backend
            .set(
                KeyType::Ref,
                RepoRefType::Committed.as_str().as_bytes(),
                &hash.0,
            )
            .map_err(|e| e.to_repo_error())?;

        let iroh_secret = SecretKey::generate(&mut rand::rng());
        self.backend
            .set(
                KeyType::Secret,
                IROH_SECRET.as_bytes(),
                iroh_secret.to_bytes().as_slice(),
            )
            .map_err(|e| e.to_repo_error())?;
        Ok(())
    }

    pub fn get_ref(&self, ref_type: RepoRefType) -> Result<RepoRef<'_, B>, RepoError> {
        let ref_name = ref_type.as_str();
        let hash_bytes = self
            .backend
            .get(KeyType::Ref, ref_name.as_bytes())
            .map_err(|e| e.to_repo_error())?
            .ok_or_else(|| RepoError::RefNotFound(ref_name.to_string()))?;
        let hash = Hash(
            hash_bytes
                .try_into()
                .map_err(|_| RepoError::HashParse("Invalid hash length in ref".to_string()))?,
        );
        Ok(RepoRef {
            repo: self,
            hash,
            name: ref_name.to_string(),
        })
    }

    pub fn commit(&mut self) -> Result<(), RepoError> {
        let root_hash_bytes = self
            .backend
            .get(KeyType::Ref, RepoRefType::Working.as_str().as_bytes())
            .map_err(|e| e.to_repo_error())?;
        if let Some(h_bytes) = root_hash_bytes {
            self.backend
                .set(
                    KeyType::Ref,
                    RepoRefType::Committed.as_str().as_bytes(),
                    &h_bytes,
                )
                .map_err(|e| e.to_repo_error())?;
        }
        Ok(())
    }

    pub fn abandon(&mut self) -> Result<(), RepoError> {
        let root_hash_bytes = self
            .backend
            .get(KeyType::Ref, RepoRefType::Committed.as_str().as_bytes())
            .map_err(|e| e.to_repo_error())?;
        if let Some(h_bytes) = root_hash_bytes {
            self.backend
                .set(
                    KeyType::Ref,
                    RepoRefType::Working.as_str().as_bytes(),
                    &h_bytes,
                )
                .map_err(|e| e.to_repo_error())?;
        }
        Ok(())
    }

    pub fn stats(&self) -> Result<RepoStats, RepoError> {
        let root_hash_bytes = self
            .backend
            .get(KeyType::Ref, RepoRefType::Working.as_str().as_bytes())
            .map_err(|e| e.to_repo_error())?;
        let mut kv_count = 0;
        let mut total_value_size = 0;
        let mut value_sizes = std::collections::BTreeMap::new();

        if let Some(h_bytes) = root_hash_bytes {
            let h = Hash(
                h_bytes
                    .try_into()
                    .map_err(|_| RepoError::HashParse("Invalid hash length".to_string()))?,
            );
            self.traverse_stats(&h, &mut kv_count, &mut total_value_size, &mut value_sizes)?;
        }

        let (node_count, node_sizes) = self
            .backend
            .stats(KeyType::Node)
            .map_err(|e| e.to_repo_error())?;
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
        let ref_names = self
            .backend
            .list(KeyType::Ref)
            .map_err(|e| e.to_repo_error())?;

        for name in ref_names {
            if let Some(hash_bytes) = self
                .backend
                .get(KeyType::Ref, &name)
                .map_err(|e| e.to_repo_error())?
            {
                let hash = Hash(
                    hash_bytes
                        .try_into()
                        .map_err(|_| RepoError::HashParse("Invalid hash length".to_string()))?,
                );
                self.traverse_reachable(&hash, &mut reachable)?;
            }
        }

        let all_hashes = self
            .backend
            .list(KeyType::Node)
            .map_err(|e| e.to_repo_error())?;
        let mut to_delete: Vec<Vec<u8>> = Vec::new();

        for hash_bytes in all_hashes {
            let h = Hash(
                hash_bytes
                    .clone()
                    .try_into()
                    .map_err(|_| RepoError::HashParse("Invalid hash length".to_string()))?,
            );
            if !reachable.contains(&h) {
                to_delete.push(hash_bytes);
            }
        }

        let deleted = if to_delete.is_empty() {
            0
        } else {
            let refs: Vec<&[u8]> = to_delete.iter().map(|s| s.as_slice()).collect();
            self.backend
                .delete(KeyType::Node, &refs)
                .map_err(|e| e.to_repo_error())?
        };

        self.backend.vacuum().map_err(|e| e.to_repo_error())?;

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

// TODO Split into ReadOnly and ReadWrite refs. Only working ref needs to be
// ReadWrite. committed ref should be ReadOnly. Or does &mut elf already
// differentiate the behavior?
pub struct RepoRef<'a, B: Backend> {
    pub repo: &'a Repo<B>,
    pub hash: Hash,
    pub name: String,
}

impl<'a, B: Backend> RepoRef<'a, B>
where
    B::Error: ToRepoError,
{
    pub fn iterate_diff(&self, other: &RepoRef<'a, B>) -> Result<DiffIterator<'a, B>, RepoError> {
        Ok(DiffIterator::new(
            self.repo,
            Some(self.hash.clone()),
            Some(other.hash.clone()),
        ))
    }

    pub fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, RepoError> {
        let root_node = self.repo.read_node(&self.hash)?;
        root_node.get(self.repo, key)
    }

    pub fn iter_prefix(&self, prefix: &[u8]) -> Result<PrefixIterator<'a, Repo<B>>, RepoError> {
        let root_node = self.repo.read_node(&self.hash)?;
        Ok(PrefixIterator::new(self.repo, prefix, Some(root_node)))
    }

    pub fn write(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), RepoError> {
        let mut root_node = self.repo.read_node(&self.hash)?;

        let new_root_hash = root_node.upsert(self.repo, key, value)?;
        self.repo
            .backend
            .set(KeyType::Ref, self.name.as_bytes(), &new_root_hash.0)
            .map_err(|e| e.to_repo_error())?;
        self.hash = new_root_hash;
        Ok(())
    }

    pub fn remove(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, RepoError> {
        let mut root_node = self.repo.read_node(&self.hash)?;

        let (new_root_hash, value) = root_node.remove(self.repo, key)?;
        self.repo
            .backend
            .set(KeyType::Ref, self.name.as_bytes(), &new_root_hash.0)
            .map_err(|e| e.to_repo_error())?;
        self.hash = new_root_hash;
        Ok(value)
    }

    pub fn commit_id(&self) -> Result<Hash, RepoError> {
        Ok(self.hash.clone())
    }
}

pub trait Store {
    fn write_node(&self, node: &MstNode) -> Result<Hash, RepoError>;
    fn read_node(&self, hash: &Hash) -> Result<MstNode, RepoError>;
}

impl<B: Backend> Store for Repo<B>
where
    B::Error: ToRepoError,
{
    fn write_node(&self, node: &MstNode) -> Result<Hash, RepoError> {
        let bytes = postcard::to_stdvec(node)?;
        let compressed = lz4_flex::compress_prepend_size(&bytes);
        let hasher = blake3::hash(&compressed);
        let hash = Hash(*hasher.as_bytes());

        self.backend
            .set(KeyType::Node, &hash.0, &compressed)
            .map_err(|e| e.to_repo_error())?;

        Ok(hash)
    }

    fn read_node(&self, hash: &Hash) -> Result<MstNode, RepoError> {
        let compressed = self
            .backend
            .get(KeyType::Node, &hash.0)
            .map_err(|e| e.to_repo_error())?
            .ok_or_else(|| RepoError::HashParse(format!("node not found: {}", hash)))?;
        let decompressed = lz4_flex::decompress_size_prepended(&compressed)?;
        let node = postcard::from_bytes(&decompressed)?;
        Ok(node)
    }
}

pub struct DiffIterator<'a, B: Backend> {
    repo: &'a Repo<B>,
    stack: Vec<DiffIterState>,
}

struct DiffIterState {
    old_node: Option<MstNode>,
    new_node: Option<MstNode>,
    old_node_hash: Option<Hash>,
    new_node_hash: Option<Hash>,
    old_level: u32,
    new_level: u32,
    old_idx: usize,
    new_idx: usize,
    old_child_processed: bool,
    new_child_processed: bool,
}

// TODO Why does DiffIterator need to accept Option<Hash>? Can we enforce
// that we always need to diff existing trees only?
impl<'a, B: Backend> DiffIterator<'a, B>
where
    B::Error: ToRepoError,
{
    fn new(repo: &'a Repo<B>, old_root: Option<Hash>, new_root: Option<Hash>) -> Self {
        let mut stack = Vec::new();
        if old_root != new_root {
            let old_node = old_root.as_ref().and_then(|h| repo.read_node(h).ok());
            let new_node = new_root.as_ref().and_then(|h| repo.read_node(h).ok());
            let old_level = old_node.as_ref().and_then(|n| n.estimate_level()).unwrap_or(0);
            let new_level = new_node.as_ref().and_then(|n| n.estimate_level()).unwrap_or(0);

            stack.push(DiffIterState {
                old_node,
                new_node,
                old_node_hash: old_root,
                new_node_hash: new_root,
                old_level,
                new_level,
                old_idx: 0,
                new_idx: 0,
                old_child_processed: false,
                new_child_processed: false,
            });
        }
        Self { repo, stack }
    }
}

impl<'a, B: Backend> Iterator for DiffIterator<'a, B>
where
    B::Error: ToRepoError,
{
    type Item = Result<Diff, RepoError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (push_state, pop_state, result) = {
                let state = self.stack.last_mut()?;

                if state.old_level > state.new_level {
                    // Descend old tree
                    if !state.old_child_processed {
                        state.old_child_processed = true;
                        let old_child_hash = state.old_node.as_ref().and_then(|n| n.get_child_hash(state.old_idx));
                        if let Some(h) = old_child_hash {
                            if Some(h) == state.new_node_hash.as_ref() {
                                // Subtree matches new_node! Short-circuit:
                                // Instead of saying old items are removed, we align them.
                                (
                                    Some(DiffIterState {
                                        old_node: self.repo.read_node(h).ok(),
                                        new_node: state.new_node.clone(),
                                        old_node_hash: Some(h.clone()),
                                        new_node_hash: state.new_node_hash.clone(),
                                        old_level: state.new_level,
                                        new_level: state.new_level,
                                        old_idx: 0,
                                        new_idx: 0,
                                        old_child_processed: false,
                                        new_child_processed: false,
                                    }),
                                    false,
                                    None,
                                )
                            } else {
                                let child_node = self.repo.read_node(h).ok();
                                let child_level = child_node.as_ref().and_then(|n| n.estimate_level()).unwrap_or(0);
                                (
                                    Some(DiffIterState {
                                        old_node: child_node,
                                        new_node: None,
                                        old_node_hash: Some(h.clone()),
                                        new_node_hash: None,
                                        old_level: child_level,
                                        new_level: 0,
                                        old_idx: 0,
                                        new_idx: 0,
                                        old_child_processed: false,
                                        new_child_processed: false,
                                    }),
                                    false,
                                    None,
                                )
                            }
                        } else {
                            continue;
                        }
                    } else {
                        // Process item from old tree as Removed
                        let old_item = state.old_node.as_ref().and_then(|n| n.items.get(state.old_idx));
                        if let Some(oi) = old_item {
                            let res = Some(Ok(Diff::Removed(oi.key.clone(), oi.value.clone())));
                            state.old_idx += 1;
                            state.old_child_processed = false;
                            (None, false, res)
                        } else {
                            // Done with this node
                            (None, true, None)
                        }
                    }
                } else if state.new_level > state.old_level {
                    // Descend new tree
                    if !state.new_child_processed {
                        state.new_child_processed = true;
                        let new_child_hash = state.new_node.as_ref().and_then(|n| n.get_child_hash(state.new_idx));
                        if let Some(h) = new_child_hash {
                            if Some(h) == state.old_node_hash.as_ref() {
                                // Subtree matches old_node! Short-circuit:
                                (
                                    Some(DiffIterState {
                                        old_node: state.old_node.clone(),
                                        new_node: self.repo.read_node(h).ok(),
                                        old_node_hash: state.old_node_hash.clone(),
                                        new_node_hash: Some(h.clone()),
                                        old_level: state.old_level,
                                        new_level: state.old_level,
                                        old_idx: 0,
                                        new_idx: 0,
                                        old_child_processed: false,
                                        new_child_processed: false,
                                    }),
                                    false,
                                    None,
                                )
                            } else {
                                let child_node = self.repo.read_node(h).ok();
                                let child_level = child_node.as_ref().and_then(|n| n.estimate_level()).unwrap_or(0);
                                (
                                    Some(DiffIterState {
                                        old_node: None,
                                        new_node: child_node,
                                        old_node_hash: None,
                                        new_node_hash: Some(h.clone()),
                                        old_level: 0,
                                        new_level: child_level,
                                        old_idx: 0,
                                        new_idx: 0,
                                        old_child_processed: false,
                                        new_child_processed: false,
                                    }),
                                    false,
                                    None,
                                )
                            }
                        } else {
                            continue;
                        }
                    } else {
                        // Process item from new tree as Added
                        let new_item = state.new_node.as_ref().and_then(|n| n.items.get(state.new_idx));
                        if let Some(ni) = new_item {
                            let res = Some(Ok(Diff::Added(ni.key.clone(), ni.value.clone())));
                            state.new_idx += 1;
                            state.new_child_processed = false;
                            (None, false, res)
                        } else {
                            (None, true, None)
                        }
                    }
                } else {
                    // Levels match
                    let old_item = state.old_node.as_ref().and_then(|n| n.items.get(state.old_idx));
                    let new_item = state.new_node.as_ref().and_then(|n| n.items.get(state.new_idx));

                    match (old_item, new_item) {
                        (Some(oi), Some(ni)) => match oi.key.cmp(&ni.key) {
                            std::cmp::Ordering::Equal => {
                                if !state.old_child_processed {
                                    state.old_child_processed = true;
                                    state.new_child_processed = true;
                                    let old_child_hash = state.old_node.as_ref().and_then(|n| n.get_child_hash(state.old_idx));
                                    let new_child_hash = state.new_node.as_ref().and_then(|n| n.get_child_hash(state.new_idx));
                                    if old_child_hash != new_child_hash {
                                        let old_child_node = old_child_hash.and_then(|h| self.repo.read_node(h).ok());
                                        let new_child_node = new_child_hash.and_then(|h| self.repo.read_node(h).ok());
                                        let old_child_lvl = old_child_node.as_ref().and_then(|n| n.estimate_level()).unwrap_or(0);
                                        let new_child_lvl = new_child_node.as_ref().and_then(|n| n.estimate_level()).unwrap_or(0);
                                        (
                                            Some(DiffIterState {
                                                old_node: old_child_node,
                                                new_node: new_child_node,
                                                old_node_hash: old_child_hash.cloned(),
                                                new_node_hash: new_child_hash.cloned(),
                                                old_level: old_child_lvl,
                                                new_level: new_child_lvl,
                                                old_idx: 0,
                                                new_idx: 0,
                                                old_child_processed: false,
                                                new_child_processed: false,
                                            }),
                                            false,
                                            None,
                                        )
                                    } else {
                                        let res = if oi.value != ni.value {
                                            Some(Ok(Diff::Changed(oi.key.clone(), oi.value.clone(), ni.value.clone())))
                                        } else {
                                            None
                                        };
                                        state.old_idx += 1;
                                        state.new_idx += 1;
                                        state.old_child_processed = false;
                                        state.new_child_processed = false;
                                        (None, false, res)
                                    }
                                } else {
                                    let res = if oi.value != ni.value {
                                        Some(Ok(Diff::Changed(oi.key.clone(), oi.value.clone(), ni.value.clone())))
                                    } else {
                                        None
                                    };
                                    state.old_idx += 1;
                                    state.new_idx += 1;
                                    state.old_child_processed = false;
                                    state.new_child_processed = false;
                                    (None, false, res)
                                }
                            }
                            std::cmp::Ordering::Less => {
                                if !state.old_child_processed {
                                    state.old_child_processed = true;
                                    let old_child_hash = state.old_node.as_ref().and_then(|n| n.get_child_hash(state.old_idx));
                                    if let Some(h) = old_child_hash {
                                        let child_node = self.repo.read_node(h).ok();
                                        let child_lvl = child_node.as_ref().and_then(|n| n.estimate_level()).unwrap_or(0);
                                        (
                                            Some(DiffIterState {
                                                old_node: child_node,
                                                new_node: None,
                                                old_node_hash: Some(h.clone()),
                                                new_node_hash: None,
                                                old_level: child_lvl,
                                                new_level: 0,
                                                old_idx: 0,
                                                new_idx: 0,
                                                old_child_processed: false,
                                                new_child_processed: false,
                                            }),
                                            false,
                                            None,
                                        )
                                    } else {
                                        let res = Some(Ok(Diff::Removed(oi.key.clone(), oi.value.clone())));
                                        state.old_idx += 1;
                                        state.old_child_processed = false;
                                        (None, false, res)
                                    }
                                } else {
                                    let res = Some(Ok(Diff::Removed(oi.key.clone(), oi.value.clone())));
                                    state.old_idx += 1;
                                    state.old_child_processed = false;
                                    (None, false, res)
                                }
                            }
                            std::cmp::Ordering::Greater => {
                                if !state.new_child_processed {
                                    state.new_child_processed = true;
                                    let new_child_hash = state.new_node.as_ref().and_then(|n| n.get_child_hash(state.new_idx));
                                    if let Some(h) = new_child_hash {
                                        let child_node = self.repo.read_node(h).ok();
                                        let child_lvl = child_node.as_ref().and_then(|n| n.estimate_level()).unwrap_or(0);
                                        (
                                            Some(DiffIterState {
                                                old_node: None,
                                                new_node: child_node,
                                                old_node_hash: None,
                                                new_node_hash: Some(h.clone()),
                                                old_level: 0,
                                                new_level: child_lvl,
                                                old_idx: 0,
                                                new_idx: 0,
                                                old_child_processed: false,
                                                new_child_processed: false,
                                            }),
                                            false,
                                            None,
                                        )
                                    } else {
                                        let res = Some(Ok(Diff::Added(ni.key.clone(), ni.value.clone())));
                                        state.new_idx += 1;
                                        state.new_child_processed = false;
                                        (None, false, res)
                                    }
                                } else {
                                    let res = Some(Ok(Diff::Added(ni.key.clone(), ni.value.clone())));
                                    state.new_idx += 1;
                                    state.new_child_processed = false;
                                    (None, false, res)
                                }
                            }
                        }
                        (Some(oi), None) => {
                            if !state.old_child_processed {
                                state.old_child_processed = true;
                                let old_child_hash = state.old_node.as_ref().and_then(|n| n.get_child_hash(state.old_idx));
                                if let Some(h) = old_child_hash {
                                    let child_node = self.repo.read_node(h).ok();
                                    let child_lvl = child_node.as_ref().and_then(|n| n.estimate_level()).unwrap_or(0);
                                    (
                                        Some(DiffIterState {
                                            old_node: child_node,
                                            new_node: None,
                                            old_node_hash: Some(h.clone()),
                                            new_node_hash: None,
                                            old_level: child_lvl,
                                            new_level: 0,
                                            old_idx: 0,
                                            new_idx: 0,
                                            old_child_processed: false,
                                            new_child_processed: false,
                                        }),
                                        false,
                                        None,
                                    )
                                } else {
                                    let res = Some(Ok(Diff::Removed(oi.key.clone(), oi.value.clone())));
                                    state.old_idx += 1;
                                    state.old_child_processed = false;
                                    (None, false, res)
                                }
                            } else {
                                let res = Some(Ok(Diff::Removed(oi.key.clone(), oi.value.clone())));
                                state.old_idx += 1;
                                state.old_child_processed = false;
                                (None, false, res)
                            }
                        }
                        (None, Some(ni)) => {
                            if !state.new_child_processed {
                                state.new_child_processed = true;
                                let new_child_hash = state.new_node.as_ref().and_then(|n| n.get_child_hash(state.new_idx));
                                if let Some(h) = new_child_hash {
                                    let child_node = self.repo.read_node(h).ok();
                                    let child_lvl = child_node.as_ref().and_then(|n| n.estimate_level()).unwrap_or(0);
                                    (
                                        Some(DiffIterState {
                                            old_node: None,
                                            new_node: child_node,
                                            old_node_hash: None,
                                            new_node_hash: Some(h.clone()),
                                            old_level: 0,
                                            new_level: child_lvl,
                                            old_idx: 0,
                                            new_idx: 0,
                                            old_child_processed: false,
                                            new_child_processed: false,
                                        }),
                                        false,
                                        None,
                                    )
                                } else {
                                    let res = Some(Ok(Diff::Added(ni.key.clone(), ni.value.clone())));
                                    state.new_idx += 1;
                                    state.new_child_processed = false;
                                    (None, false, res)
                                }
                            } else {
                                let res = Some(Ok(Diff::Added(ni.key.clone(), ni.value.clone())));
                                state.new_idx += 1;
                                state.new_child_processed = false;
                                (None, false, res)
                            }
                        }
                        (None, None) => {
                            if !state.old_child_processed || !state.new_child_processed {
                                let old_child_hash = state.old_node.as_ref().and_then(|n| n.get_child_hash(state.old_idx));
                                let new_child_hash = state.new_node.as_ref().and_then(|n| n.get_child_hash(state.new_idx));
                                state.old_child_processed = true;
                                state.new_child_processed = true;
                                if old_child_hash != new_child_hash {
                                    let old_child_node = old_child_hash.and_then(|h| self.repo.read_node(h).ok());
                                    let new_child_node = new_child_hash.and_then(|h| self.repo.read_node(h).ok());
                                    let old_child_lvl = old_child_node.as_ref().and_then(|n| n.estimate_level()).unwrap_or(0);
                                    let new_child_lvl = new_child_node.as_ref().and_then(|n| n.estimate_level()).unwrap_or(0);
                                    (
                                        Some(DiffIterState {
                                            old_node: old_child_node,
                                            new_node: new_child_node,
                                            old_node_hash: old_child_hash.cloned(),
                                            new_node_hash: new_child_hash.cloned(),
                                            old_level: old_child_lvl,
                                            new_level: new_child_lvl,
                                            old_idx: 0,
                                            new_idx: 0,
                                            old_child_processed: false,
                                            new_child_processed: false,
                                        }),
                                        false,
                                        None,
                                    )
                                } else {
                                    (None, true, None)
                                }
                            } else {
                                (None, true, None)
                            }
                        }
                    }
                }
            };

            if let Some(s) = push_state {
                self.stack.push(s);
            } else if pop_state {
                self.stack.pop();
            } else if result.is_some() {
                return result;
            }

            if self.stack.is_empty() {
                return None;
            }
        }
    }
}
