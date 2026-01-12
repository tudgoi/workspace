use std::fmt::Display;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::repo::mst::MstNode;
pub use crate::repo::mst::PrefixIterator;

mod mst;

#[cfg(test)]
pub mod test_backend;

#[cfg(test)]
mod tests;

const WORKING_REF: &str = "working";
const COMMITTED_REF: &str = "committed";

#[derive(Debug, PartialEq, Eq)]
pub enum Diff {
    Added(Vec<u8>, Vec<u8>),
    Changed(Vec<u8>, Vec<u8>, Vec<u8>), // key, old_value, new_value
    Removed(Vec<u8>, Vec<u8>),
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Hash, Debug)]
pub struct Hash(pub [u8; 32]);

impl Display for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
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
    Backend(String),
    #[error("`{0}` ref not found")]
    RefNotFound(String),
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
    pub backend: B,
}

impl<B: Backend> Repo<B> {
    pub fn new(backend: B) -> Self {
        Self { backend }
    }

    pub fn working(&self) -> Result<RepoRef<'_, B>, RepoError> {
        let hash = self.backend.get_ref(WORKING_REF)?;
        Ok(RepoRef {
            repo: self,
            hash,
            name: Some(WORKING_REF),
        })
    }

    pub fn committed(&self) -> Result<RepoRef<'_, B>, RepoError> {
        let hash = self.backend.get_ref(COMMITTED_REF)?;
        Ok(RepoRef {
            repo: self,
            hash,
            name: Some(COMMITTED_REF),
        })
    }

    pub fn commit(&mut self) -> Result<(), RepoError> {
        let root_hash = self.backend.get_ref(WORKING_REF)?;
        if let Some(h) = root_hash {
            self.backend.set_ref(COMMITTED_REF, &h)?;
        }
        Ok(())
    }

    pub fn stats(&self) -> Result<RepoStats, RepoError> {
        let root_hash = self.backend.get_ref(WORKING_REF)?;
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

pub struct RepoRef<'a, B: Backend> {
    pub repo: &'a Repo<B>,
    pub hash: Option<Hash>,
    pub name: Option<&'static str>,
}

impl<'a, B: Backend> RepoRef<'a, B> {
    pub fn iterate_diff(&self, other: &RepoRef<'a, B>) -> Result<DiffIterator<'a, B>, RepoError> {
        Ok(DiffIterator::new(
            self.repo,
            self.hash.clone(),
            other.hash.clone(),
        ))
    }

    pub fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, RepoError> {
        match self.hash {
            Some(ref h) => {
                let root_node = self.repo.read_node(h)?;
                root_node.get(self.repo, key)
            }
            None => Ok(None),
        }
    }

    pub fn iter_prefix(&self, prefix: &[u8]) -> Result<PrefixIterator<'a, Repo<B>>, RepoError> {
        let root_node = match self.hash {
            Some(ref h) => Some(self.repo.read_node(h)?),
            None => None,
        };
        Ok(PrefixIterator::new(self.repo, prefix, root_node))
    }

    pub fn write(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), RepoError> {
        let mut root_node = match self.hash {
            Some(ref h) => self.repo.read_node(h)?,
            None => MstNode::empty(),
        };

        let new_root_hash = root_node.upsert(self.repo, key, value)?;
        if let Some(name) = self.name {
            self.repo.backend.set_ref(name, &new_root_hash)?;
        }
        self.hash = Some(new_root_hash);
        Ok(())
    }

    pub fn commit_id(&self) -> Result<String, RepoError> {
        self.hash
            .as_ref()
            .ok_or(RepoError::RefNotFound(
                self.name.unwrap_or_default().to_string(),
            ))
            .map(|h| h.to_string())
    }
}

pub trait Store {
    fn write_node(&self, node: &MstNode) -> Result<Hash, RepoError>;
    fn read_node(&self, hash: &Hash) -> Result<MstNode, RepoError>;
}

impl<B: Backend> Store for Repo<B> {
    fn write_node(&self, node: &MstNode) -> Result<Hash, RepoError> {
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

pub struct DiffIterator<'a, B: Backend> {
    repo: &'a Repo<B>,
    stack: Vec<DiffIterState>,
}

struct DiffIterState {
    old_node: Option<MstNode>,
    new_node: Option<MstNode>,
    old_idx: usize,
    new_idx: usize,
    old_child_processed: bool,
    new_child_processed: bool,
}

impl<'a, B: Backend> DiffIterator<'a, B> {
    fn new(repo: &'a Repo<B>, old_root: Option<Hash>, new_root: Option<Hash>) -> Self {
        let mut stack = Vec::new();
        if old_root != new_root {
            stack.push(DiffIterState {
                old_node: old_root.and_then(|h| repo.read_node(&h).ok()),
                new_node: new_root.and_then(|h| repo.read_node(&h).ok()),
                old_idx: 0,
                new_idx: 0,
                old_child_processed: false,
                new_child_processed: false,
            });
        }
        Self { repo, stack }
    }
}

impl<'a, B: Backend> Iterator for DiffIterator<'a, B> {
    type Item = Result<Diff, RepoError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (push_state, pop_state, result) = {
                let state = self.stack.last_mut()?;

                let old_item = state
                    .old_node
                    .as_ref()
                    .and_then(|n| n.items.get(state.old_idx));
                let new_item = state
                    .new_node
                    .as_ref()
                    .and_then(|n| n.items.get(state.new_idx));

                match (old_item, new_item) {
                    (Some(oi), Some(ni)) => match oi.key.cmp(&ni.key) {
                        std::cmp::Ordering::Equal => {
                            if !state.old_child_processed {
                                state.old_child_processed = true;
                                state.new_child_processed = true;
                                let old_child_hash = state
                                    .old_node
                                    .as_ref()
                                    .and_then(|n| n.get_child_hash(state.old_idx));
                                let new_child_hash = state
                                    .new_node
                                    .as_ref()
                                    .and_then(|n| n.get_child_hash(state.new_idx));
                                if old_child_hash != new_child_hash {
                                    (
                                        Some(DiffIterState {
                                            old_node: old_child_hash
                                                .and_then(|h| self.repo.read_node(h).ok()),
                                            new_node: new_child_hash
                                                .and_then(|h| self.repo.read_node(h).ok()),
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
                                        Some(Ok(Diff::Changed(
                                            oi.key.clone(),
                                            oi.value.clone(),
                                            ni.value.clone(),
                                        )))
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
                                    Some(Ok(Diff::Changed(
                                        oi.key.clone(),
                                        oi.value.clone(),
                                        ni.value.clone(),
                                    )))
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
                                let old_child_hash = state
                                    .old_node
                                    .as_ref()
                                    .and_then(|n| n.get_child_hash(state.old_idx));
                                if old_child_hash.is_some() {
                                    (
                                        Some(DiffIterState {
                                            old_node: old_child_hash
                                                .and_then(|h| self.repo.read_node(h).ok()),
                                            new_node: None,
                                            old_idx: 0,
                                            new_idx: 0,
                                            old_child_processed: false,
                                            new_child_processed: false,
                                        }),
                                        false,
                                        None,
                                    )
                                } else {
                                    let res =
                                        Some(Ok(Diff::Removed(oi.key.clone(), oi.value.clone())));
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
                                let new_child_hash = state
                                    .new_node
                                    .as_ref()
                                    .and_then(|n| n.get_child_hash(state.new_idx));
                                if new_child_hash.is_some() {
                                    (
                                        Some(DiffIterState {
                                            old_node: None,
                                            new_node: new_child_hash
                                                .and_then(|h| self.repo.read_node(h).ok()),
                                            old_idx: 0,
                                            new_idx: 0,
                                            old_child_processed: false,
                                            new_child_processed: false,
                                        }),
                                        false,
                                        None,
                                    )
                                } else {
                                    let res =
                                        Some(Ok(Diff::Added(ni.key.clone(), ni.value.clone())));
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
                    },
                    (Some(oi), None) => {
                        if !state.old_child_processed {
                            state.old_child_processed = true;
                            let old_child_hash = state
                                .old_node
                                .as_ref()
                                .and_then(|n| n.get_child_hash(state.old_idx));
                            if old_child_hash.is_some() {
                                (
                                    Some(DiffIterState {
                                        old_node: old_child_hash
                                            .and_then(|h| self.repo.read_node(h).ok()),
                                        new_node: None,
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
                            let new_child_hash = state
                                .new_node
                                .as_ref()
                                .and_then(|n| n.get_child_hash(state.new_idx));
                            if new_child_hash.is_some() {
                                (
                                    Some(DiffIterState {
                                        old_node: None,
                                        new_node: new_child_hash
                                            .and_then(|h| self.repo.read_node(h).ok()),
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
                            let old_child_hash = state
                                .old_node
                                .as_ref()
                                .and_then(|n| n.get_child_hash(state.old_idx));
                            let new_child_hash = state
                                .new_node
                                .as_ref()
                                .and_then(|n| n.get_child_hash(state.new_idx));
                            state.old_child_processed = true;
                            state.new_child_processed = true;
                            if old_child_hash != new_child_hash {
                                (
                                    Some(DiffIterState {
                                        old_node: old_child_hash
                                            .and_then(|h| self.repo.read_node(h).ok()),
                                        new_node: new_child_hash
                                            .and_then(|h| self.repo.read_node(h).ok()),
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
            };

            if let Some(s) = push_state {
                self.stack.push(s);
            } else if pop_state {
                self.stack.pop();
            } else if result.is_some() {
                return result;
            }
        }
    }
}
