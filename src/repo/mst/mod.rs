use crate::repo::Store;
use serde::{Deserialize, Serialize};

use super::{Hash, RepoError};

/// Calculates the level of a key based on its hash.
/// The level is the number of leading zero nibbles in the BLAKE3 hash.
pub fn key_level(key: &[u8]) -> u32 {
    let hash = blake3::hash(key);
    let mut level = 0;

    for &byte in hash.as_bytes() {
        let lz = byte.leading_zeros();
        level += lz / 4;
        if lz < 8 {
            break;
        }
    }
    level
}

/// Stores a (K, V) pair and optionally hash of a subtree with keys greater
/// than current item.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct MstItem {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub right: Option<Hash>,
}

/// A node in the Merkle Search Tree.
///
/// Its purpose is to maintain a list of (K, V) pairs ordered by K.
///
/// `left` stores the left subtree with keys smaller than the keys of all items in this node.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct MstNode {
    pub left: Option<Hash>,
    pub items: Vec<MstItem>,
}

impl MstNode {
    /// Creates a new, empty MST node.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Gets the hash of the child node at the given index.
    /// Index 0 is the `left` child, index `i > 0` is the `right` child of `items[i-1]`.
    fn get_child_hash(&self, idx: usize) -> Option<&Hash> {
        if idx == 0 {
            self.left.as_ref()
        } else {
            self.items.get(idx - 1).and_then(|item| item.right.as_ref())
        }
    }

    /// Sets the hash of the child node at the given index.
    fn set_child_hash(&mut self, idx: usize, hash: Option<Hash>) {
        if idx == 0 {
            self.left = hash;
        } else if let Some(item) = self.items.get_mut(idx - 1) {
            item.right = hash;
        }
    }

    /// Inserts or updates a key-value pair in the MST rooted at this node.
    pub fn upsert<S: Store>(
        &mut self,
        store: &mut S,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<Hash, RepoError> {
        let req_level = key_level(&key);
        let node_level = self.estimate_level().unwrap_or(req_level);

        match req_level.cmp(&node_level) {
            std::cmp::Ordering::Equal => {
                self.upsert_local(store, key, value)?;
            }
            std::cmp::Ordering::Less => {
                // Find where the key should go
                let idx = self
                    .items
                    .binary_search_by(|item| item.key.as_slice().cmp(&key))
                    .unwrap_err();

                let child_hash = self.get_child_hash(idx).cloned();
                let mut child_node = match child_hash {
                    Some(h) => store.read_node(&h)?,
                    None => MstNode::empty(),
                };

                let new_child_hash = child_node.upsert(store, key, value)?;
                self.set_child_hash(idx, Some(new_child_hash));
            }
            std::cmp::Ordering::Greater => {
                // Higher level: split current node around the new key
                let (l_hash, r_hash) = self.split(store, &key)?;
                self.items.clear();
                self.left = l_hash;
                self.items.push(MstItem {
                    key,
                    value,
                    right: r_hash,
                });
            }
        }

        store.write_node(self)
    }

    pub fn get<S: Store>(&self, store: &S, key: &[u8]) -> Result<Option<Vec<u8>>, RepoError> {
        match self
            .items
            .binary_search_by(|item| item.key.as_slice().cmp(key))
        {
            Ok(idx) => Ok(Some(self.items[idx].value.clone())),
            Err(idx) => match self.get_child_hash(idx) {
                Some(h) => {
                    let child_node = store.read_node(h)?;
                    child_node.get(store, key)
                }
                None => Ok(None),
            },
        }
    }

    /// Inserts a key-value pair directly into the current node.
    fn upsert_local<S: Store>(
        &mut self,
        store: &mut S,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), RepoError> {
        match self.items.binary_search_by(|item| item.key.cmp(&key)) {
            Ok(idx) => {
                self.items[idx].value = value;
            }
            Err(idx) => {
                // Split child at insertion point
                let child_hash = self.get_child_hash(idx).cloned();
                let (l_hash, r_hash) = Self::split_hash(store, child_hash, &key)?;

                self.set_child_hash(idx, l_hash);
                self.items.insert(
                    idx,
                    MstItem {
                        key,
                        value,
                        right: r_hash,
                    },
                );
            }
        }
        Ok(())
    }

    /// Splits the node into two nodes based on a split key.
    fn split<S: Store>(
        &mut self,
        store: &mut S,
        split_key: &[u8],
    ) -> Result<(Option<Hash>, Option<Hash>), RepoError> {
        let idx = self
            .items
            .binary_search_by(|item| item.key.as_slice().cmp(split_key))
            .unwrap_err();

        let child_hash_to_split = self.get_child_hash(idx).cloned();
        let (mid_l, mid_r) = Self::split_hash(store, child_hash_to_split, split_key)?;

        let right_items = self.items.split_off(idx);
        let left_items = std::mem::take(&mut self.items);

        let mut left_node = MstNode {
            left: self.left.take(),
            items: left_items,
        };

        // Fix the rightmost pointer of left_node
        if idx == 0 {
            left_node.left = mid_l;
        } else if let Some(last) = left_node.items.last_mut() {
            last.right = mid_l;
        }

        let right_node = MstNode {
            left: mid_r,
            items: right_items,
        };

        let l_hash = if left_node.items.is_empty() && left_node.left.is_none() {
            None
        } else {
            Some(store.write_node(&left_node)?)
        };

        let r_hash = if right_node.items.is_empty() && right_node.left.is_none() {
            None
        } else {
            Some(store.write_node(&right_node)?)
        };

        Ok((l_hash, r_hash))
    }

    fn split_hash<S: Store>(
        store: &mut S,
        hash: Option<Hash>,
        split_key: &[u8],
    ) -> Result<(Option<Hash>, Option<Hash>), RepoError> {
        match hash {
            None => Ok((None, None)),
            Some(h) => {
                let mut node = store.read_node(&h)?;
                node.split(store, split_key)
            }
        }
    }

    /// Estimates the level of the current node based on the keys it contains.
    fn estimate_level(&self) -> Option<u32> {
        self.items.first().map(|item| key_level(&item.key))
    }
}

#[cfg(test)]
pub mod tests;
