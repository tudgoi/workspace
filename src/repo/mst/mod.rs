use serde::Deserialize;
use serde::Serialize;
use super::{Repo, RepoError, Backend};

#[cfg(test)]
mod tests;

pub trait Key: Ord {
    fn level(&self) -> u32;
}

/// Stores a (K, V) pair and optionally hash of a subtree with keys greater
/// than current item.
#[derive(Serialize, Deserialize, Clone)]
pub struct MstItem<K: Key, V> {
    key: K,
    value: V,
    right: Option<[u8; 32]>,
}

/// A node in the Merkle Search Tree.
///
/// It's purpose it maintain a list of (K, V) ordered by K.
///
/// left stores the left subtree with keys < keys of all items.
#[derive(Serialize, Deserialize, Clone)]
pub struct MstNode<K: Key, V> {
    pub left: Option<[u8; 32]>,
    pub items: Vec<MstItem<K, V>>,
}

impl<K, V> MstNode<K, V> 
where 
    K: Key + Serialize + for<'de> Deserialize<'de> + Clone,
    V: Serialize + for<'de> Deserialize<'de> + Clone,
{
    /// Creates a new, empty MST node.
    pub fn empty() -> Self {
        MstNode {
            left: None,
            items: Vec::new(),
        }
    }

    /// Inserts or updates a key-value pair in the MST rooted at this node.
    pub fn upsert<B: Backend>(
        &mut self,
        repo: &mut Repo<K, V, B>,
        key: K,
        value: V,
    ) -> Result<[u8; 32], RepoError> {
        let req_level = key.level();
        let node_level = self.estimate_level().unwrap_or(req_level);

        if req_level == node_level {
            // same level
            self.upsert_local(repo, key, value)?;
        } else if req_level < node_level {
            // lower level
            let idx = self
                .items
                .iter()
                .position(|item| item.key > key)
                .unwrap_or(self.items.len());

            let child_hash = if idx == 0 {
                self.left.clone()
            } else {
                self.items[idx - 1].right.clone()
            };

            let mut child_node = match child_hash {
                Some(ref h) => repo.read_node(h)?,
                None => MstNode::empty(),
            };

            let new_child_hash = child_node.upsert(repo, key, value)?;

            if idx == 0 {
                self.left = Some(new_child_hash);
            } else {
                self.items[idx - 1].right = Some(new_child_hash);
            }
        } else {
            // higher level
            let (l_hash, r_hash) = self.split(repo, &key)?;
            self.items.clear();
            self.left = l_hash;
            self.items.push(MstItem {
                key,
                value,
                right: r_hash,
            });
        }

        repo.write_node(self)
    }

    /// Inserts a key-value pair directly into the current node.
    fn upsert_local<B: Backend>(
        &mut self,
        repo: &mut Repo<K, V, B>,
        key: K,
        value: V,
    ) -> Result<(), RepoError> {
        match self.items.binary_search_by(|item| item.key.cmp(&key)) {
            Ok(idx) => {
                self.items[idx].value = value;
            }
            Err(idx) => {
                // Insert new
                // Split child at idx
                let child_hash = if idx == 0 {
                    self.left.clone()
                } else {
                    self.items[idx - 1].right.clone()
                };

                let (l_hash, r_hash) = Self::split_hash(repo, child_hash, &key)?;

                // Update left neighbor
                if idx == 0 {
                    self.left = l_hash;
                } else {
                    self.items[idx - 1].right = l_hash;
                }

                // Insert item
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

    fn split<B: Backend>(
        &mut self,
        repo: &mut Repo<K, V, B>,
        split_key: &K,
    ) -> Result<(Option<[u8; 32]>, Option<[u8; 32]>), RepoError> {
        // Find index where keys become > split_key
        let idx = self
            .items
            .iter()
            .position(|item| item.key > *split_key)
            .unwrap_or(self.items.len());

        // The child that needs splitting is at idx-1 (right of previous) or self.left if idx==0
        // We take the hash out because we are about to destructively modify the node anyway.
        let child_hash_to_split = if idx == 0 {
            self.left.take()
        } else {
            self.items[idx - 1].right.take()
        };

        let (mid_l, mid_r) = Self::split_hash(repo, child_hash_to_split, split_key)?;

        // Construct Right Node first by splitting off from self.items
        // split_off moves elements at [idx, end) to a new Vec
        let right_items = self.items.split_off(idx);

        // Construct Left Node
        // self.items now contains [0, idx)
        let mut left_node = MstNode {
            left: self.left.take(), // This might be None (if idx=0 we took it, if idx>0 we want to move it here)
            items: std::mem::take(&mut self.items),
        };

        // Fix the rightmost pointer of left_node to be mid_l
        if idx == 0 {
            left_node.left = mid_l;
        } else {
            // idx > 0, so items[idx-1] exists.
            if let Some(last) = left_node.items.last_mut() {
                last.right = mid_l;
            }
        }

        // Construct Right Node
        let right_node = MstNode {
            left: mid_r, // inherits split result as left
            items: right_items,
        };

        // Write nodes
        let l_hash = if left_node.items.is_empty() && left_node.left.is_none() {
            None
        } else {
            Some(repo.write_node(&left_node)?)
        };

        let r_hash = if right_node.items.is_empty() && right_node.left.is_none() {
            None
        } else {
            Some(repo.write_node(&right_node)?)
        };

        Ok((l_hash, r_hash))
    }

    fn split_hash<B: Backend>(
        repo: &mut Repo<K, V, B>,
        hash: Option<[u8; 32]>,
        split_key: &K,
    ) -> Result<(Option<[u8; 32]>, Option<[u8; 32]>), RepoError> {
        match hash {
            None => Ok((None, None)),
            Some(h) => {
                let mut node = repo.read_node(&h)?;
                node.split(repo, split_key)
            }
        }
    }


    /// Estimates the level of the current node based on the keys it contains.
    ///
    /// We can decide to store the level within the node to avoid recomputation if needed.
    fn estimate_level(&self) -> Option<u32> {
        if let Some(item) = self.items.first() {
            return Some(item.key.level());
        }
        None
    }
}