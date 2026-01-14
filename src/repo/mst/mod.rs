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
        level += lz / 6;
        if lz < 8 {
            break;
        }
    }
    level
}

/// Returns the length of the shared prefix between two byte slices.
fn shared_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
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
    #[serde(with = "serde_impl")]
    pub items: Vec<MstItem>,
}

impl MstNode {
    /// Creates a new, empty MST node.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Gets the hash of the child node at the given index.
    /// Index 0 is the `left` child, index `i > 0` is the `right` child of `items[i-1]`.
    pub(crate) fn get_child_hash(&self, idx: usize) -> Option<&Hash> {
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
        store: &S,
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

    pub fn remove<S: Store>(
        &mut self,
        store: &S,
        key: &[u8],
    ) -> Result<(Hash, Option<Vec<u8>>), RepoError> {
        match self.items.binary_search_by(|item| item.key.as_slice().cmp(key)) {
            Ok(idx) => {
                // Found the key in this node.
                let item = self.items.remove(idx);
                let value = Some(item.value);

                // We need to merge the left child (at idx) and the right child (item.right).
                // Before removal: child[idx] < item < child[idx+1] (which was item.right).
                // After removal, the gap needs to be filled by merging these two children.
                
                // Get the left child hash (it was at idx).
                // Note: since we removed item at idx, get_child_hash(idx) now refers to 
                // what was previously at idx+1 (if we didn't adjust).
                // But we need the ORIGINAL children.
                // The left child of the removed item was `self.get_child_hash(idx)` BEFORE removal.
                // But we already removed the item.
                // Let's re-think access.
                
                // We should have retrieved children before removing?
                // `items` is a Vec.
                // item at `idx` has `right` field.
                // `left` field of `items[idx]` is implicitly `items[idx-1].right` or `self.left`.
                
                // Let's undo remove for a second conceptually.
                // left_child = if idx == 0 { self.left } else { self.items[idx-1].right }
                // right_child = item.right
                
                // Since we removed `item` at `idx`:
                // If idx == 0, `self.left` is the left child.
                // If idx > 0, `self.items[idx-1].right` is the left child.
                
                let left_child_hash = if idx == 0 {
                    self.left.clone()
                } else {
                    self.items[idx - 1].right.clone()
                };
                
                let right_child_hash = item.right;
                
                let merged_hash = Self::merge(store, left_child_hash, right_child_hash)?;
                
                if idx == 0 {
                    self.left = merged_hash;
                } else {
                    self.items[idx - 1].right = merged_hash;
                }
                
                // If the node becomes empty, we should return its only child (if any) or empty.
                if self.items.is_empty() {
                    if let Some(h) = self.left.clone() {
                         return Ok((h, value));
                    }
                }
                
                Ok((store.write_node(self)?, value))
            }
            Err(idx) => {
                // Key not in this node, try child.
                let child_hash = self.get_child_hash(idx).cloned();
                
                if let Some(h) = child_hash {
                    let mut child_node = store.read_node(&h)?;
                    let (new_child_hash, removed_val) = child_node.remove(store, key)?;
                    
                    self.set_child_hash(idx, Some(new_child_hash));
                    
                    // Cleanup: If child became empty/merged, we might want to do something?
                    // But MST properties are generally self-maintaining via merge/split.
                    // We just updated the pointer.
                    
                    Ok((store.write_node(self)?, removed_val))
                } else {
                    // Key not found
                    Ok((store.write_node(self)?, None))
                }
            }
        }
    }

    fn merge<S: Store>(
        store: &S,
        left_hash: Option<Hash>,
        right_hash: Option<Hash>,
    ) -> Result<Option<Hash>, RepoError> {
        match (left_hash, right_hash) {
            (None, None) => Ok(None),
            (Some(h), None) => Ok(Some(h)),
            (None, Some(h)) => Ok(Some(h)),
            (Some(lh), Some(rh)) => {
                let mut left_node = store.read_node(&lh)?;
                let mut right_node = store.read_node(&rh)?;
                
                if left_node.items.is_empty() {
                    return Self::merge(store, left_node.left, Some(rh));
                }
                if right_node.items.is_empty() {
                    return Self::merge(store, Some(lh), right_node.left);
                }
                
                let l_lvl = left_node.estimate_level().unwrap();
                let r_lvl = right_node.estimate_level().unwrap();
                
                if l_lvl > r_lvl {
                     let idx = left_node.items.len(); 
                     let child_hash = left_node.get_child_hash(idx).cloned();
                     let merged_child = Self::merge(store, child_hash, Some(rh))?;
                     left_node.set_child_hash(idx, merged_child);
                     Ok(Some(store.write_node(&left_node)?))
                } else if r_lvl > l_lvl {
                     let child_hash = right_node.left.clone();
                     let merged_child = Self::merge(store, Some(lh), child_hash)?;
                     right_node.left = merged_child;
                     Ok(Some(store.write_node(&right_node)?))
                } else {
                     let l_child = left_node.get_child_hash(left_node.items.len()).cloned();
                     let r_child = right_node.left.clone();
                     
                     let mid = Self::merge(store, l_child, r_child)?;
                     
                     if let Some(last) = left_node.items.last_mut() {
                         last.right = mid;
                     } else {
                         left_node.left = mid;
                     }
                     
                     left_node.items.extend(right_node.items);
                     Ok(Some(store.write_node(&left_node)?))
                }
            }
        }
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
        store: &S,
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
        store: &S,
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
        store: &S,
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

mod serde_impl {
    use super::*;
    use serde::ser::SerializeSeq;
    use serde::{self, Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(items: &[MstItem], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct CompressedItemRef<'a> {
            prefix_len: u32,
            suffix: &'a [u8],
            value: &'a [u8],
            right: Option<&'a Hash>,
        }

        let mut seq = serializer.serialize_seq(Some(items.len()))?;
        let mut last_key: &[u8] = &[];
        for item in items {
            let p_len = shared_prefix_len(last_key, &item.key);
            let compressed = CompressedItemRef {
                prefix_len: p_len as u32,
                suffix: &item.key[p_len..],
                value: &item.value,
                right: item.right.as_ref(),
            };
            seq.serialize_element(&compressed)?;
            last_key = &item.key;
        }
        seq.end()
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<MstItem>, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct CompressedItem {
            prefix_len: u32,
            suffix: Vec<u8>,
            value: Vec<u8>,
            right: Option<Hash>,
        }

        let compressed_items: Vec<CompressedItem> = Vec::deserialize(deserializer)?;
        let mut items = Vec::with_capacity(compressed_items.len());
        let mut last_key = Vec::new();
        for c in compressed_items {
            last_key.truncate(c.prefix_len as usize);
            last_key.extend_from_slice(&c.suffix);
            items.push(MstItem {
                key: last_key.clone(),
                value: c.value,
                right: c.right,
            });
        }
        Ok(items)
    }
}

pub struct PrefixIterator<'a, S> {
    store: &'a S,
    prefix: Vec<u8>,
    stack: Vec<IterState>,
}

struct IterState {
    node: MstNode,
    index: usize,
    child_processed: bool,
}

impl<'a, S: Store> PrefixIterator<'a, S> {
    pub fn new(store: &'a S, prefix: &[u8], root_node: Option<MstNode>) -> Self {
        let mut stack = Vec::new();
        if let Some(node) = root_node {
            stack.push(IterState {
                node,
                index: 0,
                child_processed: false,
            });
        }
        Self {
            store,
            prefix: prefix.to_vec(),
            stack,
        }
    }
}

impl<'a, S: Store> Iterator for PrefixIterator<'a, S> {
    type Item = Result<(Vec<u8>, Vec<u8>), RepoError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let frame = self.stack.last_mut()?;
            let idx = frame.index;

            if idx < frame.node.items.len() {
                let item = &frame.node.items[idx];

                if !frame.child_processed {
                    frame.child_processed = true;

                    // Optimization: If item.key < prefix, then child[idx] < item < prefix.
                    if item.key.as_slice() < self.prefix.as_slice() {
                        continue;
                    }

                    // Optimization: If item.key == prefix, then child[idx] < item == prefix.
                    // child[idx] keys are strictly less than item.key.
                    if item.key.as_slice() == self.prefix.as_slice() {
                         // child[idx] keys < prefix. Skip child.
                         // But we still need to process the item itself in the next iteration.
                         continue;
                    }

                    if let Some(h) = frame.node.get_child_hash(idx) {
                        match self.store.read_node(h) {
                            Ok(node) => {
                                self.stack.push(IterState {
                                    node,
                                    index: 0,
                                    child_processed: false,
                                });
                                continue;
                            }
                            Err(e) => return Some(Err(e)),
                        }
                    }
                } else {
                    // Returned from child[idx]
                    frame.index += 1;
                    frame.child_processed = false;

                    // item.key < prefix -> skip
                    if item.key.as_slice() < self.prefix.as_slice() {
                        continue;
                    }

                    if item.key.starts_with(&self.prefix) {
                        return Some(Ok((item.key.clone(), item.value.clone())));
                    }

                    // item.key > prefix and not match -> done with this node
                    self.stack.pop();
                }
            } else {
                // idx == items.len() -> last child
                if !frame.child_processed {
                    frame.child_processed = true;
                    // We only visit the last child if we haven't bailed out yet.
                    // If we are here, it means all previous items were < prefix or matched prefix.
                    // So the last child might have matches.
                    
                    if let Some(h) = frame.node.get_child_hash(idx) {
                         match self.store.read_node(h) {
                            Ok(node) => {
                                self.stack.push(IterState {
                                    node,
                                    index: 0,
                                    child_processed: false,
                                });
                                continue;
                            }
                            Err(e) => return Some(Err(e)),
                        }
                    }
                } else {
                    // Done with last child
                    self.stack.pop();
                }
            }
        }
    }
}

#[cfg(test)]
pub mod tests;