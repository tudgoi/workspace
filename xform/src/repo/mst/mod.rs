use serde::Deserialize;
use serde::Serialize;

#[cfg(test)]
mod tests;

pub trait Key: Ord {
    fn level(&self) -> u32;
}

/// Stores a (K, V) pair and optionally hash of a subtree with keys greater
/// than current item.
#[derive(Serialize, Deserialize)]
pub struct MstItem<K: Key, V, H> {
    key: K,
    value: V,
    right: Option<H>,
}

/// A node in the Merkle Search Tree.
///
/// It's purpose it maintain a list of (K, V) ordered by K.
///
/// left stores the left subtree with keys < keys of all items.
#[derive(Serialize, Deserialize)]
pub struct MstNode<R: Repo> {
    pub left: Option<R::Hash>,
    pub items: Vec<MstItem<R::Key, R::Value, R::Hash>>,
}

impl<R: Repo> MstNode<R> {
    /// Creates a new, empty MST node.
    pub fn empty() -> Self {
        MstNode {
            left: None,
            items: Vec::new(),
        }
    }

    /// Inserts or updates a key-value pair in the MST rooted at this node.
    pub fn upsert(
        &mut self,
        repo: &mut R,
        key: R::Key,
        value: R::Value,
    ) -> Result<R::Hash, R::Error> {
        let req_level = key.level();
        let node_level = self.estimate_level().unwrap_or(req_level);

        if req_level == node_level {
            // same level
            self.upsert_local(repo, key, value)?;
        } else if req_level < node_level {
            // lower level
            todo!()
        } else {
            // higher level
            todo!()
        }

        repo.write_node(self)
    }

    /// Inserts a key-value pair directly into the current node.
    fn upsert_local(&mut self, repo: &R, key: R::Key, value: R::Value) -> Result<(), R::Error> {
        match self.items.iter().position(|item| key < item.key) {
            Some(pos) => {
                if pos > 0 {
                    let prev_item = self.items.get_mut(pos - 1).unwrap();
                    if prev_item.key == key {
                        // update the value
                        prev_item.value = value;
                    } else {
                        let right = match &prev_item.right {
                            Some(hash) => {
                                // split and insert into left subtree
                                let node = repo.read_node(hash)?;
                                let (left, right) = node.split(repo, &key)?;
                                prev_item.right = Some(left);

                                Some(right)
                            }
                            None => {
                                // No split. So no subtree to the right
                                None
                            }
                        };
                        self.items.insert(
                            pos,
                            MstItem {
                                key: key,
                                value: value,
                                right,
                            },
                        );
                    }
                } else {
                    // pos = 0. So this is going to be the first item
                    let right = match &self.left {
                        Some(hash) => {
                            // left subtree needs to be split
                            let node = repo.read_node(hash)?;
                            let (left, right) = node.split(repo, &key)?;
                            self.left = Some(left);

                            Some(right)
                        }
                        None => {
                            // No split. So no subtree to the right.
                            None
                        }
                    };
                    self.items.insert(
                        pos,
                        MstItem {
                            key: key,
                            value: value,
                            right,
                        },
                    );
                }
            }
            None => {
                // this key is greater than or equal to all the items
                let prev_item = self.items.last_mut();
                if let Some(prev_item) = prev_item {
                    if prev_item.key == key {
                        // update the value
                        prev_item.value = value;
                    } else {
                        match &prev_item.right {
                            Some(hash) => {
                                // there is a subtree to the right. split and insert
                                todo!()
                            }
                            None => {
                                // this is the last item in the entire tree. Just append to node.
                                self.items.push(MstItem {
                                    key,
                                    value,
                                    right: None,
                                })
                            }
                        }
                    }
                } else {
                    // the node is empty
                    self.items.push(MstItem {
                        key,
                        value,
                        right: None,
                    })
                }
            }
        }

        Ok(())
    }

    fn split(&self, repo: &R, key: &R::Key) -> Result<(R::Hash, R::Hash), R::Error> {
        todo!()
    }

    /// Estimates the level of the current node based on the keys it contains.
    ///
    /// We can decide to store the level within the node to avoid recomputation if needed.
    fn estimate_level(&self) -> Option<u32> {
        for item in &self.items {
            return Some(item.key.level());
        }
        None
    }
}

pub trait Repo {
    type Error;
    type Key: for<'a> Deserialize<'a> + Key + Clone + Serialize;
    type Value: for<'a> Deserialize<'a> + Clone + Serialize;
    type Hash: for<'a> Deserialize<'a> + Clone + Serialize;
    fn write_node(&mut self, node: &MstNode<Self>) -> Result<Self::Hash, Self::Error>
    where
        Self: Sized;
    fn read_node(&self, hash: &Self::Hash) -> Result<MstNode<Self>, Self::Error>
    where
        Self: Sized;
}
