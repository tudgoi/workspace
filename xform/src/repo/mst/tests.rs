use crate::repo::mst::{Key, MstNode, Repo};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Serialize, Deserialize, Clone)]
pub struct TestKey {
    level: u32,
    key: String,
}

impl TestKey {
    pub fn new(level: u32, key: &str) -> Self {
        Self {
            level,
            key: key.to_string(),
        }
    }
}

impl PartialEq for TestKey {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for TestKey {}

impl PartialOrd for TestKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.key.partial_cmp(&other.key)
    }
}

impl Ord for TestKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl Key for TestKey {
    fn level(&self) -> u32 {
        self.level
    }
}

#[derive(Error, Debug)]
pub enum TestRepoError {
    #[error("could not serialize")]
    SerializeError(#[from] toml::ser::Error),

    #[error("could not deserialize")]
    DeserializeError(#[from] toml::de::Error),

    #[error("hash {0:?} not found")]
    HashNotFound(<TestRepo as Repo>::Hash),
}

pub struct TestRepo {
    store: BTreeMap<[u8; 32], String>,
}

impl TestRepo {
    pub fn new() -> Self {
        TestRepo {
            store: BTreeMap::new(),
        }
    }
}

impl Repo for TestRepo {
    type Error = TestRepoError;
    type Key = TestKey;
    type Value = String;
    type Hash = [u8; 32];

    fn write_node(&mut self, node: &super::MstNode<Self>) -> Result<Self::Hash, Self::Error>
    where
        Self: Sized,
    {
        let str = toml::to_string(node)?;
        let hash = blake3::hash(str.as_bytes());
        let hash_bytes = *hash.as_bytes();
        self.store.insert(hash_bytes, str);

        Ok(hash_bytes)
    }

    fn read_node(&self, hash: &Self::Hash) -> Result<super::MstNode<Self>, Self::Error>
    where
        Self: Sized,
    {
        let str = self
            .store
            .get(hash)
            .ok_or(TestRepoError::HashNotFound(*hash))?;

        let node = toml::from_str(str)?;

        Ok(node)
    }
}

#[test]
fn test_upsert_empty_tree() {
    let mut repo = TestRepo::new();
    let mut root_node = MstNode::empty();
    let root_hash = repo.write_node(&root_node).unwrap();

    let new_root_hash = root_node
        .upsert(
            &mut repo,
            TestKey::new(0, "name"),
            "value".to_string(),
        )
        .unwrap();

    assert_ne!(root_hash, new_root_hash);
    assert_eq!(root_node.left, None);
    assert_eq!(root_node.items.len(), 1);
    let item = root_node.items.get(0).unwrap();
    assert_eq!(item.key.key, "name");
    assert_eq!(item.value, "value");
    assert_eq!(item.right, None);
}

#[test]
fn test_upsert_existing_changed_value() {
    let mut repo = TestRepo::new();
    let mut root_node = MstNode::empty();
    let mut root_hash = repo.write_node(&root_node).unwrap();
    
    root_hash = root_node
        .upsert(
            &mut repo,
            TestKey::new(0, "name"),
            "value".to_string(),
        )
        .unwrap();
    
    let new_root_hash = root_node.upsert(
        &mut repo,
        TestKey::new(0, "name"),
        "new value".to_string(),
    ).unwrap();
    
    assert_ne!(root_hash, new_root_hash);
    assert_eq!(root_node.items.len(), 1);
    assert_eq!(root_node.left, None);

    let item = root_node.items.get(0).unwrap();
    assert_eq!(item.key.level, 0);
    assert_eq!(item.key.key, "name");
    assert_eq!(item.value, "new value");
}

#[test]
fn test_upsert_same_level_beginning() {
    let mut repo = TestRepo::new();
    let mut root_node = MstNode::empty();
    let mut root_hash = repo.write_node(&root_node).unwrap();
    
    
    root_hash = root_node
        .upsert(
            &mut repo,
            TestKey::new(0, "name"),
            "value".to_string(),
        )
        .unwrap();
    
    let new_root_hash = root_node.upsert(
        &mut repo,
        TestKey::new(0, "age"),
        "value".to_string(),
    ).unwrap();
    
    assert_ne!(root_hash, new_root_hash);
    assert_eq!(root_node.items.len(), 2);
    assert_eq!(root_node.left, None);

    let item = root_node.items.get(0).unwrap();
    assert_eq!(item.key.level, 0);
    assert_eq!(item.key.key, "age");

    let item = root_node.items.get(1).unwrap();
    assert_eq!(item.key.level, 0);
    assert_eq!(item.key.key, "name");
}

#[test]
fn test_upsert_same_level_ending() {
    let mut repo = TestRepo::new();
    let mut root_node = MstNode::empty();
    let mut root_hash = repo.write_node(&root_node).unwrap();
    
    
    root_hash = root_node
        .upsert(
            &mut repo,
            TestKey::new(0, "name"),
            "value".to_string(),
        )
        .unwrap();
    
    let new_root_hash = root_node.upsert(
        &mut repo,
        TestKey::new(0, "weight"),
        "value".to_string(),
    ).unwrap();
    
    assert_ne!(root_hash, new_root_hash);
    assert_eq!(root_node.items.len(), 2);
    assert_eq!(root_node.left, None);

    let item = root_node.items.get(0).unwrap();
    assert_eq!(item.key.level, 0);
    assert_eq!(item.key.key, "name");

    let item = root_node.items.get(1).unwrap();
    assert_eq!(item.key.level, 0);
    assert_eq!(item.key.key, "weight");
}

#[test]
fn test_upsert_same_level_between() {
    let mut repo = TestRepo::new();
    let mut root_node = MstNode::empty();
    let mut root_hash = repo.write_node(&root_node).unwrap();
    
    root_hash = root_node
        .upsert(
            &mut repo,
            TestKey::new(0, "age"),
            "value".to_string(),
        )
        .unwrap();
    
    root_hash = root_node
        .upsert(
            &mut repo,
            TestKey::new(0, "weight"),
            "value".to_string(),
        )
        .unwrap();

    let new_root_hash = root_node.upsert(
        &mut repo,
        TestKey::new(0, "name"),
        "value".to_string(),
    ).unwrap();
    
    assert_ne!(root_hash, new_root_hash);
    assert_eq!(root_node.items.len(), 3);
    assert_eq!(root_node.left, None);

    let item = root_node.items.get(0).unwrap();
    assert_eq!(item.key.level, 0);
    assert_eq!(item.key.key, "age");
    assert_eq!(item.right, None);

    let item = root_node.items.get(1).unwrap();
    assert_eq!(item.key.level, 0);
    assert_eq!(item.key.key, "name");
    assert_eq!(item.right, None);

    let item = root_node.items.get(2).unwrap();
    assert_eq!(item.key.level, 0);
    assert_eq!(item.key.key, "weight");
    assert_eq!(item.right, None);
}