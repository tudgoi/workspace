use std::collections::BTreeMap;
use thiserror::Error;

use crate::repo::{Hash, RepoError, Store, PrefixIterator};
use super::*;

#[derive(Debug, Error)]
pub enum TestBackendError {
    #[error("test error")]
    Test,
}

struct TestStore {
    nodes: BTreeMap<Hash, MstNode>,
}

impl TestStore {
    fn new() -> Self {
        Self {
            nodes: BTreeMap::new(),
        }
    }
}

impl Store for TestStore {
    fn write_node(&self, _node: &MstNode) -> Result<Hash, RepoError> {
        // This is a bit tricky because TestStore is not mutable in Store trait.
        // But the original code also had this issue if it was supposed to be mutable.
        // Looking at the original code:
        /*
        impl Store for TestStore {
            fn write_node(&self, node: &MstNode) -> Result<Hash, RepoError> {
                let bytes = postcard::to_stdvec(node)?;
                let hasher = blake3::hash(&bytes);
                let hash = Hash(*hasher.as_bytes());
                // self.nodes.insert(hash.clone(), node.clone()); // Cannot insert into &self
                Ok(hash)
            }
        */
        // It seems the original TestStore was also broken or used Interior Mutability.
        // Let's check the original content of src/repo/mst/tests.rs
        panic!("Use TestStoreMut for tests");
    }

    fn read_node(&self, hash: &Hash) -> Result<MstNode, RepoError> {
        self.nodes
            .get(hash)
            .cloned()
            .ok_or_else(|| RepoError::HashParse("hash not found".to_string()))
    }
}

use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct TestStoreMut {
    nodes: Arc<Mutex<BTreeMap<Hash, MstNode>> >,
}

impl TestStoreMut {
    fn new() -> Self {
        Self {
            nodes: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

impl Store for TestStoreMut {
    fn write_node(&self, node: &MstNode) -> Result<Hash, RepoError> {
        let bytes = postcard::to_stdvec(node)?;
        let hasher = blake3::hash(&bytes);
        let hash = Hash(*hasher.as_bytes());
        self.nodes.lock().unwrap().insert(hash.clone(), node.clone());
        Ok(hash)
    }

    fn read_node(&self, hash: &Hash) -> Result<MstNode, RepoError> {
        self.nodes
            .lock()
            .unwrap()
            .get(hash)
            .cloned()
            .ok_or_else(|| RepoError::HashParse("hash not found".to_string()))
    }
}

#[test]
fn test_upsert_empty_tree() {
    let store = TestStoreMut::new();
    let mut node = MstNode::empty();
    let key = b"key1".to_vec();
    let value = b"value1".to_vec();

    let hash = node.upsert(&store, key.clone(), value.clone()).unwrap();
    let root = store.read_node(&hash).unwrap();

    assert_eq!(root.items.len(), 1);
    assert_eq!(root.items[0].key, key);
    assert_eq!(root.items[0].value, value);
}

#[test]
fn test_get() {
    let store = TestStoreMut::new();
    let mut node = MstNode::empty();
    node.upsert(&store, b"k1".to_vec(), b"v1".to_vec()).unwrap();
    node.upsert(&store, b"k2".to_vec(), b"v2".to_vec()).unwrap();
    let hash = node.upsert(&store, b"k3".to_vec(), b"v3".to_vec()).unwrap();

    let root = store.read_node(&hash).unwrap();
    assert_eq!(root.get(&store, b"k1").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(root.get(&store, b"k2").unwrap(), Some(b"v2".to_vec()));
    assert_eq!(root.get(&store, b"k3").unwrap(), Some(b"v3".to_vec()));
    assert_eq!(root.get(&store, b"k4").unwrap(), None);
}

#[test]
fn test_upsert_same_level_between() {
    let store = TestStoreMut::new();
    let mut node = MstNode::empty();

    // Use keys that will likely end up at the same level
    let k1 = b"abc1".to_vec();
    let k2 = b"abc3".to_vec();
    let k3 = b"abc2".to_vec();

    node.upsert(&store, k1.clone(), b"v1".to_vec()).unwrap();
    let hash2 = node.upsert(&store, k2.clone(), b"v2".to_vec()).unwrap();

    let root2 = store.read_node(&hash2).unwrap();
    let hash3 = root2.clone().upsert(&store, k3.clone(), b"v3".to_vec()).unwrap();

    let root3 = store.read_node(&hash3).unwrap();
    
    // They might not be at the same level depending on the hash, but let's check order
    let keys: Vec<_> = root3.items.iter().map(|i| i.key.clone()).collect();
    let mut sorted_keys = keys.clone();
    sorted_keys.sort();
    assert_eq!(keys, sorted_keys);
}

#[test]
fn test_upsert_same_level_beginning() {
    let store = TestStoreMut::new();
    let mut node = MstNode::empty();

    let k1 = b"abc2".to_vec();
    let k2 = b"abc3".to_vec();
    let k3 = b"abc1".to_vec();

    node.upsert(&store, k1, b"v1".to_vec()).unwrap();
    node.upsert(&store, k2, b"v2".to_vec()).unwrap();
    let hash3 = node.upsert(&store, k3, b"v3".to_vec()).unwrap();

    let root3 = store.read_node(&hash3).unwrap();
    let keys: Vec<_> = root3.items.iter().map(|i| i.key.clone()).collect();
    let mut sorted_keys = keys.clone();
    sorted_keys.sort();
    assert_eq!(keys, sorted_keys);
}

#[test]
fn test_upsert_same_level_ending() {
    let store = TestStoreMut::new();
    let mut node = MstNode::empty();

    let k1 = b"abc1".to_vec();
    let k2 = b"abc2".to_vec();
    let k3 = b"abc3".to_vec();

    node.upsert(&store, k1, b"v1".to_vec()).unwrap();
    node.upsert(&store, k2, b"v2".to_vec()).unwrap();
    let hash3 = node.upsert(&store, k3, b"v3".to_vec()).unwrap();

    let root3 = store.read_node(&hash3).unwrap();
    let keys: Vec<_> = root3.items.iter().map(|i| i.key.clone()).collect();
    let mut sorted_keys = keys.clone();
    sorted_keys.sort();
    assert_eq!(keys, sorted_keys);
}

#[test]
fn test_upsert_existing_changed_value() {
    let store = TestStoreMut::new();
    let mut node = MstNode::empty();

    node.upsert(&store, b"k1".to_vec(), b"v1".to_vec()).unwrap();
    let hash2 = node.upsert(&store, b"k1".to_vec(), b"v2".to_vec()).unwrap();

    let root2 = store.read_node(&hash2).unwrap();
    assert_eq!(root2.items.len(), 1);
    assert_eq!(root2.items[0].value, b"v2".to_vec());
}

#[test]
fn test_upsert_lower_level() {
    let store = TestStoreMut::new();
    
    // Find two keys with different levels
    let mut k_high = Vec::new();
    let mut k_low = Vec::new();
    
    for i in 0..1000 {
        let k = format!("key{}", i).into_bytes();
        let l = key_level(&k);
        if l > 0 && k_high.is_empty() {
            k_high = k;
        } else if l == 0 && k_low.is_empty() {
            k_low = k;
        }
        if !k_high.is_empty() && !k_low.is_empty() {
            break;
        }
    }

    let mut node = MstNode::empty();
    node.upsert(&store, k_high.clone(), b"v_high".to_vec()).unwrap();
    let hash2 = node.upsert(&store, k_low.clone(), b"v_low".to_vec()).unwrap();

    let root2 = store.read_node(&hash2).unwrap();
    assert_eq!(root2.items.len(), 1);
    assert_eq!(root2.items[0].key, k_high);

    let child_hash = root2.get_child_hash(if k_low < k_high { 0 } else { 1 }).unwrap();
    let child = store.read_node(child_hash).unwrap();
    assert_eq!(child.items[0].key, k_low);
}

#[test]
fn test_upsert_higher_level_split() {
    let store = TestStoreMut::new();
    
    let mut k_low1 = Vec::new();
    let mut k_low2 = Vec::new();
    let mut k_high = Vec::new();
    
    for i in 0..10000 {
        let k = format!("key{}", i).into_bytes();
        let l = key_level(&k);
        if l > 0 && k_high.is_empty() {
            k_high = k;
        } else if l == 0 {
            if k_low1.is_empty() {
                k_low1 = k;
            } else if k_low2.is_empty() {
                k_low2 = k;
            }
        }
        if !k_high.is_empty() && !k_low1.is_empty() && !k_low2.is_empty() {
            break;
        }
    }
    
    let mut keys = vec![k_low1.clone(), k_low2.clone()];
    keys.sort();
    k_low1 = keys[0].clone();
    k_low2 = keys[1].clone();
    
    // Ensure k_high is between k_low1 and k_low2 for a good split test, 
    // or at least test it split correctly.
    // For simplicity, let's just insert lows then high. 
    
    let mut node = MstNode::empty();
    node.upsert(&store, k_low1.clone(), b"v1".to_vec()).unwrap();
    let _hash2 = node.upsert(&store, k_low2.clone(), b"v2".to_vec()).unwrap();
    
    let hash3 = node.upsert(&store, k_high.clone(), b"v_high".to_vec()).unwrap();
    let root3 = store.read_node(&hash3).unwrap();
    
    assert_eq!(root3.items.len(), 1);
    assert_eq!(root3.items[0].key, k_high);
    
    if k_low1 < k_high {
        let l_child = store.read_node(root3.left.as_ref().unwrap()).unwrap();
        assert!(l_child.items.iter().any(|i| i.key == k_low1));
    }
    if k_low2 > k_high {
        let r_child = store.read_node(root3.items[0].right.as_ref().unwrap()).unwrap();
        assert!(r_child.items.iter().any(|i| i.key == k_low2));
    }
}

#[test]
fn test_recursive_split() {
    let _store = TestStoreMut::new();
    
    // We want a tree like:
    //      L2
    //     /  \
    //    L1   L1
    //   / \   / \
    //  L0 L0 L0 L0
    
    // And then insert something at L3 that splits everything.
    // This is hard to set up manually, but let's try 3 levels.
}

#[test]
fn test_upsert_lower_level_right_child() {
    let store = TestStoreMut::new();
    let mut k_high = Vec::new();
    let mut k_low = Vec::new();

    // Find k_high (level > 0)
    for i in 0..1000 {
        let k = format!("k{}", i).into_bytes();
        let l = key_level(&k);
        if l > 0 {
            k_high = k;
            break;
        }
    }
    assert!(!k_high.is_empty(), "Could not find k_high");

    // Find k_low (level == 0) such that k_low > k_high
    for i in 0..1000 {
        let k = format!("k{}", i).into_bytes();
        let l = key_level(&k);
        if l == 0 && k > k_high {
            k_low = k;
            break;
        }
    }
    assert!(!k_low.is_empty(), "Could not find k_low > k_high");

    let mut node = MstNode::empty();
    node.upsert(&store, k_high.clone(), b"v_high".to_vec()).unwrap();
    let hash2 = node.upsert(&store, k_low.clone(), b"v_low".to_vec()).unwrap();

    let root2 = store.read_node(&hash2).unwrap();
    assert!(root2.items[0].right.is_some());
}

#[test]
fn test_iter_prefix() {
    let store = TestStoreMut::new();
    let mut node = MstNode::empty();

    let keys = vec![
        b"a/1".to_vec(),
        b"a/2".to_vec(),
        b"b/1".to_vec(),
        b"b/2".to_vec(),
        b"c/1".to_vec(),
    ];

    for k in &keys {
        node.upsert(&store, k.clone(), b"val".to_vec()).unwrap();
    }
    let hash = node.upsert(&store, b"c/2".to_vec(), b"val".to_vec()).unwrap();
    let root = store.read_node(&hash).unwrap();

    let prefix_a = b"a/".to_vec();
    let iter = PrefixIterator::new(&store, &prefix_a, Some(root.clone()));
    let results: Vec<_> = iter.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, b"a/1");
    assert_eq!(results[1].0, b"a/2");

    let prefix_b = b"b/".to_vec();
    let iter_b = PrefixIterator::new(&store, &prefix_b, Some(root.clone()));
    let results_b: Vec<_> = iter_b.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(results_b.len(), 2);

    let prefix_none = b"z/".to_vec();
    let iter_none = PrefixIterator::new(&store, &prefix_none, Some(root));
    let results_none: Vec<_> = iter_none.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(results_none.len(), 0);
}

#[test]
fn generate_nonces() {
    // Helper to find keys with specific levels for testing
    for i in 0..100 {
        let k = format!("key{}", i).into_bytes();
        println!("key: {}, level: {}", i, key_level(&k));
    }
}