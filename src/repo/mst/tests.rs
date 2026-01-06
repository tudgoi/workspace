use crate::repo::mst::{Key, MstNode};
use crate::repo::{Backend, Repo, RepoError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

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

#[derive(Clone)]
pub struct TestBackend {
    store: Arc<Mutex<BTreeMap<[u8; 32], Vec<u8>>>>,
}

impl TestBackend {
    pub fn new() -> Self {
        TestBackend {
            store: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

impl Backend for TestBackend {
    fn read(&self, hash: &[u8; 32]) -> Result<Vec<u8>, RepoError> {
        let store = self.store.lock().unwrap();
        store
            .get(hash)
            .cloned()
            .ok_or_else(|| RepoError::Backend("hash not found".to_string()))
    }

    fn write(&self, hash: &[u8; 32], blob: &Vec<u8>) -> Result<(), RepoError> {
        let mut store = self.store.lock().unwrap();
        store.insert(*hash, blob.clone());
        Ok(())
    }
}

#[test]
fn test_upsert_empty_tree() {
    let backend = TestBackend::new();
    let mut repo = Repo::<TestKey, String, TestBackend>::new(backend);
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
    let backend = TestBackend::new();
    let mut repo = Repo::<TestKey, String, TestBackend>::new(backend);
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
    let backend = TestBackend::new();
    let mut repo = Repo::<TestKey, String, TestBackend>::new(backend);
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
    let backend = TestBackend::new();
    let mut repo = Repo::<TestKey, String, TestBackend>::new(backend);
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
    let backend = TestBackend::new();
    let mut repo = Repo::<TestKey, String, TestBackend>::new(backend);
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

#[test]
fn test_upsert_lower_level() {
    let backend = TestBackend::new();
    let mut repo = Repo::<TestKey, String, TestBackend>::new(backend);
    let mut root_node = MstNode::empty();
    
    // Insert level 1 item
    let root_hash = root_node
        .upsert(
            &mut repo,
            TestKey::new(1, "middle"),
            "val1".to_string(),
        )
        .unwrap();
    
    // Insert level 0 item (should go to child)
    // "alpha" < "middle", so should go to left child
    let new_root_hash = root_node.upsert(
        &mut repo,
        TestKey::new(0, "alpha"),
        "val2".to_string(),
    ).unwrap();
    
    assert_ne!(root_hash, new_root_hash);
    
    // Check root structure
    assert_eq!(root_node.items.len(), 1);
    assert_eq!(root_node.items[0].key.key, "middle");
    
    // Check left child
    let left_hash = root_node.left.expect("Should have left child");
    let left_node = repo.read_node(&left_hash).unwrap();
    assert_eq!(left_node.items.len(), 1);
    assert_eq!(left_node.items[0].key.key, "alpha");
}

#[test]
fn test_upsert_lower_level_right_child() {
    let backend = TestBackend::new();
    let mut repo = Repo::<TestKey, String, TestBackend>::new(backend);
    let mut root_node = MstNode::empty();
    
    // Insert level 1 item
    root_node.upsert(
        &mut repo,
        TestKey::new(1, "middle"),
        "val1".to_string(),
    ).unwrap();
    
    // Insert level 0 item "zebra" > "middle", so right child
    root_node.upsert(
        &mut repo,
        TestKey::new(0, "zebra"),
        "val2".to_string(),
    ).unwrap();
    
    // Check root
    let item = &root_node.items[0];
    assert_eq!(item.key.key, "middle");
    
    // Check right child
    let right_hash = item.right.expect("Should have right child");
    let right_node = repo.read_node(&right_hash).unwrap();
    assert_eq!(right_node.items[0].key.key, "zebra");
}

#[test]
fn test_upsert_higher_level_split() {
    let backend = TestBackend::new();
    let mut repo = Repo::<TestKey, String, TestBackend>::new(backend);
    let mut root_node = MstNode::empty();
    
    // Insert level 0 items
    root_node.upsert(&mut repo, TestKey::new(0, "a"), "v".to_string()).unwrap();
    root_node.upsert(&mut repo, TestKey::new(0, "c"), "v".to_string()).unwrap();
    
    // Insert level 1 item "b". Should split "a" and "c".
    root_node.upsert(
        &mut repo,
        TestKey::new(1, "b"),
        "higher".to_string()
    ).unwrap();
    
    assert_eq!(root_node.items.len(), 1);
    assert_eq!(root_node.items[0].key.key, "b");
    
    // Check left child (should contain "a")
    let left_hash = root_node.left.expect("Should have left");
    let left_node = repo.read_node(&left_hash).unwrap();
    assert_eq!(left_node.items.len(), 1);
    assert_eq!(left_node.items[0].key.key, "a");

    // Check right child (should contain "c")
    let right_hash = root_node.items[0].right.expect("Should have right");
    let right_node = repo.read_node(&right_hash).unwrap();
    assert_eq!(right_node.items.len(), 1);
    assert_eq!(right_node.items[0].key.key, "c");
}

#[test]
fn test_recursive_split() {
    // A complex case where a higher level key splits a deep tree
    let backend = TestBackend::new();
    let mut repo = Repo::<TestKey, String, TestBackend>::new(backend);
    let mut root_node = MstNode::empty();
    
    // Level 0: a, c, e, g
    root_node.upsert(&mut repo, TestKey::new(0, "a"), "0".to_string()).unwrap();
    root_node.upsert(&mut repo, TestKey::new(0, "c"), "0".to_string()).unwrap();
    root_node.upsert(&mut repo, TestKey::new(0, "e"), "0".to_string()).unwrap();
    root_node.upsert(&mut repo, TestKey::new(0, "g"), "0".to_string()).unwrap();
    
    // Level 1: d (splits c and e) - effectively puts d above them
    root_node.upsert(&mut repo, TestKey::new(1, "d"), "1".to_string()).unwrap();
    
    // Current state (ideal):
    // d (L1)
    // left -> [a, c] (L0)
    // right -> [e, g] (L0)
    
    assert_eq!(root_node.items.len(), 1);
    assert_eq!(root_node.items[0].key.key, "d");
    
    let left_node = repo.read_node(&root_node.left.unwrap()).unwrap();
    assert_eq!(left_node.items.len(), 2);
    assert_eq!(left_node.items[0].key.key, "a");
    assert_eq!(left_node.items[1].key.key, "c");
    
    let right_node = repo.read_node(&root_node.items[0].right.unwrap()).unwrap();
    assert_eq!(right_node.items.len(), 2);
    assert_eq!(right_node.items[0].key.key, "e");
    assert_eq!(right_node.items[1].key.key, "g");
    
    // Now insert Level 2: "f"
    // "f" > "d".
    // "f" is higher level than "d".
    // It splits "d" (the root).
    // "d" < "f", so "d" goes to left.
    // "d"'s right child [e, g] must be split by "f".
    // [e] < "f" -> stays with "d".
    // [g] > "f" -> goes to new right node of "f".
    
    root_node.upsert(&mut repo, TestKey::new(2, "f"), "2".to_string()).unwrap();
    
    assert_eq!(root_node.items.len(), 1);
    assert_eq!(root_node.items[0].key.key, "f");
    
    // Left of "f" should be "d" with right child "e"
    let l_hash = root_node.left.unwrap();
    let l_node = repo.read_node(&l_hash).unwrap();
    assert_eq!(l_node.items.len(), 1);
    assert_eq!(l_node.items[0].key.key, "d");
    
    let d_right_hash = l_node.items[0].right.unwrap();
    let d_right = repo.read_node(&d_right_hash).unwrap();
    assert_eq!(d_right.items.len(), 1);
    assert_eq!(d_right.items[0].key.key, "e"); // "e" < "f"
    
    // Right of "f" should be an empty node (L1) pointing to "g" (L0)
    let r_hash = root_node.items[0].right.unwrap();
    let r_node = repo.read_node(&r_hash).unwrap();
    assert_eq!(r_node.items.len(), 0);
    
    let g_hash = r_node.left.unwrap();
    let g_node = repo.read_node(&g_hash).unwrap();
    assert_eq!(g_node.items.len(), 1);
    assert_eq!(g_node.items[0].key.key, "g"); // "g" > "f"
}