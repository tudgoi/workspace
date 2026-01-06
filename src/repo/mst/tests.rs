use crate::repo::mst::{self, MstNode};
use crate::repo::{Backend, Repo, RepoError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct TestKey(String);

impl TestKey {
    pub fn new(level: u32, prefix: &str) -> Self {
        let nonce = match (prefix, level) {
            ("name", 0) => 0,
            ("age", 0) => 0,
            ("weight", 0) => 0,
            ("alpha", 0) => 0,
            ("zebra", 0) => 0,
            ("a", 0) => 0,
            ("c", 0) => 0,
            ("e", 0) => 0,
            ("g", 0) => 0,
            ("d", 1) => 0,
            ("b", 1) => 13,
            ("middle", 1) => 9,
            ("f", 2) => 148,
            _ => panic!("Unknown test key configuration: prefix={}, level={}", prefix, level),
        };
        
        let candidate = format!("{}-", prefix, nonce);
        // Verify constraint in debug builds just to be sure we didn't mess up
        debug_assert_eq!(mst::key_level(&candidate), level, "Key {} level mismatch", candidate);
        
        TestKey(candidate)
    }
}

impl AsRef<[u8]> for TestKey {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

// impl Key for TestKey {}

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
#[ignore]
fn generate_nonces() {
    fn find_nonce(prefix: &str, level: u32) -> u32 {
        let mut nonce = 0;
        loop {
            let candidate = format!("{}-{}", prefix, nonce);
            if mst::key_level(&candidate) == level {
                return nonce;
            }
            nonce += 1;
        }
    }

    let keys_l0 = vec!["name", "age", "weight", "alpha", "zebra", "a", "c", "e", "g", "aaaa", "zzzz"];
    let keys_l1 = vec!["middle", "b", "d"];
    let keys_l2 = vec!["f"];

    for k in keys_l0 {
        println!("(\"{}\", 0) => {},", k, find_nonce(k, 0));
    }
    for k in keys_l1 {
        println!("(\"{}\", 1) => {},", k, find_nonce(k, 1));
    }
    for k in keys_l2 {
        println!("(\"{}\", 2) => {},", k, find_nonce(k, 2));
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
    assert!(item.key.0.starts_with("name"));
    assert_eq!(item.value, "value");
    assert_eq!(item.right, None);
}

#[test]
fn test_upsert_existing_changed_value() {
    let backend = TestBackend::new();
    let mut repo = Repo::<TestKey, String, TestBackend>::new(backend);
    let mut root_node = MstNode::empty();
    let mut root_hash = repo.write_node(&root_node).unwrap();
    
    let key = TestKey::new(0, "name");
    
    root_hash = root_node
        .upsert(
            &mut repo,
            key.clone(),
            "value".to_string(),
        )
        .unwrap();
    
    let new_root_hash = root_node.upsert(
        &mut repo,
        key.clone(),
        "new value".to_string(),
    ).unwrap();
    
    assert_ne!(root_hash, new_root_hash);
    assert_eq!(root_node.items.len(), 1);
    assert_eq!(root_node.left, None);

    let item = root_node.items.get(0).unwrap();
    assert_eq!(mst::key_level(&item.key), 0);
    assert_eq!(item.key, key);
    assert_eq!(item.value, "new value");
}

#[test]
fn test_upsert_same_level_beginning() {
    let backend = TestBackend::new();
    let mut repo = Repo::<TestKey, String, TestBackend>::new(backend);
    let mut root_node = MstNode::empty();
    let mut root_hash = repo.write_node(&root_node).unwrap();
    
    let key1 = TestKey::new(0, "name");
    let key2 = TestKey::new(0, "age"); // "age" < "name" ?
    
    // Ensure order.
    let (first, second) = if key1 < key2 { (key1, key2) } else { (key2, key1) };

    root_hash = root_node
        .upsert(
            &mut repo,
            second.clone(),
            "value".to_string(),
        )
        .unwrap();
    
    let new_root_hash = root_node.upsert(
        &mut repo,
        first.clone(),
        "value".to_string(),
    ).unwrap();
    
    assert_ne!(root_hash, new_root_hash);
    assert_eq!(root_node.items.len(), 2);
    assert_eq!(root_node.left, None);

    let item0 = root_node.items.get(0).unwrap();
    assert_eq!(mst::key_level(&item0.key), 0);
    assert_eq!(item0.key, first);

    let item1 = root_node.items.get(1).unwrap();
    assert_eq!(mst::key_level(&item1.key), 0);
    assert_eq!(item1.key, second);
}

#[test]
fn test_upsert_same_level_ending() {
    let backend = TestBackend::new();
    let mut repo = Repo::<TestKey, String, TestBackend>::new(backend);
    let mut root_node = MstNode::empty();
    let mut root_hash = repo.write_node(&root_node).unwrap();
    
    let key1 = TestKey::new(0, "name");
    let key2 = TestKey::new(0, "weight"); // "name" < "weight" usually
    
    let (first, second) = if key1 < key2 { (key1, key2) } else { (key2, key1) };

    root_hash = root_node
        .upsert(
            &mut repo,
            first.clone(),
            "value".to_string(),
        )
        .unwrap();
    
    let new_root_hash = root_node.upsert(
        &mut repo,
        second.clone(),
        "value".to_string(),
    ).unwrap();
    
    assert_ne!(root_hash, new_root_hash);
    assert_eq!(root_node.items.len(), 2);
    assert_eq!(root_node.left, None);

    let item0 = root_node.items.get(0).unwrap();
    assert_eq!(mst::key_level(&item0.key), 0);
    assert_eq!(item0.key, first);

    let item1 = root_node.items.get(1).unwrap();
    assert_eq!(mst::key_level(&item1.key), 0);
    assert_eq!(item1.key, second);
}

#[test]
fn test_upsert_same_level_between() {
    let backend = TestBackend::new();
    let mut repo = Repo::<TestKey, String, TestBackend>::new(backend);
    let mut root_node = MstNode::empty();
    let mut root_hash = repo.write_node(&root_node).unwrap();
    
    let k1 = TestKey::new(0, "age");
    let k2 = TestKey::new(0, "weight");
    let k3 = TestKey::new(0, "name");
    
    // Sort them to ensure expectations
    let mut keys = vec![k1, k2, k3];
    keys.sort();
    let (first, middle, last) = (keys[0].clone(), keys[1].clone(), keys[2].clone());
    
    root_hash = root_node
        .upsert(
            &mut repo,
            first.clone(),
            "value".to_string(),
        )
        .unwrap();
    
    root_hash = root_node
        .upsert(
            &mut repo,
            last.clone(),
            "value".to_string(),
        )
        .unwrap();

    let new_root_hash = root_node.upsert(
        &mut repo,
        middle.clone(),
        "value".to_string(),
    ).unwrap();
    
    assert_ne!(root_hash, new_root_hash);
    assert_eq!(root_node.items.len(), 3);
    assert_eq!(root_node.left, None);

    let item0 = root_node.items.get(0).unwrap();
    assert_eq!(item0.key, first);
    assert_eq!(item0.right, None);

    let item1 = root_node.items.get(1).unwrap();
    assert_eq!(item1.key, middle);
    assert_eq!(item1.right, None);

    let item2 = root_node.items.get(2).unwrap();
    assert_eq!(item2.key, last);
    assert_eq!(item2.right, None);
}

#[test]
fn test_upsert_lower_level() {
    let backend = TestBackend::new();
    let mut repo = Repo::<TestKey, String, TestBackend>::new(backend);
    let mut root_node = MstNode::empty();
    
    // Insert level 1 item
    let high = TestKey::new(1, "middle");
    let root_hash = root_node
        .upsert(
            &mut repo,
            high.clone(),
            "val1".to_string(),
        )
        .unwrap();
    
    // Insert level 0 item (should go to child)
    // Find a key < high with level 0
    let low = TestKey::new(0, "alpha");
    // "alpha" < "middle"
    assert!(low < high);
    
    let new_root_hash = root_node.upsert(
        &mut repo,
        low.clone(),
        "val2".to_string(),
    ).unwrap();
    
    assert_ne!(root_hash, new_root_hash);
    
    // Check root structure
    assert_eq!(root_node.items.len(), 1);
    assert_eq!(root_node.items[0].key, high);
    
    // Check left child
    let left_hash = root_node.left.expect("Should have left child");
    let left_node = repo.read_node(&left_hash).unwrap();
    assert_eq!(left_node.items.len(), 1);
    assert_eq!(left_node.items[0].key, low);
}

#[test]
fn test_upsert_lower_level_right_child() {
    let backend = TestBackend::new();
    let mut repo = Repo::<TestKey, String, TestBackend>::new(backend);
    let mut root_node = MstNode::empty();
    
    // Insert level 1 item
    let high = TestKey::new(1, "middle");
    root_node.upsert(
        &mut repo,
        high.clone(),
        "val1".to_string(),
    ).unwrap();
    
    // Insert level 0 item > high
    let low = TestKey::new(0, "zebra");
    // "zebra" > "middle"
    assert!(low > high);

    root_node.upsert(
        &mut repo,
        low.clone(),
        "val2".to_string(),
    ).unwrap();
    
    // Check root
    let item = &root_node.items[0];
    assert_eq!(item.key, high);
    
    // Check right child
    let right_hash = item.right.expect("Should have right child");
    let right_node = repo.read_node(&right_hash).unwrap();
    assert_eq!(right_node.items[0].key, low);
}

#[test]
fn test_upsert_higher_level_split() {
    let backend = TestBackend::new();
    let mut repo = Repo::<TestKey, String, TestBackend>::new(backend);
    let mut root_node = MstNode::empty();
    
    let k_a = TestKey::new(0, "a");
    let k_c = TestKey::new(0, "c");
    let k_b = TestKey::new(1, "b");
    
    // assert ordering
    assert!(k_a < k_b);
    assert!(k_b < k_c);

    // Insert level 0 items
    root_node.upsert(&mut repo, k_a.clone(), "v".to_string()).unwrap();
    root_node.upsert(&mut repo, k_c.clone(), "v".to_string()).unwrap();
    
    // Insert level 1 item "b". Should split "a" and "c".
    root_node.upsert(
        &mut repo,
        k_b.clone(),
        "higher".to_string()
    ).unwrap();
    
    assert_eq!(root_node.items.len(), 1);
    assert_eq!(root_node.items[0].key, k_b);
    
    // Check left child (should contain "a")
    let left_hash = root_node.left.expect("Should have left");
    let left_node = repo.read_node(&left_hash).unwrap();
    assert_eq!(left_node.items.len(), 1);
    assert_eq!(left_node.items[0].key, k_a);

    // Check right child (should contain "c")
    let right_hash = root_node.items[0].right.expect("Should have right");
    let right_node = repo.read_node(&right_hash).unwrap();
    assert_eq!(right_node.items.len(), 1);
    assert_eq!(right_node.items[0].key, k_c);
}

#[test]
fn test_recursive_split() {
    // A complex case where a higher level key splits a deep tree
    let backend = TestBackend::new();
    let mut repo = Repo::<TestKey, String, TestBackend>::new(backend);
    let mut root_node = MstNode::empty();
    
    let k_a = TestKey::new(0, "a");
    let k_c = TestKey::new(0, "c");
    let k_e = TestKey::new(0, "e");
    let k_g = TestKey::new(0, "g");
    
    let k_d = TestKey::new(1, "d");
    
    let k_f = TestKey::new(2, "f");
    
    // Ensure ordering
    assert!(k_a < k_c && k_c < k_d && k_d < k_e && k_e < k_f && k_f < k_g);

    // Level 0: a, c, e, g
    root_node.upsert(&mut repo, k_a.clone(), "0".to_string()).unwrap();
    root_node.upsert(&mut repo, k_c.clone(), "0".to_string()).unwrap();
    root_node.upsert(&mut repo, k_e.clone(), "0".to_string()).unwrap();
    root_node.upsert(&mut repo, k_g.clone(), "0".to_string()).unwrap();
    
    // Level 1: d (splits c and e) - effectively puts d above them
    root_node.upsert(&mut repo, k_d.clone(), "1".to_string()).unwrap();
    
    // Current state (ideal):
    // d (L1)
    // left -> [a, c] (L0)
    // right -> [e, g] (L0)
    
    assert_eq!(root_node.items.len(), 1);
    assert_eq!(root_node.items[0].key, k_d);
    
    let left_node = repo.read_node(&root_node.left.unwrap()).unwrap();
    assert_eq!(left_node.items.len(), 2);
    assert_eq!(left_node.items[0].key, k_a);
    assert_eq!(left_node.items[1].key, k_c);
    
    let right_node = repo.read_node(&root_node.items[0].right.unwrap()).unwrap();
    assert_eq!(right_node.items.len(), 2);
    assert_eq!(right_node.items[0].key, k_e);
    assert_eq!(right_node.items[1].key, k_g);
    
    // Now insert Level 2: "f"
    // "f" > "d".
    // "f" is higher level than "d".
    // It splits "d" (the root).
    // "d" < "f", so "d" goes to left.
    // "d"'s right child [e, g] must be split by "f".
    // [e] < "f" -> stays with "d".
    // [g] > "f" -> goes to new right node of "f".
    
    root_node.upsert(&mut repo, k_f.clone(), "2".to_string()).unwrap();
    
    assert_eq!(root_node.items.len(), 1);
    assert_eq!(root_node.items[0].key, k_f);
    
    // Left of "f" should be "d" with right child "e"
    let l_hash = root_node.left.unwrap();
    let l_node = repo.read_node(&l_hash).unwrap();
    assert_eq!(l_node.items.len(), 1);
    assert_eq!(l_node.items[0].key, k_d);
    
    let d_right_hash = l_node.items[0].right.unwrap();
    let d_right = repo.read_node(&d_right_hash).unwrap();
    assert_eq!(d_right.items.len(), 1);
    assert_eq!(d_right.items[0].key, k_e); // "e" < "f"
    
    // Right of "f" should be an empty node (L1) pointing to "g" (L0)
    let r_hash = root_node.items[0].right.unwrap();
    let r_node = repo.read_node(&r_hash).unwrap();
    assert_eq!(r_node.items.len(), 0);
    
    let g_hash = r_node.left.unwrap();
    let g_node = repo.read_node(&g_hash).unwrap();
    assert_eq!(g_node.items.len(), 1);
    assert_eq!(g_node.items[0].key, k_g); // "g" > "f"
}