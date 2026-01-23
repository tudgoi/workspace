// TODO Keep unit tests in the same file as the module source. This seems to be
// the convention in rust. Currently I have a few modules where unit tests
// are in separate tests.rs file.

use crate::repo::{Backend, Repo, RepoRefType, test_backend::TestBackend};

#[test]
fn test_repo() {
    let backend = TestBackend::new();
    let repo = Repo::new(backend);
    repo.init().unwrap();

    // key level 0
    let k1: Vec<u8> = "name-0".as_bytes().to_vec();
    let k2: Vec<u8> = "age-0".as_bytes().to_vec();

    assert_eq!(
        repo.get_ref(RepoRefType::Working)
            .unwrap()
            .read(&k1)
            .unwrap(),
        None
    );

    repo.get_ref(RepoRefType::Working)
        .unwrap()
        .write(k1.clone(), "val1".as_bytes().to_vec())
        .unwrap();
    assert_eq!(
        repo.get_ref(RepoRefType::Working)
            .unwrap()
            .read(&k1)
            .unwrap(),
        Some("val1".as_bytes().to_vec())
    );

    repo.get_ref(RepoRefType::Working)
        .unwrap()
        .write(k2.clone(), "val2".as_bytes().to_vec())
        .unwrap();
    assert_eq!(
        repo.get_ref(RepoRefType::Working)
            .unwrap()
            .read(&k2)
            .unwrap(),
        Some("val2".as_bytes().to_vec())
    );
    assert_eq!(
        repo.get_ref(RepoRefType::Working)
            .unwrap()
            .read(&k1)
            .unwrap(),
        Some("val1".as_bytes().to_vec())
    );

    repo.get_ref(RepoRefType::Working)
        .unwrap()
        .write(k1.clone(), "val1_updated".as_bytes().to_vec())
        .unwrap();
    assert_eq!(
        repo.get_ref(RepoRefType::Working)
            .unwrap()
            .read(&k1)
            .unwrap(),
        Some("val1_updated".as_bytes().to_vec())
    );
}

#[test]
fn test_hash_display() {
    use crate::repo::Hash;
    let mut data = [0u8; 32];
    data[0] = 0x12;
    data[1] = 0xab;
    data[31] = 0xff;
    let hash = Hash(data);
    let s = hash.to_string();
    assert!(s.starts_with("12ab00"));
    assert_eq!(s.len(), 8);
}

#[test]
fn test_hash_hex() {
    use crate::repo::Hash;
    let mut data = [0u8; 32];
    data[0] = 0x12;
    data[1] = 0xab;
    data[31] = 0xff;
    let hash = Hash(data);
    let s = hash.to_hex();
    assert!(s.starts_with("12ab00"));
    assert!(s.ends_with("ff"));
    assert_eq!(s.len(), 64);
}

#[test]
fn test_iter_prefix() {
    let backend = TestBackend::new();
    let repo = Repo::new(backend);
    repo.init().unwrap();

    repo.get_ref(RepoRefType::Working)
        .unwrap()
        .write(b"apple".to_vec(), b"val1".to_vec())
        .unwrap();
    repo.get_ref(RepoRefType::Working)
        .unwrap()
        .write(b"apricot".to_vec(), b"val2".to_vec())
        .unwrap();
    repo.get_ref(RepoRefType::Working)
        .unwrap()
        .write(b"banana".to_vec(), b"val3".to_vec())
        .unwrap();
    repo.get_ref(RepoRefType::Working)
        .unwrap()
        .write(b"application".to_vec(), b"val4".to_vec())
        .unwrap();
    repo.get_ref(RepoRefType::Working)
        .unwrap()
        .write(b"apply".to_vec(), b"val5".to_vec())
        .unwrap();

    let mut visited = Vec::new();
    for item in repo
        .get_ref(RepoRefType::Working)
        .unwrap()
        .iter_prefix(b"app")
        .unwrap()
    {
        let (k, _) = item.unwrap();
        visited.push(k);
    }

    assert_eq!(
        visited,
        vec![
            b"apple".to_vec(),
            b"application".to_vec(),
            b"apply".to_vec()
        ]
    );

    visited.clear();
    for item in repo
        .get_ref(RepoRefType::Working)
        .unwrap()
        .iter_prefix(b"ban")
        .unwrap()
    {
        let (k, _) = item.unwrap();
        visited.push(k);
    }
    assert_eq!(visited, vec![b"banana".to_vec()]);

    visited.clear();
    for item in repo
        .get_ref(RepoRefType::Working)
        .unwrap()
        .iter_prefix(b"z")
        .unwrap()
    {
        let (k, _) = item.unwrap();
        visited.push(k);
    }
    assert!(visited.is_empty());
}

#[test]
fn test_committed_ref() {
    use crate::repo::{KeyType, RepoRefType};
    let backend = TestBackend::new();
    let mut repo = Repo::new(backend);
    repo.init().unwrap();

    repo.get_ref(RepoRefType::Working)
        .unwrap()
        .write(b"k1".to_vec(), b"v1".to_vec())
        .unwrap();
    let root_hash = repo
        .backend
        .get(KeyType::Ref, RepoRefType::Working.as_str().as_bytes())
        .unwrap();
    let committed_hash = repo
        .backend
        .get(KeyType::Ref, RepoRefType::Committed.as_str().as_bytes())
        .unwrap();

    assert!(root_hash.is_some());
    assert!(committed_hash.is_some());
    assert_ne!(root_hash, committed_hash);

    repo.commit().unwrap();
    let committed_hash = repo
        .backend
        .get(KeyType::Ref, RepoRefType::Committed.as_str().as_bytes())
        .unwrap();
    assert_eq!(root_hash, committed_hash);

    repo.get_ref(RepoRefType::Working)
        .unwrap()
        .write(b"k2".to_vec(), b"v2".to_vec())
        .unwrap();
    let new_root_hash = repo
        .backend
        .get(KeyType::Ref, RepoRefType::Working.as_str().as_bytes())
        .unwrap();
    let committed_hash_after = repo
        .backend
        .get(KeyType::Ref, RepoRefType::Committed.as_str().as_bytes())
        .unwrap();

    assert_ne!(new_root_hash, committed_hash);
    assert_eq!(committed_hash_after, committed_hash);
}

#[test]
fn test_iterate_diff() {
    use crate::repo::Diff;
    let backend = TestBackend::new();
    let mut repo = Repo::new(backend);
    repo.init().unwrap();

    // Initial state
    repo.get_ref(RepoRefType::Working)
        .unwrap()
        .write(b"a".to_vec(), b"1".to_vec())
        .unwrap();
    repo.get_ref(RepoRefType::Working)
        .unwrap()
        .write(b"b".to_vec(), b"2".to_vec())
        .unwrap();
    repo.commit().unwrap();

    // Modifications
    repo.get_ref(RepoRefType::Working)
        .unwrap()
        .write(b"b".to_vec(), b"22".to_vec())
        .unwrap(); // Changed
    repo.get_ref(RepoRefType::Working)
        .unwrap()
        .write(b"c".to_vec(), b"3".to_vec())
        .unwrap(); // Added
    // MST doesn't support deletion yet, so we only test Added and Changed.

    let root = repo.get_ref(RepoRefType::Working).unwrap();
    let committed = repo.get_ref(RepoRefType::Committed).unwrap();
    let diffs: Vec<_> = committed
        .iterate_diff(&root)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(diffs.len(), 2);

    let mut found_added = false;
    let mut found_changed = false;

    for diff in diffs {
        match diff {
            Diff::Added(k, v) => {
                assert_eq!(k, b"c");
                assert_eq!(v, b"3");
                found_added = true;
            }
            Diff::Changed(k, old_v, new_v) => {
                assert_eq!(k, b"b");
                assert_eq!(old_v, b"2");
                assert_eq!(new_v, b"22");
                found_changed = true;
            }
            Diff::Removed(_, _) => panic!("Unexpected removal"),
        }
    }

    assert!(found_added);
    assert!(found_changed);
}

#[test]
fn test_iterate_diff_different_levels() {
    use crate::repo::{Diff, RepoRefType};
    let backend = TestBackend::new();
    let mut repo = Repo::new(backend);
    repo.init().unwrap();

    // "k0" is Level 0
    // "k75" is Level 1
    // "k1966" is Level 2

    // Tree 1: Just k0 (Level 0 root)
    repo.get_ref(RepoRefType::Working)
        .unwrap()
        .write(b"k0".to_vec(), b"v0".to_vec())
        .unwrap();
    repo.commit().unwrap();

    // Tree 2: k0 and k75 (Level 1 root, k0 is a child)
    repo.get_ref(RepoRefType::Working)
        .unwrap()
        .write(b"k75".to_vec(), b"v75".to_vec())
        .unwrap();

    let root = repo.get_ref(RepoRefType::Working).unwrap();
    let committed = repo.get_ref(RepoRefType::Committed).unwrap();
    
    // This should only show "k75" as Added. 
    // If it's broken, it might show "k0" as Removed and "k75", "k0" as Added (or similar).
    let diffs: Vec<_> = committed
        .iterate_diff(&root)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(diffs.len(), 1, "Should only have one change (k75 added)");
    match &diffs[0] {
        Diff::Added(k, v) => {
            assert_eq!(k, b"k75");
            assert_eq!(v, b"v75");
        }
        _ => panic!("Expected Added(k75), got {:?}", diffs[0]),
    }
}

#[test]
fn test_iterate_diff_changed_different_height() {
    use crate::repo::{Diff, RepoRefType};
    let backend = TestBackend::new();
    let mut repo = Repo::new(backend);
    repo.init().unwrap();

    // k0: L0, k75: L1

    // Tree 1: Just k0=v1 (L0 root)
    repo.get_ref(RepoRefType::Working)
        .unwrap()
        .write(b"k0".to_vec(), b"v1".to_vec())
        .unwrap();
    repo.commit().unwrap();

    // Tree 2: k0=v2 and k75=v75 (L1 root)
    repo.get_ref(RepoRefType::Working)
        .unwrap()
        .write(b"k0".to_vec(), b"v2".to_vec())
        .unwrap();
    repo.get_ref(RepoRefType::Working)
        .unwrap()
        .write(b"k75".to_vec(), b"v75".to_vec())
        .unwrap();

    let root = repo.get_ref(RepoRefType::Working).unwrap();
    let committed = repo.get_ref(RepoRefType::Committed).unwrap();
    
    let diffs: Vec<_> = committed
        .iterate_diff(&root)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    // Should have: Changed(k0, v1, v2) and Added(k75)
    assert_eq!(diffs.len(), 2);
    
    let mut found_changed = false;
    let mut found_added = false;
    for d in diffs {
        match d {
            Diff::Changed(k, v1, v2) => {
                assert_eq!(k, b"k0");
                assert_eq!(v1, b"v1");
                assert_eq!(v2, b"v2");
                found_changed = true;
            }
            Diff::Added(k, v) => {
                assert_eq!(k, b"k75");
                assert_eq!(v, b"v75");
                found_added = true;
            }
            _ => panic!("Unexpected diff: {:?}", d),
        }
    }
    assert!(found_changed, "Should have found Changed(k0)");
    assert!(found_added, "Should have found Added(k75)");
}
